"""
regenerate_prophet_parquet.py
─────────────────────────────
Re-fits Prophet for ALL qualifying SKUs (≥ MIN_WEEKS weeks of history) with a
26-week horizon and saves prophet_curves.parquet.  The app reads CI bands and
30-day demand forecasts directly from this file — no live re-fitting needed.

Auditor-upgraded version (v2):
  • Unified Prophet config via get_prophet_config()
  • Outlier Winsorisation at 99th percentile (dampens pack-explosion spikes)
  • Multiplicative seasonality for high-volume SKUs
  • Stockout censoring (trailing zeros → imputed demand)

Run with:
    .venv\Scripts\python regenerate_prophet_parquet.py
"""

import os
import warnings
import pandas as pd
import numpy as np
from prophet import Prophet

warnings.filterwarnings("ignore")

HERE = os.path.dirname(os.path.abspath(__file__))

# ── ACTUAL_STOCK — required for stockout censoring ───────────────────────────
# Mirror of the ACTUAL_STOCK dict in the notebook / app.py so we know which
# SKUs are out of stock (trailing zeros = censored demand, not real zeros).
ACTUAL_STOCK = {
    'SPIRO Card':              0,
    'SPIRO Card X':          600,
    'SPIRO Disc':            200,
    'SPIRO Square X':        700,
    'BioLight Full Spectrum Bulb': 0,
    'Red Light Therapy':       0,
    'SPIRO Disc PRO':          0,
    'SPIRO Square PRO':        0,
    'BioLight Lamp':           0,
    'SPIRO Square':            0,
    'SPIRO Disc X':            0,
    'Stroom Master PRO':     600,
    'Funda':                   0,
    'Amber Light Bulb':      250,
    'NoBlue Amber Book Light': 30,
    'Amber Light Lamp':        0,
    'Stroom Master':           0,
    'SPIRO Ring':              0,
    'Stroom Master X':        40,
    'NoBlue Amber Desk Light': 60,
    'NoBlue Daylight Desk Light': 60,
}
STOCKOUT_SKUS = {sku for sku, qty in ACTUAL_STOCK.items() if qty == 0}

# High-volume SKUs with multiplicative seasonality (seasonal effect scales
# proportionally to trend level — identified via decomposition in the notebook)
# Empirically validated: only Funda benefits; other candidates (SPIRO Card,
# Disc, Stroom Master PRO, etc.) produce worse forecasts with multiplicative.
MULTIPLICATIVE_SKUS = {
    'Funda',
}

# ── Load historical daily sales ───────────────────────────────────────────────
print("Loading daily_sales.parquet …")
daily = pd.read_parquet(os.path.join(HERE, "daily_sales.parquet"))
if "Date" not in daily.columns:
    daily = daily.rename(columns={"date": "Date", "units": "Units",
                                   "producto": "Producto"})

print(f"  → {len(daily):,} rows, columns: {list(daily.columns)}")

# ── All SKUs that have enough history ────────────────────────────────────────
all_skus = daily["Producto"].unique().tolist()
print(f"\nTotal SKUs in daily_sales.parquet: {len(all_skus)}")


# ═══════════════════════════════════════════════════════════════════════════════
#  UNIFIED PROPHET CONFIG — single source of truth
#  (Same logic as the notebook upgraded cell. Keep in sync.)
# ═══════════════════════════════════════════════════════════════════════════════

def get_holiday_df():
    bf = pd.DataFrame({
        "holiday": "black_friday",
        "ds": pd.to_datetime(["2023-11-24", "2024-11-29", "2025-11-28", "2026-11-27"]),
        "lower_window": -1, "upper_window": 2,
    })
    cm = pd.DataFrame({
        "holiday": "cyber_monday",
        "ds": pd.to_datetime(["2023-11-27", "2024-12-02", "2025-12-01", "2026-11-30"]),
        "lower_window": 0, "upper_window": 0,
    })
    return pd.concat([bf, cm], ignore_index=True)


def get_prophet_config(sku_name: str, n_weeks: int) -> dict:
    """Return unified Prophet config dict for any SKU."""
    has_full_year = n_weeks >= 52
    is_mult = sku_name in MULTIPLICATIVE_SKUS and has_full_year
    return dict(
        yearly_seasonality      = has_full_year,
        weekly_seasonality      = False,
        daily_seasonality       = False,
        seasonality_mode        = "multiplicative" if is_mult else "additive",
        changepoint_prior_scale = 0.05 if not has_full_year else 0.08,
        seasonality_prior_scale = 5.0,
        interval_width          = 0.95,
        holidays                = get_holiday_df(),
        growth                  = "linear",
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  OUTLIER WINSORISATION + STOCKOUT CENSORING
# ═══════════════════════════════════════════════════════════════════════════════

def prepare_weekly_series(sku_name: str) -> pd.DataFrame:
    """Aggregate daily → weekly, censor trailing stockout zeros, Winsorise."""
    sku_data = daily[daily["Producto"] == sku_name].copy()
    if len(sku_data) == 0:
        return pd.DataFrame(columns=["ds", "y"])

    sku_weekly = (sku_data
                  .set_index("Date")["Units"]
                  .resample("W-MON").sum()
                  .reset_index())
    sku_weekly.columns = ["ds", "y"]

    # Fill gaps
    full_weeks = pd.date_range(sku_weekly["ds"].min(),
                               sku_weekly["ds"].max(), freq="W-MON")
    sku_weekly = (sku_weekly.set_index("ds")
                  .reindex(full_weeks, fill_value=0)
                  .reset_index())
    sku_weekly.columns = ["ds", "y"]

    if len(sku_weekly) < 12:
        return pd.DataFrame(columns=["ds", "y"])

    # ── Stockout censoring ──
    if sku_name in STOCKOUT_SKUS:
        vals = sku_weekly["y"].values
        trailing = 0
        for v in vals[::-1]:
            if v == 0:
                trailing += 1
            else:
                break
        if trailing >= 3:
            non_zero_period = vals[:len(vals) - trailing]
            if len(non_zero_period) > 0 and non_zero_period.sum() > 0:
                impute = non_zero_period[non_zero_period > 0].mean()
                sku_weekly.iloc[-trailing:, sku_weekly.columns.get_loc("y")] = impute
                print(f"  ⚕ Censored {trailing} trailing zero-weeks → imputed at {impute:.1f}/wk")

    # ── Winsorise at 99th percentile ──
    p99 = sku_weekly["y"].quantile(0.99)
    n_clipped = (sku_weekly["y"] > p99).sum()
    if n_clipped > 0 and p99 > 0:
        sku_weekly["y"] = sku_weekly["y"].clip(upper=p99)
        print(f"  ✂ Winsorised {n_clipped} weeks at p99={p99:.0f}")

    return sku_weekly


# ── Fit Prophet for each SKU ─────────────────────────────────────────────────
prophet_forecasts = {}
PERIODS   = 26   # 26 weeks ≈ 6 months
MIN_WEEKS = 12   # skip SKUs with fewer weeks of history

for sku in sorted(all_skus):
    sku_weekly = prepare_weekly_series(sku)

    if len(sku_weekly) < MIN_WEEKS:
        print(f"  ⚠ {sku}: only {len(sku_weekly)} weeks — skipping (need ≥{MIN_WEEKS})")
        continue

    n_weeks = len(sku_weekly)
    config = get_prophet_config(sku, n_weeks)

    print(f"\nFitting {sku} ({n_weeks} weeks, mode={config['seasonality_mode']}, "
          f"yearly={config['yearly_seasonality']}, cp={config['changepoint_prior_scale']}) …")
    model = Prophet(**config)
    model.add_country_holidays(country_name="ES")

    try:
        model.fit(sku_weekly)
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        continue

    future = model.make_future_dataframe(periods=PERIODS, freq="W-MON")
    forecast = model.predict(future)

    prophet_forecasts[sku] = forecast
    last_hist = sku_weekly["ds"].max()
    last_fc   = forecast["ds"].max()
    print(f"  ✓ history ends {last_hist.date()} | forecast to {last_fc.date()}")

# ── Save prophet_curves.parquet ───────────────────────────────────────────────
print("\nSaving prophet_curves.parquet …")
curves = []
for sku, fc in prophet_forecasts.items():
    slim = fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    slim["Producto"] = sku
    # Clip lower bound to 0 (units can't be negative); NO upper cap
    slim["yhat"]       = slim["yhat"].clip(lower=0)
    slim["yhat_lower"] = slim["yhat_lower"].clip(lower=0)
    # yhat_upper is NOT clipped — this is where CI widening was being lost
    curves.append(slim)

if not curves:
    print("ERROR: no forecasts generated — check daily_sales.parquet columns.")
    raise SystemExit(1)

df_out = pd.concat(curves, ignore_index=True)
out_path = os.path.join(HERE, "prophet_curves.parquet")
df_out.to_parquet(out_path, index=False)

print(f"  ✓ Saved {len(df_out):,} rows to {out_path}")
print(f"  SKUs: {df_out['Producto'].nunique()}")
print(f"  Date range: {df_out['ds'].min().date()} → {df_out['ds'].max().date()}")
print("\n  Per-SKU forecast end dates:")
for sku, grp in df_out.groupby("Producto"):
    future_rows = grp[grp["ds"] > pd.Timestamp.today()]
    print(f"    {sku:50s}  ends {grp['ds'].max().date()}  "
          f"({len(future_rows)} future weeks)")

# ── Also save Prophet 30-day demand forecast ─────────────────────────────────────
# Sum of the next 4 future weekly yhat values (28 days) per SKU.
# Used by the Streamlit app as the forward-looking consumer demand estimate
# so every column in the Hoja de Compras table is genuinely forecast-driven.
today = pd.Timestamp.today().normalize()
demand_rows = []
for sku, fc in prophet_forecasts.items():
    next4 = fc[fc["ds"] > today].head(4)  # first 4 future weeks
    demand_30d = float(next4["yhat"].clip(lower=0).sum())
    demand_rows.append({"Producto": sku, "Prophet_Demand_30d": round(demand_30d, 1)})

demand_df = pd.DataFrame(demand_rows)
demand_path = os.path.join(HERE, "prophet_demand_30d.parquet")
demand_df.to_parquet(demand_path, index=False)
print(f"\n  ✓ Saved prophet_demand_30d.parquet ({len(demand_df)} SKUs)")
print("\nProphet consumer demand (next 4 weeks):")
for _, row in demand_df.sort_values("Prophet_Demand_30d", ascending=False).iterrows():
    print(f"  {row['Producto']:50s}  {row['Prophet_Demand_30d']:>8.1f} units")

print("\n✅  Done — prophet_curves.parquet + prophet_demand_30d.parquet refreshed.")
