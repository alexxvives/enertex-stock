"""
Enertex — Panel de Inteligencia de Cadena de Suministro (v4)
Ejecutar:  streamlit run app.py

Arquitectura:
  • Totalmente autónomo — calcula métricas de inventario y ROP desde data.xlsx.
  • Opcional pero recomendado — prophet_curves.parquet (notebook) aporta
    curvas ML de 26 semanas con IC que se ensancha, evitando recalcular en
    tiempo real. Si no existe, Prophet se re-entrena al vuelo en Tab 5.
  • model_comparison.parquet — resultados de holdout Prophet vs LGBM vs XGB.

Tabs:
  1  Ventas Históricas  — tendencias, top SKUs, heatmap
  2  Inventario         — días de cobertura por SKU
  3  Punto de Reorden   — ROP descompuesto (demanda LT + SS + buffer reseller)
  4  Hoja de Compras    — acción recomendada + CSV export
  5  Forecast vs Real   — Prophet con IC al 95%, fuente: parquet / live
  6  Comparación Interanual — año vs año mensual/semanal
  7  Detalle por SKU    — historial semanal + distribución diaria
"""

import warnings, os
warnings.filterwarnings('ignore')

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats
from datetime import date, timedelta

# ──────────────────────────────────────────────────────────────
#  CONFIG
# ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Enertex · Cadena de Suministro",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Paleta corporativa
C_BRAND  = "#1A237E"
C_BLUE   = "#1565C0"
C_RED    = "#C62828"
C_ORANGE = "#E65100"
C_GREEN  = "#2E7D32"
C_AMBER  = "#F57F17"
C_GREY   = "#455A64"

# ── CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.main-header {
    background: linear-gradient(135deg, #1A237E 0%, #1565C0 100%);
    color: white; padding: 1.2rem 2rem; border-radius: 12px;
    margin-bottom: 1.5rem; box-shadow: 0 4px 16px rgba(26,35,126,0.15);
}
.main-header h1 { margin:0; font-size:1.6rem; font-weight:700; color:white; }
.main-header p  { margin:0.25rem 0 0 0; font-size:0.9rem; opacity:0.85; color:#E3F2FD; }

.kpi-grid {
    display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr));
    gap:0.9rem; margin-bottom:1.5rem;
}
.kpi-card {
    background:white; border-radius:10px; padding:1rem 1.1rem;
    box-shadow:0 1px 8px rgba(0,0,0,0.06); border-left:4px solid #1565C0;
}
.kpi-card.red    { border-left-color:#C62828; }
.kpi-card.orange { border-left-color:#E65100; }
.kpi-card.green  { border-left-color:#2E7D32; }
.kpi-card.amber  { border-left-color:#F57F17; }
.kpi-card .kpi-value { font-size:1.65rem; font-weight:700; color:#1A237E; margin:0; }
.kpi-card .kpi-label { font-size:0.78rem; color:#546E7A; margin:0.2rem 0 0 0;
                       text-transform:uppercase; letter-spacing:0.3px; }

.info-box  { background:#E3F2FD; border-left:4px solid #1565C0; border-radius:8px;
             padding:0.9rem 1.2rem; margin:0.8rem 0; font-size:0.88rem; color:#1A237E; }
.warn-box  { background:#FFF3E0; border-left:4px solid #E65100; border-radius:8px;
             padding:0.9rem 1.2rem; margin:0.8rem 0; font-size:0.88rem; color:#BF360C; }
.ok-box    { background:#E8F5E9; border-left:4px solid #2E7D32; border-radius:8px;
             padding:0.9rem 1.2rem; margin:0.8rem 0; font-size:0.88rem; color:#1B5E20; }

.explain-grid {
    display:grid; grid-template-columns:repeat(auto-fill,minmax(290px,1fr));
    gap:0.6rem; margin:0.8rem 0;
}
.explain-item {
    background:#F5F7FA; border-radius:8px; padding:0.65rem 0.9rem;
    border-left:3px solid #90A4AE;
}
.explain-item strong { color:#1A237E; font-size:0.85rem; }
.explain-item span   { color:#546E7A; font-size:0.8rem; display:block; margin-top:2px; }

section[data-testid="stSidebar"] { background:#FAFBFC; border-right:1px solid #E0E0E0; }
.block-container { padding-top: 1rem !important; }
#MainMenu {visibility:hidden;} footer {visibility:hidden;} header {visibility:hidden;}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
#  CONSTANTES
# ──────────────────────────────────────────────────────────────
LEAD_TIME_DAYS        = 10
SERVICE_LEVEL         = 0.95
Z_SCORE               = stats.norm.ppf(SERVICE_LEVEL)
FORECAST_HORIZON_DAYS = 30
STATS_WINDOW_DAYS     = 180   # window (calendar days) for Avg_Daily_Sales and σ

# Both Title-Case and lower-case variants of the same pack names appear in
# the raw export — each variant is keyed intentionally so the `isin()` lookup
# handles any capitalisation found in data.xlsx without needing to normalise.
PACK_COMPONENTS = {
    'Pack contra la electricidad sucia':
        {'BEEM – EMI METER': 1, 'Stroom Master PRO': 4},
    'Kit de Protección a la Radiación para Bebés y Niños':
        {'SPIRO Disc': 1, 'SPIRO Square': 1, 'SPIRO Card': 2},
    'Protección a Exposición Alta Individual':
        {'SPIRO Card': 1, 'SPIRO Disc X': 1},
    'Protección a exposición alta individual':   # lowercase variant
        {'SPIRO Card': 1, 'SPIRO Disc X': 1},
    'Protección a Exposición Severa Casos con EHS':
        {'SPIRO Card': 1, 'SPIRO Disc Ultra': 1},
    'Protección a exposición severa casos con EHS':  # lowercase variant
        {'SPIRO Card': 1, 'SPIRO Disc Ultra': 1},
    'Protección Básica Individual':
        {'SPIRO Disc': 1, 'SPIRO Card': 1},
    'Protección Estándar Espacios':
        {'SPIRO Square': 1, 'SPIRO Disc': 1, 'SPIRO Disc X': 1,
         'SPIRO Disc Ultra': 1, 'Stroom Master PRO': 1},
    'Protección Estándar Oficina':
        {'Stroom Master PRO': 2},
}

BULB_CANON = {
    'Pack Ruby Light Bulb':           'Ruby Light Bulb',
    'Pack Ruby Light Bulb 1 Unidad':  'Ruby Light Bulb',
    'Ruby Light Bulb':                'Ruby Light Bulb',
    'Pack Amber Light Bulb':          'Amber Light Bulb',
    'Pack Amber Light Bulb 1unidad':  'Amber Light Bulb',
    'Amber Light Bulb VENTA AFILIADO':'Amber Light Bulb',
    'Amber Light Bulb':               'Amber Light Bulb',
    'Pack Full Spectrum Bulb':        'BioLight™ - Full Spectrum Bulb',
    'Full Spectrum Bulb':             'BioLight™ - Full Spectrum Bulb',
    'BioLight™ - Full Spectrum Bulb': 'BioLight™ - Full Spectrum Bulb',
}

PROD_NAME_MAP = {
    'Stroom Master':                    'Stroom Master PRO',
    'Full Spectrum Lamp':               'BioLight™ - Full Spectrum Lamp',
    'Amber Book Light':                 'NoBlue Amber Book Light',
    'Ruby Book Light':                  'Book Light Ruby',
    'NoBlue Ruby Book Light':           'Book Light Ruby',
    'Protector pantalla':               'Protector de Pantalla Anti Luz Azul',
    'Protector pantalla Iphone 15':     'Protector de Pantalla Anti Luz Azul',
    'Protector pantalla Iphone 15 Venta afiliada Elisabeth':
                                        'Protector de Pantalla Anti Luz Azul',
    'Protector de Pantalla Anti Luz Azul - iPhone 15 / 15 Pro':
                                        'Protector de Pantalla Anti Luz Azul',
    'Protector de Pantalla Anti Luz Azul - IPhone 15 Plus / 15 Pro Max':
                                        'Protector de Pantalla Anti Luz Azul',
    'Protector de Pantalla Anti Luz Azul - Iphone 15 Plus / 15 Pro Max':
                                        'Protector de Pantalla Anti Luz Azul',
    'Full Spectrum Bulb':               'BioLight™ - Full Spectrum Bulb',
    'Pack Full Spectrum Bulb':          'BioLight™ - Full Spectrum Bulb',
    'Pack Ruby Light Bulb':             'Ruby Light Bulb',
    'Pack Ruby Light Bulb 1 Unidad':    'Ruby Light Bulb',
    'Pack Amber Light Bulb':            'Amber Light Bulb',
    'Pack Amber Light Bulb 1unidad':    'Amber Light Bulb',
    'Amber Light Bulb VENTA AFILIADO':  'Amber Light Bulb',
}

ACTUAL_STOCK = {
    'Stroom Master PRO': 971, 'SPIRO Card': 703, 'SPIRO Disc': 466,
    'SG - 001 - Tarjetero Magnético Doble Capa': 165, 'SPIRO Square': 217,
    'SPIRO Square X': 209, 'Amber Light Bulb': 149, 'Ruby Light Bulb': 133,
    'SG - 004 - Tarjetero Doble Capa': 133,
    'SG - 002 - Tarjetero Magnético Doble Capa': 110,
    'SG - 005 - Tarjetero Magnético': 106,
    'BioLight™ - Full Spectrum Lamp': 98,
    'SG - 003 - Tarjetero Doble Capa': 84,
    'Luz Ruby con sensor de movimiento': 83, 'BEEM – EMI METER': 83,
    'SPIRO Card X': 78, 'Protector de Pantalla Anti Luz Azul': 66,
    'SPIRO Disc Ultra': 55, 'Ruby Light Lamp': 42, 'Amber Light Lamp': 39,
    'SPIRO Disc X': 17, 'Book Light Ruby': 6, 'NoBlue Amber Book Light': 5,
    'BioLight™ - Full Spectrum Bulb': 0, 'Red Light Therapy MultiSpectral PRO': 0,
}

EXCLUDE_PRODUCTS = [
    'Envio', 'Envío gratuito', 'Seguro', 'Seguro (1,5%)',
    'Batch', 'Batch costes', 'Zona EU 1', 'Zona EU 2', 'Zona EU 3',
    'Funda',
]

# Etiquetas de estado (sin emojis)
ST_CRITICAL = 'PEDIR AHORA'
ST_WARNING  = 'PEDIR ESTA SEMANA'
ST_OK       = 'COBERTURA OK'


# ──────────────────────────────────────────────────────────────
#  HELPERS — UI components and chart utilities
# ──────────────────────────────────────────────────────────────
def kpi_card(label, value, css=""):
    return (f'<div class="kpi-card {css}">'
            f'<p class="kpi-value">{value}</p>'
            f'<p class="kpi-label">{label}</p></div>')


def action_color(a):
    if a == ST_CRITICAL: return C_RED
    if a == ST_WARNING:  return C_ORANGE
    return C_GREEN


PLOTLY_COMMON = dict(
    font=dict(family="Inter, sans-serif", size=12, color="#37474F"),
    paper_bgcolor="white", plot_bgcolor="#FAFBFC",
    hoverlabel=dict(bgcolor="white", font_size=12,
                    font_family="Inter, sans-serif", align="left"),
    xaxis=dict(gridcolor="#ECEFF1", zeroline=False),
    yaxis=dict(gridcolor="#ECEFF1", zeroline=False),
)


def styled_fig(fig, **kw):
    fig.update_layout(**{**PLOTLY_COMMON, **kw})
    return fig


# ──────────────────────────────────────────────────────────────
#  FORECASTING HELPERS
#  _prophet_forecast  — cached Prophet fit (used when parquet doesn't cover
#                       the requested horizon or the SKU is missing)
#  get_forecast       — routing layer: parquet → parquet_extended → prophet_only
#  _render_prophet_chart — shared chart renderer used by Tab 5 in both the
#                          parquet-available and no-parquet fallback paths
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Generando previsión Prophet…")
def _prophet_forecast(product, daily_json, horizon_weeks):
    """Fit Prophet on weekly sales and return forecast dataframe as JSON.

    Cached so selecting a different horizon for the same SKU re-uses the
    previously fitted model if the horizon is the same.
    Uses unified config (interval_width=0.95, cp=0.08, holidays) aligned
    with regenerate_prophet_parquet.py.
    """
    try:
        from prophet import Prophet  # noqa
    except ImportError:
        return None
    df = pd.read_json(daily_json)
    # Robust column access — don’t assume column ORDER from JSON
    if 'ds' not in df.columns or 'y' not in df.columns:
        if df.shape[1] == 2:
            df.columns = ['ds', 'y']
        else:
            return None
    df['ds'] = pd.to_datetime(df['ds'])
    df = df[df['y'] >= 0].dropna()
    n_nonzero = (df['y'] > 0).sum()
    if len(df) < 12 or n_nonzero < 6:
        return None
    # Unified config — aligned with regenerate_prophet_parquet.py
    # Build holidays first, then pass to constructor (clean Prophet pattern)
    _bf = pd.DataFrame({
        'holiday': 'black_friday',
        'ds': pd.to_datetime(['2023-11-24', '2024-11-29', '2025-11-28', '2026-11-27']),
        'lower_window': -1, 'upper_window': 2,
    })
    _cm = pd.DataFrame({
        'holiday': 'cyber_monday',
        'ds': pd.to_datetime(['2023-11-27', '2024-12-02', '2025-12-01', '2026-11-30']),
        'lower_window': 0, 'upper_window': 0,
    })
    _holidays = pd.concat([_bf, _cm], ignore_index=True)
    m = Prophet(
        yearly_seasonality=len(df) >= 52,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.08,
        seasonality_prior_scale=5.0,
        interval_width=0.95,
        uncertainty_samples=300,
        holidays=_holidays,
    )
    try:
        m.fit(df)
    except Exception:
        return None
    future = m.make_future_dataframe(periods=horizon_weeks, freq='W')
    fc = m.predict(future)
    # Only clip negatives — do NOT cap yhat_upper so the CI visually widens with time
    fc['yhat']       = fc['yhat'].clip(lower=0)
    fc['yhat_lower'] = fc['yhat_lower'].clip(lower=0)
    fc['yhat_upper'] = fc['yhat_upper'].clip(lower=0)
    return fc[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_json(date_format='iso')


def get_forecast(product, daily_df, forecast_curves, horizon_weeks):
    """Route a forecast request to the best available data source.

    Returns
    -------
    (fc_df, source) where source is one of:
      'parquet'          — parquet covers the full horizon → use directly
      'parquet_extended' — SKU in parquet but horizon too short → live re-fit
      'prophet_only'     — SKU never in parquet → live fit from scratch
      'none'             — not enough history to fit any model (< 12 weeks)
    """
    in_parquet = False
    if forecast_curves is not None:
        base = forecast_curves[forecast_curves['Producto'] == product].sort_values('ds')
        in_parquet = len(base) > 0
    else:
        base = pd.DataFrame()

    today = pd.Timestamp.now()
    future_needed = today + pd.Timedelta(weeks=horizon_weeks)

    # Parquet fully covers the horizon — use it directly
    if in_parquet and base['ds'].max() >= future_needed:
        for col in ['yhat', 'yhat_lower', 'yhat_upper']:
            if col in base.columns:
                base[col] = base[col].clip(lower=0)
        return base, 'parquet'

    # Need live Prophet (either to extend parquet or from scratch)
    sku_w = (daily_df[daily_df['Producto'] == product]
             .set_index('Date')['Units']
             .resample('W-MON').sum()
             .reset_index())
    sku_w.columns = ['ds', 'y']
    json_str = sku_w.to_json(date_format='iso')
    result_json = _prophet_forecast(product, json_str, horizon_weeks + 8)
    if result_json is None:
        return base if in_parquet else pd.DataFrame(), 'none'
    fc = pd.read_json(result_json)
    fc['ds'] = pd.to_datetime(fc['ds'])
    for col in ['yhat', 'yhat_lower', 'yhat_upper']:
        if col in fc.columns:
            fc[col] = fc[col].clip(lower=0)
    source = 'parquet_extended' if in_parquet else 'prophet_only'
    return fc, source


def _render_prophet_chart(fc_df, act_w, height=450):
    """Render the Prophet forecast chart with CI bands and actual sales overlay.

    Both the 'parquet available' and 'no-parquet fallback' branches in Tab 5
    call this function so the rendering logic lives in exactly one place.

    Parameters
    ----------
    fc_df  : DataFrame — columns: ds, yhat, yhat_lower, yhat_upper
    act_w  : DataFrame — columns: ds, actual  (weekly actual sales)
    height : chart height in pixels
    """
    fc = fc_df.sort_values('ds').reset_index(drop=True)
    last_real = act_w['ds'].max() if len(act_w) else fc['ds'].min()

    # Split into in-sample (historical fit) and out-of-sample (future forecast)
    fc_hist   = fc[fc['ds'] <= last_real]
    fc_future = fc[fc['ds'] > last_real]

    # Connect the two segments with a bridge point so the lines don't gap
    if len(fc_hist) and len(fc_future):
        bridge    = fc_hist.iloc[[-1]]
        fc_future = pd.concat([bridge, fc_future], ignore_index=True)

    fig = go.Figure()

    # ── CI band — historical (blue, subtle) ──────────────────────────────────
    if len(fc_hist) > 1:
        fig.add_trace(go.Scatter(
            x=pd.concat([fc_hist['ds'], fc_hist['ds'][::-1]]),
            y=pd.concat([fc_hist['yhat_upper'], fc_hist['yhat_lower'][::-1]]),
            fill='toself', fillcolor='rgba(21,101,192,0.10)',
            line=dict(color='rgba(0,0,0,0)'),
            name='IC 95% (histórico)', hoverinfo='skip'))

    # ── CI band — future (green, widens with time because no upper cap) ──────
    if len(fc_future) > 1:
        fig.add_trace(go.Scatter(
            x=pd.concat([fc_future['ds'], fc_future['ds'][::-1]]),
            y=pd.concat([fc_future['yhat_upper'], fc_future['yhat_lower'][::-1]]),
            fill='toself', fillcolor='rgba(46,125,50,0.15)',
            line=dict(color='rgba(0,0,0,0)'),
            name='IC 95% (futuro)', hoverinfo='skip'))

    # ── Prediction line — historical (blue) ──────────────────────────────────
    if len(fc_hist):
        fig.add_trace(go.Scatter(
            x=fc_hist['ds'], y=fc_hist['yhat'],
            mode='lines', name='Predicción (histórico)',
            line=dict(color=C_BLUE, width=2),
            hovertemplate='%{x|%d %b %Y}<br>Predicción: %{y:.1f} u/sem<extra></extra>'))

    # ── Prediction line — future (green, dashed-dash) ────────────────────────
    if len(fc_future):
        fig.add_trace(go.Scatter(
            x=fc_future['ds'], y=fc_future['yhat'],
            mode='lines', name='Predicción (futuro)',
            line=dict(color=C_GREEN, width=2.5),
            hovertemplate='%{x|%d %b %Y}<br>Predicción: %{y:.1f} u/sem<extra></extra>'))

    # ── Vertical cutoff line between history and future ───────────────────────
    fig.add_vline(x=last_real.isoformat(), line_dash='dash', line_color='#9E9E9E')

    # ── Actual weekly sales overlay (grey dots + dotted line) ────────────────
    fig.add_trace(go.Scatter(
        x=act_w['ds'], y=act_w['actual'],
        mode='markers+lines', name='Ventas reales',
        marker=dict(color=C_GREY, size=4),
        line=dict(color=C_GREY, width=1, dash='dot'),
        hovertemplate='%{x|%d %b %Y}<br>Real: %{y:.0f} u/sem<extra></extra>'))

    styled_fig(fig, height=height, hovermode='x unified',
        xaxis_title='Semana', yaxis_title='Unidades semanales',
        legend=dict(orientation='h', y=1.08, x=0.5, xanchor='center'),
        margin=dict(t=20, b=50, l=60, r=20))
    st.plotly_chart(fig, use_container_width=True)


# ──────────────────────────────────────────────────────────────
#  CARGA DE DATOS — Amphora /sales-history (live) → data.xlsx (fallback)
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Cargando datos…", ttl=300)
def load_all():
    """Carga y calcula todo. Fuente primaria: Amphora /sales-history. Fallback: data.xlsx."""
    import json as _json
    base = os.path.dirname(os.path.abspath(__file__))

    # ── 1a. Try Amphora /sales-history first ──────────────────────────────────
    #   Returns daily rows: [{date, product, variant, units}]
    #   This covers ALL channels Amphora fulfils (D2C + B2B/resellers).
    _webhook_url = (st.secrets.get("AMPHORA_WEBHOOK_URL") or
                    os.environ.get("AMPHORA_WEBHOOK_URL", "")).rstrip("/")
    _amphora_daily = None
    if _webhook_url:
        try:
            import urllib.request as _ur
            with _ur.urlopen(f"{_webhook_url}/sales-history", timeout=45) as _r:
                _sh = _json.loads(_r.read())
            if _sh.get("daily"):
                _rows = []
                _PACK_NAMES = set(PACK_COMPONENTS.keys())
                _EXCLUDE_SET = set(EXCLUDE_PRODUCTS)
                for _e in _sh["daily"]:
                    _prod = PROD_NAME_MAP.get(_e["product"], _e["product"])
                    _units = int(_e["units"])
                    _dt = pd.to_datetime(_e["date"])
                    if _prod in _EXCLUDE_SET:
                        continue
                    # Expand pack products into their components
                    if _prod in _PACK_NAMES:
                        for _comp, _qty_per in PACK_COMPONENTS[_prod].items():
                            _rows.append({"Date": _dt, "Producto": _comp,
                                          "Units": _units * _qty_per,
                                          "Revenue": 0.0, "Is_Reseller": False})
                        continue
                    _var = _e.get("variant", "")
                    _label = f"{_prod} - {_var}" if _var else _prod
                    _rows.append({"Date": _dt, "Producto": _label,
                                  "Units": _units, "Revenue": 0.0,
                                  "Is_Reseller": False})
                if _rows:
                    _amphora_daily = pd.DataFrame(_rows)
        except Exception:
            _amphora_daily = None

    # ── 1b. Require Amphora data — no Excel fallback ──────────────────────────
    _using_amphora = _amphora_daily is not None and len(_amphora_daily) > 0

    if not _using_amphora:
        raise RuntimeError("Amphora sin historial")

    daily     = (_amphora_daily.groupby(['Date', 'Producto'])
                 .agg(Units=('Units', 'sum'), Revenue=('Revenue', 'sum'))
                 .reset_index())
    sales_all = _amphora_daily.copy()

    # ── 2. Compute inventory metrics per SKU ──
    #  All rolling metrics use the same STATS_WINDOW_DAYS-day window so that
    #  Avg and σ reflect the same current demand velocity.
    #
    #  IMPORTANT: zero-demand days must be included when computing σ.
    #  `daily` only has rows for days with sales — quiet days are absent.
    #  We reindex to a full calendar grid and fill missing days with 0.
    _d_max      = daily['Date'].max()
    _d_start_win = _d_max - pd.Timedelta(days=STATS_WINDOW_DAYS - 1)
    _last_win    = daily[daily['Date'] >= _d_start_win].copy()

    # Full (product × date) grid for the stats window
    _win_dates  = pd.date_range(_d_start_win, _d_max, freq='D')
    _all_skus   = daily['Producto'].unique()
    _win_grid   = (pd.MultiIndex
                   .from_product([_all_skus, _win_dates], names=['Producto', 'Date'])
                   .to_frame(index=False))
    _win_full   = (_win_grid
                   .merge(_last_win[['Producto', 'Date', 'Units']],
                          on=['Producto', 'Date'], how='left')
                   .fillna({'Units': 0}))

    _avg_win  = _win_full.groupby('Producto')['Units'].mean().rename('Avg_Daily_Sales')
    _std_win  = _win_full.groupby('Producto')['Units'].std().fillna(0).rename('Std_Daily_Sales')

    sku_stats = (daily.groupby('Producto')
                 .agg(Total_Units=('Units', 'sum'),
                      Total_Revenue=('Revenue', 'sum'))
                 .reset_index())
    sku_stats = sku_stats.join(_avg_win, on='Producto').join(_std_win, on='Producto')
    sku_stats['Avg_Daily_Sales']  = sku_stats['Avg_Daily_Sales'].fillna(0)
    sku_stats['Std_Daily_Sales']  = sku_stats['Std_Daily_Sales'].fillna(0)

    sku_stats['Safety_Stock'] = Z_SCORE * sku_stats['Std_Daily_Sales'] * np.sqrt(LEAD_TIME_DAYS)
    sku_stats['LT_Demand']    = sku_stats['Avg_Daily_Sales'] * LEAD_TIME_DAYS

    # Buffer revendedor
    # When using Amphora data all shipments are already included in velocity
    # (Amphora ships D2C + B2B), so no separate reseller buffer is needed.
    if not _using_amphora and 'Is_Reseller' in sales_all.columns:
        res = sales_all[sales_all['Is_Reseller']].copy()
        res['Date'] = pd.to_datetime(res['Fecha']).dt.date
    else:
        res = pd.DataFrame()
    if len(res):
        n_months = max(1, (pd.to_datetime(res['Date'].max()) -
                           pd.to_datetime(res['Date'].min())).days / 30)
        res_daily = res.groupby('Producto')['Unidades'].sum() / n_months / 30
        sku_stats['Reseller_LT_Buffer'] = (sku_stats['Producto']
                                            .map(res_daily).fillna(0) * LEAD_TIME_DAYS)
        sku_stats['Reseller_Demand_30d'] = (sku_stats['Producto']
                                             .map(res_daily).fillna(0) * 30)
    else:
        sku_stats['Reseller_LT_Buffer']  = 0.0
        sku_stats['Reseller_Demand_30d'] = 0.0

    sku_stats['Reorder_Point'] = (sku_stats['LT_Demand'] +
                                  sku_stats['Safety_Stock'] +
                                  sku_stats['Reseller_LT_Buffer'])

    # Stock priority:
    #   1) Amphora Render webhook   (AMPHORA_WEBHOOK_URL env var)
    #   2) amphora_stock.json       (local / Render disk)
    #   3) stock.xlsx               (manually maintained)
    #   4) hardcoded ACTUAL_STOCK   (fallback)
    import json as _json
    _webhook_url  = (st.secrets.get("AMPHORA_WEBHOOK_URL") or os.environ.get("AMPHORA_WEBHOOK_URL", "")).rstrip("/")
    _amphora_json = os.path.join(base, 'amphora_stock.json')
    _stock_file   = os.path.join(base, 'stock.xlsx')
    _stock_map      = None
    _stock_source   = "hardcoded"  # will be updated below to reflect the real source used
    _variant_parent = {}   # maps "Product - Variant" → "Product" for velocity inheritance

    if _webhook_url:
        try:
            import urllib.request as _ur
            # Render free tier may be sleeping — fall through to next source on failure.
            with _ur.urlopen(f"{_webhook_url}/current-stock", timeout=45) as _resp:
                _d = _json.loads(_resp.read())
            if isinstance(_d.get("stock"), dict) and _d["stock"]:
                _stock_map = dict(_d["stock"])  # copy so we can extend with variants
                _stock_source = "amphora_live"
                for _item in _d.get("stock_by_sku", []):
                    _prod = _item.get("product", "")
                    _var  = _item.get("variant", "")
                    if _prod and _var:
                        _vkey = f"{_prod} - {_var}"
                        _stock_map[_vkey] = _item.get("quantity", 0)
                        _variant_parent[_vkey] = _prod
        except Exception:
            _stock_map = None   # fall through to next source

    if _stock_map is None and os.path.exists(_amphora_json):
        try:
            _d = _json.loads(open(_amphora_json, encoding='utf-8').read())
            _stock_map = dict(_d.get('stock') or _d)
            if not isinstance(_stock_map, dict):
                raise ValueError
            _stock_source = "amphora_json"
            for _item in _d.get("stock_by_sku", []):
                _prod = _item.get("product", "")
                _var  = _item.get("variant", "")
                if _prod and _var:
                    _vkey = f"{_prod} - {_var}"
                    _stock_map[_vkey] = _item.get("quantity", 0)
                    _variant_parent[_vkey] = _prod
        except Exception:
            _stock_map = None

    if _stock_map is None and os.path.exists(_stock_file):
        try:
            _stock_df  = pd.read_excel(_stock_file)
            _stock_map = dict(zip(_stock_df['Producto'], _stock_df['Stock']))
            _stock_source = "stock_xlsx"
        except Exception:
            _stock_map = None

    if _stock_map is None:
        _stock_map = ACTUAL_STOCK
        # _stock_source stays "hardcoded"

    # Inject any products that exist in stock but have no sales history.
    # Variant rows ("Product - Variant") inherit their parent's velocity split equally
    # across all known variants of that product (velocity / n_variants per product).
    _parent_velocity = sku_stats.set_index('Producto')['Avg_Daily_Sales'].to_dict()
    _parent_std      = sku_stats.set_index('Producto')['Std_Daily_Sales'].to_dict()
    # Count how many variants each parent product has in the stock map
    _variant_counts: dict[str, int] = {}
    for _vk, _vp in _variant_parent.items():
        _variant_counts[_vp] = _variant_counts.get(_vp, 0) + 1
    _known_skus = set(sku_stats['Producto'])
    _extra_rows = []
    for _pname, _qty in _stock_map.items():
        if _pname not in _known_skus:
            _par = _variant_parent.get(_pname, '')
            _n   = max(_variant_counts.get(_par, 1), 1) if _par else 1
            _vel = _parent_velocity.get(_par, 0.0) / _n if _par else 0.0
            _std = _parent_std.get(_par, 0.0) / _n if _par else 0.0
            _extra_rows.append({'Producto': _pname, 'Total_Units': 0,
                                 'Total_Revenue': 0.0, 'Avg_Daily_Sales': _vel,
                                 'Std_Daily_Sales': _std, 'Safety_Stock': 0.0,
                                 'LT_Demand': 0.0, 'Reseller_LT_Buffer': 0.0,
                                 'Reseller_Demand_30d': 0.0, 'Reorder_Point': 0.0})
    if _extra_rows:
        sku_stats = pd.concat([sku_stats, pd.DataFrame(_extra_rows)],
                               ignore_index=True)

    sku_stats['Stock'] = sku_stats['Producto'].map(_stock_map).fillna(0)

    # When per-variant rows were injected (e.g. "SPIRO Card - Azul" / "Blanca"),
    # drop the parent-level row to avoid showing it twice with the summed stock.
    _parents_with_variants = set(_variant_parent.values())
    if _parents_with_variants:
        sku_stats = sku_stats[
            ~sku_stats['Producto'].isin(_parents_with_variants)
        ].reset_index(drop=True)

    sku_stats['Below_ROP'] = sku_stats['Stock'] < sku_stats['Reorder_Point']

    avg_d = sku_stats.set_index('Producto')['Avg_Daily_Sales'].clip(lower=0.001)
    sku_stats['Days_of_Stock'] = sku_stats.apply(
        lambda r: r['Stock'] / avg_d.get(r['Producto'], 0.001), axis=1)

    def classify(r):
        if r['Below_ROP']:          return ST_CRITICAL
        if r['Days_of_Stock'] < 30: return ST_WARNING
        return ST_OK
    sku_stats['Action'] = sku_stats.apply(classify, axis=1)

    # ── Consumer & total demand must be computed BEFORE Suggested_Order_Qty ──
    # Consumer forecast: Prophet's summed yhat for the next 4 weeks (28 days).
    # Loaded from prophet_demand_30d.parquet (generated by regenerate_prophet_parquet.py).
    # Falls back to Avg_Daily_Sales × 30 for any SKU not covered by the parquet.
    p_demand = os.path.join(base, 'prophet_demand_30d.parquet')
    if os.path.exists(p_demand):
        prophet_d30 = pd.read_parquet(p_demand).set_index('Producto')['Prophet_Demand_30d']
        sku_stats['Consumer_Demand_30d'] = (
            sku_stats['Producto'].map(prophet_d30)
            .fillna(sku_stats['Avg_Daily_Sales'] * 30)
            .round(0)
        )
    else:
        sku_stats['Consumer_Demand_30d'] = (sku_stats['Avg_Daily_Sales'] * 30).round(0)
    # Total = consumer (Prophet) + reseller
    sku_stats['Forecast_Demand_30d'] = sku_stats['Consumer_Demand_30d'] + sku_stats['Reseller_Demand_30d']

    # Suggested order = cover 60 days of forecast demand minus current stock
    sku_stats['Suggested_Order_Qty'] = sku_stats.apply(
        lambda r: max(0, r['Forecast_Demand_30d'] * 2 - r['Stock'])
        if r['Below_ROP'] else 0, axis=1).round()

    # ── 3. Load pre-computed ML curves from notebook (optional) ──
    #        If parquet files are missing the app still works; Tab 5
    #        falls back to live Prophet re-fitting on demand.
    curves, comp = None, None
    try:
        p_curves = os.path.join(base, 'prophet_curves.parquet')
        p_comp   = os.path.join(base, 'model_comparison.parquet')
        if os.path.exists(p_curves):
            curves = pd.read_parquet(p_curves)
            curves['ds'] = pd.to_datetime(curves['ds'])
        if os.path.exists(p_comp):
            comp = pd.read_parquet(p_comp)
    except Exception:
        pass

    return daily, sku_stats, curves, comp, _using_amphora, _stock_source


# ──────────────────────────────────────────────────────────────
try:
    daily, proc, forecast_curves, model_comp, _data_from_amphora, _stock_source = load_all()
except (FileNotFoundError, RuntimeError):
    st.error("Amphora todavía no tiene historial de ventas. "
             "Ejecuta `python backfill_fulfilled.py` para sembrar el historial.")
    st.stop()

has_ml = forecast_curves is not None and len(forecast_curves) > 0


# ──────────────────────────────────────────────────────────────
#  BARRA LATERAL
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:0.5rem;">
        <div style="background:linear-gradient(135deg,#1A237E,#1565C0);width:40px;height:40px;
                     border-radius:8px;display:flex;align-items:center;justify-content:center;">
            <span style="color:white;font-size:20px;font-weight:700;">E</span>
        </div>
        <div>
            <span style="font-size:1.15rem;font-weight:700;color:#1A237E;">Enertex Analytics</span><br>
            <span style="font-size:0.75rem;color:#78909C;">Panel de Cadena de Suministro</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    data_last = daily['Date'].max().date()
    days_stale = (date.today() - data_last).days
    stale_color = '#C62828' if days_stale > 14 else ('#E65100' if days_stale > 7 else '#2E7D32')
    st.caption(f"Último dato: {data_last.strftime('%d/%m/%Y')}")
    if days_stale > 0:
        st.markdown(
            f'<span style="font-size:0.72rem;color:{stale_color};font-weight:600;">'
            f'⏱ Datos con {days_stale} días de antigüedad'
            f'</span>', unsafe_allow_html=True)
    st.divider()

    st.markdown("**Rango de Fechas**")
    min_date = daily['Date'].min().date()
    max_date = daily['Date'].max().date()
    date_range = st.date_input(
        "Selecciona el período",
        value=(max(min_date, max_date - timedelta(days=365)), max_date),
        min_value=min_date, max_value=max_date,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        d_start, d_end = date_range
    else:
        d_start, d_end = min_date, max_date

    st.divider()
    st.markdown("**Filtrar por Producto**")
    _sku_opts = ["Todos los productos"] + sorted(daily['Producto'].unique().tolist())
    _sel_lbl  = st.selectbox(
        "Producto (filtro global)", _sku_opts,
        key='product_filter', label_visibility='collapsed')
    sel_product: str | None = None if _sel_lbl == "Todos los productos" else _sel_lbl

    st.divider()

    # Use the source flags returned by load_all() — reflects what actually happened
    _src_map = {
        "amphora_live":  ("Amphora Render live ✓",    "#2E7D32"),
        "amphora_json":  ("Amphora JSON local ✓",      "#2E7D32"),
        "stock_xlsx":    ("stock.xlsx ✓",               "#1565C0"),
        "hardcoded":     ("hardcoded (Render inactivo)", "#E65100"),
    }
    _stock_label, _stock_color = _src_map.get(_stock_source, ("desconocido", "#E65100"))
    _sales_label = "Amphora ✓"
    _sales_color = "#2E7D32"   if _data_from_amphora else "#1565C0"
    st.markdown(
        f"<div style='font-size:0.78rem;color:#78909C;'>"
        f"Plazo de entrega: <b>{LEAD_TIME_DAYS} días</b> · "
        f"Nivel de servicio: <b>{SERVICE_LEVEL*100:.0f}%</b><br>"
        f"Ventana estadística: <b>{STATS_WINDOW_DAYS} días</b><br>"
        f"Stock: <b style='color:{_stock_color}'>{_stock_label}</b><br>"
        f"Ventas: <b style='color:{_sales_color}'>{_sales_label}</b>"
        f"</div>",
        unsafe_allow_html=True)

daily_filtered = daily[(daily['Date'] >= pd.Timestamp(d_start)) &
                       (daily['Date'] <= pd.Timestamp(d_end))].copy()


# ──────────────────────────────────────────────────────────────
#  CABECERA + KPIs
# ──────────────────────────────────────────────────────────────
n_crit  = (proc['Action'] == ST_CRITICAL).sum()
n_warn  = (proc['Action'] == ST_WARNING).sum()
n_ok    = (proc['Action'] == ST_OK).sum()
n_over  = ((proc['Days_of_Stock'] > 180) & (proc['Stock'] > 0)).sum()
total_o = int(proc['Suggested_Order_Qty'].sum())

st.markdown("""
<div class="main-header">
    <h1>Enertex · Inteligencia de Cadena de Suministro</h1>
    <p>Análisis de inventario, previsión de demanda y recomendaciones de compra</p>
</div>
""", unsafe_allow_html=True)

kpi_html = '<div class="kpi-grid">'
kpi_html += kpi_card("SKUs analizados", len(proc))
kpi_html += kpi_card("Pedir ahora",     n_crit,  "red")
kpi_html += kpi_card("Pedir esta semana", n_warn, "orange")
kpi_html += kpi_card("Cobertura OK",     n_ok,    "green")
kpi_html += kpi_card("Sobrestock >180 d", n_over, "amber")
kpi_html += kpi_card("Uds. a pedir",    f"{total_o:,}")
kpi_html += '</div>'
st.markdown(kpi_html, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
#  PESTAÑAS
# ──────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Ventas Históricas",
    "Inventario",
    "Punto de Reorden",
    "Hoja de Compras",
    "Forecast vs Real",
    "Comparación Interanual",
    "Detalle por SKU",
])


# ══════════════════════════════════════════════════════════════
#  1 — VENTAS HISTÓRICAS
# ══════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Demanda Diaria del Consumidor")
    st.caption(f"Período: {d_start.strftime('%d/%m/%Y')} — {d_end.strftime('%d/%m/%Y')}")

    overall = (daily_filtered.groupby('Date')['Units'].sum()
               .reset_index().sort_values('Date'))
    overall['MA7'] = overall['Units'].rolling(7).mean()

    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=overall['Date'], y=overall['Units'],
        name='Unidades diarias', marker_color='rgba(21,101,192,0.25)',
        hovertemplate='%{x|%d %b %Y}<br>%{y:.0f} unidades<extra></extra>'))
    fig1.add_trace(go.Scatter(
        x=overall['Date'], y=overall['MA7'],
        name='Media móvil 7 días', line=dict(color=C_BLUE, width=2.5),
        hovertemplate='%{x|%d %b %Y}<br>MA7: %{y:.1f} u/día<extra></extra>'))
    styled_fig(fig1, height=340, hovermode='x unified',
        xaxis_title='Fecha', yaxis_title='Unidades vendidas',
        legend=dict(orientation='h', y=1.08, x=0.5, xanchor='center'),
        margin=dict(t=20, b=50, l=60, r=20))
    st.plotly_chart(fig1, use_container_width=True)

    with st.container():
        st.markdown("#### Top 10 SKUs — Volumen")
        top_uni = (daily_filtered.groupby('Producto')['Units'].sum()
                   .sort_values(ascending=False).head(10).reset_index())
        f_uni = px.bar(top_uni, x='Units', y='Producto', orientation='h',
                       color='Units', color_continuous_scale='Greens',
                       labels={'Units': 'Unidades', 'Producto': ''})
        f_uni.update_traces(hovertemplate='<b>%{y}</b><br>%{x:,.0f} u<extra></extra>')
        if sel_product and sel_product in top_uni['Producto'].values:
            _uni_sorted = top_uni.sort_values('Units', ascending=True)['Producto'].tolist()
            _sp_uni_idx = _uni_sorted.index(sel_product)
            f_uni.add_shape(type='rect', xref='paper', x0=0, x1=1,
                            yref='y', y0=_sp_uni_idx - 0.45, y1=_sp_uni_idx + 0.45,
                            fillcolor='rgba(255,183,0,0.25)',
                            line=dict(color='#F57F17', width=1.5), layer='below')
        styled_fig(f_uni, height=400, showlegend=False, coloraxis_showscale=False,
                   yaxis={'categoryorder': 'total ascending'},
                   margin=dict(t=10, b=40, l=10, r=10))
        st.plotly_chart(f_uni, use_container_width=True)

    # Heatmap semanal
    st.markdown("#### Mapa de Calor — Demanda Semanal por SKU")
    heat = daily_filtered.copy()
    heat['Week'] = heat['Date'].dt.to_period('W').dt.start_time
    piv = heat.pivot_table(index='Producto', columns='Week',
                           values='Units', aggfunc='sum', fill_value=0)
    top15 = piv.sum(axis=1).sort_values(ascending=False).head(15).index
    piv = piv.loc[top15]
    piv.columns = [c.strftime('%d %b') for c in piv.columns]
    fh = px.imshow(piv, color_continuous_scale='YlOrRd', aspect='auto',
                   labels=dict(x="Semana", y="Producto", color="Unidades"))
    styled_fig(fh, height=max(350, 26*len(top15)),
               margin=dict(t=10, b=50, l=10, r=20))
    fh.update_xaxes(tickangle=-45, tickfont_size=9)
    st.plotly_chart(fh, use_container_width=True)


# ══════════════════════════════════════════════════════════════
#  2 — INVENTARIO (DÍAS DE COBERTURA)
# ══════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### Días de Cobertura por SKU")
    st.caption("Todos los SKUs por estado de inventario. SKUs sin stock aparecen arriba (barra roja, 0 días). Las líneas verticales marcan umbrales clave.")

    disp = proc.copy()
    # Orden explícito: zero-stock al FINAL de categoryarray → aparecen arriba con autorange='reversed'
    active_sorted = disp[disp['Stock'] > 0].sort_values('Days_of_Stock')['Producto'].tolist()
    zero_sorted   = disp[disp['Stock'] == 0]['Producto'].tolist()
    cat_order     = active_sorted + zero_sorted  # con autorange='reversed': último = arriba
    disp = disp.sort_values('Days_of_Stock')  # orden para las trazas
    disp['DoS_clip'] = disp['Days_of_Stock'].clip(upper=500).replace(np.inf, 500)
    disp['DoS_lbl'] = disp['Days_of_Stock'].apply(
        lambda x: '∞' if x == np.inf or x >= 9999 else f'{x:.0f}')

    fig2 = go.Figure()
    # Todos los SKUs agrupados por acción — incluye stock=0 clasificados correctamente
    # (esto alinea los recuentos con Tab 3 Punto de Reorden y Tab 4 Hoja de Compras)
    for st_val, lbl, col in [
        (ST_CRITICAL, 'Pedir ahora',       C_RED),
        (ST_WARNING,  'Pedir esta semana',  C_ORANGE),
        (ST_OK,       'Cobertura correcta', C_GREEN)]:
        sub = disp[disp['Action'] == st_val]
        if not len(sub): continue
        fig2.add_trace(go.Bar(
            y=sub['Producto'], x=sub['DoS_clip'],
            orientation='h', name=lbl, marker_color=col,
            customdata=np.stack([sub['Stock'], sub['Avg_Daily_Sales'],
                                 sub['DoS_lbl'], sub['Reorder_Point']], axis=1),
            hovertemplate=(
                '<b>%{y}</b><br>'
                'Días de stock: %{customdata[2]}<br>'
                'Stock: %{customdata[0]:.0f} u<br>'
                'Venta media/día: %{customdata[1]:.2f} u<br>'
                'Punto de reorden: %{customdata[3]:.0f} u'
                '<extra></extra>')))

    fig2.add_vline(x=LEAD_TIME_DAYS, line_dash='dash', line_color=C_RED,
                   annotation_text=f'Plazo entrega ({LEAD_TIME_DAYS} d)',
                   annotation_position='top right', annotation_font_size=9,
                   annotation_yshift=0)
    fig2.add_vline(x=30, line_dash='dash', line_color=C_AMBER,
                   annotation_text='Alerta 30 d', annotation_font_size=9,
                   annotation_position='top right', annotation_yshift=-18)
    fig2.add_vline(x=90, line_dash='dot', line_color='#78909C',
                   annotation_text='Saludable 90 d', annotation_font_size=9,
                   annotation_position='top right', annotation_yshift=-36)

    styled_fig(fig2,
        height=max(450, 25*len(disp)), hovermode='y unified', barmode='stack',
        legend=dict(orientation='h', y=1.06, x=0.5, xanchor='center',
                    font_size=11, bgcolor='rgba(255,255,255,0.9)',
                    bordercolor='#E0E0E0', borderwidth=1),
        xaxis_title='Días de stock restantes',
        yaxis=dict(autorange='reversed', categoryorder='array',
                   categoryarray=cat_order, tickfont_size=10),
        margin=dict(t=60, b=50, l=10, r=20))
    if sel_product and sel_product in cat_order:
        _sp2_idx = cat_order.index(sel_product)
        fig2.add_shape(type='rect', xref='paper', x0=0, x1=1,
                       yref='y', y0=_sp2_idx - 0.45, y1=_sp2_idx + 0.45,
                       fillcolor='rgba(255,183,0,0.22)',
                       line=dict(color='#F57F17', width=1.5), layer='below')
    st.plotly_chart(fig2, use_container_width=True)

    # Explicaciones de las métricas
    st.markdown("""
<div class="explain-grid">
<div class="explain-item"><strong>Días de stock</strong>
<span>Cuántos días puede cubrir el stock actual al ritmo de venta promedio.
Fórmula: <code>Stock actual / Venta media diaria</code>.</span></div>

<div class="explain-item"><strong>Stock actual</strong>
<span>Unidades físicas disponibles en almacén a fecha de hoy.</span></div>

<div class="explain-item"><strong>Venta media diaria</strong>
<span>Promedio de unidades vendidas por día a consumidores finales,
calculado sobre los últimos """ + str(STATS_WINDOW_DAYS) + """ días (ventana deslizante).</span></div>

<div class="explain-item"><strong>Punto de reorden (ROP)</strong>
<span>Umbral mínimo de stock. Si el inventario cae por debajo,
debe lanzarse un pedido. Incluye demanda durante el plazo de entrega,
stock de seguridad y buffer de revendedores.</span></div>

<div class="explain-item"><strong>Línea roja — Plazo de entrega</strong>
<span>Si un SKU tiene menos días de stock que el plazo de entrega
(""" + str(LEAD_TIME_DAYS) + """ días), el stock se agotará antes de recibir
un nuevo pedido.</span></div>

<div class="explain-item"><strong>Línea naranja — Alerta 30 d</strong>
<span>SKUs con menos de 30 días de stock necesitan un pedido urgente
esta semana para evitar roturas de stock.</span></div>

<div class="explain-item"><strong>SKUs sin stock (gris)</strong>
<span>Productos con 0 unidades en almacén. Su barra es invisible porque
no tienen días de cobertura. Necesitan reposición inmediata si tienen
demanda activa.</span></div>
</div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  3 — PUNTO DE REORDEN
# ══════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### Análisis del Punto de Reorden (ROP)")
    st.caption("El rombo indica el stock actual. Si está a la izquierda de la barra apilada, significa que el stock es inferior al punto de reorden.")

    rop = proc.sort_values(['Below_ROP', 'Reorder_Point'],
                            ascending=[False, False]).copy()

    # Preparar customdata con toda la info para hover unificado
    rop['_rop_total'] = rop['Reorder_Point'].round(0)
    rop['_lt'] = rop['LT_Demand'].fillna(0).round(0)
    rop['_ss'] = rop['Safety_Stock'].fillna(0).round(0)
    rop['_rb'] = rop['Reseller_LT_Buffer'].fillna(0).round(0) if 'Reseller_LT_Buffer' in rop.columns else 0
    rop['_stock'] = rop['Stock'].round(0)
    rop['_status'] = rop['Action']
    rop['_avg'] = rop['Avg_Daily_Sales'].round(2)
    rop['_std'] = rop['Std_Daily_Sales'].round(2) if 'Std_Daily_Sales' in rop.columns else 0

    # Build unified hover text per row (so hovering ANY element shows everything)
    hover_texts = []
    for _, rr in rop.iterrows():
        txt = (f"<b>{rr['Producto']}</b><br>"
               f"Estado: {rr['_status']}<br>"
               f"Stock actual: {rr['_stock']:.0f} u<br>"
               f"Punto de reorden: {rr['_rop_total']:.0f} u<br>"
               f"—————————<br>"
               f"Demanda LT: {rr['_lt']:.0f} u<br>"
               f"Stock seguridad: {rr['_ss']:.0f} u<br>"
               f"Buffer reseller: {rr['_rb']:.0f} u<br>"
               f"—————————<br>"
               f"Venta media: {rr['_avg']:.2f} u/día<br>"
               f"σ diaria ({STATS_WINDOW_DAYS}d): {rr['_std']:.2f} u")
        hover_texts.append(txt)
    rop['_hover'] = hover_texts

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(
        y=rop['Producto'],
        x=rop['_lt'],
        orientation='h', name='Demanda en tránsito',
        marker_color=C_BLUE,
        hoverinfo='skip'))   # hover only on the diamond marker, not on bars
    fig3.add_trace(go.Bar(
        y=rop['Producto'],
        x=rop['_ss'],
        orientation='h', name='Stock de seguridad',
        marker_color=C_ORANGE,
        hoverinfo='skip'))
    if 'Reseller_LT_Buffer' in rop.columns:
        fig3.add_trace(go.Bar(
            y=rop['Producto'],
            x=rop['_rb'],
            orientation='h', name='Buffer revendedor',
            marker_color=C_GREEN,
            hoverinfo='skip'))

    # Rombos — un trace por condición (no un trace por fila)
    for subset, col, nm in [
        (rop[rop['Below_ROP']],  C_RED,    'Stock actual (bajo ROP)'),
        (rop[~rop['Below_ROP']], '#1B5E20', 'Stock actual (sobre ROP)')]:
        if not len(subset): continue
        fig3.add_trace(go.Scatter(
            x=subset['Stock'], y=subset['Producto'],
            mode='markers', name=nm,
            marker=dict(symbol='diamond', size=11, color=col,
                        line=dict(color='white', width=1.5)),
            hovertext=subset['_hover'],
            hovertemplate='%{hovertext}<extra></extra>'))

    styled_fig(fig3,
        barmode='stack',
        height=max(500, 26*len(rop)),
        hovermode='closest',          # ← FIX: evita tooltips girados
        legend=dict(orientation='h', y=1.06, x=0.5, xanchor='center',
                    font_size=11, bgcolor='rgba(255,255,255,0.9)',
                    bordercolor='#E0E0E0', borderwidth=1),
        xaxis=dict(title='Unidades', rangemode='tozero'),
        yaxis=dict(autorange='reversed', tickfont_size=10),
        margin=dict(t=70, b=50, l=10, r=20))
    if sel_product and sel_product in rop['Producto'].values:
        _sp3_idx = rop['Producto'].tolist().index(sel_product)
        fig3.add_shape(type='rect', xref='paper', x0=0, x1=1,
                       yref='y', y0=_sp3_idx - 0.45, y1=_sp3_idx + 0.45,
                       fillcolor='rgba(255,183,0,0.22)',
                       line=dict(color='#F57F17', width=1.5), layer='below')
    st.plotly_chart(fig3, use_container_width=True)

    # ── Detalle del producto seleccionado ───────────────────────────────────
    if sel_product and sel_product in rop['Producto'].values:
        rop_sel = rop[rop['Producto'] == sel_product].iloc[0]
        _status_color = C_RED if rop_sel['Below_ROP'] else '#1B5E20'
        _status_label = ('🔴  PEDIR AHORA — stock por debajo del punto de reorden'
                         if rop_sel['Below_ROP']
                         else '🟢  Stock sobre el punto de reorden')
        st.markdown(f"---\n#### Detalle ROP — {sel_product}")
        st.markdown(
            f"<div style='font-size:14px;font-weight:600;color:{_status_color};margin-bottom:8px'>"
            f"{_status_label}</div>",
            unsafe_allow_html=True)

        fig3d = go.Figure()
        fig3d.add_trace(go.Bar(
            y=[sel_product], x=[rop_sel['_lt']],
            orientation='h', name='Demanda en tránsito',
            marker_color=C_BLUE, hoverinfo='skip'))
        fig3d.add_trace(go.Bar(
            y=[sel_product], x=[rop_sel['_ss']],
            orientation='h', name='Stock de seguridad',
            marker_color=C_ORANGE, hoverinfo='skip'))
        if rop_sel['_rb'] > 0:
            fig3d.add_trace(go.Bar(
                y=[sel_product], x=[rop_sel['_rb']],
                orientation='h', name='Buffer revendedor',
                marker_color=C_GREEN, hoverinfo='skip'))
        _dm_col = C_RED if rop_sel['Below_ROP'] else '#1B5E20'
        _dm_nm  = 'Stock actual (bajo ROP)' if rop_sel['Below_ROP'] else 'Stock actual (sobre ROP)'
        fig3d.add_trace(go.Scatter(
            x=[rop_sel['_stock']], y=[sel_product],
            mode='markers', name=_dm_nm,
            marker=dict(symbol='diamond', size=28, color=_dm_col,
                        line=dict(color='white', width=2.5)),
            hovertext=[rop_sel['_hover']],
            hovertemplate='%{hovertext}<extra></extra>'))
        styled_fig(fig3d,
            barmode='stack', height=180,
            hovermode='closest',
            legend=dict(orientation='h', y=1.6, x=0.5, xanchor='center', font_size=12,
                        bgcolor='rgba(255,255,255,0.9)', bordercolor='#E0E0E0', borderwidth=1),
            xaxis=dict(title='Unidades', rangemode='tozero'),
            yaxis=dict(tickfont_size=14, tickfont=dict(color=_status_color)),
            margin=dict(t=85, b=50, l=10, r=20))
        st.plotly_chart(fig3d, use_container_width=True)

        _rop_total = rop_sel['_lt'] + rop_sel['_ss'] + rop_sel['_rb']
        _avg_row   = proc[proc['Producto'] == sel_product]['Avg_Daily_Sales']
        _avg_val   = float(_avg_row.iloc[0]) if len(_avg_row) else 0.0
        c1d, c2d, c3d, c4d, c5d = st.columns(5)
        c1d.metric("Stock actual", f"{rop_sel['_stock']:.0f} u",
                   delta=f"{rop_sel['_stock'] - _rop_total:+.0f} u vs ROP",
                   delta_color='normal' if rop_sel['_stock'] >= _rop_total else 'inverse')
        c2d.metric("Punto de reorden", f"{_rop_total:.0f} u")
        c3d.metric("Demanda en tránsito", f"{rop_sel['_lt']:.0f} u")
        c4d.metric("Stock de seguridad", f"{rop_sel['_ss']:.0f} u")
        c5d.metric("Venta media / día", f"{_avg_val:.2f} u")

    # Explicaciones detalladas
    st.markdown("""
<div class="explain-grid">
<div class="explain-item"><strong>Demanda en tránsito (barra azul)</strong>
<span>Las unidades que se <b>seguirán vendiendo</b> mientras tu pedido viaja desde el proveedor.
Si pides hoy y el stock llega en """ + str(LEAD_TIME_DAYS) + """ días, durante esos días la tienda
sigue vendiendo. Hay que tener ese stock cubierto antes de pedir.
Fórmula: <code>Venta media diaria × Plazo de entrega (""" + str(LEAD_TIME_DAYS) + """ días)</code>.</span></div>

<div class="explain-item"><strong>Stock de seguridad (barra naranja)</strong>
<span>Colchón extra para absorber imprevistos: picos de demanda o retrasos en la entrega.
Fórmula: <code>Z × σ × √LT</code>, donde Z=""" + f"{Z_SCORE:.2f}" + """ (nivel de servicio 95%),
σ es la desviación estándar de ventas diarias (últimos """ + str(STATS_WINDOW_DAYS) + """ días) y LT el plazo de entrega.</span></div>

<div class="explain-item"><strong>Buffer revendedor (barra verde)</strong>
<span>Reserva adicional para cubrir pedidos de revendedores, que suelen ser
más grandes e irregulares que los de consumidores finales. Se estima con el
método de Croston a partir del histórico de pedidos B2B.</span></div>

<div class="explain-item"><strong>Rombo — Stock actual</strong>
<span><b style="color:#C62828">Rombo rojo</b>: el stock está por debajo del punto de reorden;
hay que pedir ya.<br>
<b style="color:#1B5E20">Rombo verde</b>: el stock supera el ROP; no se requiere acción inmediata.</span></div>

<div class="explain-item"><strong>¿Qué es el Nivel de Servicio (95%)?</strong>
<span>Es la probabilidad de no quedarse sin stock durante el plazo de entrega.
Un 95% significa que en 95 de cada 100 ciclos de reposición, el inventario
será suficiente para cubrir la demanda. Un nivel más alto requiere más
stock de seguridad (y más capital inmovilizado).</span></div>

<div class="explain-item"><strong>Punto de Reorden (ROP)</strong>
<span>Es la suma de las tres barras: <code>Demanda LT + Stock seguridad + Buffer reseller</code>.
Cuando el stock cae por debajo de este umbral, se recomienda lanzar un pedido
de reposición.</span></div>
</div>
    """, unsafe_allow_html=True)

    # Callout sobre SPIRO Disc / Card X
    top_ss = proc.nlargest(3, 'Safety_Stock')[['Producto', 'Safety_Stock', 'Std_Daily_Sales']]
    if len(top_ss):
        items_html = ''.join(
            f"<li><b>{r['Producto']}</b>: σ = {r['Std_Daily_Sales']:.1f} u/día → "
            f"Stock seguridad = {r['Safety_Stock']:.0f} u</li>"
            for _, r in top_ss.iterrows())
        st.markdown(f"""
<div class="warn-box">
<strong>¿Por qué algunos SKUs tienen stock de seguridad tan alto?</strong><br>
Los SKUs con ventas muy irregulares (picos grandes seguidos de días sin ventas)
tienen una desviación estándar (σ) alta, lo que infla el stock de seguridad.<br><br>
En particular, productos como <b>SPIRO Disc</b> y <b>SPIRO Card X</b> reciben
ventas indirectas de la <i>descomposición de packs</i>: cuando se vende un "Protección Estándar Espacios"
o un "Kit de Protección para Bebés", esos packs se descomponen en sus componentes individuales,
generando picos puntuales de decenas de unidades en un solo día.<br><br>
<ul>{items_html}</ul>
Esto es <b>correcto</b> — el stock de seguridad alto protege contra esos picos reales.
Si se quisiera reducir, habría que aumentar la frecuencia de reposición o reducir el
nivel de servicio por debajo del 95%.
</div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  4 — HOJA DE COMPRAS
# ══════════════════════════════════════════════════════════════
with tab4:
    st.markdown("### Hoja de Acciones de Compra")
    st.caption("Resumen con las acciones recomendadas para cada SKU")

    # Diccionario de columnas
    with st.expander("Significado de cada columna", expanded=True):
        st.markdown("""
<div class="explain-grid">
<div class="explain-item"><strong>Producto</strong>
<span>Nombre del SKU o referencia de producto.</span></div>

<div class="explain-item"><strong>Estado</strong>
<span><b style="color:#C62828">PEDIR AHORA</b>: stock por debajo del punto de reorden.
<b style="color:#E65100">PEDIR ESTA SEMANA</b>: menos de 30 días de cobertura.
<b style="color:#2E7D32">COBERTURA OK</b>: sin riesgo a corto plazo.</span></div>

<div class="explain-item"><strong>Stock</strong>
<span>Unidades físicas actualmente en almacén.</span></div>

<div class="explain-item"><strong>Días de Cob.</strong>
<span>Días que el stock actual cubre al ritmo de venta promedio.
<code>Stock / Venta media diaria</code>.</span></div>

<div class="explain-item"><strong>Prev. Cons. 30d</strong>
<span>Previsión Prophet de demanda de consumidores para los próximos 30 días
(suma de las próximas 4 semanas del modelo Prophet). Si el parquet no cubre
el SKU, se usa <code>Venta media diaria × 30</code> como respaldo.</span></div>

<div class="explain-item"><strong>Prev. Reve. 30d</strong>
<span>Previsión de demanda de revendedores para los próximos 30 días,
estimada a partir del histórico B2B.</span></div>

<div class="explain-item"><strong>Prev. Total 30d</strong>
<span>Previsión total combinada (consumidores + revendedores) para los próximos 30 días.</span></div>

<div class="explain-item"><strong>Pto. Reorden</strong>
<span>Umbral mínimo de stock = Demanda LT + Stock seguridad + Buffer reseller.
Si el stock cae por debajo, hay que pedir.</span></div>

<div class="explain-item"><strong>Cant. a Pedir</strong>
<span>Unidades sugeridas para cubrir 60 días de demanda menos el stock disponible.
Solo aplica a SKUs bajo el punto de reorden.</span></div>
</div>
        """, unsafe_allow_html=True)

    cols = ['Producto', 'Action', 'Stock', 'Days_of_Stock',
            'Consumer_Demand_30d', 'Reseller_Demand_30d',
            'Forecast_Demand_30d', 'Reorder_Point', 'Suggested_Order_Qty']
    cols = [c for c in cols if c in proc.columns]
    rename = {
        'Producto': 'Producto', 'Action': 'Estado', 'Stock': 'Stock',
        'Days_of_Stock': 'Días de Cob.',
        'Consumer_Demand_30d': 'Prev. Cons. 30d',
        'Reseller_Demand_30d': 'Prev. Reve. 30d',
        'Forecast_Demand_30d': 'Prev. Total 30d',
        'Reorder_Point': 'Pto. Reorden',
        'Suggested_Order_Qty': 'Cant. a Pedir',
    }

    tbl = proc[cols].copy().rename(columns=rename)
    num_cols = [v for k, v in rename.items()
                if k not in ('Producto', 'Action') and v in tbl.columns]
    for c in num_cols:
        tbl[c] = pd.to_numeric(tbl[c], errors='coerce').round(0).astype('Int64')
    tbl['Días de Cob.'] = tbl['Días de Cob.'].apply(
        lambda x: '∞' if pd.isna(x) or x > 900 else str(x))

    def style_estado(val):
        if val == ST_CRITICAL: return f'background:#FFEBEE;color:{C_RED};font-weight:600'
        if val == ST_WARNING:  return f'background:#FFF3E0;color:{C_ORANGE};font-weight:600'
        return f'background:#E8F5E9;color:{C_GREEN};font-weight:600'

    def style_highlight_row(row):
        if sel_product and row['Producto'] == sel_product:
            return ['background:#FFF9C4;font-weight:700;border-left:3px solid #F57F17'] * len(row)
        return [''] * len(row)

    styled = (tbl.style
              .applymap(style_estado, subset=['Estado'])
              .apply(style_highlight_row, axis=1)
              .format(na_rep='—')
              .set_properties(**{'text-align': 'left'}, subset=['Producto']))
    st.dataframe(styled, use_container_width=True, height=600)

    csv = tbl.to_csv(index=False).encode('utf-8')
    st.download_button("Descargar CSV", csv,
        file_name=f"enertex_compras_{date.today().strftime('%Y%m%d')}.csv",
        mime='text/csv')


# ══════════════════════════════════════════════════════════════
#  5 — FORECAST vs REAL
# ══════════════════════════════════════════════════════════════
with tab5:
    st.markdown("### Forecast vs Real — Prophet con Intervalos de Confianza")

    if has_ml:
        all_skus_with_sales = sorted(daily['Producto'].unique())
        col_a, col_b = st.columns([3, 1])
        with col_a:
            if sel_product and sel_product in all_skus_with_sales:
                st.session_state['fc_sku'] = sel_product
            sel_fc = st.selectbox("Selecciona un producto", all_skus_with_sales,
                                  key='fc_sku')
        with col_b:
            horizon_weeks = st.selectbox(
                "Horizonte de predicción",
                [4, 8, 13, 26], index=2,
                format_func=lambda w: f"{w} sem. (~{w//4} mes{'es' if w//4 != 1 else ''})",
                key='fc_horizon')

        fc_sku, fc_source = get_forecast(sel_fc, daily, forecast_curves, horizon_weeks)

        if fc_source == 'none' or not len(fc_sku):
            st.markdown(
                f'<div class="warn-box">No hay suficiente historial para generar una predicción '
                f'para <b>{sel_fc}</b> (mínimo 8 semanas de ventas).</div>',
                unsafe_allow_html=True)
        else:
            # Build weekly actuals for the overlay
            act_w = (daily[daily['Producto'] == sel_fc]
                     .set_index('Date')['Units']
                     .resample('W-MON').sum().reset_index())
            act_w.columns = ['ds', 'actual']

            # Draw the shared Prophet chart (CI bands + actuals)
            _render_prophet_chart(fc_sku, act_w, height=450)

            st.markdown(
                '<div class="info-box" style="font-size:0.82rem;">'
                '⚠️ La banda de confianza futura (verde) se ensancha con el tiempo: '
                'cuanto más lejos, mayor incertidumbre. Es normal y esperado. '
                'Usa el horizonte corto para decisiones de compra inmediata y '
                'el largo (6 meses) solo para planificación estratégica.'
                '</div>', unsafe_allow_html=True)

        # Tabla de modelos
        if model_comp is not None and len(model_comp) > 0:
            st.markdown("#### Comparación de Modelos (Holdout 8 semanas)")
            vis_cols = ['SKU', 'Prophet_MAE', 'LightGBM_MAE', 'XGBoost_MAE',
                        'Ensemble_MAE', 'Best_Model']
            vis_cols = [c for c in vis_cols if c in model_comp.columns]
            comp_rename = {
                'SKU': 'Producto', 'Prophet_MAE': 'MAE Prophet',
                'LightGBM_MAE': 'MAE LightGBM', 'XGBoost_MAE': 'MAE XGBoost',
                'Ensemble_MAE': 'MAE Ensemble', 'Best_Model': 'Mejor Modelo'}
            st.dataframe(model_comp[vis_cols].rename(columns=comp_rename),
                         use_container_width=True)
            st.caption("🔎 Prophet gana en todos los SKUs — es el modelo que se usa para las predicciones de arriba.")

            # Sesgo del mejor modelo (Prophet en todos los casos)
            if 'Prophet_Bias' in model_comp.columns:
                st.markdown("#### Sesgo del Modelo Usado (Prophet)")
                st.caption("Positivo = sobre-predice · Negativo = sub-predice. "
                           "Solo se muestra Prophet porque gana en todos los SKUs.")

                def bias_colors(vals):
                    return [C_RED if abs(v) > 5 else (C_ORANGE if abs(v) > 2 else C_GREEN)
                            for v in vals]

                fb = go.Figure()
                fb.add_trace(go.Bar(
                    x=model_comp['SKU'],
                    y=model_comp['Prophet_Bias'],
                    name='Prophet (modelo usado)',
                    marker_color=bias_colors(model_comp['Prophet_Bias']),
                    hovertemplate='<b>%{x}</b><br>Sesgo Prophet: %{y:+.1f} u/sem<extra></extra>'))
                fb.add_hline(y=0, line_dash='solid', line_color='black', line_width=0.8)
                styled_fig(fb, height=340, xaxis_tickangle=-35,
                           yaxis_title='Sesgo (u/semana)',
                           margin=dict(t=20, b=100, l=60, r=20))
                st.plotly_chart(fb, use_container_width=True)

                high_bias = model_comp[model_comp['Prophet_Bias'].abs() > 3]
                if len(high_bias):
                    skus_b = ', '.join(high_bias['SKU'].tolist())
                    st.markdown(f"""
<div class="warn-box">
<strong>Sesgo significativo en: {skus_b}</strong><br><br>
<b>¿Por qué ocurre?</b> Prophet aprende tendencia y estacionalidad del pasado. Cuando un
producto tiene picos irregulares (packs, revendedores, promociones), la curva de tendencia
se desvía sistemáticamente.<br><br>
<b>¿Se puede mejorar?</b> Sí: re-entrenar con más datos, añadir regresores externos
(precios, campañas) o segmentar la demanda B2B del B2C antes del modelo. Cuanto más
historial acumule, mayor precisión.
</div>
                    """, unsafe_allow_html=True)

        # Explicaciones
        st.markdown("""
<div class="explain-grid">
<div class="explain-item"><strong>MAE (Error Absoluto Medio)</strong>
<span>Promedio de la diferencia absoluta entre predicción y realidad.
Menor = mejor. Ejemplo: MAE=5 significa que el modelo se equivoca en ~5 u/semana.</span></div>
<div class="explain-item"><strong>Sesgo (Bias)</strong>
<span>Dirección del error. Positivo: sobre-estima (riesgo de sobrestock).
Negativo: sub-estima (riesgo de rotura). Ideal: próximo a 0.</span></div>
<div class="explain-item"><strong>Banda de confianza — histórica (azul)</strong>
<span>El modelo muestra cómo habría predicho en el pasado vs las ventas reales.
Permite evaluar visualmente su precisión histórica.</span></div>
<div class="explain-item"><strong>Banda de confianza — futura (verde)</strong>
<span>Rango donde esperamos que caigan las ventas reales. Se ensancha con el tiempo
(mayor incertidumbre a largo plazo). Usa el horizonte corto para compras inmediatas.</span></div>
</div>
        """, unsafe_allow_html=True)

    else:
        st.markdown("""
<div class="warn-box">
<strong>Curvas de predicción no disponibles desde parquet</strong><br>
Los archivos <code>prophet_curves.parquet</code> y <code>model_comparison.parquet</code>
no se encontraron. Ejecuta el notebook para generarlos.<br><br>
Aun así, puedes seleccionar cualquier SKU y se generará una predicción al vuelo con Prophet.
</div>
        """, unsafe_allow_html=True)

        all_skus_with_sales = sorted(daily['Producto'].unique())
        col_a, col_b = st.columns([3, 1])
        with col_a:
            _fc_def_nml = (all_skus_with_sales.index(sel_product)
                           if sel_product and sel_product in all_skus_with_sales else 0)
            sel_fc = st.selectbox("Selecciona un producto", all_skus_with_sales,
                                  index=_fc_def_nml, key='fc_sku_nml')
        with col_b:
            horizon_weeks = st.selectbox(
                "Horizonte",
                [4, 8, 13, 26], index=2,
                format_func=lambda w: f"{w} sem.",
                key='fc_horizon_nml')

        fc_sku, fc_source = get_forecast(sel_fc, daily, None, horizon_weeks)
        if fc_source == 'none' or not len(fc_sku):
            st.warning(f"No hay suficientes datos para '{sel_fc}'.")
        else:
            # Build weekly actuals and reuse the shared chart helper
            act_w = (daily[daily['Producto'] == sel_fc]
                     .set_index('Date')['Units']
                     .resample('W-MON').sum().reset_index())
            act_w.columns = ['ds', 'actual']
            _render_prophet_chart(fc_sku, act_w, height=420)


# ══════════════════════════════════════════════════════════════
#  6 — COMPARACIÓN INTERANUAL
# ══════════════════════════════════════════════════════════════
with tab6:
    st.markdown("### Comparación Año sobre Año")

    dy = daily.copy()
    dy['Year']  = dy['Date'].dt.year
    dy['Month'] = dy['Date'].dt.month
    years = sorted(dy['Year'].unique())

    if len(years) >= 2:
        c1, c2 = st.columns(2)
        with c1:
            if sel_product:
                st.session_state['yy_scope'] = 'Un SKU específico'
            yy_scope = st.radio("Alcance", ["Todos los SKUs", "Un SKU específico"],
                                horizontal=True, key='yy_scope')
        with c2:
            yy_agg = st.radio("Agregación", ["Semanal", "Mensual"],
                               horizontal=True, key='yy_agg')

        yy_data = dy.copy()
        if yy_scope == "Un SKU específico":
            _yy_list = sorted(dy['Producto'].unique())
            if sel_product and sel_product in _yy_list:
                st.session_state['yy_sku'] = sel_product
            yy_sku = st.selectbox("Producto", _yy_list, key='yy_sku')
            yy_data = yy_data[yy_data['Producto'] == yy_sku]

        colors_yy = [C_GREY, C_BLUE, C_GREEN, C_ORANGE, C_RED]
        fig6 = go.Figure()

        if yy_agg == "Mensual":
            months_es = ['Ene','Feb','Mar','Abr','May','Jun',
                         'Jul','Ago','Sep','Oct','Nov','Dic']
            for i, yr in enumerate(years):
                agg = (yy_data[yy_data['Year'] == yr]
                       .groupby('Month')['Units'].sum().reset_index())
                agg['Label'] = agg['Month'].apply(
                    lambda m: months_es[m-1] if 1 <= m <= 12 else str(m))
                fig6.add_trace(go.Scatter(
                    x=agg['Month'], y=agg['Units'],
                    mode='lines+markers', name=str(yr),
                    line=dict(color=colors_yy[i % len(colors_yy)], width=2.5),
                    marker=dict(size=6),
                    customdata=agg['Label'],
                    hovertemplate=f'{yr}<br>%{{customdata}}: %{{y:.0f}} u<extra></extra>'))
            fig6.update_xaxes(
                tickmode='array', tickvals=list(range(1, 13)),
                ticktext=months_es, title_text='Mes')
        else:
            # ── FIX: eje X numérico 1-52 para alinear semanas ──
            for i, yr in enumerate(years):
                yr_d = yy_data[yy_data['Year'] == yr].copy()
                yr_d['Week'] = yr_d['Date'].dt.isocalendar().week.astype(int)
                agg = yr_d.groupby('Week')['Units'].sum().reset_index()
                fig6.add_trace(go.Scatter(
                    x=agg['Week'], y=agg['Units'],
                    mode='lines+markers', name=str(yr),
                    line=dict(color=colors_yy[i % len(colors_yy)], width=2.5),
                    marker=dict(size=4),
                    hovertemplate=f'{yr}<br>Semana %{{x}}: %{{y:.0f}} u<extra></extra>'))
            fig6.update_xaxes(
                dtick=4, tick0=1, title_text='Semana del año',
                range=[0.5, 53])

        styled_fig(fig6, height=440, hovermode='x unified',
            yaxis_title='Unidades vendidas',
            legend=dict(orientation='h', y=1.08, x=0.5, xanchor='center'),
            margin=dict(t=20, b=60, l=60, r=20))
        st.plotly_chart(fig6, use_container_width=True)

        # Tabla de crecimiento mensual
        if len(years) >= 2 and yy_agg == "Mensual":
            st.markdown("#### Crecimiento Mensual Interanual")
            mt = yy_data.groupby(['Year', 'Month'])['Units'].sum().unstack(level=0, fill_value=0)
            last_yr, prev_yr = years[-1], years[-2]
            if last_yr in mt.columns and prev_yr in mt.columns:
                mt['Variación %'] = np.where(
                    mt[prev_yr] > 0,
                    ((mt[last_yr] - mt[prev_yr]) / mt[prev_yr] * 100), np.nan)
                months_es_full = {i: m for i, m in enumerate(
                    ['Ene','Feb','Mar','Abr','May','Jun',
                     'Jul','Ago','Sep','Oct','Nov','Dic'], 1)}
                mt.index = mt.index.map(lambda x: months_es_full.get(x, x))
                st.dataframe(
                    mt.style.format('{:.0f}').format({'Variación %': '{:+.1f}%'}),
                    use_container_width=True)
    else:
        st.markdown('<div class="info-box">Se necesitan al menos 2 años de datos.</div>',
                    unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  7 — DETALLE POR SKU
# ══════════════════════════════════════════════════════════════
with tab7:
    st.markdown("### Detalle por SKU")

    sku_list = sorted(daily['Producto'].unique())
    if sel_product and sel_product in sku_list:
        st.session_state['detail_sku'] = sel_product
    elif 'detail_sku' not in st.session_state:
        _d_default = ('Stroom Master PRO' if 'Stroom Master PRO' in sku_list else sku_list[0])
        st.session_state['detail_sku'] = _d_default
    sel_sku = st.selectbox("Selecciona un producto", sku_list, key='detail_sku')

    sku_row = proc[proc['Producto'] == sel_sku]
    if len(sku_row):
        r = sku_row.iloc[0]
        dos = r['Days_of_Stock']
        dos_s = f"{dos:.0f} d" if dos < 9000 else "∞"
        ac = action_color(r['Action'])
        st.markdown(f"""
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:0.8rem;margin:0.8rem 0 1.2rem 0;">
            <div class="kpi-card"><p class="kpi-value">{r['Stock']:.0f}</p>
                <p class="kpi-label">Stock actual (u)</p></div>
            <div class="kpi-card"><p class="kpi-value">{dos_s}</p>
                <p class="kpi-label">Días de cobertura</p></div>
            <div class="kpi-card"><p class="kpi-value">{r['Reorder_Point']:.0f}</p>
                <p class="kpi-label">Punto de reorden</p></div>
            <div class="kpi-card" style="border-left-color:{ac};">
                <p class="kpi-value" style="font-size:1rem;">{r['Action']}</p>
                <p class="kpi-label">Estado</p></div>
        </div>
        """, unsafe_allow_html=True)

    sku_w = (daily[daily['Producto'] == sel_sku]
             .set_index('Date')['Units']
             .resample('W-MON').sum().reset_index())
    sku_w.columns = ['Week', 'Units']

    fig7 = go.Figure()
    fig7.add_trace(go.Bar(
        x=sku_w['Week'], y=sku_w['Units'],
        name='Unidades semanales', marker_color='rgba(21,101,192,0.3)',
        hovertemplate='%{x|%d %b %Y}<br>%{y:.0f} u<extra></extra>'))
    if len(sku_w) >= 7:
        sku_w['MA4'] = sku_w['Units'].rolling(4).mean()
        fig7.add_trace(go.Scatter(
            x=sku_w['Week'], y=sku_w['MA4'],
            name='Media móvil 4 semanas', line=dict(color=C_BLUE, width=2.5),
            hovertemplate='MA4: %{y:.1f} u/sem<extra></extra>'))

    styled_fig(fig7, height=380, hovermode='x unified',
        xaxis_title='Semana', yaxis_title='Unidades vendidas',
        legend=dict(orientation='h', y=1.08, x=0.5, xanchor='center'),
        margin=dict(t=20, b=50, l=60, r=20))
    st.plotly_chart(fig7, use_container_width=True)

    with st.expander("Ver datos semanales en tabla"):
        st.dataframe(sku_w.sort_values('Week', ascending=False),
                     use_container_width=True, height=300)

    # Histograma de ventas diarias
    sku_d = daily[daily['Producto'] == sel_sku]
    if len(sku_d) > 10:
        st.markdown("#### Distribución de Ventas Diarias")
        fhist = go.Figure()
        fhist.add_trace(go.Histogram(
            x=sku_d['Units'], nbinsx=30,
            marker_color=C_BLUE, opacity=0.75,
            hovertemplate='%{x:.0f} u → %{y} días<extra></extra>'))
        mean_v = sku_d['Units'].mean()
        fhist.add_vline(x=mean_v, line_dash='dash', line_color=C_RED,
                        annotation_text=f'Media: {mean_v:.1f}', annotation_font_size=11)
        styled_fig(fhist, height=280,
                   xaxis_title='Unidades vendidas por día',
                   yaxis_title='Frecuencia (días)',
                   margin=dict(t=10, b=50, l=60, r=20))
        st.plotly_chart(fhist, use_container_width=True)
