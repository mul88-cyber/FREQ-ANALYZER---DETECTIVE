import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
from datetime import datetime, timedelta

# ==============================================================================
# 1. KONFIGURASI HALAMAN & CSS
# ==============================================================================
st.set_page_config(
    page_title="Market Intelligence Dashboard - MA50 Standard",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    .whale-card { background: linear-gradient(135deg, #e6fffa 0%, #b2f5ea 100%); border-left: 5px solid #00cc00; padding: 20px; border-radius: 10px; margin-bottom: 15px; }
    .split-card { background: linear-gradient(135deg, #fff5f5 0%, #fed7d7 100%); border-left: 5px solid #ff4444; padding: 20px; border-radius: 10px; margin-bottom: 15px; }
    .neutral-card { background: linear-gradient(135deg, #f7fafc 0%, #edf2f7 100%); border-left: 5px solid #718096; padding: 20px; border-radius: 10px; margin-bottom: 15px; }
    .bluechip-card { background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); border-left: 5px solid #2962ff; padding: 20px; border-radius: 10px; margin-bottom: 15px; }
    .metric-card { background: white; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05); }
    .big-text { font-size: 24px; font-weight: 800; margin-bottom: 5px; }
    .small-text { font-size: 12px; color: #718096; }
    .value-text { font-size: 20px; font-weight: 700; color: #2d3748; }
    .filter-section { background: #f8fafc; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; margin-bottom: 20px; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; margin-bottom: 20px;'>
    <div style='font-size: 48px;'>🐋</div>
    <div>
        <h1 style='margin: 0; color: #2d3748;'>Market Intelligence Dashboard</h1>
        <p style='margin: 0; color: #718096; font-size: 16px;'>MA50 Standard | Whale & Split Detection</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. LOAD DATA
# ==============================================================================
FOLDER_ID = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
FILE_NAME = 'Kompilasi_Data_1Tahun.csv'

@st.cache_resource
def get_drive_service():
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"❌ Error Auth: {e}")
        return None

@st.cache_data(ttl=1800)
def load_data():
    try:
        service = get_drive_service()
        if not service: return None
        
        query = f"'{FOLDER_ID}' in parents and name='{FILE_NAME}' and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        
        if not files: return None
        
        file_id = files[0]['id']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        
        fh.seek(0)
        df = pd.read_csv(fh)
        
        # Preprocessing
        df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
        
        numeric_cols = ['Close', 'Open Price', 'High', 'Low', 'Volume', 'Frequency', 'Avg_Order_Volume', 
                       'MA50_AOVol', 'Value', 'Change', 'Previous', 'Foreign Buy', 'Foreign Sell', 'Free Float']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'Change %' not in df.columns:
             df['Change %'] = np.where(df['Previous'] > 0, (df['Change'] / df['Previous']) * 100, 0)
        
        if 'Value' not in df.columns or df['Value'].sum() == 0:
            df['Value'] = df['Close'] * df['Volume'] * 100
            
        return df
    except Exception as e:
        st.error(f"Gagal Load Data: {e}")
        return None

with st.spinner('Sedang menyiapkan data pasar...'):
    df_raw = load_data()

if df_raw is None:
    st.stop()

# ==============================================================================
# 3. GLOBAL CALCULATION (MA50 LOGIC)
# ==============================================================================
df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# A. Pastikan MA50 Ada
if 'MA50_AOVol' not in df.columns:
    df['MA50_AOVol'] = df.groupby('Stock Code')['Avg_Order_Volume'].transform(lambda x: x.rolling(50, min_periods=1).mean())

# B. Hitung Ratio Anomali
df['AOV_Ratio'] = np.where(df['MA50_AOVol'] > 0, df['Avg_Order_Volume'] / df['MA50_AOVol'], 0)

# C. Signal Columns
df['Whale_Signal'] = df['AOV_Ratio'] >= 1.5
df['Split_Signal'] = (df['AOV_Ratio'] <= 0.6) & (df['AOV_Ratio'] > 0)

# D. Net Foreign Calc
if 'Foreign Buy' in df.columns and 'Foreign Sell' in df.columns:
    df['Net Foreign'] = df['Foreign Buy'] - df['Foreign Sell']
else:
    df['Net Foreign'] = 0

# E. Value Spike (Money Flow) - Khusus Bluechip
df['MA20_Value'] = df.groupby('Stock Code')['Value'].transform(lambda x: x.rolling(20, min_periods=1).mean())
df['Value_Ratio'] = np.where(df['MA20_Value'] > 0, df['Value'] / df['MA20_Value'], 0)

max_date = df['Last Trading Date'].max()

# ==============================================================================
# 4. DASHBOARD TABS
# ==============================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Deep Dive", 
    "🐋 Whale Screener", 
    "💎 BLUECHIP RADAR",
    "🧪 Research Lab"
])

# ==============================================================================
# TAB 1: DEEP DIVE ANALYSIS (IMPROVED LAYOUT & CUMULATIVE)
# ==============================================================================
with tab1:
    st.markdown("### 📈 Deep Dive Stock Analysis")
    
    # --- A. FILTER SECTION ---
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    c_sel1, c_sel2, c_sel3 = st.columns([2, 1, 1])
    
    with c_sel1:
        all_stocks = sorted(df['Stock Code'].unique().tolist())
        selected_stock = st.selectbox("🔍 Pilih Saham", all_stocks, key="deepdive_stock")
    
    with c_sel2:
        chart_days = st.selectbox("Rentang Chart", [30, 60, 90, 120, 200], index=3, format_func=lambda x: f"{x} Hari")
    
    with c_sel3:
        chart_type = st.radio("Tipe Chart", ["Candle", "Line"], horizontal=True, label_visibility="collapsed")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # --- B. DATA PROCESSING ---
    stock_data = df[df['Stock Code'] == selected_stock].tail(chart_days).copy()
    
    # Cek Data Ada/Tidak
    if not stock_data.empty:
        last_row = stock_data.iloc[-1]
        company_name = last_row.get('Company Name', selected_stock)
        
        # --- C. STATUS CARD (VERDICT) ---
        aov_ratio = last_row.get('AOV_Ratio', 1)
        
        # Hitung Conviction Score (0-100%)
        if aov_ratio >= 1.5:
            conviction_score = min(99, ((aov_ratio - 1.5) / (5 - 1.5)) * 80 + 20)
            card_html = f"""
            <div class="whale-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <div class="big-text">🐋 WHALE DETECTED</div>
                        <div class="small-text">Indikasi Akumulasi Besar (Lot Gede)</div>
                    </div>
                    <div style="text-align: right;">
                        <div class="value-text">Score: {conviction_score:.0f}%</div>
                        <div class="small-text">AOV Ratio: <b>{aov_ratio:.2f}x</b></div>
                    </div>
                </div>
            </div>
            """
        elif aov_ratio <= 0.6 and aov_ratio > 0:
            conviction_score = min(99, ((0.6 - aov_ratio) / 0.6) * 80 + 20)
            card_html = f"""
            <div class="split-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <div class="big-text">⚡ SPLIT / RETAIL</div>
                        <div class="small-text">Indikasi Distribusi atau Akumulasi Pecah Order</div>
                    </div>
                    <div style="text-align: right;">
                        <div class="value-text">Score: {conviction_score:.0f}%</div>
                        <div class="small-text">AOV Ratio: <b>{aov_ratio:.2f}x</b></div>
                    </div>
                </div>
            </div>
            """
        else:
            card_html = f"""
            <div class="neutral-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <div class="big-text">⚖️ NORMAL ACTIVITY</div>
                        <div class="small-text">Pergerakan Volume Wajar (Sesuai Rata-rata)</div>
                    </div>
                    <div style="text-align: right;">
                        <div class="value-text">Neutral</div>
                        <div class="small-text">AOV Ratio: <b>{aov_ratio:.2f}x</b></div>
                    </div>
                </div>
            </div>
            """
        st.markdown(card_html, unsafe_allow_html=True)

        # --- D. KEY METRICS ROW (PROFILE) ---
        m1, m2, m3, m4, m5 = st.columns(5)
        
        with m1:
            curr_price = last_row.get('Close', 0)
            chg_pct = last_row.get('Change %', 0)
            st.metric("Harga Terakhir", f"Rp {curr_price:,.0f}", f"{chg_pct:+.2f}%")
            
        with m2:
            daily_val = last_row.get('Value', 0)
            val_fmt = f"{daily_val/1e9:.1f} M" if daily_val >= 1e9 else f"{daily_val/1e6:.0f} Jt"
            st.metric("Nilai Transaksi", val_fmt, help="Total Value Transaksi Hari Ini")
            
        with m3:
            freq = last_row.get('Frequency', 0)
            st.metric("Frekuensi", f"{freq:,.0f}x", help="Jumlah kali transaksi terjadi")
            
        with m4:
            # Foreign Flow (CUMULATIVE LOGIC)
            cum_foreign = stock_data['Net Foreign'].sum()
            last_day_foreign = last_row.get('Net Foreign', 0)
            
            # Format Angka
            cum_fmt = f"{cum_foreign/1e9:+.1f} M" if abs(cum_foreign) >= 1e9 else f"{cum_foreign/1e6:+.0f} Jt"
            last_fmt = f"{last_day_foreign/1e9:+.1f} M" if abs(last_day_foreign) >= 1e9 else f"{last_day_foreign/1e6:+.0f} Jt"

            st.metric(
                label=f"Asing (Total {chart_days} Hari)", 
                value=cum_fmt, 
                delta=f"{last_fmt} (Hari Ini)", 
                delta_color="normal",
                help="Angka Besar = Total Net Buy/Sell Asing selama periode chart."
            )

        with m5:
            # Free Float
            free_float = last_row.get('Free Float', 0)
            ff_display = f"{free_float:.1f}%" if free_float > 0 else "-"
            label_ff = "Normal"
            if free_float > 0:
                if free_float < 10: label_ff = "⚠️ Kering"
                elif free_float > 40: label_ff = "💧 Liquid"
            
            st.metric("Free Float", ff_display, label_ff, delta_color="off", help="< 10% = Saham Kering.")

        st.divider()

        # --- E. CHARTING SECTION ---
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.6, 0.2, 0.2],
            specs=[[{"secondary_y": False}], [{"secondary_y": False}], [{"secondary_y": False}]]
        )
        
        # 1. Price Chart
        if chart_type == "Candle":
            valid_candle = stock_data[(stock_data['Open Price'] > 0) & (stock_data['High'] > 0)].copy()
            if not valid_candle.empty:
                fig.add_trace(go.Candlestick(
                    x=valid_candle['Last Trading Date'],
                    open=valid_candle['Open Price'], high=valid_candle['High'],
                    low=valid_candle['Low'], close=valid_candle['Close'],
                    name='OHLC', increasing_line_color='#00cc00', decreasing_line_color='#ff4444'
                ), row=1, col=1)
            else:
                fig.add_trace(go.Scatter(x=stock_data['Last Trading Date'], y=stock_data['Close'], mode='lines', line=dict(color='#2962ff'), name='Close'), row=1, col=1)
        else:
            fig.add_trace(go.Scatter(x=stock_data['Last Trading Date'], y=stock_data['Close'], mode='lines', line=dict(color='#2962ff', width=2), name='Close'), row=1, col=1)
        
        # Markers
        if 'Whale_Signal' in stock_data.columns:
            ws = stock_data[stock_data['Whale_Signal']]
            if not ws.empty and 'High' in ws.columns:
                fig.add_trace(go.Scatter(x=ws['Last Trading Date'], y=ws['High']*1.02, mode='markers', marker=dict(symbol='triangle-down', size=12, color='#00cc00', line=dict(width=1, color='black')), name='Whale'), row=1, col=1)
        
        if 'Split_Signal' in stock_data.columns:
            ss = stock_data[stock_data['Split_Signal']]
            if not ss.empty and 'Low' in ss.columns:
                fig.add_trace(go.Scatter(x=ss['Last Trading Date'], y=ss['Low']*0.98, mode='markers', marker=dict(symbol='triangle-up', size=12, color='#ff4444', line=dict(width=1, color='black')), name='Split'), row=1, col=1)

        # 2. Volume Chart
        colors = ['#00cc00' if r >= 1.5 else '#ff4444' if (r <= 0.6 and r > 0) else '#cfd8dc' for r in stock_data['AOV_Ratio']]
        fig.add_trace(go.Bar(x=stock_data['Last Trading Date'], y=stock_data['Volume'], marker_color=colors, name='Volume'), row=2, col=1)
        
        # 3. AOV Ratio Line
        ma_col = 'MA50_AOVol' if 'MA50_AOVol' in stock_data.columns else 'MA30_AOVol'
        ma_vals = stock_data[ma_col].fillna(0).values if ma_col in stock_data.columns else np.zeros(len(stock_data))
        
        fig.add_trace(go.Scatter(
            x=stock_data['Last Trading Date'], y=stock_data['AOV_Ratio'],
            mode='lines', line=dict(color='#9c88ff', width=2), name='AOV Ratio',
            customdata=np.stack((stock_data['Avg_Order_Volume'], ma_vals), axis=-1),
            hovertemplate='Ratio: %{y:.2f}x<br>Avg: %{customdata[0]:.0f}<br>MA: %{customdata[1]:.0f}'
        ), row=3, col=1)
        
        # Ref Lines
        fig.add_hline(y=1.5, line_dash="dash", line_color="green", row=3, col=1)
        fig.add_hline(y=0.6, line_dash="dash", line_color="red", row=3, col=1)

        # Gap Fixing (Anti Ompong)
        dt_all = pd.date_range(start=stock_data['Last Trading Date'].min(), end=stock_data['Last Trading Date'].max())
        dt_obs = [d.strftime("%Y-%m-%d") for d in stock_data['Last Trading Date']]
        dt_breaks = [d.strftime("%Y-%m-%d") for d in dt_all if d.strftime("%Y-%m-%d") not in dt_obs]
        
        fig.update_xaxes(rangebreaks=[dict(values=dt_breaks)])
        fig.update_layout(height=700, margin=dict(l=10, r=10, t=10, b=10), showlegend=False, hovermode="x unified")
        fig.update_yaxes(title_text="Price", row=1, col=1)
        fig.update_yaxes(title_text="Vol", row=2, col=1)
        fig.update_yaxes(title_text="AOV", row=3, col=1)

        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.warning("Data tidak tersedia untuk saham ini.")

# ==============================================================================
# TAB 2: WHALE SCREENER (Lapis 2-3 Focus)
# ==============================================================================
with tab2:
    st.markdown("### 🐋 Whale Screener (Lapis 2 & 3)")
    st.caption("Fokus pada anomali ekstrim (>2x) yang umum terjadi di saham Lapis 2 & 3.")
    
    with st.container():
        scan_mode = st.radio("Metode Scanning:", ("📸 Daily Snapshot", "🗓️ Period Scanner"), horizontal=True)
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            anomaly_type = st.radio("Target:", ("🐋 Whale (High AOV)", "⚡ Split (Low AOV)"))
        with c2:
            if scan_mode == "📸 Daily Snapshot":
                date_val = st.date_input("Tanggal", max_date)
                sel_date = pd.to_datetime(date_val)
            else:
                p_days = st.selectbox("Rentang Waktu", [5, 10, 20, 60], index=1)
                start_scan = max_date - timedelta(days=p_days * 1.5)
        with c3:
            min_val = st.number_input("Min Value (Rp Miliar)", 1_000_000_000, step=500_000_000)

    # Context Filter
    price_cond = st.selectbox("Kondisi Harga:", ["🔍 SEMUA", "💎 HIDDEN GEM (Sideways)", "⚓ BOTTOM FISHING", "🚀 EARLY MOVE"])

    # Data Prep
    if scan_mode == "📸 Daily Snapshot":
        t_df = df[df['Last Trading Date'] == sel_date].copy()
    else:
        t_df = df[df['Last Trading Date'] >= start_scan].copy()

    # Filtering Logic
    if anomaly_type == "🐋 Whale (High AOV)":
        suspects = t_df[(t_df['AOV_Ratio'] >= 2.0) & (t_df['Value'] >= min_val)]
        cmap = 'Greens'
    else:
        suspects = t_df[(t_df['AOV_Ratio'] <= 0.6) & (t_df['AOV_Ratio'] > 0) & (t_df['Value'] >= min_val)]
        cmap = 'Reds_r'

    if not suspects.empty:
        # VWMA Logic
        if 'VWMA_20D' not in suspects.columns:
            suspects['TP'] = (suspects['High'] + suspects['Low'] + suspects['Close']) / 3
            suspects['VP'] = suspects['TP'] * suspects['Volume']
            suspects['VWMA_20D'] = suspects.groupby('Stock Code')['VP'].transform(lambda x: x.rolling(20).sum() / x.rolling(20).sum())

        if price_cond == "💎 HIDDEN GEM (Sideways)": suspects = suspects[suspects['Change %'].between(-2, 2)]
        elif price_cond == "⚓ BOTTOM FISHING": suspects = suspects[(suspects['Close'] < suspects['VWMA_20D']) | (suspects['Change %'] < 0)]
        elif price_cond == "🚀 EARLY MOVE": suspects = suspects[suspects['Change %'].between(0, 4)]

    # Display Result
    if not suspects.empty:
        if scan_mode == "📸 Daily Snapshot":
            suspects = suspects.sort_values('AOV_Ratio', ascending=False)
            st.dataframe(
                suspects[['Stock Code', 'Close', 'Change %', 'Value', 'Avg_Order_Volume', 'AOV_Ratio', 'Frequency']]
                .style.format({'Close': 'Rp {:,.0f}', 'Value': 'Rp {:,.0f}', 'Avg_Order_Volume': '{:,.0f}', 'AOV_Ratio': '{:.2f}x', 'Change %': '{:+.2f}%', 'Frequency': '{:,.0f}'})
                .background_gradient(subset=['AOV_Ratio'], cmap=cmap),
                use_container_width=True
            )
        else:
            summ = suspects.groupby(['Stock Code']).agg(freq=('Last Trading Date','count'), avg_aov=('AOV_Ratio','mean'), last_date=('Last Trading Date','max')).reset_index().sort_values('freq', ascending=False).head(50)
            st.dataframe(summ.style.background_gradient(subset=['freq'], cmap='Blues'), use_container_width=True)
    else:
        st.warning("Tidak ada data.")

# ==============================================================================
# TAB 3: BLUECHIP RADAR (PRO: DUAL MODE + PRICE CONTEXT)
# ==============================================================================
with tab3:
    st.markdown("### 💎 Bluechip Radar (Big Caps Only)")
    st.markdown("""
    <div class="bluechip-card">
        <b>Strategi Bluechip Pro:</b> Melacak arus dana Institusi & Asing pada saham likuid.
        Fitur baru: Bisa scan periode (akumulasi mingguan/bulanan) dan filter kondisi harga.
    </div>
    """, unsafe_allow_html=True)
    
    # --- 1. SETTINGS CONTAINER ---
    with st.container():
        # A. Mode Scanning
        bc_scan_mode = st.radio(
            "Metode Scanning:",
            ("📸 Daily Snapshot (Harian)", "🗓️ Period Scanner (Rentang Waktu)"),
            horizontal=True,
            key="bc_scan_mode"
        )
        st.divider()

        col_bc1, col_bc2, col_bc3 = st.columns(3)
        
        with col_bc1:
            if bc_scan_mode == "📸 Daily Snapshot (Harian)":
                st.markdown("#### 📅 Tanggal Pantau")
                bc_date_val = st.date_input("Pilih Tanggal", max_date, key="bc_date_daily")
                bc_date = pd.to_datetime(bc_date_val)
            else:
                st.markdown("#### ⏳ Rentang Waktu")
                bc_period = st.selectbox("Analisa Data Terakhir:", [5, 10, 20, 60], index=1, format_func=lambda x: f"{x} Hari Kerja", key="bc_period")
                bc_start_date = max_date - timedelta(days=bc_period * 1.5)

        with col_bc2:
            st.markdown("#### 💰 Min. Transaksi (Likuiditas)")
            min_bc_value = st.number_input("Rp (Miliar)", value=20_000_000_000, step=5_000_000_000, format="%d", help="Saring saham kecil.")
        
        with col_bc3:
            st.markdown("#### 🎯 Sensitivitas AOV")
            bc_aov_threshold = st.slider("Min. AOV Ratio", 1.1, 2.0, 1.25, 0.05, key="bc_threshold", help="1.25x sudah cukup signifikan untuk Bluechip.")

    # --- 2. PRICE CONTEXT FILTER ---
    st.markdown("#### 📉 Kondisi Harga (Price Context)")
    bc_price_cond = st.selectbox(
        "Filter Kondisi Harga:",
        [
            "🔍 SEMUA FASE (Tampilkan Semua)",
            "💎 HIDDEN GEM (Sideways/Datar)", 
            "⚓ BOTTOM FISHING (Lagi Turun/Downtrend)",
            "🚀 EARLY MOVE (Baru Mulai Naik)"
        ],
        key="bc_price_cond"
    )

    # --- 3. DATA PREPARATION ---
    if bc_scan_mode == "📸 Daily Snapshot (Harian)":
        df_bc = df[df['Last Trading Date'] == bc_date].copy()
    else:
        df_bc = df[df['Last Trading Date'] >= bc_start_date].copy()

    # --- 4. DATA ENRICHMENT (CEK KOLOM) ---
    # Net Foreign
    if 'Net Foreign' not in df_bc.columns:
        if 'Foreign Buy' in df_bc.columns and 'Foreign Sell' in df_bc.columns:
            df_bc['Net Foreign'] = df_bc['Foreign Buy'] - df_bc['Foreign Sell']
        else:
            df_bc['Net Foreign'] = 0 

    # Value Ratio (Spike Uang Masuk)
    if 'Value_Ratio' not in df_bc.columns:
        df_bc['Value_Ratio'] = 0 

    # --- 5. FILTERING LOGIC ---
    # Filter Utama: Value Besar + AOV agak naik
    bc_suspects = df_bc[
        (df_bc['Value'] >= min_bc_value) & 
        (df_bc['AOV_Ratio'] >= bc_aov_threshold)
    ]

    # Filter Price Context
    if not bc_suspects.empty:
        # VWMA Logic (On the fly check)
        if 'VWMA_20D' not in bc_suspects.columns:
             bc_suspects['TP'] = (bc_suspects['High'] + bc_suspects['Low'] + bc_suspects['Close']) / 3
             bc_suspects['VP'] = bc_suspects['TP'] * bc_suspects['Volume']
             bc_suspects['VWMA_20D'] = bc_suspects.groupby('Stock Code')['VP'].transform(lambda x: x.rolling(20).sum() / x.rolling(20).sum())

        if bc_price_cond == "💎 HIDDEN GEM (Sideways/Datar)":
            bc_suspects = bc_suspects[(bc_suspects['Change %'] >= -2.0) & (bc_suspects['Change %'] <= 2.0)]
        elif bc_price_cond == "⚓ BOTTOM FISHING (Lagi Turun/Downtrend)":
            bc_suspects = bc_suspects[(bc_suspects['Close'] < bc_suspects['VWMA_20D']) | (bc_suspects['Change %'] < 0)]
        elif bc_price_cond == "🚀 EARLY MOVE (Baru Mulai Naik)":
            bc_suspects = bc_suspects[(bc_suspects['Change %'] > 0) & (bc_suspects['Change %'] <= 4.0)]

    # --- 6. DISPLAY RESULTS ---
    if not bc_suspects.empty:
        
        # === A. TAMPILAN HARIAN ===
        if bc_scan_mode == "📸 Daily Snapshot (Harian)":
            bc_suspects = bc_suspects.sort_values(by='Value', ascending=False)
            
            st.success(f"Ditemukan {len(bc_suspects)} Bluechip Potensial (Fase: {bc_price_cond})")
            
            cols_bc = ['Stock Code', 'Close', 'Change %', 'Net Foreign', 'Value', 'Value_Ratio', 'AOV_Ratio', 'Avg_Order_Volume']
            valid_cols = [c for c in cols_bc if c in bc_suspects.columns]
            
            styled_bc = bc_suspects[valid_cols].style
            
            # Highlight Logic
            if 'Net Foreign' in bc_suspects.columns:
                def color_foreign(val):
                    if val > 5_000_000_000: return 'color: #00cc00; font-weight: bold' # Asing Beli > 5M
                    if val < -5_000_000_000: return 'color: #ff4444; font-weight: bold' # Asing Jual > 5M
                    return 'color: gray'
                styled_bc = styled_bc.map(color_foreign, subset=['Net Foreign'])
            
            if 'Value_Ratio' in bc_suspects.columns:
                def color_val(val):
                    if val > 1.5: return 'background-color: #e3f2fd; color: #2962ff; font-weight: bold'
                    return ''
                styled_bc = styled_bc.map(color_val, subset=['Value_Ratio'])

            styled_bc = styled_bc.background_gradient(subset=['AOV_Ratio'], cmap='Blues', vmin=1.0, vmax=2.0)
            
            # Formatting
            format_dict = {'Close': 'Rp {:,.0f}', 'Change %': '{:+.2f}%', 'Value': 'Rp {:,.0f}', 'Avg_Order_Volume': '{:,.0f}', 'Net Foreign': 'Rp {:,.0f}', 'Value_Ratio': '{:.1f}x', 'AOV_Ratio': '{:.2f}x'}
            styled_bc = styled_bc.format({k: v for k, v in format_dict.items() if k in valid_cols})

            st.dataframe(styled_bc, use_container_width=True, hide_index=True)

        # === B. TAMPILAN PERIODE (AGGREGATION) ===
        else:
            st.info(f"📊 Statistik Big Caps selama **{bc_period} hari terakhir**. Mencari akumulasi konsisten.")
            
            # Agregasi Data
            summary = bc_suspects.groupby(['Stock Code', 'Company Name']).agg(
                Freq_Muncul=('Last Trading Date', 'count'),
                Total_Net_Foreign=('Net Foreign', 'sum'),
                Avg_Value=('Value', 'mean'),
                Avg_AOV_Ratio=('AOV_Ratio', 'mean'),
                Last_Close=('Close', 'last'),
                Avg_Change=('Change %', 'mean')
            ).reset_index()
            
            summary = summary.sort_values(by='Total_Net_Foreign', ascending=False).head(50)
            
            c1, c2 = st.columns(2)
            c1.metric("Emiten Terdeteksi", len(summary))
            top_foreign = summary.iloc[0]
            c2.metric(f"Top Foreign Flow ({top_foreign['Stock Code']})", f"Rp {top_foreign['Total_Net_Foreign']/1e9:,.1f} M")

            styled_sum = summary.style
            
            def color_sum_foreign(val):
                if val > 0: return 'color: #00cc00; font-weight: bold'
                return 'color: #ff4444'
            styled_sum = styled_sum.map(color_sum_foreign, subset=['Total_Net_Foreign'])
            
            styled_sum = styled_sum.background_gradient(subset=['Freq_Muncul'], cmap='Blues')
            
            styled_sum = styled_sum.format({
                'Total_Net_Foreign': 'Rp {:,.0f}',
                'Avg_Value': 'Rp {:,.0f}',
                'Avg_AOV_Ratio': '{:.2f}x',
                'Last_Close': 'Rp {:,.0f}',
                'Avg_Change': '{:+.2f}%'
            })
            
            st.dataframe(
                styled_sum,
                use_container_width=True,
                column_config={
                    "Total_Net_Foreign": st.column_config.Column("Total Asing (Net)", help="Total Net Buy/Sell Asing selama periode ini."),
                    "Freq_Muncul": st.column_config.Column("Freq Anomali", help="Berapa hari terdeteksi AOV tinggi."),
                    "Avg_Value": st.column_config.Column("Rata2 Transaksi")
                },
                hide_index=True
            )
            st.caption("💡 **Tips:** Di mode periode, urutan otomatis berdasarkan **Total Net Buy Asing**. Cari saham dengan Asing Hijau Besar tapi Avg Change kecil (Akumulasi).")

    else:
        st.warning(f"Tidak ditemukan Bluechip dengan kriteria: **{bc_price_cond}**.")

# ==============================================================================
# TAB 4: RESEARCH LAB (Backtesting)
# ==============================================================================
with tab4:
    st.markdown("### 🧪 Research Lab: Uji Hipotesis")
    st.markdown("Menguji profitabilitas sinyal MA50 AOV dalam 1 tahun terakhir.")
    
    with st.container():
        col_res1, col_res2, col_res3 = st.columns(3)
        with col_res1:
            test_mode = st.selectbox("Sinyal yang Diuji:", ["Whale (AOV Tinggi)", "Split (AOV Rendah)"])
        with col_res2:
            hold_days = st.multiselect("Periode Simpan (Hari):", [5, 10, 20], default=[5, 10])
        with col_res3:
            min_tx_test = st.number_input("Filter Saham Liquid (Min Rp):", value=500_000_000)

        if st.button("🚀 JALANKAN BACKTEST", type="primary", use_container_width=True):
            with st.spinner("Sedang memproses data historis..."):
                df_test = df.sort_values(['Stock Code', 'Last Trading Date']).copy()
                
                # Definisi Sinyal MA50
                if test_mode == "Whale (AOV Tinggi)":
                    df_test['Signal'] = (df_test['AOV_Ratio'] >= 2.0) & (df_test['Value'] >= min_tx_test)
                else:
                    df_test['Signal'] = (df_test['AOV_Ratio'] <= 0.6) & (df_test['AOV_Ratio'] > 0) & (df_test['Value'] >= min_tx_test)
                
                # Hitung Forward Return
                for d in hold_days:
                    df_test[f'Return_{d}D'] = df_test.groupby('Stock Code')['Close'].transform(lambda x: x.shift(-d) / x - 1)
                
                signals = df_test[df_test['Signal']].copy()
                
                if signals.empty:
                    st.warning("Tidak ditemukan sinyal historis dengan filter ini.")
                else:
                    st.success(f"Ditemukan {len(signals):,} Sinyal Historis!")
                    stats_cols = st.columns(len(hold_days))
                    
                    for idx, d in enumerate(hold_days):
                        col_name = f'Return_{d}D'
                        valid_signals = signals.dropna(subset=[col_name])
                        
                        avg_ret = valid_signals[col_name].mean() * 100
                        win_rate = (valid_signals[col_name] > 0).mean() * 100
                        
                        with stats_cols[idx]:
                            st.markdown(f"#### Simpan {d} Hari")
                            st.metric("Rata-rata Profit", f"{avg_ret:+.2f}%")
                            st.metric("Win Rate (Peluang Naik)", f"{win_rate:.1f}%")
                            
                            fig_hist = px.histogram(valid_signals, x=col_name, nbins=50, title=f"Distribusi Profit {d} Hari",
                                                  labels={col_name: "Return"}, color_discrete_sequence=['#2962ff'])
                            fig_hist.add_vline(x=0, line_dash="dash", line_color="red")
                            st.plotly_chart(fig_hist, use_container_width=True)

                    st.markdown("#### 🏆 Top Gainers (Contoh Sinyal Sukses)")
                    sort_col = f'Return_{hold_days[0]}D'
                    top_signals = signals.dropna(subset=[sort_col]).sort_values(sort_col, ascending=False).head(10)
                    
                    st.dataframe(
                        top_signals[['Last Trading Date', 'Stock Code', 'Close', 'AOV_Ratio'] + [f'Return_{d}D' for d in hold_days]]
                        .style.format({
                            'Last Trading Date': lambda x: x.strftime('%d %b %Y'),
                            'Close': 'Rp {:,.0f}',
                            'AOV_Ratio': '{:.2f}x',
                            **{f'Return_{d}D': '{:+.2f}%' for d in hold_days}
                        }).background_gradient(subset=[f'Return_{d}D' for d in hold_days], cmap='RdYlGn'),
                        use_container_width=True
                    )
