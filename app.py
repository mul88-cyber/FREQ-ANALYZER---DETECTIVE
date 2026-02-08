import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ==============================================================================
# 1. KONFIGURASI HALAMAN
# ==============================================================================
st.set_page_config(
    page_title="Market Intelligence Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    .status-card {
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #ddd;
        margin-bottom: 20px;
    }
    .whale { background-color: #e6fffa; border-color: #00cc00; color: #006600; }
    .retail { background-color: #fff5f5; border-color: #ff4444; color: #cc0000; }
    .neutral { background-color: #f0f2f6; border-color: #ccc; color: #333; }
    .big-text { font-size: 24px; font-weight: bold; }
    .small-text { font-size: 14px; opacity: 0.8; }
</style>
""", unsafe_allow_html=True)

st.title("📈 Market Intelligence: Price & Volume Analysis")

# ==============================================================================
# 2. LOAD DATA DARI GDRIVE
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
        st.error(f"Error Auth: {e}")
        return None

@st.cache_data(ttl=3600)
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
        
        numeric_cols = ['Close', 'Open Price', 'High', 'Low', 'Volume', 'Frequency', 'Avg_Order_Volume', 'MA30_AOVol', 'Value', 'Change', 'Previous']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Hitung Change %
        if 'Change %' not in df.columns:
             df['Change %'] = np.where(df['Previous'] > 0, (df['Change'] / df['Previous']) * 100, 0)
        
        # Hitung Value (Rp)
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
# 3. GLOBAL CALCULATION
# ==============================================================================
df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# AOV Ratio
df['AOV_Ratio'] = np.where(df['MA30_AOVol'] > 0, df['Avg_Order_Volume'] / df['MA30_AOVol'], 0)

# ==============================================================================
# 4. SIDEBAR (HANYA UNTUK SCREENER)
# ==============================================================================
st.sidebar.header("⚙️ Filter Screener")
st.sidebar.caption("Filter ini hanya berlaku untuk Tab 'Screener'.")

max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Tanggal Analisa", max_date)
selected_date = pd.to_datetime(selected_date)

min_whale_ratio = st.sidebar.slider("Min. Whale Ratio (x Lipat)", 1.5, 10.0, 2.0)
min_value = st.sidebar.number_input("Min. Transaksi Harian (Rp)", value=1_000_000_000, step=500_000_000)

df_daily = df[df['Last Trading Date'] == selected_date].copy()

# ==============================================================================
# 5. TABS LAYOUT
# ==============================================================================
tab1, tab2 = st.tabs(["📈 Deep Dive Chart (All Stocks)", "📋 Screener Anomali"])

# --- TAB 1: CHART ANALYSIS (UTAMA) ---
with tab1:
    all_stocks = sorted(df['Stock Code'].unique().tolist())
    
    col_sel1, col_sel2 = st.columns([1, 3])
    with col_sel1:
        selected_stock = st.selectbox("🔍 Pilih Saham:", all_stocks)
    
    df_chart = df[df['Stock Code'] == selected_stock].tail(120).copy()
    
    if not df_chart.empty:
        last_row = df_chart.iloc[-1]
        
        # ======================================================================
        # 2. STATUS CARD (CONVICTION SYSTEM)
        # ======================================================================
        aov_ratio = last_row['AOV_Ratio']
        daily_val = last_row['Value']
        
        status_html = ""
        if aov_ratio >= 1.5: 
            score = min(99, (aov_ratio / 2.0) * 80)
            status_html = f"""<div class="status-card whale"><div class="big-text">🐋 DETEKSI PAUS (WHALE)</div><div><b>Conviction: {score:.0f}%</b></div><div class="small-text">AOV Ratio: {aov_ratio:.2f}x</div></div>"""
        elif aov_ratio <= 0.6 and aov_ratio > 0: 
            score = min(99, (0.6 / aov_ratio) * 60)
            status_html = f"""<div class="status-card retail"><div class="big-text">⚡ DETEKSI SPLIT / RETAIL</div><div><b>Conviction: {score:.0f}%</b></div><div class="small-text">AOV Ratio: {aov_ratio:.2f}x (Low)</div></div>"""
        else:
            status_html = f"""<div class="status-card neutral"><div class="big-text">⚖️ STATUS: NORMAL</div><div class="small-text">AOV Ratio: {aov_ratio:.2f}x (Wajar)</div></div>"""
        
        st.markdown(status_html, unsafe_allow_html=True)

        # ======================================================================
        # 3. COMBO CHART (LINE + VOLUME + MARKERS)
        # ======================================================================
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3], specs=[[{"secondary_y": False}], [{"secondary_y": False}]])

        # A. LINE CHART (HARGA)
        fig.add_trace(go.Scatter(
            x=df_chart['Last Trading Date'], y=df_chart['Close'], mode='lines', 
            line=dict(color='#2962ff', width=2), name='Close Price',
            customdata=df_chart['Change %'],
            hovertemplate='<b>Close</b>: Rp %{y:,.0f}<br><b>Change</b>: %{customdata:.2f}%<extra></extra>'
        ), row=1, col=1)

        # B. MARKERS (SINYAL PAUS & SPLIT) -> INI YANG BARU
        # Filter Data Whale & Split
        whale_signals = df_chart[df_chart['AOV_Ratio'] >= 1.5]
        split_signals = df_chart[(df_chart['AOV_Ratio'] <= 0.6) & (df_chart['AOV_Ratio'] > 0)]

        # Marker Hijau (Paus)
        fig.add_trace(go.Scatter(
            x=whale_signals['Last Trading Date'], y=whale_signals['Close'],
            mode='markers', marker=dict(symbol='triangle-up', size=12, color='#00cc00', line=dict(width=1, color='black')),
            name='Whale Signal', hovertemplate='<b>WHALE SIGNAL</b><br>Lot Gede Masuk!<extra></extra>'
        ), row=1, col=1)

        # Marker Merah (Split)
        fig.add_trace(go.Scatter(
            x=split_signals['Last Trading Date'], y=split_signals['Close'],
            mode='markers', marker=dict(symbol='triangle-down', size=12, color='#ff4444', line=dict(width=1, color='black')),
            name='Split Signal', hovertemplate='<b>SPLIT/RETAIL</b><br>Lot Kecil Dominan<extra></extra>'
        ), row=1, col=1)

        # C. VOLUME BAR (Color Coded)
        vol_colors = ['#00cc00' if r >= 1.5 else '#ff4444' if (r <= 0.6 and r > 0) else '#cfd8dc' for r in df_chart['AOV_Ratio']]
        
        fig.add_trace(go.Bar(
            x=df_chart['Last Trading Date'], y=df_chart['Volume'],
            marker_color=vol_colors, name='Volume',
            customdata=df_chart['Avg_Order_Volume'],
            hovertemplate='<b>Volume</b>: %{y:,.0f}<br><b>Avg Lot</b>: %{customdata:,.0f}<extra></extra>'
        ), row=2, col=1)

        fig.update_layout(height=600, showlegend=True, hovermode="x unified", xaxis_rangeslider_visible=False, 
                          plot_bgcolor='white', yaxis=dict(showgrid=True, gridcolor='#f0f0f0'), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        
        st.plotly_chart(fig, use_container_width=True)

# --- TAB 2: SCREENER ---
with tab2:
    st.subheader(f"📋 Screener Anomali ({selected_date.strftime('%d %b %Y')})")
    suspects = df_daily[(df_daily['AOV_Ratio'] >= min_whale_ratio) & (df_daily['Value'] >= min_value)].sort_values(by='AOV_Ratio', ascending=False)
    
    col_met1, col_met2 = st.columns(2)
    col_met1.metric("Total Saham Terdeteksi", len(suspects))
    
    if not suspects.empty:
        st.dataframe(suspects[['Stock Code', 'Close', 'Change %', 'Volume', 'Avg_Order_Volume', 'AOV_Ratio', 'Value']].style.format({'Close': 'Rp {:,.0f}', 'Change %': '{:.2f}%', 'Volume': '{:,.0f}', 'Avg_Order_Volume': '{:,.1f} Lot', 'AOV_Ratio': '{:.2f}x', 'Value': 'Rp {:,.0f}'}).background_gradient(subset=['AOV_Ratio'], cmap='Greens'), use_container_width=True)
    else:
        st.info("Tidak ada saham Whale pada tanggal ini.")
