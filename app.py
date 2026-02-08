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

# Custom CSS untuk Status Card
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
        
        # Hitung Change % jika belum ada atau error
        if 'Change %' not in df.columns:
             df['Change %'] = np.where(df['Previous'] > 0, (df['Change'] / df['Previous']) * 100, 0)
        
        # Hitung Value (Rp) jika 0
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

# AOV Ratio: (Avg Lot per Trade Hari Ini) / (Rata-rata 30 Hari)
df['AOV_Ratio'] = np.where(df['MA30_AOVol'] > 0, df['Avg_Order_Volume'] / df['MA30_AOVol'], 0)

# ==============================================================================
# 4. SIDEBAR (HANYA UNTUK SCREENER)
# ==============================================================================
st.sidebar.header("⚙️ Filter Screener")
st.sidebar.caption("Filter ini hanya berlaku untuk Tab 'Screener'. Tab 'Chart' menampilkan semua saham.")

max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Tanggal Analisa", max_date)
selected_date = pd.to_datetime(selected_date)

# Threshold Screener
min_whale_ratio = st.sidebar.slider("Min. Whale Ratio (x Lipat)", 1.5, 10.0, 2.0)
min_value = st.sidebar.number_input("Min. Transaksi Harian (Rp)", value=1_000_000_000, step=500_000_000)

df_daily = df[df['Last Trading Date'] == selected_date].copy()

# ==============================================================================
# 5. TABS LAYOUT
# ==============================================================================
tab1, tab2 = st.tabs(["📈 Deep Dive Chart (All Stocks)", "📋 Screener Anomali"])

# --- TAB 1: CHART ANALYSIS (UTAMA) ---
with tab1:
    # 1. DROPDOWN PILIH SAHAM (SEMUA SAHAM)
    all_stocks = sorted(df['Stock Code'].unique().tolist())
    
    # Fitur pencarian cepat
    col_sel1, col_sel2 = st.columns([1, 3])
    with col_sel1:
        selected_stock = st.selectbox("🔍 Pilih Saham:", all_stocks)
    
    # Ambil Data Saham Terpilih (120 Hari Terakhir)
    df_chart = df[df['Stock Code'] == selected_stock].tail(120).copy()
    
    if not df_chart.empty:
        last_row = df_chart.iloc[-1]
        
        # ======================================================================
        # 2. STATUS & CONVICTION SYSTEM
        # ======================================================================
        # Logic Conviction:
        # Whale = AOV Ratio Tinggi (> 2x) & Value Besar
        # Retail/Split = AOV Ratio Rendah (< 0.6x) & Freq Tinggi
        
        aov_ratio = last_row['AOV_Ratio']
        daily_val = last_row['Value']
        change_pct = last_row['Change %']
        
        # Hitung Skor Conviction (0-100%)
        # Base score dari seberapa ekstrem AOV Ratio-nya
        
        status_html = ""
        
        if aov_ratio >= 1.5: # POTENSI WHALE
            # Rumus Conviction Whale: Semakin besar rasio & value, semakin yakin.
            score = min(100, (aov_ratio / 2.0) * 80) # Max 100
            if daily_val > 5_000_000_000: score += 10 # Tambah 10% jika transaksi > 5M
            score = min(99, score)
            
            status_html = f"""
            <div class="status-card whale">
                <div class="big-text">🐋 DETEKSI PAUS (WHALE)</div>
                <div><b>Conviction Rate: {score:.0f}%</b></div>
                <div class="small-text">
                    • Volume Besar dengan Frekuensi Rendah (Avg Order: {last_row['Avg_Order_Volume']:,.0f} Lot)<br>
                    • AOV Ratio: <b>{aov_ratio:.2f}x</b> dari rata-rata bulanan.<br>
                    • Total Transaksi: Rp {daily_val/1e9:,.1f} Miliar.
                </div>
            </div>
            """
            
        elif aov_ratio <= 0.6 and aov_ratio > 0: # POTENSI SPLIT/RETAIL
            # Rumus Conviction Split
            score = min(100, (0.6 / aov_ratio) * 60)
            if last_row['Frequency'] > last_row['MA30_AOVol']: score += 10 # Freq tinggi
            score = min(99, score)
            
            status_html = f"""
            <div class="status-card retail">
                <div class="big-text">⚡ DETEKSI SPLIT / RETAIL</div>
                <div><b>Conviction Rate: {score:.0f}%</b></div>
                <div class="small-text">
                    • Volume Besar tapi Frekuensi SANGAT TINGGI (Avg Order: {last_row['Avg_Order_Volume']:,.0f} Lot)<br>
                    • Indikasi: Distribusi ke Ritel atau Akumulasi Pecah Order.<br>
                    • AOV Ratio: <b>{aov_ratio:.2f}x</b> (Drop di bawah rata-rata).
                </div>
            </div>
            """
            
        else: # NETRAL
            status_html = f"""
            <div class="status-card neutral">
                <div class="big-text">⚖️ STATUS: NORMAL / NETRAL</div>
                <div class="small-text">
                    Tidak ada anomali volume per order yang signifikan.<br>
                    AOV Ratio: {aov_ratio:.2f}x (Wajar).
                </div>
            </div>
            """
        
        st.markdown(status_html, unsafe_allow_html=True)

        # ======================================================================
        # 3. COMBO CHART (LINE PRICE + VOLUME BAR)
        # ======================================================================
        
        # Buat Subplots (Baris 1: Harga, Baris 2: Volume)
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True, 
            vertical_spacing=0.03, 
            row_heights=[0.7, 0.3],
            specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
        )

        # --- CHART 1: LINE CHART HARGA (CLOSE) ---
        # Warna garis tergantung trend hari terakhir (Hijau jika naik, Merah jika turun)
        line_color = '#00cc00' if change_pct >= 0 else '#ff4444'
        
        fig.add_trace(go.Scatter(
            x=df_chart['Last Trading Date'],
            y=df_chart['Close'],
            mode='lines',
            line=dict(color='#2962ff', width=2), # Biru profesional
            name='Close Price',
            # Custom Hover Data (Price + Change%)
            customdata=np.stack((df_chart['Change %'], df_chart['Open Price'], df_chart['High'], df_chart['Low']), axis=-1),
            hovertemplate='<b>Tanggal</b>: %{x}<br>' +
                          '<b>Close</b>: Rp %{y:,.0f}<br>' +
                          '<b>Change</b>: %{customdata[0]:.2f}%<br>' + # Menampilkan Change%
                          'OHLC: %{customdata[1]:,.0f} - %{customdata[2]:,.0f} - %{customdata[3]:,.0f}<extra></extra>'
        ), row=1, col=1)

        # Tambahkan area fill di bawah garis (opsional, biar cantik)
        fig.add_trace(go.Scatter(
            x=df_chart['Last Trading Date'],
            y=df_chart['Close'],
            fill='tozeroy',
            fillcolor='rgba(41, 98, 255, 0.1)', # Transparan biru
            line=dict(width=0),
            showlegend=False,
            hoverinfo='skip'
        ), row=1, col=1)

        # --- CHART 2: VOLUME BAR ---
        # Warna Bar Volume berdasarkan Status Anomali (Hijau=Whale, Merah=Retail, Abu=Normal)
        vol_colors = []
        for val, ma in zip(df_chart['Avg_Order_Volume'], df_chart['MA30_AOVol']):
            ratio = val / ma if ma > 0 else 0
            if ratio >= 1.5:
                vol_colors.append('#00cc00') # Hijau (Whale)
            elif ratio <= 0.6:
                vol_colors.append('#ff4444') # Merah (Split)
            else:
                vol_colors.append('#cfd8dc') # Abu (Normal)

        fig.add_trace(go.Bar(
            x=df_chart['Last Trading Date'],
            y=df_chart['Volume'],
            marker_color=vol_colors,
            name='Volume',
            customdata=df_chart['Avg_Order_Volume'],
            hovertemplate='<b>Volume</b>: %{y:,.0f} Lembar<br>' +
                          '<b>Avg Lot/Trade</b>: %{customdata:,.0f}<extra></extra>'
        ), row=2, col=1)

        # Layout styling
        fig.update_layout(
            height=600,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            hovermode="x unified", # Tooltip gabungan vertical
            xaxis_rangeslider_visible=False,
            plot_bgcolor='white',
            paper_bgcolor='white',
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title="Harga Saham"),
            yaxis2=dict(showgrid=False, title="Volume")
        )

        st.plotly_chart(fig, use_container_width=True)

# --- TAB 2: SCREENER (LIST ANOMALI) ---
with tab2:
    st.subheader(f"📋 Screener Saham Anomali (Tanggal: {selected_date.strftime('%d %b %Y')})")
    
    # Filter Logic
    suspects = df_daily[
        (df_daily['AOV_Ratio'] >= min_whale_ratio) & 
        (df_daily['Value'] >= min_value)
    ].sort_values(by='AOV_Ratio', ascending=False)
    
    col_met1, col_met2 = st.columns(2)
    col_met1.metric("Total Saham Terdeteksi", len(suspects))
    
    if not suspects.empty:
        st.dataframe(
            suspects[['Stock Code', 'Close', 'Change %', 'Volume', 'Avg_Order_Volume', 'AOV_Ratio', 'Value']].style.format({
                'Close': 'Rp {:,.0f}',
                'Change %': '{:.2f}%',
                'Volume': '{:,.0f}',
                'Avg_Order_Volume': '{:,.1f} Lot',
                'AOV_Ratio': '{:.2f}x',
                'Value': 'Rp {:,.0f}'
            }).background_gradient(subset=['AOV_Ratio'], cmap='Greens'),
            use_container_width=True
        )
    else:
        st.info("Tidak ada saham yang memenuhi kriteria Whale pada tanggal ini.")
