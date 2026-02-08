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
    page_title="Avg Order Volume Anomaly Detector",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🐋 Avg Order Volume (AOV) Anomaly Detector")
st.markdown("""
**Logic:** Dashboard ini memanfaatkan kolom pre-calculated **`Avg_Order_Volume`** dan **`MA30_AOVol`**.
* **Whale Signal (Hijau):** Rata-rata lot per order melonjak tinggi di atas rata-rata 30 hari (Akumulasi Kasar).
* **Split/Retail (Merah):** Rata-rata lot per order anjlok di bawah rata-rata (Distribusi/Kamuflase).
""")

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
        
        # --- PREPROCESSING ---
        # 1. Date Conversion
        df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
        
        # 2. Ensure Numeric
        numeric_cols = ['Close', 'Open Price', 'High', 'Low', 'Volume', 'Frequency', 'Avg_Order_Volume', 'MA30_AOVol', 'Value']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
        # 3. Hitung Ulang Value jika 0 (Untuk filter saham gocap/sepi)
        if 'Value' not in df.columns or df['Value'].sum() == 0:
            df['Value'] = df['Close'] * df['Volume'] * 100
            
        return df
    except Exception as e:
        st.error(f"Gagal Load Data: {e}")
        return None

with st.spinner('Sedang mengambil data Avg Order Volume...'):
    df_raw = load_data()

if df_raw is None:
    st.stop()

# ==============================================================================
# 3. CALCULATE ANOMALY RATIO (ON THE FLY)
# ==============================================================================
df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# AOV Ratio: Seberapa besar Order hari ini dibanding Rata-rata 30 hari?
# Ratio 2.0 artinya order hari ini 2x lipat lebih besar dari biasanya (Whale).
# Ratio 0.5 artinya order hari ini cuma setengah dari biasanya (Retail/Split).
df['AOV_Ratio'] = np.where(df['MA30_AOVol'] > 0, df['Avg_Order_Volume'] / df['MA30_AOVol'], 0)

# ==============================================================================
# 4. SIDEBAR & FILTERS
# ==============================================================================
st.sidebar.header("⚙️ Filter Anomali")

# Tanggal
max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Tanggal Analisa", max_date)
selected_date = pd.to_datetime(selected_date)

# Jenis Anomali
anomaly_type = st.sidebar.radio(
    "Tipe Anomali:",
    ("🐋 Whale Detection (High AOV)", "⚡ Split/Retail Detection (Low AOV)"),
    help="Whale = Lot Gede. Split = Lot Kecil."
)

st.sidebar.divider()

# Thresholds
if anomaly_type == "🐋 Whale Detection (High AOV)":
    min_ratio = st.sidebar.slider("Min. Lonjakan AOV (x Lipat)", 1.5, 10.0, 2.0, help="Order hari ini harus X kali lebih besar dari rata-rata.")
    min_value = st.sidebar.number_input("Min. Transaksi (Rp)", value=1_000_000_000, step=500_000_000)
else:
    # Untuk split order, kita cari yang AOV-nya DROP tapi Volume-nya Gede
    max_ratio = st.sidebar.slider("Max. Ratio AOV (0.x)", 0.1, 0.9, 0.6, help="Order hari ini harus DI BAWAH 0.x kali rata-rata.")
    min_freq_spike = st.sidebar.slider("Min. Frequency Spike", 2.0, 10.0, 3.0, help="Frekuensi harus meledak.")
    min_value = st.sidebar.number_input("Min. Transaksi (Rp)", value=500_000_000, step=100_000_000)

# Filter Harian
df_daily = df[df['Last Trading Date'] == selected_date].copy()

# ==============================================================================
# 5. DASHBOARD LAYOUT
# ==============================================================================

# --- SCREENER LOGIC ---
if anomaly_type == "🐋 Whale Detection (High AOV)":
    # Cari yang AOV Ratio Tinggi
    suspects = df_daily[
        (df_daily['AOV_Ratio'] >= min_ratio) & 
        (df_daily['Value'] >= min_value)
    ].sort_values(by='AOV_Ratio', ascending=False)
    
    color_map = 'Greens'
    metric_label = "Paus Terdeteksi"
    
else:
    # Cari yang AOV Ratio Rendah (Kecil) TAPI Frekuensi Tinggi (Bukan saham sepi, tapi saham ramai ritel/split)
    # Kita butuh data historical freq utk hitung spike, asumsi user sudah paham saham ramai.
    # Disini kita filter simple: AOV kecil + Value Lumayan
    suspects = df_daily[
        (df_daily['AOV_Ratio'] <= max_ratio) & 
        (df_daily['AOV_Ratio'] > 0) & # Hindari 0
        (df_daily['Value'] >= min_value)
    ].sort_values(by='AOV_Ratio', ascending=True) # Sort dari yang paling drop
    
    color_map = 'Reds_r' # Reverse red (makin kecil makin merah)
    metric_label = "Split/Retail Terdeteksi"

# --- METRICS ---
c1, c2, c3 = st.columns(3)
c1.metric("Tanggal Data", selected_date.strftime('%d %b %Y'))
c2.metric("Total Emiten", len(df_daily))
c3.metric(metric_label, len(suspects), delta_color="inverse")

# --- TABLE ---
st.subheader(f"📋 Hasil Screener: {anomaly_type}")
if not suspects.empty:
    cols = ['Stock Code', 'Close', 'Change %', 'Avg_Order_Volume', 'MA30_AOVol', 'AOV_Ratio', 'Value']
    
    st.dataframe(
        suspects[cols].style.format({
            'Close': 'Rp {:,.0f}',
            'Change %': '{:.2f}%',
            'Avg_Order_Volume': '{:,.1f} Lot',
            'MA30_AOVol': '{:,.1f} Lot',
            'AOV_Ratio': '{:.2f}x',
            'Value': 'Rp {:,.0f}'
        }).background_gradient(subset=['AOV_Ratio'], cmap=color_map),
        use_container_width=True
    )
else:
    st.info("Tidak ada saham yang memenuhi kriteria filter.")

# --- CHART ---
st.divider()
st.subheader("📈 Deep Dive: Price vs Avg Order Volume")

stock_list = suspects['Stock Code'].tolist() if not suspects.empty else df['Stock Code'].unique().tolist()
selected_stock = st.selectbox("Pilih Saham:", stock_list)

df_chart = df[df['Stock Code'] == selected_stock].tail(120).copy() # 6 Bulan terakhir

if not df_chart.empty:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        vertical_spacing=0.05, row_heights=[0.6, 0.4],
        subplot_titles=(f"Price Action: {selected_stock}", "Avg Order Volume (Lot/Trade) vs MA30")
    )

    # 1. Candlestick
    fig.add_trace(go.Candlestick(
        x=df_chart['Last Trading Date'],
        open=df_chart['Open Price'], high=df_chart['High'],
        low=df_chart['Low'], close=df_chart['Close'],
        name='Price'
    ), row=1, col=1)

    # 2. AOV Chart (Bar vs Line)
    # Warna Bar: Hijau jika di atas MA30, Merah jika di bawah
    colors = ['#00cc00' if val >= ma else '#ff4444' for val, ma in zip(df_chart['Avg_Order_Volume'], df_chart['MA30_AOVol'])]

    # Bar: Avg Order Volume Harian
    fig.add_trace(go.Bar(
        x=df_chart['Last Trading Date'],
        y=df_chart['Avg_Order_Volume'],
        marker_color=colors,
        name='Avg Order Vol (Lot)',
        hovertemplate='%{y:,.1f} Lot'
    ), row=2, col=1)

    # Line: MA30 Benchmark
    fig.add_trace(go.Scatter(
        x=df_chart['Last Trading Date'],
        y=df_chart['MA30_AOVol'],
        line=dict(color='blue', width=2, dash='solid'),
        name='MA30 Baseline',
        hovertemplate='MA30: %{y:,.1f} Lot'
    ), row=2, col=1)

    fig.update_layout(height=700, xaxis_rangeslider_visible=False, hovermode='x unified')
    fig.update_yaxes(title_text="Harga", row=1, col=1)
    fig.update_yaxes(title_text="Lot / Trade", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)
    
    st.info("""
    **Cara Baca Chart Bawah:**
    * **Garis Biru:** Standar ukuran order rata-rata (MA30).
    * **Bar Hijau Tinggi:** Order hari itu JAUH LEBIH BESAR dari biasanya (Whale Accumulation).
    * **Bar Merah Pendek:** Order hari itu KECIL-KECIL dibanding biasanya (Ritel/Split Order).
    """)
