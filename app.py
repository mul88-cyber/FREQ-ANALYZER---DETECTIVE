import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
from datetime import timedelta

# ==============================================================================
# 1. KONFIGURASI HALAMAN
# ==============================================================================
st.set_page_config(
    page_title="Frequency Analyzer - Bandar Detector",
    page_icon="🕵️‍♂️",
    layout="wide"
)

# Judul & Intro
st.title("🕵️‍♂️ Frequency Analyzer: Deteksi Gerak-Gerik Bandar")
st.markdown("""
Dashboard ini mendeteksi **Anomali Frekuensi** (Kode Morse) di mana terjadi lonjakan aktivitas transaksi 
saat harga saham cenderung *sideways* (datar).
""")

# ==============================================================================
# 2. FUNGSI LOAD DATA (GOOGLE DRIVE)
# ==============================================================================
# ID Folder & Nama File (Sesuai Info Bapak)
FOLDER_ID = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
FILE_NAME = 'Kompilasi_Data_1Tahun.csv'

@st.cache_resource
def get_drive_service():
    """Membuat service Google Drive dari Secrets."""
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=creds)

@st.cache_data(ttl=3600) # Cache data selama 1 jam
def load_data_from_drive():
    """Download dan baca CSV dari Google Drive."""
    try:
        service = get_drive_service()
        # Cari File ID berdasarkan Nama
        query = f"'{FOLDER_ID}' in parents and name='{FILE_NAME}' and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        if not files:
            st.error(f"File '{FILE_NAME}' tidak ditemukan di folder ID '{FOLDER_ID}'.")
            return None

        file_id = files[0]['id']
        
        # Download File
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        df = pd.read_csv(fh)
        
        # Preprocessing Dasar
        df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
        
        # Pastikan kolom numerik aman
        num_cols = ['Close', 'Open Price', 'High', 'Low', 'Frequency', 'Volume', 'Avg_Order_Value']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat load data: {e}")
        return None

# Load Data
with st.spinner('Sedang mengambil data dari Gudang Data (GDrive)...'):
    df_raw = load_data_from_drive()

if df_raw is None:
    st.stop()

# ==============================================================================
# 3. PERHITUNGAN INDIKATOR (REVISI LOGIC BANDARMOLOGY)
# ==============================================================================
df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# A. Hitung Rata-rata 20 Hari (Baseline)
df['MA20_Freq'] = df.groupby('Stock Code')['Frequency'].transform(lambda x: x.rolling(20).mean())
df['MA20_Vol'] = df.groupby('Stock Code')['Volume'].transform(lambda x: x.rolling(20).mean())

# B. Hitung "Spike Ratio" (Hari ini vs Rata-rata)
# Hindari pembagian dengan nol
df['Freq_Spike_Ratio'] = np.where(df['MA20_Freq'] > 0, df['Frequency'] / df['MA20_Freq'], 0)
df['Vol_Spike_Ratio'] = np.where(df['MA20_Vol'] > 0, df['Volume'] / df['MA20_Vol'], 0)

# C. BANDAR SPLIT ORDER DETECTION (THE CORE LOGIC)
# Kita mencari kondisi dimana Frekuensi meledak, tapi Volume "biasa saja" atau naiknya tidak sebanding.
# Rumus: Seberapa jauh pertumbuhan Frekuensi melampaui pertumbuhan Volume?
# Jika nilainya > 1.5 atau 2.0, artinya terjadi Split Order masif.
df['Split_Order_Anomaly'] = np.where(df['Vol_Spike_Ratio'] > 0, df['Freq_Spike_Ratio'] / df['Vol_Spike_Ratio'], 0)

# D. Lot Per Trade (Ukuran Order)
# Bandar split order = Lot per Trade mengecil drastis
df['Lot_Per_Trade'] = np.where(df['Frequency'] > 0, df['Volume'] / df['Frequency'], 0)
df['MA20_LPT'] = df.groupby('Stock Code')['Lot_Per_Trade'].transform(lambda x: x.rolling(20).mean())

# ==============================================================================
# 4. SIDEBAR & FILTERING (SCREENER)
# ==============================================================================
st.sidebar.header("⚙️ Filter Parameter (Smart Money)")

# Filter Tanggal
max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Pilih Tanggal Analisa", max_date)
selected_date = pd.to_datetime(selected_date)

st.sidebar.subheader("Parameter 'Kode Morse'")
# 1. Frekuensi minimal harus naik berapa kali lipat? (Misal 2x lipat rata-rata)
min_freq_spike = st.sidebar.slider("Min. Frequency Spike (x Rata-rata)", 1.5, 10.0, 2.0)

# 2. Anomaly Score: Frekuensi harus tumbuh lebih cepat dari Volume
# Nilai 1.0 = Sebanding. Nilai > 1.2 = Frekuensi "Overheat" dibanding Volume (Split Order)
min_anomaly_score = st.sidebar.slider("Min. Split Order Ratio (Freq Growth / Vol Growth)", 0.8, 5.0, 1.2, help="Nilai > 1.2 indikasi order dipecah kecil-kecil.")

# 3. Filter Saham "Mati"
min_freq_val = st.sidebar.number_input("Min. Total Frequency Harian", value=100)

# Filter Dataframe Sesuai Tanggal
df_daily = df[df['Last Trading Date'] == selected_date].copy()

# --- LOGIC SCREENER BARU ---
suspects = df_daily[
    (df_daily['Freq_Spike_Ratio'] >= min_freq_spike) &      # Frekuensi Meledak
    (df_daily['Split_Order_Anomaly'] >= min_anomaly_score) & # Meledaknya LEBIH TINGGI dari Volume
    (df_daily['Frequency'] >= min_freq_val)                 # Bukan saham mati
].sort_values(by='Split_Order_Anomaly', ascending=False)

# ==============================================================================
# UPDATE VISUAL TABLE
# ==============================================================================
# Di bagian displaying dataframe, tambahkan kolom baru ini agar Bapak bisa analisa
if not suspects.empty:
    st.success(f"Ditemukan {len(suspects)} saham dengan indikasi Split Order!")
    
    display_cols = ['Stock Code', 'Close', 'Change %', 'Frequency', 'Freq_Spike_Ratio', 'Vol_Spike_Ratio', 'Split_Order_Anomaly']
    
    st.dataframe(
        suspects[display_cols].style.format({
            'Close': 'Rp {:,.0f}',
            'Change %': '{:.2f}%',
            'Frequency': '{:,.0f}',
            'Freq_Spike_Ratio': '{:.2f}x',
            'Vol_Spike_Ratio': '{:.2f}x',
            'Split_Order_Anomaly': '{:.2f} ⭐' # Semakin tinggi semakin mencurigakan
        }).background_gradient(subset=['Split_Order_Anomaly'], cmap='Reds'),
        use_container_width=True
    )
else:
    st.warning("Tidak ditemukan anomali frekuensi pada tanggal ini. Coba turunkan threshold.")

# ==============================================================================
# 5. DASHBOARD LAYOUT
# ==============================================================================

# --- BAGIAN A: METRICS ---
col1, col2, col3 = st.columns(3)
col1.metric("Data Terakhir", selected_date.strftime('%d %B %Y'))
col2.metric("Total Emiten Discan", f"{len(df_daily)}")
col3.metric("🚨 Suspect Ditemukan", f"{len(suspects)} Saham")

# --- BAGIAN B: TABEL SCREENER ---
st.subheader("📋 Hasil Screener: Potensi Hidden Gem")
st.caption("Daftar saham dengan lonjakan frekuensi tinggi tapi harga belum bergerak signifikan.")

if not suspects.empty:
    display_cols = ['Stock Code', 'Close', 'Change %', 'Frequency', 'MA20_Freq', 'Freq_Spike_Ratio', 'Avg_Order_Value', 'Sector']
    
    # Formatting tampilan
    st.dataframe(
        suspects[display_cols].style.format({
            'Close': 'Rp {:,.0f}',
            'Change %': '{:.2f}%',
            'Frequency': '{:,.0f}',
            'MA20_Freq': '{:,.0f}',
            'Freq_Spike_Ratio': '{:.2f}x',
            'Avg_Order_Value': 'Rp {:,.0f}'
        }).background_gradient(subset=['Freq_Spike_Ratio'], cmap='Reds'),
        use_container_width=True
    )
else:
    st.info("Tidak ada saham yang memenuhi kriteria 'Kode Morse' pada tanggal ini. Coba turunkan parameter Spike.")

# --- BAGIAN C: ANALISA DALAM (DEEP DIVE CHART) ---
st.divider()
st.subheader("📈 Bedah Chart: Price vs Frequency Analyzer")

# Pilihan Saham (Default ambil dari Top Suspect kalau ada)
default_stock = suspects.iloc[0]['Stock Code'] if not suspects.empty else df['Stock Code'].unique()[0]
selected_stock = st.selectbox("Pilih Saham untuk Dianalisa:", df['Stock Code'].unique(), index=list(df['Stock Code'].unique()).index(default_stock))

# Ambil data historis saham terpilih (misal 3 bulan terakhir)
df_chart = df[df['Stock Code'] == selected_stock].tail(90) # 90 Hari bursa terakhir

# MEMBUAT CHART DENGAN PLOTLY
# Row 1: Candlestick (Harga)
# Row 2: Frequency Bar (Dengan Highlight Spike)

fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                    vertical_spacing=0.05, row_heights=[0.7, 0.3],
                    subplot_titles=(f"Price Action: {selected_stock}", "Frequency Analyzer (Histogram)"))

# 1. Candlestick
fig.add_trace(go.Candlestick(
    x=df_chart['Last Trading Date'],
    open=df_chart['Open Price'], high=df_chart['High'],
    low=df_chart['Low'], close=df_chart['Close'],
    name='Price'
), row=1, col=1)

# Tambah VWMA (Garis Indikator Trend)
fig.add_trace(go.Scatter(
    x=df_chart['Last Trading Date'], y=df_chart['VWMA_20D'],
    line=dict(color='orange', width=1), name='VWMA 20'
), row=1, col=1)

# 2. Frequency Bar (Warna Merah jika Spike > Threshold, Abu jika Normal)
colors = ['red' if r >= min_freq_spike else 'gray' for r in df_chart['Freq_Spike_Ratio']]

fig.add_trace(go.Bar(
    x=df_chart['Last Trading Date'],
    y=df_chart['Frequency'],
    marker_color=colors,
    name='Frequency'
), row=2, col=1)

# Garis Rata-rata Frekuensi
fig.add_trace(go.Scatter(
    x=df_chart['Last Trading Date'], y=df_chart['MA20_Freq'],
    line=dict(color='blue', width=1, dash='dash'), name='Avg Freq (20)'
), row=2, col=1)

# Layout Styling
fig.update_layout(
    xaxis_rangeslider_visible=False,
    height=600,
    showlegend=False,
    margin=dict(l=10, r=10, t=30, b=10),
    hovermode='x unified'
)

st.plotly_chart(fig, use_container_width=True)

# --- BAGIAN D: ANOMALI DATA ---
with st.expander(f"Lihat Data Mentah {selected_stock}"):
    st.dataframe(df_chart.sort_values(by='Last Trading Date', ascending=False))
