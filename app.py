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
# 1. KONFIGURASI HALAMAN & CSS
# ==============================================================================
st.set_page_config(
    page_title="Bandar Detector - Frequency Analyzer",
    page_icon="🕵️‍♂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS untuk tampilan lebih bersih
st.markdown("""
<style>
    .metric-card {
        background-color: #0e1117;
        border: 1px solid #262730;
        padding: 15px;
        border-radius: 5px;
        color: white;
    }
    div[data-testid="stMetricValue"] {
        font-size: 24px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🕵️‍♂️ Frequency Analyzer: Deteksi 'Kode Morse' Bandar")
st.markdown("""
**Logic Deteksi:** Mencari aktivitas **Split Order** dimana Frekuensi melonjak drastis (Spike), 
namun Volume tidak naik setinggi Frekuensi (Lot per Trade Drop), disertai dengan Value transaksi yang valid.
""")

# ==============================================================================
# 2. KONEKSI GOOGLE DRIVE
# ==============================================================================
FOLDER_ID = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
FILE_NAME = 'Kompilasi_Data_1Tahun.csv'

@st.cache_resource
def get_drive_service():
    """Membuat service Google Drive dari Secrets."""
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"Gagal memuat kredensial: {e}")
        return None

@st.cache_data(ttl=3600)
def load_data_from_drive():
    """Download dan baca CSV dari Google Drive."""
    try:
        service = get_drive_service()
        if not service: return None

        query = f"'{FOLDER_ID}' in parents and name='{FILE_NAME}' and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        if not files:
            st.error(f"File '{FILE_NAME}' tidak ditemukan.")
            return None

        file_id = files[0]['id']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        df = pd.read_csv(fh)
        
        # Cleaning Dasar
        df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
        num_cols = ['Close', 'Open Price', 'High', 'Low', 'Frequency', 'Volume', 'Value', 'Previous', 'Change']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

# Load Data
with st.spinner('Sedang mengintip gudang data Bandar...'):
    df_raw = load_data_from_drive()

if df_raw is None:
    st.stop()

# ==============================================================================
# 3. CORE LOGIC: BANDARMOLOGY CALCULATION
# ==============================================================================
# Logic ini dijalankan on-the-fly agar responsif terhadap update data

df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# A. Moving Average 20 Hari (Baseline)
# Ditambah +1 untuk menghindari pembagian dengan nol
df['MA20_Freq'] = df.groupby('Stock Code')['Frequency'].transform(lambda x: x.rolling(20, min_periods=1).mean())
df['MA20_Vol']  = df.groupby('Stock Code')['Volume'].transform(lambda x: x.rolling(20, min_periods=1).mean())

# B. SPIKE RATIO (Kekuatan Lonjakan)
# Logic: Hari ini / Rata-rata 20 hari
df['Freq_Spike'] = df['Frequency'] / (df['MA20_Freq'] + 1)
df['Vol_Spike']  = df['Volume'] / (df['MA20_Vol'] + 1)

# C. DETEKSI SPLIT ORDER (Anomaly Score)
# Logic: Jika Freq Spike JAUH LEBIH TINGGI dari Vol Spike, berarti order dipecah.
# Rumus: Freq Spike / Vol Spike.
# Contoh: Freq naik 5x, Vol cuma naik 1x -> Score 5.0 (Sangat Mencurigakan)
df['Anomaly_Score'] = np.where(df['Vol_Spike'] > 0.1, df['Freq_Spike'] / (df['Vol_Spike'] + 0.1), 0)

# D. ESTIMASI VALUE (Filter Noise)
# Asumsi Volume dalam lembar atau lot, sesuaikan. Di IDX biasanya Lot (100 lembar).
# Kita gunakan kolom 'Value' jika ada, jika tidak kita hitung manual.
if 'Value' not in df.columns or df['Value'].sum() == 0:
    df['Tx_Value'] = df['Close'] * df['Volume'] * 100 
else:
    df['Tx_Value'] = df['Value']

# E. Lot Per Trade (LPT) - Indikator Tambahan
df['LPT'] = np.where(df['Frequency'] > 0, df['Volume'] / df['Frequency'], 0)

# ==============================================================================
# 4. SIDEBAR & PARAMETER
# ==============================================================================
st.sidebar.header("⚙️ Filter Parameter")

# Tanggal Analisa
max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Pilih Tanggal", max_date)
selected_date = pd.to_datetime(selected_date)

st.sidebar.subheader("Parameter 'Kode Morse'")
# Threshold Freq Spike
min_freq_spike = st.sidebar.slider("1. Min. Frequency Spike (x Rata-rata)", 1.5, 10.0, 2.5, help="Frekuensi hari ini harus X kali lipat dari biasanya.")

# Threshold Anomaly (Split Order)
min_anomaly_score = st.sidebar.slider("2. Min. Split Intensity (Freq vs Vol)", 0.8, 5.0, 1.2, help="Nilai > 1.2 artinya Frekuensi tumbuh lebih cepat dari Volume (Indikasi Split).")

# Threshold Value (Duit)
min_tx_value = st.sidebar.number_input("3. Min. Value Transaksi (Rp)", value=500_000_000, step=100_000_000, help="Filter saham 'gocap' atau sepi peminat.")

# Filter Data Harian
df_daily = df[df['Last Trading Date'] == selected_date].copy()

# ==============================================================================
# 5. DASHBOARD UTAMA
# ==============================================================================

# --- A. METRICS ---
col1, col2, col3 = st.columns(3)
col1.metric("Tanggal Data", selected_date.strftime('%d %B %Y'))
col2.metric("Total Saham", f"{len(df_daily)}")

# SCREENER LOGIC
# 1. Freq Spike Tinggi
# 2. Split Order Terdeteksi (Anomaly Score)
# 3. Value Transaksi Cukup (Bukan noise)
suspects = df_daily[
    (df_daily['Freq_Spike'] >= min_freq_spike) &
    (df_daily['Anomaly_Score'] >= min_anomaly_score) &
    (df_daily['Tx_Value'] >= min_tx_value)
].sort_values(by='Freq_Spike', ascending=False)

col3.metric("🚨 Suspect Ditemukan", f"{len(suspects)} Emiten", delta_color="inverse")

# --- B. TABEL HASIL ---
st.subheader("📋 Daftar Saham Terdeteksi (High Conviction)")

if not suspects.empty:
    display_cols = ['Stock Code', 'Close', 'Change', 'Frequency', 'Freq_Spike', 'Vol_Spike', 'Anomaly_Score', 'Tx_Value']
    
    st.dataframe(
        suspects[display_cols].style.format({
            'Close': 'Rp {:,.0f}',
            'Change': '{:,.0f}',
            'Frequency': '{:,.0f}',
            'Freq_Spike': '{:.1f}x',     # Berapa kali lipat
            'Vol_Spike': '{:.1f}x',
            'Anomaly_Score': '{:.2f} ⭐', # Semakin tinggi semakin valid
            'Tx_Value': 'Rp {:,.0f}'
        }).background_gradient(subset=['Freq_Spike'], cmap='Reds'),
        use_container_width=True
    )
else:
    st.info("Tidak ada anomali frekuensi yang signifikan pada tanggal ini. Coba turunkan threshold di sidebar.")

# --- C. DEEP DIVE CHART ---
st.divider()
st.subheader("📈 Chart Analisis: Price vs Frequency Spike")

# Pilih Saham
stock_list = suspects['Stock Code'].tolist() if not suspects.empty else df['Stock Code'].unique().tolist()
selected_stock = st.selectbox("Pilih Saham:", stock_list)

# Ambil Data Historis Saham Terpilih (90 Hari Terakhir)
df_chart = df[df['Stock Code'] == selected_stock].tail(90).copy()

if not df_chart.empty:
    # BUAT CHART PLOTLY
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        vertical_spacing=0.05, row_heights=[0.7, 0.3],
        subplot_titles=(f"Price Action: {selected_stock}", "Frequency Analyzer (Spike Intensity)")
    )

    # 1. Candlestick Price
    fig.add_trace(go.Candlestick(
        x=df_chart['Last Trading Date'],
        open=df_chart['Open Price'], high=df_chart['High'],
        low=df_chart['Low'], close=df_chart['Close'],
        name='Price'
    ), row=1, col=1)

    # VWMA Overlay
    # Hitung VWMA simple untuk chart line
    df_chart['TP'] = (df_chart['High'] + df_chart['Low'] + df_chart['Close']) / 3
    df_chart['VP'] = df_chart['TP'] * df_chart['Volume']
    df_chart['VWMA'] = df_chart['VP'].rolling(20).sum() / df_chart['Volume'].rolling(20).sum()

    fig.add_trace(go.Scatter(
        x=df_chart['Last Trading Date'], y=df_chart['VWMA'],
        line=dict(color='orange', width=1.5), name='VWMA 20'
    ), row=1, col=1)

    # 2. Logic Warna Bar (Frequency Analyzer)
    # Ini logic kunci agar visualisasinya benar
    bar_colors = []
    hover_texts = []
    
    for idx, row in df_chart.iterrows():
        # Kategori 1: BANDAR SPLIT (Merah)
        # Freq naik tinggi, tapi Vol growth kalah jauh, dan Value valid
        if (row['Freq_Spike'] >= min_freq_spike) and (row['Anomaly_Score'] >= min_anomaly_score) and (row['Tx_Value'] >= min_tx_value):
            color = 'red'
            status = "🚨 BANDAR SPLIT"
        
        # Kategori 2: RETAIL/NEWS FOMO (Oranye)
        # Freq naik tinggi, Vol juga naik tinggi (Anomaly Score rendah ~1)
        elif (row['Freq_Spike'] >= min_freq_spike):
            color = 'orange'
            status = "⚠️ RETAIL / NEWS"
            
        # Kategori 3: NORMAL / NOISE (Abu-abu)
        else:
            color = 'lightgray'
            status = "Normal"
            
        bar_colors.append(color)
        hover_texts.append(
            f"<b>{status}</b><br>" +
            f"Freq Spike: {row['Freq_Spike']:.2f}x<br>" +
            f"Vol Spike: {row['Vol_Spike']:.2f}x<br>" +
            f"Anomaly Score: {row['Anomaly_Score']:.2f}"
        )

    # Plot Bar Chart (Menggunakan Freq_Spike sebagai tinggi bar)
    fig.add_trace(go.Bar(
        x=df_chart['Last Trading Date'],
        y=df_chart['Freq_Spike'], # Plotting rasio lonjakan, bukan raw frequency
        marker_color=bar_colors,
        name='Anomaly Level',
        hovertext=hover_texts,
        hoverinfo="text"
    ), row=2, col=1)

    # Garis Threshold Merah
    fig.add_hline(y=min_freq_spike, line_dash="dot", row=2, col=1, line_color="red", annotation_text="Threshold")
    
    # Layout
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=700,
        showlegend=False,
        hovermode='x unified',
        bargap=0.2,
        title_text=f"Analisis Detil: {selected_stock}"
    )
    
    # Update Y-Axis Title
    fig.update_yaxes(title_text="Harga", row=1, col=1)
    fig.update_yaxes(title_text="Spike Ratio (x)", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)

    # Data Raw Table
    with st.expander("Lihat Data Mentah (Last 5 Days)"):
        cols_raw = ['Last Trading Date', 'Close', 'Frequency', 'Freq_Spike', 'Vol_Spike', 'Anomaly_Score', 'Tx_Value']
        st.dataframe(df_chart[cols_raw].tail(5).sort_values(by='Last Trading Date', ascending=False))
