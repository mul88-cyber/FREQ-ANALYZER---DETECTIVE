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
    page_title="Smart Money Detector - Value/Freq Analyzer",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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

st.title("🐋 Freq Analyzer: Value/Freq Anomaly")
st.markdown("""
**Core Logic:** Analisa berbasis **Average Order Value (AOV)**.
Rumus: `Freq Analyzer = Value / Frequency`.
Alat ini mendeteksi perubahan drastis pada nilai rata-rata per transaksi untuk menemukan jejak **Smart Money** (Paus) atau **Split Order**.
""")

# ==============================================================================
# 2. KONEKSI GOOGLE DRIVE
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
        st.error(f"Gagal memuat kredensial: {e}")
        return None

@st.cache_data(ttl=3600)
def load_data_from_drive():
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
        
        # Cleaning & Conversion
        df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
        
        # Pastikan kolom Value ada (kalau tidak ada, estimasi dari Price * Vol * 100)
        num_cols = ['Close', 'Open Price', 'High', 'Low', 'Frequency', 'Volume', 'Value', 'Previous', 'Change']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if 'Value' not in df.columns or df['Value'].sum() == 0:
            df['Value'] = df['Close'] * df['Volume'] * 100 
            
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

with st.spinner('Sedang menghitung Value/Frequency...'):
    df_raw = load_data_from_drive()

if df_raw is None:
    st.stop()

# ==============================================================================
# 3. CORE LOGIC: FREQ ANALYZER (VALUE / FREQUENCY)
# ==============================================================================

df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# --- A. RUMUS UTAMA (Permintaan Bapak) ---
# Freq Analyzer = Value / Frequency
# Kita sebut ini AOV (Average Order Value) agar coding lebih rapi, tapi konsepnya sama.
df['Freq_Analyzer_Val'] = np.where(df['Frequency'] > 0, df['Value'] / df['Frequency'], 0)

# --- B. BASELINE (Rata-rata Historis) ---
# Kita butuh pembanding. Apakah Value/Freq hari ini ANOMALI dibanding 20 hari terakhir?
df['MA20_Freq_Analyzer'] = df.groupby('Stock Code')['Freq_Analyzer_Val'].transform(lambda x: x.rolling(20, min_periods=1).mean())
df['MA20_Freq'] = df.groupby('Stock Code')['Frequency'].transform(lambda x: x.rolling(20, min_periods=1).mean())

# --- C. DETEKSI ANOMALI ---
# 1. WHALE DETECTION (Big Player)
# Kondisi: Value/Freq MELONJAK Tinggi (Ratio > 1).
# Artinya: Ada order-order jumbo yang masuk.
df['Whale_Ratio'] = np.where(df['MA20_Freq_Analyzer'] > 0, df['Freq_Analyzer_Val'] / df['MA20_Freq_Analyzer'], 0)

# 2. SPLIT ORDER DETECTION (Kode Morse)
# Kondisi: Frequency MELONJAK (Spike), TAPI Value/Freq DROP atau Normal.
# Artinya: Transaksi banyak banget, tapi nilainya receh (dipecah).
df['Freq_Spike_Ratio'] = np.where(df['MA20_Freq'] > 0, df['Frequency'] / df['MA20_Freq'], 0)

# ==============================================================================
# 4. SIDEBAR & PARAMETER
# ==============================================================================
st.sidebar.header("⚙️ Parameter Freq Analyzer")

# Tanggal Analisa
max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Pilih Tanggal", max_date)
selected_date = pd.to_datetime(selected_date)

st.sidebar.divider()

# Mode Analisa
analysis_mode = st.sidebar.radio(
    "Pilih Mode Deteksi:",
    ("🐋 Big Player (Paus)", "⚡ Split Order (Kode Morse)"),
    help="Paus = Value/Freq Tinggi. Split Order = Freq Tinggi tapi Value/Freq Kecil."
)

st.sidebar.subheader("Threshold")

if analysis_mode == "🐋 Big Player (Paus)":
    min_val_freq_ratio = st.sidebar.slider("Min. Lonjakan Value/Freq (x Lipat)", 1.5, 10.0, 2.0, help="Order hari ini X kali lebih besar dari biasanya.")
    min_tx_value = st.sidebar.number_input("Min. Total Value (Rp)", value=1_000_000_000, step=500_000_000)
else:
    min_freq_spike = st.sidebar.slider("Min. Freq Spike (x Lipat)", 1.5, 10.0, 3.0, help="Frekuensi transaksi X kali lebih ramai dari biasanya.")
    min_tx_value = st.sidebar.number_input("Min. Total Value (Rp)", value=500_000_000, step=100_000_000)

# Filter Data Harian
df_daily = df[df['Last Trading Date'] == selected_date].copy()

# ==============================================================================
# 5. DASHBOARD UTAMA
# ==============================================================================

# --- A. LOGIC SCREENER ---
if analysis_mode == "🐋 Big Player (Paus)":
    # Cari saham yang Freq Analyzer-nya (Value/Freq) meledak
    suspects = df_daily[
        (df_daily['Whale_Ratio'] >= min_val_freq_ratio) &
        (df_daily['Value'] >= min_tx_value)
    ].sort_values(by='Whale_Ratio', ascending=False)
    
    metric_title = "🐋 Paus Terdeteksi"
    ratio_col = 'Whale_Ratio'
    ratio_label = 'AOV Spike'

else: # Split Order / Kode Morse
    # Cari saham yang Frekuensi Spike, tapi Value/Freq-nya wajar/turun (Split)
    suspects = df_daily[
        (df_daily['Freq_Spike_Ratio'] >= min_freq_spike) &
        (df_daily['Value'] >= min_tx_value)
    ].sort_values(by='Freq_Spike_Ratio', ascending=False)
    
    metric_title = "⚡ Kode Morse Terdeteksi"
    ratio_col = 'Freq_Spike_Ratio'
    ratio_label = 'Freq Spike'

# --- B. METRICS ---
col1, col2, col3 = st.columns(3)
col1.metric("Tanggal Data", selected_date.strftime('%d %B %Y'))
col2.metric("Total Value Transaksi Pasar", f"Rp {df_daily['Value'].sum()/1e9:,.0f} M")
col3.metric(metric_title, f"{len(suspects)} Emiten", delta_color="inverse")

# --- C. TABEL HASIL ---
st.subheader(f"📋 Hasil Analisa: {analysis_mode}")

if not suspects.empty:
    display_cols = ['Stock Code', 'Close', 'Change', 'Frequency', 'Value', 'Freq_Analyzer_Val', 'MA20_Freq_Analyzer', ratio_col]
    
    st.dataframe(
        suspects[display_cols].style.format({
            'Close': 'Rp {:,.0f}',
            'Change': '{:,.0f}',
            'Frequency': '{:,.0f}',
            'Value': 'Rp {:,.0f}',
            'Freq_Analyzer_Val': 'Rp {:,.0f}', # Ini kolom Value/Freq
            'MA20_Freq_Analyzer': 'Rp {:,.0f}',
            ratio_col: '{:.2f}x'
        }).background_gradient(subset=[ratio_col], cmap='Greens' if analysis_mode == "🐋 Big Player (Paus)" else 'Reds'),
        use_container_width=True
    )
    st.caption(f"💡 **Freq_Analyzer_Val** adalah rata-rata Rupiah per 1x transaksi. Jika angka ini jauh di atas **MA20**, artinya Big Player masuk.")
else:
    st.info("Tidak ada anomali yang memenuhi kriteria filter saat ini.")

# --- D. DEEP DIVE CHART ---
st.divider()
st.subheader("📈 Visualisasi Anomali: Value/Freq vs Frequency")

# Pilih Saham
stock_list = suspects['Stock Code'].tolist() if not suspects.empty else df['Stock Code'].unique().tolist()
selected_stock = st.selectbox("Pilih Saham:", stock_list)

# Ambil Data
df_chart = df[df['Stock Code'] == selected_stock].tail(90).copy()

if not df_chart.empty:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        vertical_spacing=0.05, row_heights=[0.6, 0.4],
        subplot_titles=(f"Price Action: {selected_stock}", "Freq Analyzer (Rata-rata Rp per Transaksi)")
    )

    # 1. Candlestick
    fig.add_trace(go.Candlestick(
        x=df_chart['Last Trading Date'],
        open=df_chart['Open Price'], high=df_chart['High'],
        low=df_chart['Low'], close=df_chart['Close'],
        name='Price'
    ), row=1, col=1)

    # 2. Bar Chart Logic: MENAMPILKAN VALUE/FREQUENCY
    # Jika Bapak ingin melihat "Freq Analyzer" sesuai request, maka yang di-plot adalah Value/Freq.
    
    # Warna: Hijau Tua jika AOV > Rata-rata (Whale), Merah jika Freq Spike tinggi (Split)
    colors = []
    for idx, row in df_chart.iterrows():
        val_freq_spike = row['Whale_Ratio'] # Value/Freq dibanding rata-rata
        freq_spike = row['Freq_Spike_Ratio']
        
        if val_freq_spike >= 2.0: # AOV Naik 2x lipat
            colors.append('#00cc00') # 🟢 WHALE (Paus)
        elif freq_spike >= 3.0: # Frekuensi Naik 3x lipat (biasanya AOV turun/kecil)
            colors.append('#ff0000') # 🔴 SPLIT/RETAIL
        else:
            colors.append('lightgray')

    # Plot Bar: Value per Frequency
    fig.add_trace(go.Bar(
        x=df_chart['Last Trading Date'],
        y=df_chart['Freq_Analyzer_Val'], # Sumbu Y adalah Value/Frequency (Rp)
        marker_color=colors,
        name='Value/Freq (AOV)',
        hovertemplate='Tanggal: %{x}<br>AOV: Rp %{y:,.0f}<br>Ratio: %{customdata:.2f}x<extra></extra>',
        customdata=df_chart['Whale_Ratio']
    ), row=2, col=1)

    # Garis Rata-rata AOV (MA20)
    fig.add_trace(go.Scatter(
        x=df_chart['Last Trading Date'], y=df_chart['MA20_Freq_Analyzer'],
        line=dict(color='blue', width=1, dash='dash'), name='Avg AOV (20)'
    ), row=2, col=1)

    fig.update_layout(height=700, xaxis_rangeslider_visible=False, hovermode='x unified', title_text=f"Analisa Value/Frequency: {selected_stock}")
    fig.update_yaxes(title_text="Harga Saham", row=1, col=1)
    fig.update_yaxes(title_text="Rp per Transaksi", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("""
    **Cara Baca Chart Bawah:**
    * **Batang Hijau (Tinggi):** Rata-rata nilai transaksi (AOV) melonjak. Indikasi **Paus Masuk** (Beli barang jumlah besar dalam sedikit order).
    * **Batang Merah (Pendek/Banyak):** Terjadi lonjakan frekuensi tapi nilai per transaksinya kecil/wajar. Indikasi **Split Order** atau Ritel FOMO.
    * **Garis Putus Biru:** Rata-rata normal nilai transaksi saham ini.
    """)
