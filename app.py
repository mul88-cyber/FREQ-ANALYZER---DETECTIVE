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
    page_title="Frequency Analyzer - Pure Volume Logic",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 Frequency Analyzer: Pure Volume Logic")
st.markdown("""
**Logic Anti-Bias Harga:** Menggunakan **Volume (Lot)** sebagai basis analisa untuk menghilangkan distorsi kenaikan harga.
Fokus mendeteksi: **Split Order** (Frekuensi Meledak, tapi Rata-rata Lot per Order mengecil).
""")

# ==============================================================================
# 2. LOAD DATA (Sama seperti sebelumnya)
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
        st.error(f"Error Creds: {e}")
        return None

@st.cache_data(ttl=3600)
def load_data_from_drive():
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
        
        # Cleaning
        df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
        num_cols = ['Close', 'Open Price', 'High', 'Low', 'Frequency', 'Volume', 'Value']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Pastikan Value ada untuk filter noise
        if 'Value' not in df.columns or df['Value'].sum() == 0:
            df['Value'] = df['Close'] * df['Volume'] * 100 
            
        return df
    except Exception as e:
        st.error(f"Error: {e}")
        return None

with st.spinner('Loading Data...'):
    df_raw = load_data_from_drive()

if df_raw is None: st.stop()

# ==============================================================================
# 3. CORE LOGIC: VOLUME BASED (LOT PER TRADE)
# ==============================================================================

df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# A. METRIK UTAMA: Lot Per Trade (LPT)
# Ini murni "Lembar Saham", tidak terpengaruh harga naik/turun.
# Semakin KECIL angka ini dibanding rata-rata, semakin indikasi SPLIT ORDER.
df['LPT'] = np.where(df['Frequency'] > 0, df['Volume'] / df['Frequency'], 0)

# B. BASELINE (Rata-rata 20 Hari)
df['MA20_Freq'] = df.groupby('Stock Code')['Frequency'].transform(lambda x: x.rolling(20, min_periods=1).mean())
df['MA20_Vol'] = df.groupby('Stock Code')['Volume'].transform(lambda x: x.rolling(20, min_periods=1).mean())
df['MA20_LPT']  = df.groupby('Stock Code')['LPT'].transform(lambda x: x.rolling(20, min_periods=1).mean())

# C. DETEKSI ANOMALI (KODE MORSE)
# 1. Frequency Spike Ratio (Seberapa gila kenaikan frekuensinya?)
df['Freq_Spike_Ratio'] = np.where(df['MA20_Freq'] > 0, df['Frequency'] / df['MA20_Freq'], 0)

# 2. Volume Spike Ratio (Seberapa banyak barang yang pindah?)
df['Vol_Spike_Ratio'] = np.where(df['MA20_Vol'] > 0, df['Volume'] / df['MA20_Vol'], 0)

# 3. ANOMALY SCORE (Split Intensity)
# Logic: Frequency Spike / Volume Spike
# Jika Freq naik 10x, tapi Volume cuma naik 1x -> Score 10 (SANGAT ANOMALI).
# Jika Freq naik 10x, Volume naik 10x -> Score 1 (Normal Market Ramai).
# Kita tambah 0.1 di pembagi untuk hindari error division by zero.
df['Anomaly_Score'] = df['Freq_Spike_Ratio'] / (df['Vol_Spike_Ratio'] + 0.01)

# ==============================================================================
# 4. SIDEBAR & PARAMETER
# ==============================================================================
st.sidebar.header("⚙️ Filter Parameter")

max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Pilih Tanggal", max_date)
selected_date = pd.to_datetime(selected_date)

st.sidebar.subheader("Threshold Deteksi")
min_freq_spike = st.sidebar.slider("1. Min. Frequency Spike (x Rata-rata)", 1.5, 10.0, 3.0, help="Frekuensi hari ini harus X kali lipat rata-rata.")
min_anomaly_score = st.sidebar.slider("2. Min. Anomaly Score", 1.0, 5.0, 1.5, help="Nilai > 1.5 artinya Frekuensi tumbuh lebih cepat drpd Volume.")
min_tx_value = st.sidebar.number_input("3. Min. Total Value (Rp)", value=500_000_000, step=100_000_000, help="Filter saham sepi/gocap.")

df_daily = df[df['Last Trading Date'] == selected_date].copy()

# ==============================================================================
# 5. DASHBOARD UTAMA
# ==============================================================================

# SCREEENER
suspects = df_daily[
    (df_daily['Freq_Spike_Ratio'] >= min_freq_spike) &
    (df_daily['Anomaly_Score'] >= min_anomaly_score) &
    (df_daily['Value'] >= min_tx_value)
].sort_values(by='Anomaly_Score', ascending=False)

col1, col2, col3 = st.columns(3)
col1.metric("Tanggal Data", selected_date.strftime('%d %B %Y'))
col2.metric("Total Saham Discan", len(df_daily))
col3.metric("⚡ Suspect Split Order", f"{len(suspects)} Emiten", delta_color="inverse")

# TABEL
st.subheader("📋 Hasil Screener: Pure Volume Analysis")
if not suspects.empty:
    display_cols = ['Stock Code', 'Close', 'Change', 'Frequency', 'Freq_Spike_Ratio', 'Vol_Spike_Ratio', 'Anomaly_Score', 'LPT', 'MA20_LPT']
    
    st.dataframe(
        suspects[display_cols].style.format({
            'Close': 'Rp {:,.0f}',
            'Change': '{:,.0f}',
            'Frequency': '{:,.0f}',
            'Freq_Spike_Ratio': '{:.1f}x',
            'Vol_Spike_Ratio': '{:.1f}x',
            'Anomaly_Score': '{:.2f} ⭐',
            'LPT': '{:,.1f} Lot',     # Lot per Trade hari ini
            'MA20_LPT': '{:,.1f} Lot' # Rata-rata Lot per Trade
        }).background_gradient(subset=['Anomaly_Score'], cmap='Reds'),
        use_container_width=True
    )
    st.caption("💡 **Anomaly Score tinggi** berarti Frekuensi meledak tapi Volume (Lot) tidak meledak setinggi itu. Indikasi order dipecah.")
else:
    st.info("Tidak ada anomali split order yang signifikan pada tanggal ini.")

# CHART VISUALIZATION
st.divider()
stock_list = suspects['Stock Code'].tolist() if not suspects.empty else df['Stock Code'].unique().tolist()
selected_stock = st.selectbox("Pilih Saham untuk Chart:", stock_list)

df_chart = df[df['Stock Code'] == selected_stock].tail(90).copy()

if not df_chart.empty:
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        vertical_spacing=0.05, row_heights=[0.6, 0.4],
        subplot_titles=(f"Price Action: {selected_stock}", "Frequency Analyzer (Volume Based)")
    )

    # 1. Price Chart
    fig.add_trace(go.Candlestick(
        x=df_chart['Last Trading Date'],
        open=df_chart['Open Price'], high=df_chart['High'],
        low=df_chart['Low'], close=df_chart['Close'],
        name='Price'
    ), row=1, col=1)

    # 2. Logic Warna Bar (Frequency Analyzer)
    # Merah = Anomaly Score Tinggi (Split Order)
    # Abu = Normal
    colors = []
    hover_texts = []
    
    for idx, row in df_chart.iterrows():
        is_spike = row['Freq_Spike_Ratio'] >= min_freq_spike
        is_split = row['Anomaly_Score'] >= min_anomaly_score
        is_valid_val = row['Value'] >= min_tx_value
        
        if is_spike and is_split and is_valid_val:
            colors.append('red') # 🚨 MURNI SPLIT ORDER
            status = "🚨 BANDAR SPLIT"
        elif is_spike:
            colors.append('orange') # ⚠️ RAMAI (Volume juga naik)
            status = "⚠️ MARKET RAMAI"
        else:
            colors.append('lightgray')
            status = "Normal"
            
        hover_texts.append(
            f"<b>{status}</b><br>" +
            f"Freq Spike: {row['Freq_Spike_Ratio']:.1f}x<br>" +
            f"Vol Spike: {row['Vol_Spike_Ratio']:.1f}x<br>" +
            f"Lot/Trade: {row['LPT']:.1f} (Avg: {row['MA20_LPT']:.1f})"
        )

    # Plot Bar: Frequency Spike Ratio (Agar visual tetap "Tiang Listrik")
    fig.add_trace(go.Bar(
        x=df_chart['Last Trading Date'],
        y=df_chart['Freq_Spike_Ratio'], # Sumbu Y = Lonjakan Frekuensi
        marker_color=colors,
        name='Freq Activity',
        hovertext=hover_texts,
        hoverinfo="text"
    ), row=2, col=1)

    # Threshold Line
    fig.add_hline(y=min_freq_spike, line_dash="dot", row=2, col=1, line_color="red", annotation_text="Threshold")

    fig.update_layout(height=700, xaxis_rangeslider_visible=False, hovermode='x unified', title_text=f"Analisa Anomali: {selected_stock}")
    fig.update_yaxes(title_text="Harga", row=1, col=1)
    fig.update_yaxes(title_text="Lonjakan Freq (x)", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)
