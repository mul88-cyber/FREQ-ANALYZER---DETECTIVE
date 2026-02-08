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
    page_title="Avg Order Volume Anomaly",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🐋 Avg Order Volume (AOV) Anomaly Detector")
st.markdown("""
**Workflow:** 1. Gunakan **Tab Screener** untuk mencari saham yang mengalami anomali Volume per Order.
2. Pindah ke **Tab Deep Dive** untuk memvalidasi chart dan melihat tren historisnya.
""")

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
        
        numeric_cols = ['Close', 'Open Price', 'High', 'Low', 'Volume', 'Frequency', 'Avg_Order_Volume', 'MA30_AOVol', 'Value']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Hitung Value jika 0
        if 'Value' not in df.columns or df['Value'].sum() == 0:
            df['Value'] = df['Close'] * df['Volume'] * 100
            
        return df
    except Exception as e:
        st.error(f"Gagal Load Data: {e}")
        return None

with st.spinner('Sedang menyiapkan data Paus & Semut...'):
    df_raw = load_data()

if df_raw is None:
    st.stop()

# ==============================================================================
# 3. CALCULATE METRICS (GLOBAL)
# ==============================================================================
df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# AOV Ratio: Ratio Lot per Trade hari ini dibanding rata-rata 30 hari
df['AOV_Ratio'] = np.where(df['MA30_AOVol'] > 0, df['Avg_Order_Volume'] / df['MA30_AOVol'], 0)

# ==============================================================================
# 4. SIDEBAR CONTROLS
# ==============================================================================
st.sidebar.header("⚙️ Filter Settings")

# Tanggal & Mode
max_date = df['Last Trading Date'].max()
selected_date = st.sidebar.date_input("Tanggal Analisa", max_date)
selected_date = pd.to_datetime(selected_date)

anomaly_type = st.sidebar.radio(
    "Target Deteksi:",
    ("🐋 Whale Signal (High AOV)", "⚡ Split/Retail Signal (Low AOV)"),
    help="Whale = Akumulasi Kasar (Lot Gede). Split = Distribusi/Akumulasi Senyap (Lot Kecil)."
)

st.sidebar.divider()

# Dynamic Thresholds
if anomaly_type == "🐋 Whale Signal (High AOV)":
    st.sidebar.subheader("Parameter Paus")
    min_ratio = st.sidebar.slider("Min. Lonjakan AOV (x Lipat)", 1.5, 10.0, 2.0, help="Order hari ini harus X kali lebih besar dari rata-rata.")
    min_value = st.sidebar.number_input("Min. Transaksi (Rp)", value=1_000_000_000, step=500_000_000)
else:
    st.sidebar.subheader("Parameter Semut")
    max_ratio = st.sidebar.slider("Max. AOV Ratio (0.x)", 0.1, 0.9, 0.6, help="Order hari ini harus DI BAWAH 0.x kali rata-rata.")
    min_value = st.sidebar.number_input("Min. Transaksi (Rp)", value=500_000_000, step=100_000_000)

# Filter Data Harian (Global Filter)
df_daily = df[df['Last Trading Date'] == selected_date].copy()

# ==============================================================================
# 5. LOGIC SCREENING (Dilakukan di luar Tab agar hasil bisa dipakai di kedua Tab)
# ==============================================================================
if anomaly_type == "🐋 Whale Signal (High AOV)":
    suspects = df_daily[
        (df_daily['AOV_Ratio'] >= min_ratio) & 
        (df_daily['Value'] >= min_value)
    ].sort_values(by='AOV_Ratio', ascending=False)
    
    table_color_map = 'Greens'
    metric_label = "Paus Terdeteksi"
    
else:
    suspects = df_daily[
        (df_daily['AOV_Ratio'] <= max_ratio) & 
        (df_daily['AOV_Ratio'] > 0) & 
        (df_daily['Value'] >= min_value)
    ].sort_values(by='AOV_Ratio', ascending=True) # Urut dari yang paling anjlok
    
    table_color_map = 'Reds_r'
    metric_label = "Split/Retail Terdeteksi"

# ==============================================================================
# 6. TABS LAYOUT
# ==============================================================================
tab1, tab2 = st.tabs(["📋 Screener Results", "📈 Deep Dive & Validation"])

# --- TAB 1: HASIL SCANNER ---
with tab1:
    # Metrics Bar
    c1, c2, c3 = st.columns(3)
    c1.metric("Tanggal Data", selected_date.strftime('%d %b %Y'))
    c2.metric("Total Emiten Discan", len(df_daily))
    c3.metric(metric_label, len(suspects), delta_color="inverse")

    st.subheader(f"Hasil Pencarian: {anomaly_type}")
    
    if not suspects.empty:
        # Menampilkan Tabel
        cols = ['Stock Code', 'Close', 'Change %', 'Avg_Order_Volume', 'MA30_AOVol', 'AOV_Ratio', 'Value']
        
        st.dataframe(
            suspects[cols].style.format({
                'Close': 'Rp {:,.0f}',
                'Change %': '{:.2f}%',
                'Avg_Order_Volume': '{:,.1f} Lot',
                'MA30_AOVol': '{:,.1f} Lot',
                'AOV_Ratio': '{:.2f}x',
                'Value': 'Rp {:,.0f}'
            }).background_gradient(subset=['AOV_Ratio'], cmap=table_color_map),
            use_container_width=True
        )
        st.success(f"💡 **Tip:** Pilih salah satu kode saham di atas, lalu pindah ke tab **'Deep Dive & Validation'** untuk analisa chart.")
    else:
        st.info("Tidak ada saham yang memenuhi kriteria filter saat ini. Coba longgarkan parameter di Sidebar.")

# --- TAB 2: DEEP DIVE CHART ---
with tab2:
    st.header("🔍 Validasi Chart: Price vs Avg Order Volume")
    
    # Smart Dropdown:
    # Jika ada suspect dari Tab 1, jadikan itu prioritas di list pilihan.
    # Jika tidak ada, tampilkan semua saham.
    
    if not suspects.empty:
        stock_options = suspects['Stock Code'].tolist()
        st.info(f"List di bawah otomatis terisi dengan {len(suspects)} saham hasil screener.")
    else:
        stock_options = df['Stock Code'].unique().tolist()
        st.warning("Belum ada hasil screener. Menampilkan semua saham.")

    selected_stock = st.selectbox("Pilih Saham untuk Validasi:", stock_options)

    # Ambil Data Historis (6 Bulan Terakhir)
    df_chart = df[df['Stock Code'] == selected_stock].tail(120).copy()

    if not df_chart.empty:
        # Plotting Chart
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True, 
            vertical_spacing=0.05, row_heights=[0.6, 0.4],
            subplot_titles=(f"Price Action: {selected_stock}", "Avg Order Volume Analysis")
        )

        # 1. Candlestick Price
        fig.add_trace(go.Candlestick(
            x=df_chart['Last Trading Date'],
            open=df_chart['Open Price'], high=df_chart['High'],
            low=df_chart['Low'], close=df_chart['Close'],
            name='Price'
        ), row=1, col=1)

        # 2. Bar Chart AOV
        # Logic Warna:
        # Hijau Terang = Whale (Ratio > 1.5x dari MA30)
        # Merah = Split/Retail (Ratio < 0.6x dari MA30)
        # Abu = Normal
        
        bar_colors = []
        for val, ma in zip(df_chart['Avg_Order_Volume'], df_chart['MA30_AOVol']):
            if ma == 0: ratio = 0
            else: ratio = val / ma
            
            if ratio >= 1.5:
                bar_colors.append('#00cc00') # 🟢 WHALE STRONG
            elif ratio <= 0.6:
                bar_colors.append('#ff4444') # 🔴 SPLIT STRONG
            else:
                bar_colors.append('lightgray') # Normal

        fig.add_trace(go.Bar(
            x=df_chart['Last Trading Date'],
            y=df_chart['Avg_Order_Volume'],
            marker_color=bar_colors,
            name='Lot per Trade',
            hovertemplate='%{y:,.1f} Lot'
        ), row=2, col=1)

        # Garis MA30 (Baseline)
        fig.add_trace(go.Scatter(
            x=df_chart['Last Trading Date'],
            y=df_chart['MA30_AOVol'],
            line=dict(color='blue', width=2),
            name='MA30 (Rata-rata)',
        ), row=2, col=1)

        fig.update_layout(height=700, xaxis_rangeslider_visible=False, hovermode='x unified')
        fig.update_yaxes(title_text="Harga", row=1, col=1)
        fig.update_yaxes(title_text="Lot / Trade", row=2, col=1)

        st.plotly_chart(fig, use_container_width=True)

        # Penjelasan Cara Validasi
        with st.expander("📖 Panduan Membaca Chart Deep Dive"):
            st.markdown("""
            **Validasi Sinyal WHALE (Bar Hijau):**
            * Apakah muncul saat harga *Breakout*? ✅ **Valid Buy**.
            * Apakah muncul saat harga di Pucuk? ⚠️ **Hati-hati Distribusi/Crossing**.

            **Validasi Sinyal SPLIT (Bar Merah):**
            * Apakah muncul saat harga *Sideways* di bawah? ✅ **Valid Akumulasi Senyap**.
            * Apakah muncul saat harga terbang tinggi? ⚠️ **Bahaya (Ritel FOMO)**.
            """)
