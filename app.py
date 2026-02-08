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
# 1. KONFIGURASI HALAMAN
# ==============================================================================
st.set_page_config(
    page_title="Market Intelligence Dashboard - Research Lab",
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
    .metric-card { background: white; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05); }
    .big-text { font-size: 28px; font-weight: 800; margin-bottom: 5px; }
    .medium-text { font-size: 16px; font-weight: 600; margin-bottom: 5px; }
    .small-text { font-size: 12px; color: #718096; }
    .value-text { font-size: 24px; font-weight: 700; color: #2d3748; }
    .filter-section { background: #f8fafc; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; margin-bottom: 20px; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; margin-bottom: 20px;'>
    <div style='font-size: 48px;'>🐋</div>
    <div>
        <h1 style='margin: 0; color: #2d3748;'>Market Intelligence Dashboard</h1>
        <p style='margin: 0; color: #718096; font-size: 16px;'>Advanced Whale Detection & Research Lab</p>
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
        
        numeric_cols = ['Close', 'Open Price', 'High', 'Low', 'Volume', 'Frequency', 'Avg_Order_Volume', 'MA30_AOVol', 'Value', 'Change', 'Previous']
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
# 3. GLOBAL CALCULATION
# ==============================================================================
df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()
df['AOV_Ratio'] = np.where(df['MA30_AOVol'] > 0, df['Avg_Order_Volume'] / df['MA30_AOVol'], 0)

# ==============================================================================
# 4. DASHBOARD TABS
# ==============================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Deep Dive", 
    "🐋 Screener", 
    "📊 Market Overview",
    "🧪 Research Lab"  # TAB BARU!
])

# ==============================================================================
# TAB 1: DEEP DIVE (Chart)
# ==============================================================================
with tab1:
    st.markdown("### 📈 Deep Dive Analysis")
    col1, col2 = st.columns([1, 3])
    with col1:
        all_stocks = sorted(df['Stock Code'].unique().tolist())
        selected_stock = st.selectbox("Pilih Saham", all_stocks)
    
    df_chart = df[df['Stock Code'] == selected_stock].tail(120).copy()
    
    if not df_chart.empty:
        last_row = df_chart.iloc[-1]
        aov_ratio = last_row['AOV_Ratio']
        
        # Status Card
        if aov_ratio >= 1.5:
            st.markdown(f"""<div class="whale-card"><div class="big-text">🐋 WHALE DETECTED</div><div>AOV Ratio: {aov_ratio:.2f}x</div></div>""", unsafe_allow_html=True)
        elif aov_ratio <= 0.6 and aov_ratio > 0:
            st.markdown(f"""<div class="split-card"><div class="big-text">⚡ RETAIL/SPLIT</div><div>AOV Ratio: {aov_ratio:.2f}x</div></div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="neutral-card"><div class="big-text">⚖️ NORMAL</div><div>AOV Ratio: {aov_ratio:.2f}x</div></div>""", unsafe_allow_html=True)

        # Chart
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3])
        
        # Price Line
        fig.add_trace(go.Scatter(x=df_chart['Last Trading Date'], y=df_chart['Close'], mode='lines', line=dict(color='#2962ff', width=2), name='Close'), row=1, col=1)
        
        # Markers
        whales = df_chart[df_chart['AOV_Ratio'] >= 1.5]
        splits = df_chart[(df_chart['AOV_Ratio'] <= 0.6) & (df_chart['AOV_Ratio'] > 0)]
        
        fig.add_trace(go.Scatter(x=whales['Last Trading Date'], y=whales['Close'], mode='markers', marker=dict(symbol='triangle-up', size=12, color='#00cc00', line=dict(width=1, color='black')), name='Whale'), row=1, col=1)
        fig.add_trace(go.Scatter(x=splits['Last Trading Date'], y=splits['Close'], mode='markers', marker=dict(symbol='triangle-down', size=12, color='#ff4444', line=dict(width=1, color='black')), name='Split'), row=1, col=1)
        
        # Volume
        colors = ['#00cc00' if r >= 1.5 else '#ff4444' if (r <= 0.6 and r > 0) else '#cfd8dc' for r in df_chart['AOV_Ratio']]
        fig.add_trace(go.Bar(x=df_chart['Last Trading Date'], y=df_chart['Volume'], marker_color=colors, name='Volume'), row=2, col=1)
        
        fig.update_layout(height=600, showlegend=False, hovermode="x unified", xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)

# ==============================================================================
# TAB 2: SCREENER
# ==============================================================================
with tab2:
    st.markdown("### 🐋 Anomaly Screener")
    col1, col2, col3 = st.columns(3)
    with col1:
        mode = st.radio("Mode", ["Whale (High AOV)", "Split (Low AOV)"])
    with col2:
        date_sel = st.date_input("Tanggal", df['Last Trading Date'].max())
        date_sel = pd.to_datetime(date_sel)
    with col3:
        min_val = st.number_input("Min Value (Rp)", value=1_000_000_000, step=500_000_000)

    df_day = df[df['Last Trading Date'] == date_sel].copy()
    
    if mode == "Whale (High AOV)":
        suspects = df_day[(df_day['AOV_Ratio'] >= 2.0) & (df_day['Value'] >= min_val)].sort_values('AOV_Ratio', ascending=False)
        cmap = 'Greens'
    else:
        suspects = df_day[(df_day['AOV_Ratio'] <= 0.6) & (df_day['AOV_Ratio'] > 0) & (df_day['Value'] >= min_val)].sort_values('AOV_Ratio', ascending=True)
        cmap = 'Reds_r'

    if not suspects.empty:
        st.dataframe(suspects[['Stock Code', 'Close', 'Change %', 'Volume', 'Avg_Order_Volume', 'AOV_Ratio', 'Value']].style.format({'Close': 'Rp {:,.0f}', 'Change %': '{:+.2f}%', 'Volume': '{:,.0f}', 'Avg_Order_Volume': '{:,.0f}', 'AOV_Ratio': '{:.2f}x', 'Value': 'Rp {:,.0f}'}).background_gradient(subset=['AOV_Ratio'], cmap=cmap), use_container_width=True)
    else:
        st.info("Tidak ada saham yang memenuhi kriteria.")

# ==============================================================================
# TAB 3: MARKET OVERVIEW
# ==============================================================================
with tab3:
    st.markdown("### 📊 Market Overview")
    latest_df = df[df['Last Trading Date'] == df['Last Trading Date'].max()]
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Stocks", len(latest_df))
    c2.metric("Whales Detected", len(latest_df[latest_df['AOV_Ratio'] >= 1.5]))
    c3.metric("Retail Detected", len(latest_df[(latest_df['AOV_Ratio'] <= 0.6) & (latest_df['AOV_Ratio'] > 0)]))
    
    if 'Sector' in latest_df.columns:
        sector_counts = latest_df[latest_df['AOV_Ratio'] >= 1.5]['Sector'].value_counts().reset_index()
        sector_counts.columns = ['Sector', 'Whale Count']
        fig = px.bar(sector_counts, x='Sector', y='Whale Count', title="Sektor Paling Banyak Paus Hari Ini", color='Whale Count', color_continuous_scale='Greens')
        st.plotly_chart(fig, use_container_width=True)

# ==============================================================================
# TAB 4: RESEARCH LAB (NEW!)
# ==============================================================================
with tab4:
    st.markdown("### 🧪 Research Lab: Uji Hipotesis")
    st.markdown("""
    **Tujuan:** Menguji apakah saham yang mengalami anomali AOV benar-benar naik di kemudian hari.
    Kami akan melihat data 1 tahun ke belakang, mencari semua sinyal, dan menghitung profitabilitasnya.
    """)
    
    with st.container(border=True):
        col_res1, col_res2, col_res3 = st.columns(3)
        with col_res1:
            test_mode = st.selectbox("Sinyal yang Diuji:", ["Whale (AOV Tinggi)", "Split (AOV Rendah)"])
        with col_res2:
            hold_days = st.multiselect("Periode Simpan (Hari):", [5, 10, 20], default=[5, 10])
        with col_res3:
            min_tx_test = st.number_input("Filter Saham Liquid (Min Rp):", value=500_000_000)

        if st.button("🚀 JALANKAN BACKTEST", type="primary", use_container_width=True):
            
            with st.spinner("Sedang memproses data historis..."):
                # 1. Siapkan Data
                df_test = df.sort_values(['Stock Code', 'Last Trading Date']).copy()
                
                # 2. Definisikan Sinyal
                if test_mode == "Whale (AOV Tinggi)":
                    df_test['Signal'] = (df_test['AOV_Ratio'] >= 2.0) & (df_test['Value'] >= min_tx_test)
                else:
                    df_test['Signal'] = (df_test['AOV_Ratio'] <= 0.6) & (df_test['AOV_Ratio'] > 0) & (df_test['Value'] >= min_tx_test)
                
                # 3. Hitung Return ke Depan (Shift negatif)
                # Shift(-5) artinya mengambil harga 5 hari ke depan
                for d in hold_days:
                    df_test[f'Return_{d}D'] = df_test.groupby('Stock Code')['Close'].transform(lambda x: x.shift(-d) / x - 1)
                
                # 4. Filter Hanya Baris yang Ada Sinyal
                signals = df_test[df_test['Signal']].copy()
                
                if signals.empty:
                    st.warning("Tidak ditemukan sinyal historis dengan filter ini.")
                else:
                    st.success(f"Ditemukan {len(signals):,} Sinyal Historis dalam 1 Tahun Terakhir!")
                    
                    # 5. Tampilkan Statistik
                    stats_cols = st.columns(len(hold_days))
                    
                    for idx, d in enumerate(hold_days):
                        col_name = f'Return_{d}D'
                        # Hapus data NaN (sinyal di hari-hari terakhir yg belum ada data masa depannya)
                        valid_signals = signals.dropna(subset=[col_name])
                        
                        avg_ret = valid_signals[col_name].mean() * 100
                        win_rate = (valid_signals[col_name] > 0).mean() * 100
                        
                        with stats_cols[idx]:
                            st.markdown(f"#### Simpan {d} Hari")
                            st.metric("Rata-rata Profit", f"{avg_ret:+.2f}%")
                            st.metric("Win Rate (Peluang Naik)", f"{win_rate:.1f}%")
                            
                            # Histogram Distribusi
                            fig_hist = px.histogram(valid_signals, x=col_name, nbins=50, title=f"Distribusi Return {d} Hari",
                                                  labels={col_name: "Return"}, color_discrete_sequence=['#2962ff'])
                            # Garis 0%
                            fig_hist.add_vline(x=0, line_dash="dash", line_color="red")
                            st.plotly_chart(fig_hist, use_container_width=True)

                    # 6. Top Performers Table
                    st.markdown("#### 🏆 Contoh Sinyal Terbaik (Top Gainers)")
                    # Ambil contoh dari periode holding pertama
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
