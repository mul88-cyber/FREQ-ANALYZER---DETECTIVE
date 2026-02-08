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
    .metric-card { background: white; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05); }
    .big-text { font-size: 24px; font-weight: 800; margin-bottom: 5px; }
    .small-text { font-size: 12px; color: #718096; }
    .value-text { font-size: 20px; font-weight: 700; color: #2d3748; }
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
        
        numeric_cols = ['Close', 'Open Price', 'High', 'Low', 'Volume', 'Frequency', 'Avg_Order_Volume', 'MA50_AOVol', 'Value', 'Change', 'Previous']
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

# C. [PENTING] BUAT KOLOM SIGNAL (Fix Error KeyError)
# Kolom ini wajib ada agar bisa dipanggil di Tab 1 (Charting)
df['Whale_Signal'] = df['AOV_Ratio'] >= 1.5
df['Split_Signal'] = (df['AOV_Ratio'] <= 0.6) & (df['AOV_Ratio'] > 0)

max_date = df['Last Trading Date'].max()

# ==============================================================================
# 4. DASHBOARD TABS
# ==============================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Deep Dive", 
    "🐋 Screener", 
    "📊 Market Overview",
    "🧪 Research Lab"
])

# ==============================================================================
# TAB 1: DEEP DIVE (Chart Analysis)
# ==============================================================================
with tab1:
    st.markdown("### 📈 Deep Dive Analysis")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        all_stocks = sorted(df['Stock Code'].unique().tolist())
        selected_stock = st.selectbox("Pilih Saham", all_stocks)
    
    # Filter Data
    df_chart = df[df['Stock Code'] == selected_stock].tail(120).copy()
    
    # CEK APAKAH DATA ADA?
    if not df_chart.empty:
        last_row = df_chart.iloc[-1]
        aov_ratio = last_row['AOV_Ratio']
        
        # Ambil Nama Perusahaan (Handle jika kolom tidak ada)
        company_name = last_row['Company Name'] if 'Company Name' in last_row else selected_stock

        # --- A. STATUS CARD ---
        if aov_ratio >= 1.5:
            st.markdown(f"""<div class="whale-card"><div class="big-text">🐋 WHALE DETECTED</div><div class="small-text">Sinyal Akumulasi Besar (Lot Gede)</div><div class="value-text">AOV Ratio: {aov_ratio:.2f}x</div></div>""", unsafe_allow_html=True)
        elif aov_ratio <= 0.6 and aov_ratio > 0:
            st.markdown(f"""<div class="split-card"><div class="big-text">⚡ RETAIL/SPLIT</div><div class="small-text">Sinyal Distribusi / Akumulasi Senyap</div><div class="value-text">AOV Ratio: {aov_ratio:.2f}x</div></div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class="neutral-card"><div class="big-text">⚖️ NORMAL</div><div class="small-text">Aktivitas Wajar (Sesuai MA50)</div><div class="value-text">AOV Ratio: {aov_ratio:.2f}x</div></div>""", unsafe_allow_html=True)

        # --- B. PLOTLY CHART ---
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3])
        
        # 1. Price Line
        fig.add_trace(go.Scatter(
            x=df_chart['Last Trading Date'], y=df_chart['Close'], 
            mode='lines', line=dict(color='#2962ff', width=2), name='Close'
        ), row=1, col=1)
        
        # 2. Markers (Sinyal)
        # Pastikan filter menggunakan boolean series yang valid
        whale_signals = df_chart[df_chart['Whale_Signal']]
        split_signals = df_chart[df_chart['Split_Signal']]
        
        fig.add_trace(go.Scatter(
            x=whale_signals['Last Trading Date'], y=whale_signals['Close'], 
            mode='markers', marker=dict(symbol='triangle-up', size=12, color='#00cc00', line=dict(width=1, color='black')), 
            name='Whale Signal'
        ), row=1, col=1)
        
        fig.add_trace(go.Scatter(
            x=split_signals['Last Trading Date'], y=split_signals['Close'], 
            mode='markers', marker=dict(symbol='triangle-down', size=12, color='#ff4444', line=dict(width=1, color='black')), 
            name='Split Signal'
        ), row=1, col=1)
        
        # 3. Volume Bar
        colors = ['#00cc00' if r >= 1.5 else '#ff4444' if (r <= 0.6 and r > 0) else '#cfd8dc' for r in df_chart['AOV_Ratio']]
        fig.add_trace(go.Bar(
            x=df_chart['Last Trading Date'], y=df_chart['Volume'], 
            marker_color=colors, name='Volume'
        ), row=2, col=1)
        
        # --- C. GAP FIX (SOLUSI CHART OMPONG) ---
        # Logic: Buat range tanggal lengkap, lalu cari yg TIDAK ADA di data (Sabtu/Minggu/Libur)
        # Penting: Ini harus di dalam blok 'if not df_chart.empty'
        
        dt_all = pd.date_range(start=df_chart['Last Trading Date'].min(), end=df_chart['Last Trading Date'].max())
        dt_obs = [d.strftime("%Y-%m-%d") for d in df_chart['Last Trading Date']]
        dt_breaks = [d.strftime("%Y-%m-%d") for d in dt_all if d.strftime("%Y-%m-%d") not in dt_obs]

        fig.update_xaxes(
            rangebreaks=[dict(values=dt_breaks)] # Sembunyikan tanggal kosong
        )

        # Update Layout
        fig.update_layout(
            height=600, 
            title=f"{company_name} ({selected_stock})",
            showlegend=True, 
            hovermode="x unified", 
            xaxis_rangeslider_visible=False, 
            margin=dict(l=10, r=10, t=30, b=10),
            plot_bgcolor='white',
            paper_bgcolor='white'
        )
        
        # Update Axis
        fig.update_yaxes(title_text="Harga", row=1, col=1, gridcolor='#f0f0f0')
        fig.update_yaxes(title_text="Volume", row=2, col=1, gridcolor='#f0f0f0')

        st.plotly_chart(fig, use_container_width=True)
        
    else:
        st.warning(f"Data tidak tersedia untuk saham {selected_stock}.")

# ==============================================================================
# TAB 2: WHALE SCREENER (Dual Mode + Context)
# ==============================================================================
with tab2:
    st.markdown("### 🐋 Whale & Retail Detection Screener")
    
    # --- Settings ---
    with st.container():
        scan_mode = st.radio(
            "Metode Scanning:",
            ("📸 Daily Snapshot (Satu Tanggal)", "🗓️ Period Scanner (Rentang Waktu)"),
            horizontal=True
        )
        st.divider()

        col_set1, col_set2, col_set3 = st.columns(3)
        with col_set1:
            st.markdown("#### 🎯 Mode Deteksi")
            anomaly_type = st.radio("Target:", ("🐋 Whale Signal (High AOV)", "⚡ Split/Retail Signal (Low AOV)"))
            
        with col_set2:
            if scan_mode == "📸 Daily Snapshot (Satu Tanggal)":
                st.markdown("#### 📅 Tanggal Analisa")
                selected_date_val = st.date_input("Pilih Tanggal", max_date)
                selected_date = pd.to_datetime(selected_date_val)
            else:
                st.markdown("#### ⏳ Rentang Waktu")
                period_days = st.selectbox("Analisa Data Terakhir:", [5, 10, 20, 60], index=1, format_func=lambda x: f"{x} Hari Kerja")
                start_date_scan = max_date - timedelta(days=period_days * 1.5)
            
        with col_set3:
            st.markdown("#### 💰 Min. Transaksi")
            min_value = st.number_input("Rp (Miliar)", value=1_000_000_000, step=500_000_000)

    # --- Price Context ---
    st.markdown("#### 📉 Kondisi Harga (Price Context)")
    price_condition = st.selectbox(
        "Filter Kondisi Harga:",
        [
            "🔍 SEMUA FASE (Tampilkan Semua)",
            "💎 HIDDEN GEM (Sideways/Datar)", 
            "⚓ BOTTOM FISHING (Lagi Turun/Downtrend)",
            "🚀 EARLY MOVE (Baru Mulai Naik)"
        ]
    )

    # --- Data Prep ---
    if scan_mode == "📸 Daily Snapshot (Satu Tanggal)":
        target_df = df[df['Last Trading Date'] == selected_date].copy()
    else:
        target_df = df[df['Last Trading Date'] >= start_date_scan].copy()

    # --- Filtering ---
    if anomaly_type == "🐋 Whale Signal (High AOV)":
        min_ratio = 2.0
        suspects = target_df[(target_df['AOV_Ratio'] >= min_ratio) & (target_df['Value'] >= min_value)]
        color_map = 'Greens'
    else:
        suspects = target_df[(target_df['AOV_Ratio'] <= 0.6) & (target_df['AOV_Ratio'] > 0) & (target_df['Value'] >= min_value)]
        color_map = 'Reds_r'

    # VWMA Logic for Price Context
    if not suspects.empty:
        if 'VWMA_20D' not in suspects.columns:
            suspects['TP'] = (suspects['High'] + suspects['Low'] + suspects['Close']) / 3
            suspects['VP'] = suspects['TP'] * suspects['Volume']
            suspects['VWMA_20D'] = suspects.groupby('Stock Code')['VP'].transform(lambda x: x.rolling(20).sum() / x.rolling(20).sum())

        if price_condition == "💎 HIDDEN GEM (Sideways/Datar)":
            suspects = suspects[(suspects['Change %'] >= -2.0) & (suspects['Change %'] <= 2.0)]
        elif price_condition == "⚓ BOTTOM FISHING (Lagi Turun/Downtrend)":
            suspects = suspects[(suspects['Close'] < suspects['VWMA_20D']) | (suspects['Change %'] < 0)]
        elif price_condition == "🚀 EARLY MOVE (Baru Mulai Naik)":
            suspects = suspects[(suspects['Change %'] > 0) & (suspects['Change %'] <= 4.0)]

    # --- Display ---
    if suspects.empty:
        st.warning("Tidak ditemukan saham dengan kriteria tersebut.")
    else:
        if scan_mode == "📸 Daily Snapshot (Satu Tanggal)":
            # --- DAILY MODE DISPLAY ---
            suspects = suspects.sort_values(by='AOV_Ratio', ascending=False)
            
            # Hitung Conviction Score Simple
            suspects['Conviction_Score'] = np.where(
                anomaly_type == "🐋 Whale Signal (High AOV)",
                (suspects['AOV_Ratio'] / 4.0) * 100,  # Whale Logic
                ((0.6 - suspects['AOV_Ratio']) / 0.6) * 100 # Split Logic
            )
            suspects['Conviction_Score'] = suspects['Conviction_Score'].clip(0, 99)

            col_met1, col_met2 = st.columns(2)
            col_met1.metric("Saham Ditemukan", len(suspects))
            col_met2.metric("Avg AOV Ratio", f"{suspects['AOV_Ratio'].mean():.2f}x")

            # Urutan Kolom Strict
            desired_order = [
                'Stock Code', 'Company Name', 'Sector', 'Close', 'Change %', 
                'Frequency', 'Volume', 'Value', 'Avg_Order_Volume', 
                'AOV_Ratio', 'Conviction_Score'
            ]
            display_cols = [col for col in desired_order if col in suspects.columns]
            display_df = suspects[display_cols].copy()

            # Styling
            styled_df = display_df.style
            
            if anomaly_type == "🐋 Whale Signal (High AOV)":
                styled_df = styled_df.background_gradient(subset=['AOV_Ratio'], cmap='Greens', vmin=2.0, vmax=5.0)
                styled_df = styled_df.background_gradient(subset=['Conviction_Score'], cmap='Greens', vmin=0, vmax=100)
                
                def color_change(val):
                    if val > 0: return 'color: #10b981' # Hijau
                    if val < 0: return 'color: #ef4444' # Merah
                    return ''
                styled_df = styled_df.map(color_change, subset=['Change %'])
            else:
                styled_df = styled_df.background_gradient(subset=['AOV_Ratio'], cmap='Reds_r', vmin=0, vmax=0.6)
                styled_df = styled_df.background_gradient(subset=['Conviction_Score'], cmap='Reds', vmin=0, vmax=100)
                
                def color_change(val):
                    if val > 0: return 'color: #3b82f6' # Biru (Ritel FOMO)
                    if val < 0: return 'color: #f59e0b' # Orange
                    return ''
                styled_df = styled_df.map(color_change, subset=['Change %'])

            # Formatting String (Terakhir)
            styled_df = styled_df.format({
                'Close': 'Rp {:,.0f}',
                'Change %': '{:+.2f}%',
                'Frequency': '{:,.0f}',
                'Volume': '{:,.0f}',
                'Value': 'Rp {:,.0f}',
                'Avg_Order_Volume': '{:,.0f}',
                'AOV_Ratio': '{:.2f}x',
                'Conviction_Score': '{:.0f}%'
            })

            st.dataframe(
                styled_df,
                use_container_width=True,
                height=min(600, 100 + len(suspects) * 35),
                column_config={
                    'Stock Code': st.column_config.TextColumn("Kode", width="small"),
                    'Company Name': st.column_config.TextColumn("Nama Perusahaan", width="medium"),
                    'Sector': st.column_config.TextColumn("Sektor", width="medium"),
                    'Close': st.column_config.Column("Harga"),
                    'Change %': st.column_config.Column("Change %"),
                    'Frequency': st.column_config.Column("Freq"),
                    'Volume': st.column_config.Column("Volume"),
                    'Value': st.column_config.Column("Value"),
                    'Avg_Order_Volume': st.column_config.Column("Avg Lot"),
                    'AOV_Ratio': st.column_config.Column("AOV Ratio"),
                    'Conviction_Score': st.column_config.Column("Conviction")
                },
                hide_index=True
            )

        else:
            # === PERIOD MODE DISPLAY (SUMMARY) ===
            st.info(f"📊 Statistik Akumulasi selama **{period_days} hari terakhir** (Fase: {price_condition})")
            
            summary = suspects.groupby(['Stock Code', 'Company Name']).agg(
                Total_Signals=('Last Trading Date', 'count'),
                Last_Signal=('Last Trading Date', 'max'),
                Avg_AOV_Ratio=('AOV_Ratio', 'mean'),
                Avg_Value=('Value', 'mean'),
                Latest_Close=('Close', 'last'),
                Avg_Change=('Change %', 'mean')
            ).reset_index()

            summary = summary.sort_values(by='Total_Signals', ascending=False).head(50)

            col_p1, col_p2 = st.columns(2)
            col_p1.metric("Emiten Terdeteksi", len(summary))
            col_p2.metric("Top Frequency", f"{summary['Total_Signals'].max()} kali")

            styled_sum = summary.style.background_gradient(subset=['Total_Signals'], cmap='Blues')
            styled_sum = styled_sum.background_gradient(subset=['Avg_AOV_Ratio'], cmap=color_map)
            
            styled_sum = styled_sum.format({
                'Last_Signal': lambda x: x.strftime('%d %b %Y'),
                'Avg_AOV_Ratio': '{:.2f}x',
                'Avg_Value': 'Rp {:,.0f}',
                'Latest_Close': 'Rp {:,.0f}',
                'Avg_Change': '{:+.2f}%'
            })

            st.dataframe(
                styled_sum, 
                use_container_width=True,
                column_config={
                    "Total_Signals": st.column_config.Column("Freq Muncul", help="Berapa kali sinyal muncul"),
                    "Avg_AOV_Ratio": st.column_config.Column("Rata2 Power (AOV)"),
                    "Avg_Change": st.column_config.Column("Rata2 Change %")
                },
                hide_index=True
            )

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
