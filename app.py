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
    page_title="Market Intelligence Dashboard - Advanced Whale Detection",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="collapsed"  # Sidebar collapsed
)

# Custom CSS
st.markdown("""
<style>
    /* Status Cards */
    .whale-card {
        background: linear-gradient(135deg, #e6fffa 0%, #b2f5ea 100%);
        border-left: 5px solid #00cc00;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 15px;
        box-shadow: 0 4px 6px rgba(0, 204, 0, 0.1);
    }
    .split-card {
        background: linear-gradient(135deg, #fff5f5 0%, #fed7d7 100%);
        border-left: 5px solid #ff4444;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 15px;
        box-shadow: 0 4px 6px rgba(255, 68, 68, 0.1);
    }
    .neutral-card {
        background: linear-gradient(135deg, #f7fafc 0%, #edf2f7 100%);
        border-left: 5px solid #718096;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 15px;
        box-shadow: 0 4px 6px rgba(113, 128, 150, 0.1);
    }
    
    /* Metrics */
    .metric-card {
        background: white;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    /* Typography */
    .big-text { font-size: 28px; font-weight: 800; margin-bottom: 5px; }
    .medium-text { font-size: 16px; font-weight: 600; margin-bottom: 5px; }
    .small-text { font-size: 12px; color: #718096; }
    .value-text { font-size: 24px; font-weight: 700; color: #2d3748; }
    
    /* Filter Section */
    .filter-section {
        background: #f8fafc;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #e2e8f0;
        margin-bottom: 20px;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0px 0px;
        padding: 10px 20px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# Title with icon
st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; margin-bottom: 20px;'>
    <div style='font-size: 48px;'>🐋</div>
    <div>
        <h1 style='margin: 0; color: #2d3748;'>Market Intelligence Dashboard</h1>
        <p style='margin: 0; color: #718096; font-size: 16px;'>Advanced Whale Detection & Volume Analysis</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. LOAD DATA DARI GDRIVE
# ==============================================================================
FOLDER_ID = '1hX2jwUrAgi4Fr8xkcFWjCW6vbk6lsIlP'
FILE_NAME = 'Kompilasi_Data_1Tahun.csv'

@st.cache_resource
def get_drive_service():
    """Initialize Google Drive service with caching"""
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"❌ Error Authentication: {e}")
        return None

@st.cache_data(ttl=1800)
def load_data():
    """Load and preprocess data"""
    try:
        with st.spinner('🔄 Loading market data from Google Drive...'):
            service = get_drive_service()
            if not service:
                return None
            
            # Search for file
            query = f"'{FOLDER_ID}' in parents and name='{FILE_NAME}' and trashed=false"
            results = service.files().list(
                q=query, 
                fields="files(id, name)",
                supportsAllDrives=True
            ).execute()
            
            files = results.get('files', [])
            if not files:
                st.error("❌ File not found in Google Drive")
                return None
            
            file_id = files[0]['id']
            
            # Download file
            request = service.files().get_media(fileId=file_id)
            file_bytes = io.BytesIO()
            downloader = MediaIoBaseDownload(file_bytes, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            file_bytes.seek(0)
            
            # Load CSV
            df = pd.read_csv(file_bytes)
            
            # Basic preprocessing
            if 'Last Trading Date' in df.columns:
                df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'], errors='coerce')
            
            # Convert numeric columns
            numeric_cols = [
                'Close', 'Open Price', 'High', 'Low', 'Volume', 'Frequency',
                'Avg_Order_Volume', 'MA30_AOVol', 'Value', 'Change', 'Previous',
                'Foreign Buy', 'Foreign Sell', 'Bid Volume', 'Offer Volume',
                'First Trade'
            ]
            
            for col in numeric_cols:
                if col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.replace(',', '').str.replace('Rp', '').str.strip()
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # ==============================================================
            # FIX: Handle Open Price = 0 atau null
            # ==============================================================
            if 'Open Price' in df.columns and 'Previous' in df.columns:
                mask_invalid_open = (df['Open Price'].isna()) | (df['Open Price'] == 0) | (df['Open Price'] < 0)
                invalid_count = mask_invalid_open.sum()
                
                if invalid_count > 0:
                    # Gunakan Previous sebagai Open Price jika tersedia
                    df.loc[mask_invalid_open, 'Open Price'] = df.loc[mask_invalid_open, 'Previous']
                    
                    # Sort dulu berdasarkan stock code dan date
                    df = df.sort_values(['Stock Code', 'Last Trading Date'])
                    
                    # Forward fill dari Close sebelumnya untuk stock yang sama
                    df['Open Price'] = df.groupby('Stock Code').apply(
                        lambda x: x['Open Price'].replace(0, np.nan).ffill()
                    ).reset_index(level=0, drop=True)
                    
                    # Jika masih ada yang null, gunakan Close price
                    mask_still_null = df['Open Price'].isna()
                    df.loc[mask_still_null, 'Open Price'] = df.loc[mask_still_null, 'Close']
            
            # Pastikan High >= Open dan High >= Close
            if all(col in df.columns for col in ['High', 'Open Price', 'Close']):
                df['High'] = df[['High', 'Open Price', 'Close']].max(axis=1)
            
            # Pastikan Low <= Open dan Low <= Close
            if all(col in df.columns for col in ['Low', 'Open Price', 'Close']):
                df['Low'] = df[['Low', 'Open Price', 'Close']].min(axis=1)
            
            # Fill NaN dengan 0 untuk kolom numeric
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].fillna(0)
            
            # Calculate derived columns
            if 'Change %' not in df.columns or df['Change %'].isna().all():
                mask = (df['Previous'] > 0) & (df['Change'].notna())
                df.loc[mask, 'Change %'] = (df.loc[mask, 'Change'] / df.loc[mask, 'Previous']) * 100
                df['Change %'] = df['Change %'].fillna(0)
            
            # Calculate Value if not present
            if 'Value' not in df.columns or (df['Value'] == 0).all():
                df['Value'] = df['Close'] * df['Volume'] * 100
            
            # Calculate AOV Ratio - FIX: Pastikan tidak error
            if 'Avg_Order_Volume' in df.columns and 'MA30_AOVol' in df.columns:
                # Ganti 0 dengan 1 untuk menghindari division by zero
                ma30_filled = df['MA30_AOVol'].replace(0, 1)
                df['AOV_Ratio'] = df['Avg_Order_Volume'] / ma30_filled
                # Batasi ratio maksimal 10x untuk menghindari outlier ekstrim
                df['AOV_Ratio'] = df['AOV_Ratio'].clip(upper=10)
            
            # Calculate Net Foreign
            if all(col in df.columns for col in ['Foreign Buy', 'Foreign Sell']):
                df['Net Foreign'] = df['Foreign Buy'] - df['Foreign Sell']
            
            # Calculate Bid/Offer Imbalance
            if all(col in df.columns for col in ['Bid Volume', 'Offer Volume']):
                total = df['Bid Volume'] + df['Offer Volume']
                df['Bid_Offer_Imbalance'] = np.where(
                    total > 0,
                    (df['Bid Volume'] - df['Offer Volume']) / total,
                    0
                )
            
            # Add anomaly flags
            df['Whale_Signal'] = df['AOV_Ratio'] >= 1.5
            df['Split_Signal'] = (df['AOV_Ratio'] <= 0.6) & (df['AOV_Ratio'] > 0)
            
            st.success(f"✅ Data loaded: {len(df):,} rows, {df['Stock Code'].nunique():,} stocks")
            
            return df
            
    except Exception as e:
        st.error(f"❌ Failed to load data: {str(e)}")
        return None

# Load data
df_raw = load_data()
if df_raw is None:
    st.stop()

# ==============================================================================
# 3. DATA PREPARATION
# ==============================================================================
df = df_raw.sort_values(['Stock Code', 'Last Trading Date']).copy()
latest_date = df['Last Trading Date'].max()
latest_df = df[df['Last Trading Date'] == latest_date].copy()

# ==============================================================================
# 4. MAIN DASHBOARD - TABS (FILTER DIPINDAH KE MASING-MASING TAB)
# ==============================================================================
tab1, tab2, tab3 = st.tabs([
    "📈 Deep Dive Analysis", 
    "🐋 Whale Screener", 
    "📊 Market Overview"
])

# ==============================================================================
# TAB 1: DEEP DIVE ANALYSIS (DENGAN FILTER)
# ==============================================================================
with tab1:
    st.markdown("### 📈 Deep Dive Stock Analysis")
    
    # FILTER SECTION - Dipindah ke tab
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown("**🔍 Filter Settings**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Stock Selection
        all_stocks = sorted(df['Stock Code'].unique().tolist())
        selected_stock = st.selectbox(
            "Select Stock",
            all_stocks,
            key="deepdive_stock"
        )
    
    with col2:
        # Chart Period
        chart_days = st.slider(
            "Chart Period (Days)",
            min_value=30,
            max_value=250,
            value=120,
            step=10,
            key="chart_days"
        )
    
    with col3:
        # Chart Type
        chart_type = st.radio(
            "Chart Type",
            ["Candlestick", "Line Chart"],
            horizontal=True,
            key="chart_type"
        )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Get stock data
    stock_data = df[df['Stock Code'] == selected_stock].tail(chart_days).copy()
    
    if not stock_data.empty:
        last_row = stock_data.iloc[-1]
        company_name = last_row.get('Company Name', selected_stock)
        
        # Enhanced Status Card
        aov_ratio = last_row.get('AOV_Ratio', 1)
        
        # Calculate conviction score
        if aov_ratio >= 1.5:  # Default whale threshold
            conviction_score = min(99, ((aov_ratio - 1.5) / (5 - 1.5)) * 80 + 20)
            card_class = "whale-card"
            status_text = "🐋 WHALE DETECTED"
        elif aov_ratio <= 0.6 and aov_ratio > 0:
            conviction_score = min(99, ((0.6 - aov_ratio) / 0.6) * 80 + 20)
            card_class = "split-card"
            status_text = "⚡ RETAIL/SPLIT DOMINANT"
        else:
            conviction_score = 50
            card_class = "neutral-card"
            status_text = "⚖️ NORMAL ACTIVITY"
        
        # Display enhanced status card
        st.markdown(f"""
        <div class="{card_class}">
            <div class="big-text">{status_text}</div>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <div class="value-text">Conviction: {conviction_score:.0f}%</div>
                    <div class="small-text">AOV Ratio: {aov_ratio:.2f}x | Avg Lot: {last_row.get('Avg_Order_Volume', 0):,.0f}</div>
                </div>
                <div style="text-align: right;">
                    <div class="medium-text">Rp {last_row.get('Close', 0):,.0f}</div>
                    <div class="small-text" style="color: {'#00cc00' if last_row.get('Change %', 0) >= 0 else '#ff4444'}">
                        {last_row.get('Change %', 0):+.2f}%
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # ======================================================================
        # ENHANCED COMBO CHART
        # ======================================================================
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.5, 0.25, 0.25],
            specs=[
                [{"secondary_y": False}],
                [{"secondary_y": False}],
                [{"secondary_y": False}]
            ]
        )
        
        # 1. PRICE CHART
        if chart_type == "Candlestick":
            # Filter data untuk candlestick
            valid_candle_data = stock_data[
                (stock_data['Open Price'] > 0) & 
                (stock_data['High'] > 0) & 
                (stock_data['Low'] > 0) & 
                (stock_data['Close'] > 0)
            ].copy()
            
            if not valid_candle_data.empty:
                fig.add_trace(
                    go.Candlestick(
                        x=valid_candle_data['Last Trading Date'],
                        open=valid_candle_data['Open Price'],
                        high=valid_candle_data['High'],
                        low=valid_candle_data['Low'],
                        close=valid_candle_data['Close'],
                        name='OHLC',
                        increasing_line_color='#2ecc71',
                        decreasing_line_color='#e74c3c'
                    ),
                    row=1, col=1
                )
            else:
                # Fallback ke line chart
                fig.add_trace(
                    go.Scatter(
                        x=stock_data['Last Trading Date'],
                        y=stock_data['Close'],
                        mode='lines',
                        line=dict(color='#2962ff', width=2),
                        name='Close Price'
                    ),
                    row=1, col=1
                )
        else:
            # Line chart
            fig.add_trace(
                go.Scatter(
                    x=stock_data['Last Trading Date'],
                    y=stock_data['Close'],
                    mode='lines',
                    line=dict(color='#2962ff', width=2),
                    name='Close Price'
                ),
                row=1, col=1
            )
        
        # Whale Signals
        whale_signals = stock_data[stock_data['Whale_Signal']]
        if not whale_signals.empty and 'High' in whale_signals.columns:
            whale_customdata = whale_signals[['AOV_Ratio']].values
            y_positions = whale_signals['High'] * 1.01
            
            fig.add_trace(
                go.Scatter(
                    x=whale_signals['Last Trading Date'],
                    y=y_positions,
                    mode='markers',
                    marker=dict(
                        symbol='triangle-up',
                        size=12,
                        color='#00cc00',
                        line=dict(width=2, color='black')
                    ),
                    name='Whale Signal',
                    hovertemplate='<b>🐋 WHALE ENTRY</b><br>Date: %{x}<br>AOV Ratio: %{customdata[0]:.2f}x<extra></extra>',
                    customdata=whale_customdata
                ),
                row=1, col=1
            )
        
        # Split Signals
        split_signals = stock_data[stock_data['Split_Signal']]
        if not split_signals.empty and 'Low' in split_signals.columns:
            split_customdata = split_signals[['AOV_Ratio']].values
            y_positions = split_signals['Low'] * 0.99
            
            fig.add_trace(
                go.Scatter(
                    x=split_signals['Last Trading Date'],
                    y=y_positions,
                    mode='markers',
                    marker=dict(
                        symbol='triangle-down',
                        size=12,
                        color='#ff4444',
                        line=dict(width=2, color='black')
                    ),
                    name='Split Signal',
                    hovertemplate='<b>⚡ RETAIL DOMINANT</b><br>Date: %{x}<br>AOV Ratio: %{customdata[0]:.2f}x<extra></extra>',
                    customdata=split_customdata
                ),
                row=1, col=1
            )
        
        # 2. VOLUME BAR CHART
        vol_colors = []
        for ratio in stock_data['AOV_Ratio']:
            if ratio >= 1.5:
                vol_colors.append('#00cc00')
            elif ratio <= 0.6 and ratio > 0:
                vol_colors.append('#ff4444')
            else:
                vol_colors.append('#718096')
        
        volume_customdata = stock_data[['Avg_Order_Volume']].values
        
        fig.add_trace(
            go.Bar(
                x=stock_data['Last Trading Date'],
                y=stock_data['Volume'],
                marker_color=vol_colors,
                name='Volume',
                opacity=0.7,
                hovertemplate='<b>Volume</b>: %{y:,.0f} lots<br><b>Avg Lot</b>: %{customdata[0]:,.0f}<extra></extra>',
                customdata=volume_customdata
            ),
            row=2, col=1
        )
        
        # 3. AOV RATIO LINE CHART
        aov_customdata = np.column_stack([
            stock_data['Avg_Order_Volume'].fillna(0).values,
            stock_data['MA30_AOVol'].fillna(0).values
        ])
        
        fig.add_trace(
            go.Scatter(
                x=stock_data['Last Trading Date'],
                y=stock_data['AOV_Ratio'],
                mode='lines+markers',
                line=dict(color='#9c88ff', width=2),
                name='AOV Ratio',
                hovertemplate='<b>AOV Ratio</b>: %{y:.2f}x<br>Avg: %{customdata[0]:,.0f} | MA30: %{customdata[1]:.0f}<extra></extra>',
                customdata=aov_customdata
            ),
            row=3, col=1
        )
        
        # Add horizontal reference lines for AOV
        fig.add_hline(
            y=1.5,
            line_dash="dash",
            line_color="#00cc00",
            opacity=0.5,
            annotation_text="Whale Threshold (1.5x)",
            annotation_position="bottom right",
            row=3, col=1
        )
        
        fig.add_hline(
            y=0.6,
            line_dash="dash",
            line_color="#ff4444",
            opacity=0.5,
            annotation_text="Retail Threshold (0.6x)",
            annotation_position="bottom left",
            row=3, col=1
        )
        
        # Update layout
        fig.update_layout(
            height=800,
            title=f"{company_name} ({selected_stock}) - Comprehensive Analysis",
            showlegend=True,
            hovermode="x unified",
            xaxis_rangeslider_visible=False,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(size=12),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Update axis labels
        fig.update_yaxes(title_text="Price (Rp)", row=1, col=1)
        fig.update_yaxes(title_text="Volume (Lots)", row=2, col=1)
        fig.update_yaxes(title_text="AOV Ratio (x)", row=3, col=1)
        
        # Display chart
        st.plotly_chart(fig, use_container_width=True)
        
        # ======================================================================
        # ADDITIONAL METRICS
        # ======================================================================
        st.markdown("### 📊 Detailed Metrics")
        
        metric_cols = st.columns(4)
        
        with metric_cols[0]:
            daily_value = last_row.get('Value', 0)
            st.markdown(f"""
            <div class="metric-card">
                <div class="small-text">Daily Value</div>
                <div class="value-text">Rp {daily_value:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with metric_cols[1]:
            frequency = last_row.get('Frequency', 0)
            st.markdown(f"""
            <div class="metric-card">
                <div class="small-text">Frequency</div>
                <div class="value-text">{frequency:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with metric_cols[2]:
            if 'Net Foreign' in last_row:
                net_foreign = last_row['Net Foreign']
                color = "#00cc00" if net_foreign >= 0 else "#ff4444"
                st.markdown(f"""
                <div class="metric-card">
                    <div class="small-text">Net Foreign</div>
                    <div class="value-text" style="color: {color}">Rp {net_foreign:,.0f}</div>
                </div>
                """, unsafe_allow_html=True)
        
        with metric_cols[3]:
            if 'Bid_Offer_Imbalance' in last_row:
                imbalance = last_row['Bid_Offer_Imbalance']
                color = "#00cc00" if imbalance >= 0 else "#ff4444"
                st.markdown(f"""
                <div class="metric-card">
                    <div class="small-text">Bid/Offer Imbalance</div>
                    <div class="value-text" style="color: {color}">{imbalance:+.2%}</div>
                </div>
                """, unsafe_allow_html=True)

# ==============================================================================
# TAB 2: WHALE SCREENER (DENGAN LOGIC BARU)
# ==============================================================================
with tab2:
    st.markdown("### 🐋 Whale & Retail Detection Screener")
    
    # FILTER SECTION - Dipindah ke tab
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown("**🔍 Detection Mode & Filters**")
    
    # Bagian 1: Mode Deteksi
    col_mode1, col_mode2 = st.columns([1, 2])
    
    with col_mode1:
        anomaly_type = st.radio(
            "Target Deteksi:",
            ("🐋 Whale Signal (High AOV)", "⚡ Split/Retail Signal (Low AOV)"),
            help="Whale = Akumulasi Kasar (Lot Gede). Split = Distribusi/Akumulasi Senyap (Lot Kecil).",
            key="detection_mode"
        )
    
    with col_mode2:
        # Date Selection
        min_date = df['Last Trading Date'].min().date()
        max_date = df['Last Trading Date'].max().date()
        selected_date = st.date_input(
            "Tanggal Analisa",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="screener_date"
        )
        selected_date = pd.to_datetime(selected_date)
    
    st.markdown("---")
    
    # Bagian 2: Dynamic Thresholds berdasarkan mode
    if anomaly_type == "🐋 Whale Signal (High AOV)":
        st.markdown("**🐋 Parameter Paus**")
        col_param1, col_param2, col_param3 = st.columns(3)
        
        with col_param1:
            min_ratio = st.slider(
                "Min. Lonjakan AOV (x Lipat)", 
                1.5, 10.0, 2.0, 0.1,
                help="Order hari ini harus X kali lebih besar dari rata-rata.",
                key="whale_ratio"
            )
        
        with col_param2:
            min_value = st.number_input(
                "Min. Transaksi (Rp)", 
                value=1_000_000_000, 
                step=500_000_000,
                format="%d",
                key="whale_value"
            )
        
        with col_param3:
            # Additional filter: Min Frequency
            min_freq = st.number_input(
                "Min. Frekuensi", 
                value=50, 
                step=10,
                help="Minimal jumlah transaksi",
                key="whale_freq"
            )
        
        table_color_map = 'Greens'
        metric_label = "Paus Terdeteksi"
        
    else:  # Split/Retail Signal
        st.markdown("**⚡ Parameter Semut/Retail**")
        col_param1, col_param2, col_param3 = st.columns(3)
        
        with col_param1:
            max_ratio = st.slider(
                "Max. AOV Ratio (0.x)", 
                0.1, 0.9, 0.6, 0.05,
                help="Order hari ini harus DI BAWAH 0.x kali rata-rata.",
                key="retail_ratio"
            )
        
        with col_param2:
            min_value = st.number_input(
                "Min. Transaksi (Rp)", 
                value=500_000_000, 
                step=100_000_000,
                format="%d",
                key="retail_value"
            )
        
        with col_param3:
            # Additional filter: Min Frequency
            min_freq = st.number_input(
                "Min. Frekuensi", 
                value=100, 
                step=10,
                help="Minimal jumlah transaksi (retail biasanya lebih sering)",
                key="retail_freq"
            )
        
        table_color_map = 'Reds_r'
        metric_label = "Split/Retail Terdeteksi"
    
    # Bagian 3: Additional Filters
    st.markdown("---")
    st.markdown("**🎯 Filter Tambahan**")
    
    col_add1, col_add2, col_add3 = st.columns(3)
    
    with col_add1:
        # Sector Filter
        if 'Sector' in df.columns:
            sectors = ['Semua Sektor'] + sorted(df['Sector'].dropna().unique().tolist())
            selected_sector = st.selectbox(
                "Filter Sektor",
                sectors,
                key="screener_sector"
            )
    
    with col_add2:
        # Price Change Filter
        price_change_filter = st.checkbox("Filter Perubahan Harga", value=False, key="price_filter")
        if price_change_filter:
            min_change = st.number_input("Min. Change %", value=0.0, step=0.5, format="%.1f", key="min_change")
            if anomaly_type == "🐋 Whale Signal (High AOV)":
                # Untuk whale, biasanya positif atau sideways
                change_direction = st.radio("Arah", ["Positif", "Negatif", "Netral"], horizontal=True, key="whale_dir")
            else:
                # Untuk retail, bisa positif atau negatif
                change_direction = st.radio("Arah", ["Positif", "Negatif", "Netral"], horizontal=True, key="retail_dir")
    
    with col_add3:
        # Sort Options
        sort_options = {
            "AOV Ratio (Tertinggi)": "AOV_Ratio",
            "Nilai Transaksi (Tertinggi)": "Value",
            "Volume (Tertinggi)": "Volume",
            "Frekuensi (Tertinggi)": "Frequency",
            "Perubahan % (Tertinggi)": "Change %",
            "Konviction Score (Tertinggi)": "Conviction_Score"
        } if anomaly_type == "🐋 Whale Signal (High AOV)" else {
            "AOV Ratio (Terendah)": "AOV_Ratio",
            "Nilai Transaksi (Tertinggi)": "Value",
            "Volume (Tertinggi)": "Volume",
            "Frekuensi (Tertinggi)": "Frequency",
            "Perubahan % (Terendah)": "Change %"
        }
        
        sort_by = st.selectbox(
            "Urutkan Berdasarkan",
            list(sort_options.keys()),
            key="sort_by"
        )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # ==========================================================================
    # LOGIC DETECTION
    # ==========================================================================
    # Get data for selected date
    df_daily = df[df['Last Trading Date'] == selected_date].copy()
    
    if df_daily.empty:
        st.warning(f"⚠️ Tidak ada data untuk tanggal {selected_date.strftime('%d %b %Y')}")
        
        # Fallback ke data terbaru
        df_daily = latest_df.copy()
        st.info(f"Menampilkan data terbaru: {latest_date.strftime('%d %b %Y')}")
    
    # Apply detection logic berdasarkan mode
    if anomaly_type == "🐋 Whale Signal (High AOV)":
        # Whale Detection Logic
        base_mask = (
            (df_daily['AOV_Ratio'] >= min_ratio) & 
            (df_daily['Value'] >= min_value) &
            (df_daily['Frequency'] >= min_freq)
        )
        
        suspects = df_daily[base_mask].copy()
        
        # Hitung Conviction Score untuk Whale
        if not suspects.empty:
            # Normalize AOV Ratio untuk score (1.5-10 menjadi 0-100)
            min_aov = suspects['AOV_Ratio'].min()
            max_aov = suspects['AOV_Ratio'].max()
            if max_aov > min_aov:
                suspects['Conviction_Score'] = (
                    (suspects['AOV_Ratio'] - min_aov) / (max_aov - min_aov) * 80 + 20
                ).clip(0, 100)
            else:
                suspects['Conviction_Score'] = 50
        
    else:
        # Retail/Split Detection Logic
        base_mask = (
            (df_daily['AOV_Ratio'] <= max_ratio) & 
            (df_daily['AOV_Ratio'] > 0) & 
            (df_daily['Value'] >= min_value) &
            (df_daily['Frequency'] >= min_freq)
        )
        
        suspects = df_daily[base_mask].copy()
        
        # Hitung Conviction Score untuk Retail (semakin rendah AOV, semakin tinggi conviction)
        if not suspects.empty:
            # Untuk retail, semakin rendah AOV_Ratio semakin tinggi conviction
            suspects['Conviction_Score'] = (
                (max_ratio - suspects['AOV_Ratio']) / max_ratio * 80 + 20
            ).clip(0, 100)
    
    # Apply additional filters
    if not suspects.empty:
        # Sector Filter
        if 'selected_sector' in locals() and selected_sector != 'Semua Sektor' and 'Sector' in suspects.columns:
            suspects = suspects[suspects['Sector'] == selected_sector]
        
        # Price Change Filter
        if price_change_filter and 'Change %' in suspects.columns:
            if change_direction == "Positif":
                suspects = suspects[suspects['Change %'] >= min_change]
            elif change_direction == "Negatif":
                suspects = suspects[suspects['Change %'] <= -min_change]
            elif change_direction == "Netral":
                suspects = suspects[abs(suspects['Change %']) <= abs(min_change)]
        
        # Sort results
        sort_column = sort_options[sort_by]
        ascending = False  # Default: descending (highest first)
        
        if anomaly_type == "⚡ Split/Retail Signal (Low AOV)":
            if sort_by == "AOV Ratio (Terendah)":
                ascending = True  # Untuk retail, AOV terendah paling menarik
            elif sort_by == "Perubahan % (Terendah)":
                ascending = True
        
        suspects = suspects.sort_values(by=sort_column, ascending=ascending)
    
    # ==========================================================================
    # DISPLAY RESULTS
    # ==========================================================================
    # Display filter summary
    st.markdown(f"""
    <div class="metric-card">
        <div class="big-text">{metric_label}</div>
        <div class="value-text">{len(suspects)} saham</div>
        <div class="small-text">
            Tanggal: {selected_date.strftime('%d %b %Y')} | 
            Mode: {anomaly_type.split('(')[1].replace(')', '')}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if not suspects.empty:
        # Display results table
        display_cols = [
            'Stock Code', 'Close', 'Change %', 'Volume',
            'Avg_Order_Volume', 'AOV_Ratio', 'Value',
            'Frequency'
        ]
        
        # Add conviction score jika ada
        if 'Conviction_Score' in suspects.columns:
            display_cols.insert(7, 'Conviction_Score')
        
        # Add company name jika ada
        if 'Company Name' in suspects.columns:
            display_cols.insert(1, 'Company Name')
        
        # Add sector jika ada
        if 'Sector' in suspects.columns:
            display_cols.append('Sector')
        
        display_df = suspects[display_cols].copy()
        
        # Format display
        styled_df = display_df.style.format({
            'Close': 'Rp {:,.0f}',
            'Change %': '{:+.2f}%',
            'Volume': '{:,.0f}',
            'Avg_Order_Volume': '{:,.0f}',
            'AOV_Ratio': '{:.2f}x',
            'Value': 'Rp {:,.0f}',
            'Frequency': '{:,.0f}',
            'Conviction_Score': '{:.0f}%'
        })
        
        # Apply color gradient berdasarkan mode
        if anomaly_type == "🐋 Whale Signal (High AOV)":
            # Untuk whale: hijau gradient untuk AOV Ratio dan Conviction
            styled_df = styled_df.background_gradient(
                subset=['AOV_Ratio', 'Conviction_Score'],
                cmap=table_color_map
            )
            
            # Highlight positive changes in green
            def color_whale_change(val):
                if isinstance(val, str) and '%' in val:
                    try:
                        num_val = float(val.replace('%', '').replace('+', ''))
                        if num_val > 0:
                            return 'color: #00cc00; font-weight: bold'
                        elif num_val < 0:
                            return 'color: #ff4444'
                    except:
                        pass
                return ''
            
            styled_df = styled_df.map(color_whale_change, subset=['Change %'])
            
        else:
            # Untuk retail: merah gradient untuk AOV Ratio (semakin rendah semakin merah)
            styled_df = styled_df.background_gradient(
                subset=['AOV_Ratio'],
                cmap=table_color_map
            )
            
            # Untuk retail, conviction score (jika ada)
            if 'Conviction_Score' in display_df.columns:
                styled_df = styled_df.background_gradient(
                    subset=['Conviction_Score'],
                    cmap='Reds'
                )
            
            # Highlight negative changes in red untuk retail (mungkin distribution)
            def color_retail_change(val):
                if isinstance(val, str) and '%' in val:
                    try:
                        num_val = float(val.replace('%', '').replace('+', ''))
                        if num_val < 0:
                            return 'color: #ff4444; font-weight: bold'
                        elif num_val > 0:
                            return 'color: #00cc00'
                    except:
                        pass
                return ''
            
            styled_df = styled_df.map(color_retail_change, subset=['Change %'])
        
        st.dataframe(
            styled_df,
            use_container_width=True,
            height=600
        )
        
        # Download option
        csv = suspects.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Detection List",
            data=csv,
            file_name=f"{anomaly_type.split()[0].replace('🐋', 'whale').replace('⚡', 'retail')}_detection_{selected_date.strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
        # ======================================================================
        # VISUALIZATION
        # ======================================================================
        st.markdown("### 📊 Visualization")
        
        if len(suspects) >= 3:
            # Top 5 detection visualization
            top_n = min(10, len(suspects))
            top_detection = suspects.head(top_n)
            
            # Pilih chart berdasarkan mode
            if anomaly_type == "🐋 Whale Signal (High AOV)":
                fig = px.bar(
                    top_detection,
                    x='Stock Code',
                    y='AOV_Ratio',
                    color='AOV_Ratio',
                    color_continuous_scale='Greens',
                    title=f'Top {top_n} Whales by AOV Ratio',
                    labels={'AOV_Ratio': 'AOV Ratio (x)', 'Stock Code': 'Stock'},
                    hover_data=['Value', 'Frequency', 'Change %']
                )
                fig.add_hline(
                    y=min_ratio,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text=f"Threshold: {min_ratio}x"
                )
            else:
                fig = px.bar(
                    top_detection,
                    x='Stock Code',
                    y='AOV_Ratio',
                    color='AOV_Ratio',
                    color_continuous_scale='Reds_r',  # Reverse reds (darker = lower)
                    title=f'Top {top_n} Retail/Split by AOV Ratio',
                    labels={'AOV_Ratio': 'AOV Ratio (x)', 'Stock Code': 'Stock'},
                    hover_data=['Value', 'Frequency', 'Change %']
                )
                fig.add_hline(
                    y=max_ratio,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text=f"Threshold: {max_ratio}x"
                )
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Additional metrics
            col_met1, col_met2, col_met3 = st.columns(3)
            
            with col_met1:
                avg_aov = suspects['AOV_Ratio'].mean()
                st.metric("Rata-rata AOV Ratio", f"{avg_aov:.2f}x")
            
            with col_met2:
                total_value = suspects['Value'].sum()
                st.metric("Total Nilai Transaksi", f"Rp {total_value:,.0f}")
            
            with col_met3:
                if 'Conviction_Score' in suspects.columns:
                    avg_conviction = suspects['Conviction_Score'].mean()
                    st.metric("Rata-rata Conviction", f"{avg_conviction:.0f}%")
        
        # ======================================================================
        # INTERPRETATION GUIDE
        # ======================================================================
        with st.expander("📖 Interpretation Guide"):
            if anomaly_type == "🐋 Whale Signal (High AOV)":
                st.markdown("""
                **🐋 WHALE SIGNAL (AOV Tinggi) - Akumulasi Kasar:**
                
                **Karakteristik:**
                - **AOV Ratio > 1.5x** dari rata-rata 30 hari
                - **Transaksi besar** per order (lot gede)
                - Biasanya **institusi/smart money** yang masuk
                
                **Interpretasi:**
                1. **AOV Ratio 1.5-2.5x** = Moderate accumulation
                2. **AOV Ratio 2.5-4x** = Strong accumulation  
                3. **AOV Ratio > 4x** = Extreme whale activity
                
                **Trading Implication:**
                - **Bottom price + High AOV** = Potensi akumulasi (BUY)
                - **Top price + High AOV** = Potensi distribusi (CAUTION)
                - **Sideways + High AOV** = Silent accumulation (WATCH)
                """)
            else:
                st.markdown("""
                **⚡ SPLIT/RETAIL SIGNAL (AOV Rendah) - Distribusi/Akumulasi Senyap:**
                
                **Karakteristik:**
                - **AOV Ratio < 0.6x** dari rata-rata 30 hari
                - **Transaksi kecil-kecilan** per order
                - Biasanya **retail trading** atau **split order** bandar
                
                **Interpretasi:**
                1. **Low AOV + High Frequency** = Retail dominant
                2. **Low AOV + Sideways price** = Silent accumulation (bandar pecah order)
                3. **Low AOV + Price drop** = Retail panic selling
                
                **Trading Implication:**
                - **Bottom price + Low AOV** = Bandar akumulasi senyap (BUY)
                - **Top price + Low AOV** = Retail FOMO buying (CAUTION)
                - **Falling price + Low AOV** = Distribution phase (SELL)
                """)
    
    else:
        st.info("🚫 Tidak ada sinyal terdeteksi dengan filter saat ini.")
        
        # Suggestions
        st.markdown("""
        **💡 Saran untuk mendapatkan hasil:**
        
        **Untuk Whale Detection:**
        1. Kurangi **Min. Lonjakan AOV** (coba 1.5x)
        2. Kurangi **Min. Transaksi** (coba Rp 500 juta)
        3. Kurangi **Min. Frekuensi** (coba 30)
        4. Pilih **"Semua Sektor"**
        
        **Untuk Retail Detection:**
        1. Naikkan **Max. AOV Ratio** (coba 0.7x)
        2. Kurangi **Min. Transaksi** (coba Rp 200 juta)
        3. Pilih tanggal dengan aktivitas tinggi
        4. Coba tanggal berbeda
        """)
        
        # Show today's market stats
        if len(df_daily) > 0:
            with st.expander("📊 Statistik Pasar Hari Ini"):
                col_stat1, col_stat2, col_stat3 = st.columns(3)
                
                with col_stat1:
                    if 'AOV_Ratio' in df_daily.columns:
                        whale_count = len(df_daily[df_daily['AOV_Ratio'] >= 1.5])
                        retail_count = len(df_daily[(df_daily['AOV_Ratio'] <= 0.6) & (df_daily['AOV_Ratio'] > 0)])
                        st.metric("Whale Stocks", whale_count)
                        st.metric("Retail Stocks", retail_count)
                
                with col_stat2:
                    if 'Value' in df_daily.columns:
                        total_value = df_daily['Value'].sum()
                        avg_value = df_daily['Value'].mean()
                        st.metric("Total Nilai", f"Rp {total_value:,.0f}")
                        st.metric("Rata-rata", f"Rp {avg_value:,.0f}")
                
                with col_stat3:
                    if 'Frequency' in df_daily.columns:
                        total_freq = df_daily['Frequency'].sum()
                        avg_freq = df_daily['Frequency'].mean()
                        st.metric("Total Frekuensi", f"{total_freq:,.0f}")
                        st.metric("Rata-rata", f"{avg_freq:,.0f}")

# ==============================================================================
# TAB 3: MARKET OVERVIEW (DENGAN FILTER)
# ==============================================================================
with tab3:
    st.markdown("### 📊 Market Overview")
    
    # FILTER SECTION - Dipindah ke tab
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    st.markdown("**🔍 Overview Filters**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Date range untuk historical view
        view_days = st.slider(
            "View Period (Days)",
            min_value=7,
            max_value=180,
            value=30,
            step=7,
            key="view_days"
        )
    
    with col2:
        # Whale threshold untuk overview
        overview_whale_threshold = st.slider(
            "Whale Threshold (x)",
            min_value=1.0,
            max_value=3.0,
            value=1.5,
            step=0.1,
            key="overview_threshold"
        )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Market statistics
    st.markdown("### 📈 Market Statistics")
    
    stat_cols = st.columns(4)
    
    with stat_cols[0]:
        total_stocks_today = len(latest_df)
        st.metric("Stocks Traded Today", f"{total_stocks_today:,}")
    
    with stat_cols[1]:
        if 'AOV_Ratio' in latest_df.columns:
            avg_aov = latest_df['AOV_Ratio'].mean()
            st.metric("Avg AOV Ratio", f"{avg_aov:.2f}x")
    
    with stat_cols[2]:
        if 'AOV_Ratio' in latest_df.columns:
            whale_count_today = len(latest_df[latest_df['AOV_Ratio'] >= overview_whale_threshold])
            whale_pct = (whale_count_today / total_stocks_today * 100) if total_stocks_today > 0 else 0
            st.metric(f"Whales (≥{overview_whale_threshold}x)", f"{whale_count_today}", f"{whale_pct:.1f}%")
    
    with stat_cols[3]:
        if 'Volume' in latest_df.columns:
            total_volume = latest_df['Volume'].sum()
            st.metric("Total Volume", f"{total_volume:,.0f} lots")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Whale activity trend
        st.markdown("#### 🐋 Whale Activity Trend")
        
        # Get historical data
        start_date = latest_date - timedelta(days=view_days)
        historical_data = df[df['Last Trading Date'] >= start_date].copy()
        
        if not historical_data.empty:
            # Aggregate whale count per day
            whale_daily = historical_data.groupby(historical_data['Last Trading Date'].dt.date).apply(
                lambda x: (x['AOV_Ratio'] >= overview_whale_threshold).sum()
            ).reset_index()
            whale_daily.columns = ['Date', 'Whale Count']
            
            if not whale_daily.empty:
                fig = px.area(
                    whale_daily,
                    x='Date',
                    y='Whale Count',
                    title=f'Whale Activity (Last {view_days} Days)',
                    color_discrete_sequence=['#00cc00']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # AOV Ratio distribution
        st.markdown("#### 📊 AOV Ratio Distribution")
        
        if not latest_df.empty and 'AOV_Ratio' in latest_df.columns:
            fig = px.histogram(
                latest_df,
                x='AOV_Ratio',
                nbins=30,
                title='Distribution of AOV Ratios',
                color_discrete_sequence=['#9c88ff']
            )
            fig.add_vline(
                x=overview_whale_threshold,
                line_dash="dash",
                line_color="#00cc00",
                annotation_text=f"Whale Threshold ({overview_whale_threshold}x)"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Sector analysis
    st.markdown("### 🏭 Sector Analysis")
    
    if 'Sector' in latest_df.columns and 'AOV_Ratio' in latest_df.columns:
        sector_analysis = latest_df.groupby('Sector').agg({
            'AOV_Ratio': 'mean',
            'Stock Code': 'count',
            'Change %': 'mean',
            'Value': 'sum'
        }).reset_index()
        
        sector_analysis.columns = ['Sector', 'Avg AOV Ratio', 'Stock Count', 'Avg Change %', 'Total Value']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Treemap by AOV Ratio
            fig1 = px.treemap(
                sector_analysis,
                path=['Sector'],
                values='Stock Count',
                color='Avg AOV Ratio',
                color_continuous_scale='RdYlGn',
                title='Sector AOV Ratio Heatmap'
            )
            fig1.update_layout(height=500)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Bar chart top sectors by AOV
            top_sectors = sector_analysis.nlargest(10, 'Avg AOV Ratio')
            fig2 = px.bar(
                top_sectors,
                x='Sector',
                y='Avg AOV Ratio',
                color='Avg AOV Ratio',
                color_continuous_scale='Greens',
                title='Top 10 Sectors by AOV Ratio'
            )
            fig2.update_layout(height=500, xaxis_tickangle=45)
            st.plotly_chart(fig2, use_container_width=True)

# ==============================================================================
# 5. FOOTER
# ==============================================================================
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #718096; font-size: 12px;'>
    <p>🐋 Market Intelligence Dashboard v3.0 | Advanced Whale Detection System</p>
    <p>Data Source: Google Drive | Last Updated: {}</p>
</div>
""".format(latest_date.strftime('%d %b %Y %H:%M')), unsafe_allow_html=True)
