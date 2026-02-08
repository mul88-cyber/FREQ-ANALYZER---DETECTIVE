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
# TAB 2: WHALE SCREENER (DENGAN LOGIC BARU + COLLAPSIBLE FILTERS)
# ==============================================================================
with tab2:
    st.markdown("### 🐋 Whale & Retail Detection Screener")
    
    # ==========================================================================
    # COLLAPSIBLE FILTER SECTION
    # ==========================================================================
    # State untuk expand/collapse filters
    if 'filters_expanded' not in st.session_state:
        st.session_state.filters_expanded = True
    
    # VARIABEL GLOBAL YANG DIPERLUKAN
    min_date = df['Last Trading Date'].min().date()
    max_date_global = df['Last Trading Date'].max().date()
    
    # Header dengan toggle button
    col_header1, col_header2 = st.columns([3, 1])
    
    with col_header1:
        st.markdown("**🔍 Detection Controls**")
    
    with col_header2:
        # Toggle button untuk expand/collapse filters
        if st.button(
            "📊 Tampilkan Filter" if not st.session_state.filters_expanded else "📉 Sembunyikan Filter",
            key="toggle_filters",
            use_container_width=True
        ):
            st.session_state.filters_expanded = not st.session_state.filters_expanded
    
    # FILTER SECTION (Collapsible)
    if st.session_state.filters_expanded:
        with st.container(border=True):
            # Mode Deteksi
            st.markdown("#### 🎯 Mode Deteksi")
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
                selected_date = st.date_input(
                    "Tanggal Analisa",
                    value=max_date_global,
                    min_value=min_date,
                    max_value=max_date_global,
                    key="screener_date"
                )
                selected_date = pd.to_datetime(selected_date)
            
            st.divider()
            
            # Dynamic Thresholds berdasarkan mode
            if anomaly_type == "🐋 Whale Signal (High AOV)":
                st.markdown("#### 🐋 Parameter Paus")
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
                        help=f"Rp {1_000_000_000:,.0f} = Rp 1 Miliar"
                    )
                
                with col_param3:
                    min_freq = st.number_input(
                        "Min. Frekuensi", 
                        value=50, 
                        step=10,
                        help="Minimal jumlah transaksi",
                        format="%d"
                    )
                
                table_color_map = 'Greens'
                metric_label = "Paus Terdeteksi"
                
            else:  # Split/Retail Signal
                st.markdown("#### ⚡ Parameter Semut/Retail")
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
                        help=f"Rp {500_000_000:,.0f} = Rp 500 Juta"
                    )
                
                with col_param3:
                    min_freq = st.number_input(
                        "Min. Frekuensi", 
                        value=100, 
                        step=10,
                        help="Minimal jumlah transaksi (retail biasanya lebih sering)",
                        format="%d"
                    )
                
                table_color_map = 'Reds_r'
                metric_label = "Split/Retail Terdeteksi"
            
            st.divider()
            
            # Additional Filters Section
            st.markdown("#### 🎯 Filter Tambahan (Opsional)")
            
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
                        change_direction = st.radio("Arah", ["Positif", "Negatif", "Netral"], horizontal=True, key="whale_dir")
                    else:
                        change_direction = st.radio("Arah", ["Positif", "Negatif", "Netral"], horizontal=True, key="retail_dir")
            
            with col_add3:
                # Sort Options
                sort_options = {
                    "AOV Ratio (Tertinggi)": "AOV_Ratio",
                    "Nilai Transaksi (Tertinggi)": "Value",
                    "Volume (Tertinggi)": "Volume",
                    "Frekuensi (Tertinggi)": "Frequency",
                    "Perubahan % (Tertinggi)": "Change %",
                    "Conviction Score (Tertinggi)": "Conviction_Score"
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
            
            # Quick Action Buttons
            st.divider()
            st.markdown("#### ⚡ Quick Actions")
            
            col_action1, col_action2, col_action3 = st.columns(3)
            
            with col_action1:
                if st.button("🔍 Jalankan Screening", use_container_width=True, type="primary"):
                    # Trigger screening (akan dijalankan otomatis)
                    st.rerun()
            
            with col_action2:
                if st.button("🔄 Reset ke Default", use_container_width=True):
                    # Reset semua filter ke default
                    st.session_state.filters_expanded = True
                    # Clear specific session states
                    keys_to_clear = [
                        'detection_mode', 'screener_date', 'whale_ratio', 
                        'whale_value', 'whale_freq', 'retail_ratio', 
                        'retail_value', 'retail_freq', 'screener_sector',
                        'price_filter', 'min_change', 'sort_by'
                    ]
                    for key in keys_to_clear:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
            
            with col_action3:
                # Toggle untuk sembunyikan filter setelah screening
                auto_collapse = st.checkbox(
                    "Auto-sembunyikan filter setelah screening",
                    value=False,
                    key="auto_collapse"
                )
    
    else:
        # Jika filter disembunyikan, tampilkan minimal info
        st.info("""
        🔍 **Filters are currently hidden.** 
        Click **"Tampilkan Filter"** button above to adjust screening parameters.
        """)
        
        # Tampilkan summary settings yang sedang aktif
        with st.container(border=True):
            col_sum1, col_sum2, col_sum3 = st.columns(3)
            
            with col_sum1:
                st.markdown("**Mode:**")
                # Gunakan default jika belum ada di session state
                current_mode = st.session_state.get('detection_mode', '🐋 Whale Signal (High AOV)')
                st.text(current_mode)
            
            with col_sum2:
                st.markdown("**Tanggal:**")
                # Gunakan default jika belum ada di session state
                if 'screener_date' in st.session_state:
                    current_date = st.session_state.screener_date
                    st.text(current_date.strftime('%d %b %Y'))
                else:
                    st.text(max_date_global.strftime('%d %b %Y'))
            
            with col_sum3:
                st.markdown("**Threshold:**")
                # Gunakan default berdasarkan mode
                if current_mode == "🐋 Whale Signal (High AOV)":
                    threshold_val = st.session_state.get('whale_ratio', 2.0)
                    st.text(f"AOV ≥ {threshold_val}x")
                else:
                    threshold_val = st.session_state.get('retail_ratio', 0.6)
                    st.text(f"AOV ≤ {threshold_val}x")
    
    # ==========================================================================
    # LOGIC DETECTION 
    # ==========================================================================
    # Gunakan nilai dari session state atau default
    anomaly_type = st.session_state.get('detection_mode', '🐋 Whale Signal (High AOV)')
    selected_date_value = st.session_state.get('screener_date', max_date_global)
    selected_date = pd.to_datetime(selected_date_value)
    
    # Gunakan parameter berdasarkan mode
    if anomaly_type == "🐋 Whale Signal (High AOV)":
        min_ratio = st.session_state.get('whale_ratio', 2.0)
        min_value = st.session_state.get('whale_value', 1_000_000_000)
        min_freq = st.session_state.get('whale_freq', 50)
        table_color_map = 'Greens'
        metric_label = "Paus Terdeteksi"
    else:
        max_ratio = st.session_state.get('retail_ratio', 0.6)
        min_value = st.session_state.get('retail_value', 500_000_000)
        min_freq = st.session_state.get('retail_freq', 100)
        table_color_map = 'Reds_r'
        metric_label = "Split/Retail Terdeteksi"
    
    # Get data for selected date
    df_daily = df[df['Last Trading Date'] == selected_date].copy()
    
    if df_daily.empty:
        st.warning(f"⚠️ Tidak ada data untuk tanggal {selected_date.strftime('%d %b %Y')}")
        # Gunakan data terbaru
        latest_date = df['Last Trading Date'].max()
        df_daily = df[df['Last Trading Date'] == latest_date].copy()
        st.info(f"Menampilkan data terbaru: {latest_date.strftime('%d %b %Y')}")
    
    # Apply detection logic berdasarkan mode
    if anomaly_type == "🐋 Whale Signal (High AOV)":
        base_mask = (
            (df_daily['AOV_Ratio'] >= min_ratio) & 
            (df_daily['Value'] >= min_value) &
            (df_daily['Frequency'] >= min_freq)
        )
        suspects = df_daily[base_mask].copy()
        
        if not suspects.empty:
            min_aov = suspects['AOV_Ratio'].min()
            max_aov = suspects['AOV_Ratio'].max()
            if max_aov > min_aov:
                suspects['Conviction_Score'] = (
                    (suspects['AOV_Ratio'] - min_aov) / (max_aov - min_aov) * 80 + 20
                ).clip(0, 100)
            else:
                suspects['Conviction_Score'] = 50
    else:
        base_mask = (
            (df_daily['AOV_Ratio'] <= max_ratio) & 
            (df_daily['AOV_Ratio'] > 0) & 
            (df_daily['Value'] >= min_value) &
            (df_daily['Frequency'] >= min_freq)
        )
        suspects = df_daily[base_mask].copy()
        
        if not suspects.empty:
            suspects['Conviction_Score'] = (
                (max_ratio - suspects['AOV_Ratio']) / max_ratio * 80 + 20
            ).clip(0, 100)
    
    # Apply additional filters
    if not suspects.empty:
        # Sector filter
        selected_sector = st.session_state.get('screener_sector', 'Semua Sektor')
        if selected_sector != 'Semua Sektor' and 'Sector' in suspects.columns:
            suspects = suspects[suspects['Sector'] == selected_sector]
        
        # Price change filter
        price_change_filter = st.session_state.get('price_filter', False)
        if price_change_filter and 'Change %' in suspects.columns:
            min_change = st.session_state.get('min_change', 0.0)
            change_direction = st.session_state.get(
                'whale_dir' if anomaly_type == "🐋 Whale Signal (High AOV)" else 'retail_dir', 
                'Netral'
            )
            
            if change_direction == "Positif":
                suspects = suspects[suspects['Change %'] >= min_change]
            elif change_direction == "Negatif":
                suspects = suspects[suspects['Change %'] <= -min_change]
            elif change_direction == "Netral":
                suspects = suspects[abs(suspects['Change %']) <= abs(min_change)]
        
        # Sort results
        sort_by = st.session_state.get('sort_by', 'AOV Ratio (Tertinggi)')
        sort_options = {
            "AOV Ratio (Tertinggi)": "AOV_Ratio",
            "Nilai Transaksi (Tertinggi)": "Value",
            "Volume (Tertinggi)": "Volume",
            "Frekuensi (Tertinggi)": "Frequency",
            "Perubahan % (Tertinggi)": "Change %",
            "Conviction Score (Tertinggi)": "Conviction_Score"
        } if anomaly_type == "🐋 Whale Signal (High AOV)" else {
            "AOV Ratio (Terendah)": "AOV_Ratio",
            "Nilai Transaksi (Tertinggi)": "Value",
            "Volume (Tertinggi)": "Volume",
            "Frekuensi (Tertinggi)": "Frequency",
            "Perubahan % (Terendah)": "Change %"
        }
        
        sort_column = sort_options.get(sort_by, 'AOV_Ratio')
        ascending = False
        
        if anomaly_type == "⚡ Split/Retail Signal (Low AOV)":
            if sort_by == "AOV Ratio (Terendah)":
                ascending = True
            elif sort_by == "Perubahan % (Terendah)":
                ascending = True
        
        suspects = suspects.sort_values(by=sort_column, ascending=ascending)
        
        # Auto-collapse filters jika di-set
        if st.session_state.get('auto_collapse', False):
            st.session_state.filters_expanded = False
    
    # ==========================================================================
    # DISPLAY RESULTS 
    # ==========================================================================
    # Header dengan results summary
    st.markdown(f"""
    <div class="metric-card">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <div class="big-text">{metric_label}</div>
                <div class="small-text">Tanggal: {selected_date.strftime('%d %b %Y')}</div>
            </div>
            <div style="text-align: right;">
                <div class="value-text">{len(suspects):,} saham</div>
                <div class="small-text">Mode: {anomaly_type.split('(')[1].replace(')', '')}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if not suspects.empty:
        # ==============================================================
        # 1. DEFINISI URUTAN KOLOM (STRICT ORDER)
        # ==============================================================
        # Kita definisikan urutan persis sesuai keinginan Bapak
        desired_order = [
            'Stock Code', 
            'Company Name', 
            'Sector', 
            'Close', 
            'Change %', 
            'Frequency', 
            'Volume', 
            'Value', 
            'Avg_Order_Volume', 
            'AOV_Ratio', 
            'Conviction_Score'
        ]
        
        # Filter: Hanya ambil kolom yang benar-benar ada di data (untuk hindari error)
        display_cols = [col for col in desired_order if col in suspects.columns]
        
        # Buat dataframe baru khusus untuk tampilan sesuai urutan
        display_df = suspects[display_cols].copy()

        # ==============================================================
        # 2. STYLING & FORMATTING (Background Color & Koma)
        # ==============================================================
        # Inisialisasi Styler
        styled_df = display_df.style

        # A. Background Gradient (Warna) - Dihitung saat data masih Angka
        if anomaly_type == "🐋 Whale Signal (High AOV)":
            if 'AOV_Ratio' in display_df.columns:
                styled_df = styled_df.background_gradient(subset=['AOV_Ratio'], cmap='Greens', vmin=min_ratio, vmax=display_df['AOV_Ratio'].max())
            if 'Conviction_Score' in display_df.columns:
                styled_df = styled_df.background_gradient(subset=['Conviction_Score'], cmap='Greens', vmin=0, vmax=100)
            
            # Helper function warna change %
            def color_change(val):
                if pd.isna(val): return ''
                if val > 0: return 'color: #10b981' # Hijau
                if val < 0: return 'color: #ef4444' # Merah
                return ''
            if 'Change %' in display_df.columns:
                styled_df = styled_df.map(color_change, subset=['Change %'])

        else: # Retail/Split
            if 'AOV_Ratio' in display_df.columns:
                styled_df = styled_df.background_gradient(subset=['AOV_Ratio'], cmap='Reds_r', vmin=0, vmax=max_ratio)
            if 'Conviction_Score' in display_df.columns:
                styled_df = styled_df.background_gradient(subset=['Conviction_Score'], cmap='Reds', vmin=0, vmax=100)
            
            def color_change(val):
                if pd.isna(val): return ''
                if val > 0: return 'color: #3b82f6' # Biru
                if val < 0: return 'color: #f59e0b' # Orange
                return ''
            if 'Change %' in display_df.columns:
                styled_df = styled_df.map(color_change, subset=['Change %'])

        # B. String Formatting (Koma & Rp) - Diterapkan TERAKHIR
        # Ini memastikan angka muncul sebagai "1,000" bukan "1000"
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

        # ==============================================================
        # 3. RENDER DATAFRAME (FINAL OUTPUT)
        # ==============================================================
        st.dataframe(
            styled_df,
            use_container_width=True,
            height=min(600, 100 + len(suspects) * 35),
            column_config={
                'Stock Code': st.column_config.TextColumn("Kode", width="small"),
                'Company Name': st.column_config.TextColumn("Nama Perusahaan", width="medium"),
                'Sector': st.column_config.TextColumn("Sektor", width="medium"),
                'Close': st.column_config.Column("Harga"), # Pakai 'Column' biasa agar format string (Rp/Koma) dari Pandas tidak rusak
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
        
        # Download button
        st.markdown("#### 💾 Export Results")
        col_dl1, col_dl2, col_dl3 = st.columns([2, 1, 1])
        
        with col_dl1:
            # FORMAT CSV dengan separator koma
            csv_data = suspects.copy()
            # Format kolom numeric untuk CSV
            for col in ['Close', 'Value']:
                if col in csv_data.columns:
                    csv_data[col] = csv_data[col].apply(lambda x: f"Rp {x:,.0f}" if pd.notnull(x) else "Rp 0")
            
            for col in ['Volume', 'Avg_Order_Volume', 'Frequency']:
                if col in csv_data.columns:
                    csv_data[col] = csv_data[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
            
            csv = csv_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV (Full Data)",
                data=csv,
                file_name=f"{anomaly_type.split()[0].replace('🐋', 'whale').replace('⚡', 'retail')}_detection_{selected_date.strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col_dl2:
            # Download hanya kolom tertentu dengan format
            csv_simple = display_df.copy()
            csv_simple = csv_simple.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 CSV (Simple)",
                data=csv_simple,
                file_name=f"detection_simple_{selected_date.strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col_dl3:
            # Copy to clipboard dengan format yang baik
            if st.button("📋 Copy to Clipboard", use_container_width=True):
                # Format untuk clipboard
                clip_df = display_df.copy()
                # Format angka dengan separator
                for col in ['Close', 'Value']:
                    if col in clip_df.columns:
                        clip_df[col] = clip_df[col].apply(lambda x: f"Rp {x:,.0f}" if pd.notnull(x) else "Rp 0")
                
                for col in ['Volume', 'Avg_Order_Volume', 'Frequency']:
                    if col in clip_df.columns:
                        clip_df[col] = clip_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
                
                for col in ['Change %', 'AOV_Ratio', 'Conviction_Score']:
                    if col in clip_df.columns:
                        if col == 'Change %':
                            clip_df[col] = clip_df[col].apply(lambda x: f"{x:+.2f}%" if pd.notnull(x) else "0.00%")
                        elif col == 'AOV_Ratio':
                            clip_df[col] = clip_df[col].apply(lambda x: f"{x:.2f}x" if pd.notnull(x) else "0.00x")
                        elif col == 'Conviction_Score':
                            clip_df[col] = clip_df[col].apply(lambda x: f"{x:.0f}%" if pd.notnull(x) else "0%")
                
                display_text = clip_df.to_string(index=False)
                st.code(display_text, language='text')
                st.success("✅ Data copied to clipboard!")
        
        # ======================================================================
        # INTERPRETATION GUIDE
        # ======================================================================
        with st.expander("📖 Interpretation Guide & Trading Implications"):
            if anomaly_type == "🐋 Whale Signal (High AOV)":
                st.markdown("""
                ### 🐋 **WHALE SIGNAL INTERPRETATION**
                
                **Karakteristik:**
                - **AOV Ratio > 1.5x** dari rata-rata 30 hari
                - **Transaksi besar per order** (lot gede)
                - Biasanya **institusi/smart money** yang masuk
                
                **Scoring Guide:**
                - **AOV Ratio 1.5-2.5x** = Moderate accumulation
                - **AOV Ratio 2.5-4x** = Strong accumulation  
                - **AOV Ratio > 4x** = Extreme whale activity
                - **Conviction Score > 80%** = High confidence signal
                
                **Trading Implications:**
                | Scenario | Action | Reasoning |
                |----------|--------|-----------|
                | **Bottom price + High AOV** | BUY / Accumulate | Whale accumulation at support |
                | **Top price + High AOV** | CAUTION / Take Profit | Possible distribution |
                | **Sideways + High AOV** | WATCH / Prepare | Silent accumulation phase |
                | **High AOV + Low Volume** | SUSPECT | Possible wash trading |
                
                **Risk Management:**
                - ✅ Entry: Wait for price confirmation after signal
                - ✅ Stop Loss: 5-8% below entry
                - ✅ Take Profit: 15-25% for swing trade
                """)
            else:
                st.markdown("""
                ### ⚡ **RETAIL/SPLIT SIGNAL INTERPRETATION**
                
                **Karakteristik:**
                - **AOV Ratio < 0.6x** dari rata-rata 30 hari
                - **Transaksi kecil-kecilan** per order
                - Biasanya **retail trading** atau **split order** bandar
                
                **Scoring Guide:**
                - **AOV Ratio 0.4-0.6x** = Moderate retail activity
                - **AOV Ratio 0.2-0.4x** = High retail dominance  
                - **AOV Ratio < 0.2x** = Extreme split orders
                - **High Frequency + Low AOV** = Classic retail pattern
                
                **Trading Implications:**
                | Scenario | Action | Reasoning |
                |----------|--------|-----------|
                | **Bottom price + Low AOV** | BUY / Accumulate | Bandar accumulating silently |
                | **Top price + Low AOV** | SELL / Take Profit | Retail FOMO, distribution phase |
                | **Falling price + Low AOV** | AVOID / Wait | Retail panic selling |
                | **Low AOV + High Volume** | WATCH | Possible accumulation completion |
                
                **Risk Management:**
                - ✅ Entry: Wait for reversal confirmation
                - ✅ Stop Loss: 3-5% for tight risk
                - ✅ Take Profit: 10-20% for quick trades
                - ⚠️ Caution: High volatility possible
                """)
    
    else:
        # No results found
        st.warning("""
        🚫 **Tidak ada sinyal yang terdeteksi dengan parameter saat ini.**
        
        Hal ini bisa disebabkan oleh:
        1. Parameter filter terlalu ketat
        2. Pasar sedang sepi (low activity day)
        3. Data untuk tanggal tersebut tidak lengkap
        """)
        
        # Suggestions
        st.markdown("""
        ### 💡 **Saran untuk mendapatkan hasil:**
        
        **Untuk Whale Detection (🐋):**
        ```
        1. Kurangi Min. Lonjakan AOV → coba 1.5x
        2. Kurangi Min. Transaksi → coba Rp 500 juta
        3. Kurangi Min. Frekuensi → coba 30 transaksi
        4. Pilih "Semua Sektor"
        5. Coba tanggal berbeda (market aktif)
        ```
        
        **Untuk Retail Detection (⚡):**
        ```
        1. Naikkan Max. AOV Ratio → coba 0.7x
        2. Kurangi Min. Transaksi → coba Rp 200 juta
        3. Pilih tanggal dengan volume tinggi
        4. Non-aktifkan filter tambahan
        5. Coba periode afternoon session
        ```
        """)
        
        # Show market stats untuk reference
        with st.expander("📊 Market Statistics for Reference"):
            if len(df_daily) > 0:
                col_ref1, col_ref2, col_ref3, col_ref4 = st.columns(4)
                
                with col_ref1:
                    total_stocks = len(df_daily)
                    st.metric("Total Saham", f"{total_stocks:,}")
                
                with col_ref2:
                    if 'AOV_Ratio' in df_daily.columns:
                        whale_count = len(df_daily[df_daily['AOV_Ratio'] >= 1.5])
                        retail_count = len(df_daily[(df_daily['AOV_Ratio'] <= 0.6) & (df_daily['AOV_Ratio'] > 0)])
                        st.metric("Whale Stocks", f"{whale_count:,}")
                        st.metric("Retail Stocks", f"{retail_count:,}")
                
                with col_ref3:
                    if 'Value' in df_daily.columns:
                        total_value = df_daily['Value'].sum()
                        if total_value >= 1_000_000_000:
                            display_val = f"Rp {total_value/1_000_000_000:.1f} B"
                        else:
                            display_val = f"Rp {total_value:,.0f}"
                        st.metric("Total Market Value", display_val)
                
                with col_ref4:
                    if 'Volume' in df_daily.columns:
                        total_volume = df_daily['Volume'].sum()
                        st.metric("Total Volume", f"{total_volume:,.0f} lot")

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
            st.metric(f"Whales (≥{overview_whale_threshold}x)", f"{whale_count_today:,}", f"{whale_pct:.1f}%")
    
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
