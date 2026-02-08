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
    initial_sidebar_state="expanded"
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
                'Foreign Buy', 'Foreign Sell', 'Bid Volume', 'Offer Volume'
            ]
            
            for col in numeric_cols:
                if col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.replace(',', '').str.replace('Rp', '')
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Calculate derived columns
            if 'Change %' not in df.columns or df['Change %'].isna().all():
                mask = (df['Previous'] > 0) & (df['Change'].notna())
                df.loc[mask, 'Change %'] = (df.loc[mask, 'Change'] / df.loc[mask, 'Previous']) * 100
                df['Change %'] = df['Change %'].fillna(0)
            
            # Calculate Value if not present
            if 'Value' not in df.columns or (df['Value'] == 0).all():
                df['Value'] = df['Close'] * df['Volume'] * 100
            
            # Calculate AOV Ratio
            if 'Avg_Order_Volume' in df.columns and 'MA30_AOVol' in df.columns:
                df['AOV_Ratio'] = np.where(
                    df['MA30_AOVol'] > 0,
                    df['Avg_Order_Volume'] / df['MA30_AOVol'],
                    1
                )
            
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
# 4. SIDEBAR
# ==============================================================================
with st.sidebar:
    st.markdown("### ⚙️ Control Panel")
    
    # Date Selection
    st.markdown("**📅 Date Selection**")
    selected_date = st.date_input(
        "Analysis Date",
        value=latest_date.date(),
        min_value=df['Last Trading Date'].min().date(),
        max_value=df['Last Trading Date'].max().date()
    )
    selected_date = pd.to_datetime(selected_date)
    
    st.markdown("---")
    
    # Whale Detection Parameters
    st.markdown("**🐋 Whale Detection**")
    min_whale_ratio = st.slider(
        "Min Whale Ratio (x)",
        min_value=1.0,
        max_value=5.0,
        value=1.5,
        step=0.1
    )
    
    min_value_rp = st.number_input(
        "Min Transaction Value (Rp)",
        value=5_000_000_000,
        step=1_000_000_000,
        format="%d"
    )
    
    min_frequency = st.number_input(
        "Min Frequency",
        value=100,
        step=50
    )
    
    st.markdown("---")
    
    # Sector Filter
    if 'Sector' in df.columns:
        st.markdown("**🏭 Sector Filter**")
        sectors = ['All'] + sorted(df['Sector'].dropna().unique().tolist())
        selected_sector = st.selectbox("Select Sector", sectors)
    
    st.markdown("---")
    
    # Display Metrics
    st.markdown("**📊 Market Overview**")
    total_stocks = df['Stock Code'].nunique()
    active_stocks = latest_df['Stock Code'].nunique()
    whale_count = len(latest_df[latest_df['AOV_Ratio'] >= min_whale_ratio])
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Stocks", f"{total_stocks:,}")
    with col2:
        st.metric("Active Today", f"{active_stocks:,}")
    
    st.metric("Whales Detected", f"{whale_count:,}")

# ==============================================================================
# 5. MAIN DASHBOARD - TABS
# ==============================================================================
tab1, tab2, tab3 = st.tabs([
    "📈 Deep Dive Analysis", 
    "🐋 Whale Screener", 
    "📊 Market Overview"
])

# ==============================================================================
# TAB 1: DEEP DIVE ANALYSIS
# ==============================================================================
with tab1:
    st.markdown("### 📈 Deep Dive Stock Analysis")
    
    # Stock Selection
    col1, col2 = st.columns([1, 2])
    with col1:
        all_stocks = sorted(df['Stock Code'].unique().tolist())
        selected_stock = st.selectbox(
            "Select Stock",
            all_stocks,
            key="deepdive_stock"
        )
    
    # Get stock data
    stock_data = df[df['Stock Code'] == selected_stock].tail(120).copy()
    
    if not stock_data.empty:
        last_row = stock_data.iloc[-1]
        company_name = last_row.get('Company Name', selected_stock)
        
        # Enhanced Status Card
        aov_ratio = last_row.get('AOV_Ratio', 1)
        
        # Calculate conviction score
        if aov_ratio >= min_whale_ratio:
            conviction_score = min(99, ((aov_ratio - min_whale_ratio) / (5 - min_whale_ratio)) * 80 + 20)
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
        # ENHANCED COMBO CHART - FIXED VERSION
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
        
        # 1. Price Chart with Anomaly Markers
        fig.add_trace(
            go.Candlestick(
                x=stock_data['Last Trading Date'],
                open=stock_data['Open Price'],
                high=stock_data['High'],
                low=stock_data['Low'],
                close=stock_data['Close'],
                name='OHLC',
                increasing_line_color='#2ecc71',
                decreasing_line_color='#e74c3c'
            ),
            row=1, col=1
        )
        
        # Whale Signals - FIXED: menggunakan proper customdata
        whale_signals = stock_data[stock_data['Whale_Signal']]
        if not whale_signals.empty and 'High' in whale_signals.columns:
            whale_customdata = whale_signals[['AOV_Ratio']].values
            fig.add_trace(
                go.Scatter(
                    x=whale_signals['Last Trading Date'],
                    y=whale_signals['High'] * 1.01,
                    mode='markers',
                    marker=dict(
                        symbol='triangle-up',
                        size=15,
                        color='#00cc00',
                        line=dict(width=2, color='black')
                    ),
                    name='Whale Signal',
                    hovertemplate='<b>🐋 WHALE ENTRY</b><br>Date: %{x}<br>AOV Ratio: %{customdata[0]:.2f}x<extra></extra>',
                    customdata=whale_customdata
                ),
                row=1, col=1
            )
        
        # Split Signals - FIXED: menggunakan proper customdata
        split_signals = stock_data[stock_data['Split_Signal']]
        if not split_signals.empty and 'Low' in split_signals.columns:
            split_customdata = split_signals[['AOV_Ratio']].values
            fig.add_trace(
                go.Scatter(
                    x=split_signals['Last Trading Date'],
                    y=split_signals['Low'] * 0.99,
                    mode='markers',
                    marker=dict(
                        symbol='triangle-down',
                        size=15,
                        color='#ff4444',
                        line=dict(width=2, color='black')
                    ),
                    name='Split Signal',
                    hovertemplate='<b>⚡ RETAIL DOMINANT</b><br>Date: %{x}<br>AOV Ratio: %{customdata[0]:.2f}x<extra></extra>',
                    customdata=split_customdata
                ),
                row=1, col=1
            )
        
        # 2. Volume Bar Chart with Color Coding
        vol_colors = []
        for ratio in stock_data['AOV_Ratio']:
            if ratio >= min_whale_ratio:
                vol_colors.append('#00cc00')
            elif ratio <= 0.6 and ratio > 0:
                vol_colors.append('#ff4444')
            else:
                vol_colors.append('#718096')
        
        # FIXED: customdata untuk volume bar
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
        
        # 3. AOV Ratio Line Chart - FIXED: customdata sebagai array 2D
        aov_customdata = np.column_stack([
            stock_data['Avg_Order_Volume'].values,
            stock_data['MA30_AOVol'].values
        ])
        
        fig.add_trace(
            go.Scatter(
                x=stock_data['Last Trading Date'],
                y=stock_data['AOV_Ratio'],
                mode='lines+markers',
                line=dict(color='#9c88ff', width=2),
                name='AOV Ratio',
                hovertemplate='<b>AOV Ratio</b>: %{y:.2f}x<br>Avg: %{customdata[0]:.0f} | MA30: %{customdata[1]:.0f}<extra></extra>',
                customdata=aov_customdata
            ),
            row=3, col=1
        )
        
        # Add horizontal reference lines for AOV
        fig.add_hline(
            y=min_whale_ratio,
            line_dash="dash",
            line_color="#00cc00",
            opacity=0.5,
            annotation_text=f"Whale Threshold ({min_whale_ratio}x)",
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
            font=dict(size=12)
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
        
        # Create metrics columns
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
        
        # ======================================================================
        # HISTORICAL ANOMALY ANALYSIS
        # ======================================================================
        st.markdown("### 📈 Historical Anomaly Pattern")
        
        # Calculate anomaly statistics
        whale_days = stock_data['Whale_Signal'].sum()
        split_days = stock_data['Split_Signal'].sum()
        total_days = len(stock_data)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            fig1 = go.Figure(data=[
                go.Indicator(
                    mode="gauge+number",
                    value=(whale_days / total_days * 100) if total_days > 0 else 0,
                    title={'text': "Whale Days %"},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "#00cc00"},
                        'steps': [
                            {'range': [0, 30], 'color': "#e6fffa"},
                            {'range': [30, 70], 'color': "#b2f5ea"},
                            {'range': [70, 100], 'color': "#00cc00"}
                        ]
                    }
                )
            ])
            fig1.update_layout(height=200, margin=dict(t=30, b=10, l=10, r=10))
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            fig2 = go.Figure(data=[
                go.Indicator(
                    mode="gauge+number",
                    value=(split_days / total_days * 100) if total_days > 0 else 0,
                    title={'text': "Retail Days %"},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "#ff4444"},
                        'steps': [
                            {'range': [0, 30], 'color': "#fff5f5"},
                            {'range': [30, 70], 'color': "#fed7d7"},
                            {'range': [70, 100], 'color': "#ff4444"}
                        ]
                    }
                )
            ])
            fig2.update_layout(height=200, margin=dict(t=30, b=10, l=10, r=10))
            st.plotly_chart(fig2, use_container_width=True)
        
        with col3:
            # Average AOV Ratio
            avg_aov = stock_data['AOV_Ratio'].mean()
            fig3 = go.Figure(data=[
                go.Indicator(
                    mode="number+gauge",
                    value=avg_aov,
                    title={'text': "Avg AOV Ratio"},
                    gauge={
                        'axis': {'range': [0, 5]},
                        'bar': {'color': "#9c88ff"},
                        'steps': [
                            {'range': [0, 0.6], 'color': "#fff5f5"},
                            {'range': [0.6, 1.5], 'color': "#f7fafc"},
                            {'range': [1.5, 5], 'color': "#e6fffa"}
                        ],
                        'threshold': {
                            'line': {'color': "black", 'width': 4},
                            'thickness': 0.75,
                            'value': min_whale_ratio
                        }
                    }
                )
            ])
            fig3.update_layout(height=200, margin=dict(t=30, b=10, l=10, r=10))
            st.plotly_chart(fig3, use_container_width=True)

# ==============================================================================
# TAB 2: WHALE SCREENER
# ==============================================================================
with tab2:
    st.markdown(f"### 🐋 Whale Detection Screener ({selected_date.strftime('%d %b %Y')})")
    
    # Get data for selected date
    df_daily = df[df['Last Trading Date'] == selected_date].copy()
    
    if df_daily.empty:
        st.warning(f"No data available for {selected_date.strftime('%d %b %Y')}")
    else:
        # Apply filters
        filters_applied = []
        
        # Whale filter
        whale_filter = (df_daily['AOV_Ratio'] >= min_whale_ratio)
        filters_applied.append(f"AOV Ratio ≥ {min_whale_ratio}x")
        
        # Value filter
        value_filter = (df_daily['Value'] >= min_value_rp)
        filters_applied.append(f"Value ≥ Rp {min_value_rp:,.0f}")
        
        # Frequency filter
        freq_filter = (df_daily['Frequency'] >= min_frequency)
        filters_applied.append(f"Frequency ≥ {min_frequency}")
        
        # Sector filter
        if 'selected_sector' in locals() and selected_sector != 'All':
            sector_filter = (df_daily['Sector'] == selected_sector)
            filters_applied.append(f"Sector = {selected_sector}")
        else:
            sector_filter = pd.Series([True] * len(df_daily))
        
        # Combine filters
        mask = whale_filter & value_filter & freq_filter & sector_filter
        suspects = df_daily[mask].copy()
        
        # Display filter summary
        st.markdown(f"""
        <div class="metric-card">
            <div class="medium-text">Filters Applied:</div>
            <div class="small-text">{' • '.join(filters_applied)}</div>
            <div class="value-text">{len(suspects)} stocks detected</div>
        </div>
        """, unsafe_allow_html=True)
        
        if not suspects.empty:
            # Sort by conviction
            suspects['Conviction_Score'] = ((suspects['AOV_Ratio'] - min_whale_ratio) / (5 - min_whale_ratio)) * 80 + 20
            suspects['Conviction_Score'] = suspects['Conviction_Score'].clip(0, 100)
            suspects = suspects.sort_values('Conviction_Score', ascending=False)
            
            # Display results
            display_cols = [
                'Stock Code', 'Close', 'Change %', 'Volume',
                'Avg_Order_Volume', 'AOV_Ratio', 'Conviction_Score', 'Value',
                'Frequency'
            ]
            
            # Add company name if available
            if 'Company Name' in suspects.columns:
                display_cols.insert(1, 'Company Name')
            
            # Add sector if available
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
                'Conviction_Score': '{:.0f}%',
                'Value': 'Rp {:,.0f}',
                'Frequency': '{:,.0f}'
            })
            
            # Apply color gradient
            styled_df = styled_df.background_gradient(
                subset=['AOV_Ratio', 'Conviction_Score'],
                cmap='Greens'
            )
            
            # Highlight positive/negative changes
            def color_change(val):
                if isinstance(val, str):
                    if '+' in val and '%' in val:
                        return 'color: #00cc00'
                    elif '-' in val and '%' in val:
                        return 'color: #ff4444'
                return ''
            
            styled_df = styled_df.map(color_change, subset=['Change %'])
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                height=600
            )
            
            # Download option
            csv = suspects.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Whale List",
                data=csv,
                file_name=f"whale_detection_{selected_date.strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            
            # Top 3 whales visualization
            st.markdown("### 🏆 Top Whales")
            
            if len(suspects) >= 3:
                top_whales = suspects.head(3)
                
                cols = st.columns(3)
                for idx, (_, whale) in enumerate(top_whales.iterrows()):
                    with cols[idx]:
                        company_display = whale.get('Company Name', 'N/A')
                        if isinstance(company_display, str) and len(company_display) > 30:
                            company_display = company_display[:30] + '...'
                        
                        st.markdown(f"""
                        <div class="whale-card">
                            <div class="big-text">{whale['Stock Code']}</div>
                            <div class="medium-text">{company_display}</div>
                            <div class="value-text">Rp {whale.get('Close', 0):,.0f}</div>
                            <div class="small-text">
                                AOV Ratio: <b>{whale.get('AOV_Ratio', 0):.2f}x</b><br>
                                Conviction: <b>{whale.get('Conviction_Score', 0):.0f}%</b><br>
                                Volume: {whale.get('Volume', 0):,.0f} lots
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
        else:
            st.info("🚫 No whales detected with current filters. Try adjusting parameters.")

# ==============================================================================
# TAB 3: MARKET OVERVIEW
# ==============================================================================
with tab3:
    st.markdown("### 📊 Market Overview")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Market heatmap
        st.markdown("#### 🔥 Market Heatmap")
        
        if 'Sector' in df.columns and 'Change %' in df.columns:
            # Use latest date data
            sector_data = latest_df.copy()
            sector_perf = sector_data.groupby('Sector').agg({
                'Change %': 'mean',
                'Stock Code': 'count'
            }).reset_index()
            sector_perf.columns = ['Sector', 'Avg Change %', 'Stock Count']
            
            if not sector_perf.empty:
                fig = px.treemap(
                    sector_perf,
                    path=['Sector'],
                    values='Stock Count',
                    color='Avg Change %',
                    color_continuous_scale='RdYlGn',
                    color_continuous_midpoint=0
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Whale distribution
        st.markdown("#### 🐋 Whale Distribution")
        
        # Get last 30 days data
        last_30_days = df[df['Last Trading Date'] >= (latest_date - timedelta(days=30))]
        
        if not last_30_days.empty:
            whale_daily = last_30_days.groupby(last_30_days['Last Trading Date'].dt.date).apply(
                lambda x: (x['AOV_Ratio'] >= min_whale_ratio).sum()
            ).reset_index()
            whale_daily.columns = ['Date', 'Whale Count']
            
            if not whale_daily.empty:
                fig = px.area(
                    whale_daily,
                    x='Date',
                    y='Whale Count',
                    title='Whale Activity (30 Days)',
                    color_discrete_sequence=['#00cc00']
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        # AOV Ratio distribution
        st.markdown("#### 📈 AOV Ratio Distribution")
        
        if not latest_df.empty and 'AOV_Ratio' in latest_df.columns:
            fig = px.histogram(
                latest_df,
                x='AOV_Ratio',
                nbins=30,
                title='Distribution of AOV Ratios',
                color_discrete_sequence=['#9c88ff']
            )
            fig.add_vline(
                x=min_whale_ratio,
                line_dash="dash",
                line_color="#00cc00",
                annotation_text="Whale Threshold"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Market statistics
    st.markdown("### 📊 Market Statistics")
    
    stat_cols = st.columns(4)
    
    with stat_cols[0]:
        total_volume = latest_df['Volume'].sum() if not latest_df.empty else 0
        st.metric("Total Volume", f"{total_volume:,.0f} lots")
    
    with stat_cols[1]:
        avg_aov = latest_df['AOV_Ratio'].mean() if not latest_df.empty else 0
        st.metric("Avg AOV Ratio", f"{avg_aov:.2f}x")
    
    with stat_cols[2]:
        if not latest_df.empty:
            whale_count_today = len(latest_df[latest_df['AOV_Ratio'] >= min_whale_ratio])
            whale_percentage = (whale_count_today / len(latest_df)) * 100 if len(latest_df) > 0 else 0
            st.metric("Whale % Today", f"{whale_percentage:.1f}%")
    
    with stat_cols[3]:
        if 'Net Foreign' in latest_df.columns and not latest_df.empty:
            total_net_foreign = latest_df['Net Foreign'].sum()
            st.metric("Total Net Foreign", f"Rp {total_net_foreign:,.0f}")

# ==============================================================================
# 6. FOOTER
# ==============================================================================
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #718096; font-size: 12px;'>
    <p>🐋 Market Intelligence Dashboard v2.0 | Advanced Whale Detection System</p>
    <p>Data Source: Google Drive | Last Updated: {}</p>
</div>
""".format(latest_date.strftime('%d %b %Y %H:%M')), unsafe_allow_html=True)
