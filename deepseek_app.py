import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Google Drive imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# Set page config
st.set_page_config(
    page_title="Frequency Analyzer - Deteksi Gerak Bandar",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #3B82F6;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #3B82F6;
        margin-bottom: 1rem;
    }
    .signal-buy {
        color: #10B981;
        font-weight: bold;
    }
    .signal-sell {
        color: #EF4444;
        font-weight: bold;
    }
    .signal-neutral {
        color: #6B7280;
        font-weight: bold;
    }
    .anomaly-high {
        background-color: #FEF3C7;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-weight: bold;
    }
    .anomaly-extreme {
        background-color: #FEE2E2;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-weight: bold;
        color: #DC2626;
    }
    .stProgress > div > div > div > div {
        background-color: #3B82F6;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 🔐 GOOGLE DRIVE FUNCTIONS
# ==============================================================================

@st.cache_data(ttl=3600)
def load_service_account():
    """Load Google Service Account from secrets.toml"""
    try:
        secrets = st.secrets["gcp_service_account"]
        
        creds_dict = {
            "type": secrets["type"],
            "project_id": secrets["project_id"],
            "private_key_id": secrets["private_key_id"],
            "private_key": secrets["private_key"],
            "client_email": secrets["client_email"],
            "client_id": secrets["client_id"],
            "auth_uri": secrets["auth_uri"],
            "token_uri": secrets["token_uri"],
            "auth_provider_x509_cert_url": secrets["auth_provider_x509_cert_url"],
            "client_x509_cert_url": secrets["client_x509_cert_url"],
            "universe_domain": secrets["universe_domain"]
        }
        
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        
        return credentials
    except Exception as e:
        st.error(f"Error loading service account: {str(e)}")
        return None

@st.cache_data(ttl=1800)
def load_data_from_gdrive(file_name="Kompilasi_Data_1Tahun.csv"):
    """Load data from Google Drive"""
    try:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("🔐 Menghubungkan ke Google Drive...")
        progress_bar.progress(10)
        
        credentials = load_service_account()
        if credentials is None:
            return None
        
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
        progress_bar.progress(20)
        
        status_text.text("🔍 Mencari file data...")
        
        # Search for file
        query = f"name='{file_name}' and trashed=false"
        
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            corpora='allDrives',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            st.error(f"❌ File '{file_name}' tidak ditemukan di Google Drive")
            return None
        
        file_id = files[0]['id']
        progress_bar.progress(40)
        
        status_text.text(f"📥 Mengunduh {file_name}...")
        
        # Download file
        request = service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            progress_bar.progress(40 + int(status.progress() * 40))
        
        file_stream.seek(0)
        progress_bar.progress(80)
        
        status_text.text("📊 Memproses data...")
        
        # Load CSV
        df = pd.read_csv(file_stream)
        progress_bar.progress(95)
        
        # Convert date column
        if 'Last Trading Date' in df.columns:
            df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'], errors='coerce')
        
        progress_bar.progress(100)
        status_text.text("✅ Data berhasil dimuat!")
        
        progress_bar.empty()
        status_text.empty()
        
        st.success(f"✅ Data berhasil dimuat: {len(df):,} baris, {df['Stock Code'].nunique()} saham")
        
        return df
        
    except Exception as e:
        st.error(f"❌ Error loading data from Google Drive: {str(e)}")
        return None

# ==============================================================================
# 📊 ADVANCED DATA PROCESSING FUNCTIONS
# ==============================================================================

@st.cache_data
def process_advanced_data(df):
    """Process data with advanced anomaly detection"""
    if df is None or len(df) == 0:
        return None
    
    df = df.copy()
    
    # Sort by date
    df = df.sort_values(['Stock Code', 'Last Trading Date'])
    
    # ====================================================
    # 1. FREQUENCY ANALYSIS
    # ====================================================
    df['Freq_MA20'] = df.groupby('Stock Code')['Frequency'].transform(
        lambda x: x.rolling(20, min_periods=5).mean()
    )
    
    df['Freq_Spike_Ratio'] = np.where(
        df['Freq_MA20'] > 0,
        df['Frequency'] / df['Freq_MA20'],
        1
    )
    
    df['Freq_Spike_Category'] = pd.cut(
        df['Freq_Spike_Ratio'],
        bins=[0, 1.5, 2.5, 5, np.inf],
        labels=['Normal', 'Medium', 'High', 'Extreme'],
        include_lowest=True
    )
    
    # ====================================================
    # 2. VOLUME ANALYSIS
    # ====================================================
    if 'Volume' in df.columns:
        df['Volume_MA20'] = df.groupby('Stock Code')['Volume'].transform(
            lambda x: x.rolling(20, min_periods=5).mean()
        )
        df['Volume_Spike_Ratio'] = np.where(
            df['Volume_MA20'] > 0,
            df['Volume'] / df['Volume_MA20'],
            1
        )
    
    # ====================================================
    # 3. AVG_ORDER_VOLUME ANALYSIS (NEW - VERY IMPORTANT!)
    # ====================================================
    if 'Avg_Order_Volume' in df.columns and 'MA30_AOVol' in df.columns:
        # Calculate anomaly ratio
        df['AOV_Anomaly_Ratio'] = np.where(
            df['MA30_AOVol'] > 0,
            df['Avg_Order_Volume'] / df['MA30_AOVol'],
            1
        )
        
        # Categorize anomaly level
        df['AOV_Anomaly_Level'] = pd.cut(
            df['AOV_Anomaly_Ratio'],
            bins=[0, 1.2, 1.8, 2.5, np.inf],
            labels=['Normal', 'Low Anomaly', 'Medium Anomaly', 'High Anomaly'],
            include_lowest=True
        )
        
        # Flag significant anomalies (> 2x MA30)
        df['Big_Player_Flag'] = df['AOV_Anomaly_Ratio'] > 2.0
        
        # Calculate trend of AOV (rising/falling)
        df['AOV_Trend_5D'] = df.groupby('Stock Code')['Avg_Order_Volume'].transform(
            lambda x: x.rolling(5, min_periods=3).mean().pct_change()
        )
        
    # ====================================================
    # 4. PRICE POSITION ANALYSIS
    # ====================================================
    # Calculate relative price position (0-1 scale)
    df['Price_Position'] = df.groupby('Stock Code')['Close'].transform(
        lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() > x.min() else 0.5
    )
    
    df['Position_Category'] = pd.cut(
        df['Price_Position'],
        bins=[0, 0.3, 0.7, 1],
        labels=['Bottom', 'Middle', 'Top'],
        include_lowest=True
    )
    
    # ====================================================
    # 5. BID/OFFER IMBALANCE ANALYSIS
    # ====================================================
    if 'Bid/Offer Imbalance' in df.columns:
        df['BOI_Category'] = pd.cut(
            df['Bid/Offer Imbalance'],
            bins=[-1, -0.3, 0.3, 1],
            labels=['Sell Pressure', 'Neutral', 'Buy Pressure'],
            include_lowest=True
        )
    
    # ====================================================
    # 6. COMPOSITE SIGNAL SCORE
    # ====================================================
    # Create a composite score for smart money detection
    signal_score = 0
    
    # Component 1: Frequency Spike
    if 'Freq_Spike_Ratio' in df.columns:
        signal_score += np.where(df['Freq_Spike_Ratio'] > 1.5, 1, 0)
        signal_score += np.where(df['Freq_Spike_Ratio'] > 2.0, 1, 0)
    
    # Component 2: AOV Anomaly (Most Important!)
    if 'AOV_Anomaly_Ratio' in df.columns:
        signal_score += np.where(df['AOV_Anomaly_Ratio'] > 1.5, 2, 0)
        signal_score += np.where(df['AOV_Anomaly_Ratio'] > 2.0, 3, 0)
    
    # Component 3: Price Position (Bottom is best)
    signal_score += np.where(df['Position_Category'] == 'Bottom', 2, 0)
    signal_score += np.where(df['Position_Category'] == 'Middle', 1, 0)
    
    # Component 4: Bid/Offer Imbalance
    if 'Bid/Offer Imbalance' in df.columns:
        signal_score += np.where(df['Bid/Offer Imbalance'] > 0.2, 1, 0)
    
    df['Smart_Money_Score'] = signal_score
    
    # Categorize composite signal
    df['Composite_Signal'] = pd.cut(
        df['Smart_Money_Score'],
        bins=[-1, 2, 4, 6, np.inf],
        labels=['Weak', 'Moderate', 'Strong', 'Very Strong'],
        include_lowest=True
    )
    
    return df

# ==============================================================================
# 📈 ADVANCED VISUALIZATION FUNCTIONS
# ==============================================================================

def create_advanced_stock_chart(stock_data, stock_code, company_name):
    """Create advanced chart with Avg_Order_Volume analysis"""
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(
            f'{company_name} ({stock_code}) - Harga & Volume',
            'Frekuensi Transaksi',
            'Avg Order Volume Analysis',
            'Smart Money Indicators'
        ),
        row_heights=[0.3, 0.25, 0.25, 0.2]
    )
    
    # 1. Price and Volume
    fig.add_trace(
        go.Candlestick(
            x=stock_data['Last Trading Date'],
            open=stock_data['Open Price'],
            high=stock_data['High'],
            low=stock_data['Low'],
            close=stock_data['Close'],
            name='Harga',
            showlegend=False
        ),
        row=1, col=1
    )
    
    # Add volume bars
    fig.add_trace(
        go.Bar(
            x=stock_data['Last Trading Date'],
            y=stock_data['Volume'],
            name='Volume',
            marker_color='rgba(100, 149, 237, 0.6)',
            showlegend=False,
            yaxis='y2'
        ),
        row=1, col=1
    )
    
    # 2. Frequency
    fig.add_trace(
        go.Scatter(
            x=stock_data['Last Trading Date'],
            y=stock_data['Frequency'],
            mode='lines',
            name='Frekuensi',
            line=dict(color='green', width=2),
            showlegend=False
        ),
        row=2, col=1
    )
    
    if 'Freq_MA20' in stock_data.columns:
        fig.add_trace(
            go.Scatter(
                x=stock_data['Last Trading Date'],
                y=stock_data['Freq_MA20'],
                mode='lines',
                name='MA20 Frekuensi',
                line=dict(color='orange', width=1, dash='dash'),
                showlegend=False
            ),
            row=2, col=1
        )
    
    # Highlight frequency spikes
    if 'Freq_Spike_Ratio' in stock_data.columns:
        spike_mask = stock_data['Freq_Spike_Ratio'] > 2.0
        if spike_mask.any():
            fig.add_trace(
                go.Scatter(
                    x=stock_data.loc[spike_mask, 'Last Trading Date'],
                    y=stock_data.loc[spike_mask, 'Frequency'],
                    mode='markers',
                    name='Freq Spike (>2x)',
                    marker=dict(color='red', size=8, symbol='triangle-up'),
                    showlegend=False
                ),
                row=2, col=1
            )
    
    # 3. Avg Order Volume Analysis (NEW - KEY CHART)
    if 'Avg_Order_Volume' in stock_data.columns:
        # Current AOV
        fig.add_trace(
            go.Scatter(
                x=stock_data['Last Trading Date'],
                y=stock_data['Avg_Order_Volume'],
                mode='lines',
                name='Avg Order Volume',
                line=dict(color='purple', width=3),
                showlegend=False
            ),
            row=3, col=1
        )
        
        # MA30 AOV
        if 'MA30_AOVol' in stock_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=stock_data['Last Trading Date'],
                    y=stock_data['MA30_AOVol'],
                    mode='lines',
                    name='MA30 AOV',
                    line=dict(color='orange', width=2, dash='dash'),
                    showlegend=False
                ),
                row=3, col=1
            )
        
        # Highlight anomalies (> 2x MA30)
        if 'Big_Player_Flag' in stock_data.columns:
            anomaly_mask = stock_data['Big_Player_Flag']
            if anomaly_mask.any():
                fig.add_trace(
                    go.Scatter(
                        x=stock_data.loc[anomaly_mask, 'Last Trading Date'],
                        y=stock_data.loc[anomaly_mask, 'Avg_Order_Volume'],
                        mode='markers',
                        name='Big Player Anomaly',
                        marker=dict(color='gold', size=12, symbol='star'),
                        showlegend=False
                    ),
                    row=3, col=1
                )
    
    # 4. Smart Money Indicators
    # Show composite score
    if 'Smart_Money_Score' in stock_data.columns:
        fig.add_trace(
            go.Bar(
                x=stock_data['Last Trading Date'],
                y=stock_data['Smart_Money_Score'],
                name='Smart Money Score',
                marker_color='rgba(46, 204, 113, 0.7)',
                showlegend=False
            ),
            row=4, col=1
        )
    
    # Update layout
    fig.update_layout(
        height=900,
        showlegend=False,
        hovermode='x unified',
        xaxis_rangeslider_visible=False
    )
    
    # Update y-axes labels
    fig.update_yaxes(title_text="Harga (Rp)", row=1, col=1)
    fig.update_yaxes(title_text="Frekuensi", row=2, col=1)
    
    if 'Avg_Order_Volume' in stock_data.columns:
        fig.update_yaxes(title_text="Avg Order Volume", row=3, col=1)
    
    fig.update_yaxes(title_text="Smart Money Score", row=4, col=1)
    
    # Secondary y-axis for volume
    fig.update_layout(
        yaxis2=dict(
            title="Volume",
            overlaying="y",
            side="right",
            showgrid=False
        )
    )
    
    return fig

def create_aov_heatmap(df):
    """Create heatmap of Avg Order Volume anomalies"""
    if 'Avg_Order_Volume' not in df.columns or 'MA30_AOVol' not in df.columns:
        return None
    
    # Get latest data for each stock
    latest_df = df.sort_values('Last Trading Date').groupby('Stock Code').last().reset_index()
    
    # Calculate anomaly intensity
    latest_df['AOV_Anomaly_Intensity'] = latest_df['Avg_Order_Volume'] / latest_df['MA30_AOVol']
    
    # Create pivot table for heatmap
    pivot_data = latest_df.pivot_table(
        index='Sector',
        columns='Position_Category',
        values='AOV_Anomaly_Intensity',
        aggfunc='mean',
        fill_value=1
    )
    
    # Sort sectors by total anomaly
    sector_anomaly = latest_df.groupby('Sector')['AOV_Anomaly_Intensity'].mean().sort_values(ascending=False)
    pivot_data = pivot_data.reindex(sector_anomaly.index)
    
    fig = px.imshow(
        pivot_data,
        text_auto=".2f",
        color_continuous_scale='RdYlGn_r',  # Red = high anomaly, Green = normal
        title='Heatmap: Avg Order Volume Anomaly by Sector & Price Position',
        labels=dict(color="Anomaly Ratio (AOV/MA30)")
    )
    
    fig.update_layout(height=500)
    return fig

# ==============================================================================
# 🎯 SMART MONEY DETECTION FUNCTIONS
# ==============================================================================

def detect_smart_money_patterns(df):
    """Detect sophisticated smart money patterns"""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    
    # Get latest data for each stock
    latest_df = df.sort_values('Last Trading Date').groupby('Stock Code').last().reset_index()
    
    # Define smart money detection criteria
    conditions = []
    
    # CRITERIA 1: High AOV Anomaly (> 2x MA30)
    if 'AOV_Anomaly_Ratio' in latest_df.columns:
        condition1 = latest_df['AOV_Anomaly_Ratio'] > 2.0
        conditions.append(('High_AOV_Anomaly', condition1))
    
    # CRITERIA 2: Frequency Spike with High AOV
    if 'Freq_Spike_Ratio' in latest_df.columns and 'AOV_Anomaly_Ratio' in latest_df.columns:
        condition2 = (latest_df['Freq_Spike_Ratio'] > 1.5) & (latest_df['AOV_Anomaly_Ratio'] > 1.5)
        conditions.append(('Freq_AOV_Combo', condition2))
    
    # CRITERIA 3: Bottom position with AOV anomaly
    if 'Position_Category' in latest_df.columns and 'AOV_Anomaly_Ratio' in latest_df.columns:
        condition3 = (latest_df['Position_Category'] == 'Bottom') & (latest_df['AOV_Anomaly_Ratio'] > 1.8)
        conditions.append(('Bottom_Accumulation', condition3))
    
    # CRITERIA 4: Strong bid pressure with AOV anomaly
    if 'Bid/Offer Imbalance' in latest_df.columns and 'AOV_Anomaly_Ratio' in latest_df.columns:
        condition4 = (latest_df['Bid/Offer Imbalance'] > 0.3) & (latest_df['AOV_Anomaly_Ratio'] > 1.5)
        conditions.append(('Bid_Pressure_AOV', condition4))
    
    # CRITERIA 5: Extreme composite signal
    if 'Composite_Signal' in latest_df.columns:
        condition5 = latest_df['Composite_Signal'].isin(['Strong', 'Very Strong'])
        conditions.append(('Strong_Composite', condition5))
    
    # Combine all conditions
    if not conditions:
        return pd.DataFrame()
    
    # Create detection results
    detection_results = []
    
    for pattern_name, condition in conditions:
        detected_stocks = latest_df[condition].copy()
        detected_stocks['Pattern'] = pattern_name
        detection_results.append(detected_stocks)
    
    if detection_results:
        all_detections = pd.concat(detection_results, ignore_index=True)
        
        # Aggregate by stock
        detection_summary = all_detections.groupby('Stock Code').agg({
            'Company Name': 'first',
            'Pattern': lambda x: ', '.join(x),
            'Close': 'first',
            'Frequency': 'first',
            'Avg_Order_Volume': 'first',
            'MA30_AOVol': 'first',
            'AOV_Anomaly_Ratio': 'first',
            'Position_Category': 'first',
            'Smart_Money_Score': 'first',
            'Composite_Signal': 'first'
        }).reset_index()
        
        # Count patterns per stock
        pattern_counts = all_detections.groupby('Stock Code').size().reset_index(name='Pattern_Count')
        detection_summary = detection_summary.merge(pattern_counts, on='Stock Code')
        
        # Sort by number of patterns detected
        detection_summary = detection_summary.sort_values(['Pattern_Count', 'Smart_Money_Score'], ascending=False)
        
        return detection_summary
    
    return pd.DataFrame()

# ==============================================================================
# 🎯 MAIN APP WITH ADVANCED ANALYTICS
# ==============================================================================

def main():
    # Title
    st.markdown("<h1 class='main-header'>📈 Advanced Frequency Analyzer - Smart Money Detector</h1>", unsafe_allow_html=True)
    st.markdown("**Deteksi anomali Avg Order Volume & pola akumulasi bandar canggih**")
    
    # Initialize session state
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'df_raw' not in st.session_state:
        st.session_state.df_raw = None
    if 'df_processed' not in st.session_state:
        st.session_state.df_processed = None
    
    # Sidebar
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/3135/3135715.png", width=100)
        st.title("🎯 Kontrol Panel")
        
        st.divider()
        
        # Data loading
        st.subheader("📂 Load Data")
        
        if st.button("🚀 Load Data dari Google Drive", type="primary", use_container_width=True):
            with st.spinner("Memuat data dari Google Drive..."):
                st.session_state.df_raw = load_data_from_gdrive()
                if st.session_state.df_raw is not None:
                    st.session_state.df_processed = process_advanced_data(st.session_state.df_raw)
                    st.session_state.data_loaded = True
                    st.rerun()
        
        if st.button("🔄 Clear Cache & Reset", use_container_width=True):
            st.cache_data.clear()
            st.session_state.clear()
            st.rerun()
        
        st.divider()
        
        # Analysis parameters
        if st.session_state.data_loaded:
            st.subheader("⚙️ Parameter Analisis")
            
            # Date range
            min_date = st.session_state.df_raw['Last Trading Date'].min().date()
            max_date = st.session_state.df_raw['Last Trading Date'].max().date()
            
            date_range = st.date_input(
                "Rentang Tanggal:",
                value=(max_date - timedelta(days=60), max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            # Sector filter
            if 'Sector' in st.session_state.df_raw.columns:
                sectors = ['Semua'] + sorted(st.session_state.df_raw['Sector'].dropna().unique().tolist())
                selected_sector = st.selectbox("Sektor:", sectors)
            
            # AOV Anomaly Threshold
            st.divider()
            st.subheader("🎯 Threshold Deteksi")
            
            aov_threshold = st.slider(
                "AOV Anomaly Threshold (x MA30):",
                min_value=1.0,
                max_value=4.0,
                value=2.0,
                step=0.1,
                help="Avg_Order_Volume / MA30_AOVol"
            )
            
            freq_threshold = st.slider(
                "Frekuensi Spike Threshold (x MA20):",
                min_value=1.0,
                max_value=5.0,
                value=1.8,
                step=0.1
            )
    
    # Main content
    if not st.session_state.data_loaded:
        # Welcome screen
        st.info("👈 Klik **'Load Data dari Google Drive'** di sidebar untuk memulai")
        
        # Show features
        st.subheader("✨ Fitur Analisis Baru")
        
        features = """
        ### 🔍 **Analisis Avg Order Volume (AOV)**
        
        **AOV = Volume / Frequency** → Rata-rata lot per transaksi
        
        **Deteksi Anomali:**
        1. **AOV hari ini vs MA30_AOVol** → Deteksi perubahan pola trading
        2. **AOV tinggi** = Transaksi besar per order (institusi)
        3. **AOV rendah** = Transaksi kecil-kecilan (retail)
        
        ### 🎯 **Smart Money Patterns:**
        
        **Pola 1: Silent Accumulation**
        - Frekuensi tinggi
        - AOV tinggi (>2x MA30)
        - Posisi harga di bottom
        - Harga sideways/datar
        
        **Pola 2: Big Player Entry**
        - AOV anomaly extreme (>3x MA30)
        - Volume spike
        - Strong bid pressure
        
        **Pola 3: Distribution at Top**
        - AOV tinggi di harga top
        - Frekuensi meledak
        - Price rejection/shooting star
        """
        
        st.markdown(features)
        
        # Quick stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Database", "Google Drive", "Auto-sync")
        with col2:
            st.metric("Analisis", "Avg Order Volume", "Smart Money Detection")
        with col3:
            st.metric("Indikator", "15+ Metrics", "Real-time")
        
    else:
        # Data is loaded, show analysis
        df = st.session_state.df_processed
        
        # Apply filters
        if 'date_range' in locals() and len(date_range) == 2:
            mask = (df['Last Trading Date'].dt.date >= date_range[0]) & \
                   (df['Last Trading Date'].dt.date <= date_range[1])
            df = df[mask]
        
        if 'selected_sector' in locals() and selected_sector != 'Semua':
            df = df[df['Sector'] == selected_sector]
        
        # Create tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Dashboard", 
            "🎯 Smart Money Scanner", 
            "📈 Detail Saham", 
            "🔥 AOV Heatmap",
            "📋 Data Insights"
        ])
        
        with tab1:
            st.markdown("<h2 class='sub-header'>Smart Money Dashboard</h2>", unsafe_allow_html=True)
            
            # Key metrics with AOV focus
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                total_stocks = df['Stock Code'].nunique()
                st.metric("Total Saham", f"{total_stocks:,}")
            
            with col2:
                if 'AOV_Anomaly_Ratio' in df.columns:
                    high_aov = (df['AOV_Anomaly_Ratio'] > aov_threshold).sum()
                    st.metric(f"AOV Anomaly (> {aov_threshold}x)", f"{high_aov:,}")
            
            with col3:
                if 'Big_Player_Flag' in df.columns:
                    big_players = df['Big_Player_Flag'].sum()
                    st.metric("Big Player Detected", f"{big_players:,}")
            
            with col4:
                if 'Composite_Signal' in df.columns:
                    strong_signals = (df['Composite_Signal'].isin(['Strong', 'Very Strong'])).sum()
                    st.metric("Strong Signals", f"{strong_signals:,}")
            
            with col5:
                bottom_anomaly = ((df['Position_Category'] == 'Bottom') & 
                                 (df['AOV_Anomaly_Ratio'] > 1.8)).sum() if 'AOV_Anomaly_Ratio' in df.columns else 0
                st.metric("Bottom Accumulation", f"{bottom_anomaly:,}")
            
            st.divider()
            
            # Top AOV Anomaly Stocks
            st.subheader("🚨 Top 10 Saham dengan AOV Anomaly Tertinggi")
            
            # Get latest data
            latest_df = df.sort_values('Last Trading Date').groupby('Stock Code').last().reset_index()
            
            if 'AOV_Anomaly_Ratio' in latest_df.columns:
                top_aov = latest_df.nlargest(10, 'AOV_Anomaly_Ratio')[
                    ['Stock Code', 'Company Name', 'Avg_Order_Volume', 'MA30_AOVol',
                     'AOV_Anomaly_Ratio', 'AOV_Anomaly_Level', 'Close', 
                     'Position_Category', 'Composite_Signal']
                ].copy()
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    fig = px.bar(
                        top_aov,
                        x='Stock Code',
                        y='AOV_Anomaly_Ratio',
                        color='Position_Category',
                        color_discrete_map={'Bottom': '#10B981', 'Middle': '#F59E0B', 'Top': '#EF4444'},
                        title='Top AOV Anomaly Ratio (AOV / MA30)',
                        labels={'AOV_Anomaly_Ratio': 'Anomaly Ratio', 'Position_Category': 'Posisi Harga'}
                    )
                    fig.update_layout(height=400, showlegend=True)
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Format for display
                    display_df = top_aov.copy()
                    display_df['AOV_Anomaly'] = display_df['AOV_Anomaly_Ratio'].apply(
                        lambda x: f"<span class='anomaly-extreme'>{x:.2f}x</span>" if x > 2.5 else 
                                 f"<span class='anomaly-high'>{x:.2f}x</span>" if x > 1.8 else f"{x:.2f}x"
                    )
                    display_df['Close'] = display_df['Close'].apply(lambda x: f"Rp {x:,.0f}")
                    
                    st.markdown("""
                    <style>
                    .dataframe th { text-align: left; }
                    .dataframe td { font-size: 0.9em; }
                    </style>
                    """, unsafe_allow_html=True)
                    
                    st.dataframe(
                        display_df[['Stock Code', 'AOV_Anomaly', 'Position_Category', 'Composite_Signal']],
                        use_container_width=True,
                        height=400
                    )
            
            st.divider()
            
            # Composite Signal Distribution
            st.subheader("📊 Distribution of Smart Money Signals")
            
            if 'Composite_Signal' in df.columns:
                signal_dist = df['Composite_Signal'].value_counts().reset_index()
                signal_dist.columns = ['Signal', 'Count']
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    fig = px.pie(
                        signal_dist,
                        values='Count',
                        names='Signal',
                        color='Signal',
                        color_discrete_map={
                            'Weak': '#EF4444',
                            'Moderate': '#F59E0B',
                            'Strong': '#10B981',
                            'Very Strong': '#047857'
                        },
                        title='Distribusi Smart Money Signal',
                        hole=0.4
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.dataframe(
                        signal_dist.style.format({'Count': '{:,}'}),
                        use_container_width=True,
                        height=400
                    )
        
        with tab2:
            st.markdown("<h2 class='sub-header'>🎯 Advanced Smart Money Scanner</h2>", unsafe_allow_html=True)
            
            # Scanner configuration
            col1, col2, col3 = st.columns(3)
            
            with col1:
                min_aov_ratio = st.number_input(
                    "Min AOV Anomaly Ratio:",
                    min_value=1.0,
                    max_value=5.0,
                    value=2.0,
                    step=0.1,
                    key="scanner_aov"
                )
            
            with col2:
                min_smart_score = st.number_input(
                    "Min Smart Money Score:",
                    min_value=0,
                    max_value=10,
                    value=4,
                    step=1,
                    key="scanner_score"
                )
            
            with col3:
                position_filter = st.selectbox(
                    "Posisi Harga:",
                    ['Semua', 'Bottom', 'Middle', 'Top'],
                    key="scanner_position"
                )
            
            # Run smart money detection
            if st.button("🔍 Scan Smart Money Patterns", type="primary"):
                with st.spinner("Scanning for smart money patterns..."):
                    smart_money_stocks = detect_smart_money_patterns(df)
                    
                    if not smart_money_stocks.empty:
                        # Apply additional filters
                        filtered_stocks = smart_money_stocks.copy()
                        
                        if 'AOV_Anomaly_Ratio' in filtered_stocks.columns:
                            filtered_stocks = filtered_stocks[filtered_stocks['AOV_Anomaly_Ratio'] >= min_aov_ratio]
                        
                        if 'Smart_Money_Score' in filtered_stocks.columns:
                            filtered_stocks = filtered_stocks[filtered_stocks['Smart_Money_Score'] >= min_smart_score]
                        
                        if position_filter != 'Semua':
                            filtered_stocks = filtered_stocks[filtered_stocks['Position_Category'] == position_filter]
                        
                        st.success(f"✅ Found {len(filtered_stocks)} stocks with smart money patterns!")
                        
                        # Display results
                        if len(filtered_stocks) > 0:
                            # Format for display
                            display_cols = [
                                'Stock Code', 'Company Name', 'Pattern', 'Pattern_Count',
                                'AOV_Anomaly_Ratio', 'Smart_Money_Score', 'Composite_Signal',
                                'Position_Category', 'Close'
                            ]
                            
                            display_df = filtered_stocks[display_cols].copy()
                            
                            # Format columns
                            display_df['AOV_Anomaly_Ratio'] = display_df['AOV_Anomaly_Ratio'].apply(
                                lambda x: f"<span class='anomaly-extreme'>{x:.2f}x</span>" if x > 2.5 else 
                                         f"<span class='anomaly-high'>{x:.2f}x</span>" if x > 1.8 else f"{x:.2f}x"
                            )
                            
                            display_df['Close'] = display_df['Close'].apply(lambda x: f"Rp {x:,.0f}")
                            
                            # Sort by pattern count and score
                            display_df = display_df.sort_values(['Pattern_Count', 'Smart_Money_Score'], ascending=False)
                            
                            st.dataframe(
                                display_df,
                                use_container_width=True,
                                height=500
                            )
                            
                            # Allow selection for detailed view
                            selected_scanner_stocks = st.multiselect(
                                "Pilih saham untuk analisis detail:",
                                options=display_df['Stock Code'].unique(),
                                max_selections=3,
                                key="smart_scanner_select"
                            )
                            
                            if selected_scanner_stocks:
                                st.session_state.selected_stocks = selected_scanner_stocks
                                st.info(f"📈 Saham terpilih: {', '.join(selected_scanner_stocks)} - Buka tab 'Detail Saham'")
                        else:
                            st.warning("No stocks match the current filters.")
                    else:
                        st.warning("No smart money patterns detected with current data.")
            
            else:
                st.info("Klik 'Scan Smart Money Patterns' untuk memulai deteksi")
        
        with tab3:
            st.markdown("<h2 class='sub-header'>📈 Detail Analisis Saham</h2>", unsafe_allow_html=True)
            
            # Stock selection
            if 'selected_stocks' in st.session_state and len(st.session_state.selected_stocks) > 0:
                analyze_stocks = st.session_state.selected_stocks
            else:
                analyze_stocks = st.multiselect(
                    "Pilih saham untuk dianalisis:",
                    options=df['Stock Code'].unique(),
                    default=[],
                    key="detail_select"
                )
            
            if analyze_stocks:
                for stock_code in analyze_stocks:
                    stock_data = df[df['Stock Code'] == stock_code].sort_values('Last Trading Date')
                    
                    if len(stock_data) > 0:
                        company_name = stock_data['Company Name'].iloc[0] if 'Company Name' in stock_data.columns else stock_code
                        latest = stock_data.iloc[-1]
                        
                        # Stock header
                        st.divider()
                        st.subheader(f"{company_name} ({stock_code})")
                        
                        # Key metrics row with AOV focus
                        col1, col2, col3, col4, col5 = st.columns(5)
                        
                        with col1:
                            st.metric("Harga", f"Rp {latest['Close']:,.0f}")
                        
                        with col2:
                            if 'Avg_Order_Volume' in latest:
                                aov = latest['Avg_Order_Volume']
                                st.metric("Avg Order Vol", f"{aov:,.0f} lot")
                        
                        with col3:
                            if 'AOV_Anomaly_Ratio' in latest:
                                aov_ratio = latest['AOV_Anomaly_Ratio']
                                st.metric("AOV Anomaly", f"{aov_ratio:.2f}x", 
                                         "🔥 Extreme" if aov_ratio > 2.5 else 
                                         "🚨 High" if aov_ratio > 1.8 else "✅ Normal")
                        
                        with col4:
                            if 'Smart_Money_Score' in latest:
                                score = latest['Smart_Money_Score']
                                st.metric("Smart Money Score", f"{score}/10",
                                         "🎯 Strong" if score > 6 else "📊 Moderate" if score > 3 else "📉 Weak")
                        
                        with col5:
                            if 'Position_Category' in latest:
                                pos = latest['Position_Category']
                                color = "🟢" if pos == 'Bottom' else ("🟡" if pos == 'Middle' else "🔴")
                                st.metric("Posisi", f"{color} {pos}")
                        
                        # Create advanced chart
                        fig = create_advanced_stock_chart(stock_data, stock_code, company_name)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # AOV Analysis Details
                        st.subheader("📊 Avg Order Volume Analysis")
                        
                        if 'Avg_Order_Volume' in latest and 'MA30_AOVol' in latest:
                            aov_cols = st.columns(4)
                            
                            with aov_cols[0]:
                                ma30 = latest['MA30_AOVol']
                                st.metric("MA30 AOV", f"{ma30:,.0f} lot")
                            
                            with aov_cols[1]:
                                if 'AOV_Trend_5D' in latest:
                                    trend = latest['AOV_Trend_5D']
                                    st.metric("5D Trend", f"{trend:.1%}",
                                             "📈 Rising" if trend > 0.05 else "📉 Falling" if trend < -0.05 else "➡️ Stable")
                            
                            with aov_cols[2]:
                                if 'Big_Player_Flag' in latest:
                                    flag = latest['Big_Player_Flag']
                                    st.metric("Big Player", 
                                             "✅ DETECTED" if flag else "❌ Not Detected")
                            
                            with aov_cols[3]:
                                if 'AOV_Anomaly_Level' in latest:
                                    level = latest['AOV_Anomaly_Level']
                                    st.metric("Anomaly Level", str(level))
            
            else:
                st.info("Pilih saham untuk melihat analisis detail")
        
        with tab4:
            st.markdown("<h2 class='sub-header'>🔥 AOV Anomaly Heatmap</h2>", unsafe_allow_html=True)
            
            # Create AOV heatmap
            heatmap_fig = create_aov_heatmap(df)
            
            if heatmap_fig:
                st.plotly_chart(heatmap_fig, use_container_width=True)
                
                # Heatmap interpretation
                st.subheader("📖 Interpretasi Heatmap")
                
                interpretation = """
                **🔴 MERAH TUA** = **AOV Anomaly TINGGI** (Avg_Order_Volume > 3x MA30)
                - Kemungkinan besar **institusi/smart money** sedang aktif
                - Transaksi bernilai besar per order
                - **Waspada di posisi TOP** (mungkin distribusi)
                - **Opportunity di posisi BOTTOM** (mungkin akumulasi)
                
                **🟡 KUNING** = **AOV Anomaly SEDANG** (1.5x - 2x MA30)
                - Mixed activity (retail + smart money)
                - Perlu konfirmasi indikator lain
                
                **🟢 HIJAU** = **AOV Normal** (< 1.5x MA30)
                - Dominasi retail trading
                - Aktivitas normal pasar
                """
                
                st.markdown(interpretation)
            else:
                st.warning("Data AOV tidak tersedia untuk heatmap")
        
        with tab5:
            st.markdown("<h2 class='sub-header'>📋 Data Insights & Statistics</h2>", unsafe_allow_html=True)
            
            # Data quality metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_rows = len(df)
                st.metric("Total Data Points", f"{total_rows:,}")
            
            with col2:
                unique_stocks = df['Stock Code'].nunique()
                st.metric("Unique Stocks", f"{unique_stocks:,}")
            
            with col3:
                date_range_days = (df['Last Trading Date'].max() - df['Last Trading Date'].min()).days
                st.metric("Date Range", f"{date_range_days} hari")
            
            st.divider()
            
            # AOV Statistics
            st.subheader("📊 Avg Order Volume Statistics")
            
            if 'Avg_Order_Volume' in df.columns:
                aov_stats = df['Avg_Order_Volume'].describe().reset_index()
                aov_stats.columns = ['Statistic', 'Value']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.dataframe(
                        aov_stats.style.format({'Value': '{:,.2f}'}),
                        use_container_width=True,
                        height=300
                    )
                
                with col2:
                    fig = px.histogram(
                        df,
                        x='Avg_Order_Volume',
                        nbins=50,
                        title='Distribution of Avg Order Volume',
                        log_y=True
                    )
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Correlation analysis
            st.subheader("📈 Correlation Analysis")
            
            corr_cols = ['Frequency', 'Volume', 'Close']
            if 'Avg_Order_Volume' in df.columns:
                corr_cols.append('Avg_Order_Volume')
            if 'AOV_Anomaly_Ratio' in df.columns:
                corr_cols.append('AOV_Anomaly_Ratio')
            
            corr_cols = [col for col in corr_cols if col in df.columns]
            
            if len(corr_cols) > 1:
                corr_matrix = df[corr_cols].corr().round(2)
                
                fig = px.imshow(
                    corr_matrix,
                    text_auto=True,
                    color_continuous_scale='RdBu',
                    title='Correlation Matrix',
                    zmin=-1, zmax=1
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

# ==============================================================================
# 🚀 RUN THE APP
# ==============================================================================

if __name__ == "__main__":
    main()
