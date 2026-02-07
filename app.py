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
    .stProgress > div > div > div > div {
        background-color: #3B82F6;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 🔐 GOOGLE DRIVE FUNCTIONS
# ==============================================================================

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_service_account():
    """Load Google Service Account from secrets.toml"""
    try:
        # Load from secrets
        secrets = st.secrets["gcp_service_account"]
        
        # Create credentials dictionary
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

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def load_data_from_gdrive(file_id=None, file_name="Kompilasi_Data_1Tahun.csv"):
    """Load data from Google Drive"""
    try:
        # Show loading progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("🔐 Menghubungkan ke Google Drive...")
        progress_bar.progress(10)
        
        # Load credentials
        credentials = load_service_account()
        if credentials is None:
            return None
        
        # Build Drive service
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
        progress_bar.progress(20)
        
        status_text.text("🔍 Mencari file data...")
        
        # Search for file
        if file_id:
            # If file_id is provided, use it directly
            query = f"'{file_id}' in parents and name='{file_name}'"
        else:
            # Search by name in all drives
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
        
        # Basic data cleaning
        if 'Last Trading Date' in df.columns:
            df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'], errors='coerce')
        
        progress_bar.progress(100)
        status_text.text("✅ Data berhasil dimuat!")
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        st.success(f"✅ Data berhasil dimuat: {len(df):,} baris, {df['Stock Code'].nunique()} saham")
        
        return df
        
    except Exception as e:
        st.error(f"❌ Error loading data from Google Drive: {str(e)}")
        return None

# ==============================================================================
# 📊 DATA PROCESSING FUNCTIONS
# ==============================================================================

@st.cache_data
def process_data(df):
    """Process and enrich data for analysis"""
    if df is None or len(df) == 0:
        return None
    
    df = df.copy()
    
    # Sort by date
    df = df.sort_values(['Stock Code', 'Last Trading Date'])
    
    # Calculate moving averages for frequency
    df['Freq_MA20'] = df.groupby('Stock Code')['Frequency'].transform(
        lambda x: x.rolling(20, min_periods=5).mean()
    )
    
    # Calculate frequency spike ratio
    df['Freq_Spike_Ratio'] = np.where(
        df['Freq_MA20'] > 0,
        df['Frequency'] / df['Freq_MA20'],
        1
    )
    
    # Calculate volume metrics
    if 'Volume' in df.columns:
        df['Volume_MA20'] = df.groupby('Stock Code')['Volume'].transform(
            lambda x: x.rolling(20, min_periods=5).mean()
        )
        df['Volume_Spike_Ratio'] = np.where(
            df['Volume_MA20'] > 0,
            df['Volume'] / df['Volume_MA20'],
            1
        )
    
    # Calculate price position (0-1 scale)
    df['Price_Position'] = df.groupby('Stock Code')['Close'].transform(
        lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() > x.min() else 0.5
    )
    
    # Categorize price position
    df['Position_Category'] = pd.cut(
        df['Price_Position'],
        bins=[0, 0.3, 0.7, 1],
        labels=['Bottom', 'Middle', 'Top'],
        include_lowest=True
    )
    
    return df

# ==============================================================================
# 📈 VISUALIZATION FUNCTIONS
# ==============================================================================

def create_stock_chart(stock_data, stock_code, company_name):
    """Create detailed chart for a stock"""
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(
            f'{company_name} ({stock_code}) - Harga & Volume',
            'Frekuensi Transaksi',
            'Analisis Bandar'
        ),
        row_heights=[0.4, 0.3, 0.3]
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
    
    # Add MA20 frequency
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
                    name='Spike (>2x MA20)',
                    marker=dict(color='red', size=8, symbol='triangle-up'),
                    showlegend=False
                ),
                row=2, col=1
            )
    
    # 3. Bandar Analysis
    # Show Avg Order Value if available
    if 'Avg_Order_Value' in stock_data.columns:
        fig.add_trace(
            go.Scatter(
                x=stock_data['Last Trading Date'],
                y=stock_data['Avg_Order_Value'],
                mode='lines',
                name='Avg Order Value',
                line=dict(color='purple', width=2),
                showlegend=False
            ),
            row=3, col=1
        )
    
    # Show Bid/Offer Imbalance if available
    if 'Bid/Offer Imbalance' in stock_data.columns:
        fig.add_trace(
            go.Bar(
                x=stock_data['Last Trading Date'],
                y=stock_data['Bid/Offer Imbalance'],
                name='Bid/Offer Imbalance',
                marker_color=np.where(
                    stock_data['Bid/Offer Imbalance'] > 0,
                    'rgba(0, 255, 0, 0.6)',
                    'rgba(255, 0, 0, 0.6)'
                ),
                showlegend=False,
                yaxis='y3'
            ),
            row=3, col=1
        )
    
    # Update layout
    fig.update_layout(
        height=800,
        showlegend=False,
        hovermode='x unified',
        xaxis_rangeslider_visible=False
    )
    
    # Update y-axes
    fig.update_yaxes(title_text="Harga (Rp)", row=1, col=1)
    fig.update_yaxes(title_text="Frekuensi", row=2, col=1)
    
    if 'Avg_Order_Value' in stock_data.columns:
        fig.update_yaxes(title_text="Avg Order Value", row=3, col=1)
    elif 'Bid/Offer Imbalance' in stock_data.columns:
        fig.update_yaxes(title_text="Bid/Offer Imbalance", row=3, col=1)
    
    # Secondary y-axes
    fig.update_layout(
        yaxis2=dict(
            title="Volume",
            overlaying="y",
            side="right",
            showgrid=False
        )
    )
    
    if 'Bid/Offer Imbalance' in stock_data.columns and 'Avg_Order_Value' not in stock_data.columns:
        fig.update_layout(
            yaxis3=dict(
                title="Bid/Offer Imbalance",
                overlaying="y",
                side="right",
                showgrid=False,
                anchor="free",
                position=1.0
            )
        )
    
    return fig

# ==============================================================================
# 🎯 MAIN APP
# ==============================================================================

def main():
    # Title
    st.markdown("<h1 class='main-header'>📈 Frequency Analyzer - Deteksi Gerak Bandar</h1>", unsafe_allow_html=True)
    st.markdown("**Dashboard untuk mendeteksi anomali frekuensi transaksi sebagai indikator akumulasi/distribusi bandar**")
    
    # Initialize session state
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None
    
    # Sidebar
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/1006/1006771.png", width=100)
        st.title("🔧 Kontrol Panel")
        
        st.divider()
        
        # Data loading section
        st.subheader("📂 Load Data")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Load dari Google Drive", type="primary"):
                with st.spinner("Memuat data dari Google Drive..."):
                    st.session_state.df = load_data_from_gdrive(
                        file_name="Kompilasi_Data_1Tahun.csv"
                    )
                    if st.session_state.df is not None:
                        st.session_state.processed_df = process_data(st.session_state.df)
                        st.session_state.data_loaded = True
                        st.rerun()
        
        with col2:
            if st.button("🗑️ Clear Cache"):
                st.cache_data.clear()
                st.session_state.clear()
                st.rerun()
        
        st.divider()
        
        # Analysis parameters
        st.subheader("⚙️ Parameter Analisis")
        
        if st.session_state.data_loaded:
            # Date range filter
            min_date = st.session_state.df['Last Trading Date'].min().date()
            max_date = st.session_state.df['Last Trading Date'].max().date()
            
            date_range = st.date_input(
                "Rentang Tanggal:",
                value=(max_date - timedelta(days=30), max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            # Sector filter
            if 'Sector' in st.session_state.df.columns:
                sectors = ['Semua'] + sorted(st.session_state.df['Sector'].dropna().unique().tolist())
                selected_sector = st.selectbox("Sektor:", sectors)
            
            # Stock filter
            all_stocks = sorted(st.session_state.df['Stock Code'].unique().tolist())
            selected_stocks = st.multiselect(
                "Filter Saham (opsional):",
                options=all_stocks,
                default=[]
            )
        
        st.divider()
        
        # Threshold settings
        st.subheader("🎯 Threshold Settings")
        freq_threshold = st.slider("Frekuensi Spike Threshold (x MA20):", 1.5, 5.0, 2.0, 0.1)
        volume_threshold = st.slider("Volume Spike Threshold (x MA20):", 1.5, 5.0, 2.0, 0.1)
        min_freq = st.number_input("Minimum Frekuensi Harian:", 10, 10000, 100)
    
    # Main content
    if not st.session_state.data_loaded:
        # Welcome screen
        st.info("👈 Klik 'Load dari Google Drive' di sidebar untuk memulai")
        
        # Show data structure
        st.subheader("📋 Struktur Data yang Tersedia")
        
        data_structure = """
        ### Kolom-kolom yang akan dianalisis:
        
        **Data Dasar:**
        - `Stock Code` - Kode saham
        - `Company Name` - Nama perusahaan
        - `Last Trading Date` - Tanggal transaksi
        - `Close` - Harga penutupan
        - `Volume` - Volume transaksi
        - `Value` - Nilai transaksi
        - `Frequency` - Frekuensi transaksi
        
        **Indikator Teknis:**
        - `Avg_Order_Value` - Rata-rata nilai per order
        - `Big_Player_Anomaly` - Flag aktivitas pemain besar
        - `Bid/Offer Imbalance` - Ketidakseimbangan bid/offer
        - `Final Signal` - Sinyal akhir (Akumulasi/Distribusi)
        - `Net Foreign Flow` - Aliran modal asing bersih
        
        **Analisis Tambahan:**
        - `Sector` - Sektor perusahaan
        - `Free Float` - Persentase free float
        """
        
        st.markdown(data_structure)
        
        # Quick stats about the system
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Database", "Google Drive", "Auto-sync")
        with col2:
            st.metric("Analisis", "Real-time", "Frekuensi + Harga")
        with col3:
            st.metric("Deteksi", "Anomali Bandar", "Pattern Recognition")
        
    else:
        # Data is loaded, show analysis tabs
        df = st.session_state.processed_df
        
        # Apply filters
        if 'date_range' in locals() and len(date_range) == 2:
            mask = (df['Last Trading Date'].dt.date >= date_range[0]) & \
                   (df['Last Trading Date'].dt.date <= date_range[1])
            df = df[mask]
        
        if 'selected_sector' in locals() and selected_sector != 'Semua':
            df = df[df['Sector'] == selected_sector]
        
        if 'selected_stocks' in locals() and len(selected_stocks) > 0:
            df = df[df['Stock Code'].isin(selected_stocks)]
        
        # Create tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Dashboard", 
            "🔍 Stock Scanner", 
            "📈 Detail Saham", 
            "📋 Data Insights"
        ])
        
        with tab1:
            st.markdown("<h2 class='sub-header'>Dashboard Overview</h2>", unsafe_allow_html=True)
            
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_stocks = df['Stock Code'].nunique()
                st.metric("Total Saham", f"{total_stocks:,}")
            
            with col2:
                avg_freq = df['Frequency'].mean()
                st.metric("Rata-rata Frekuensi", f"{avg_freq:,.0f}")
            
            with col3:
                spike_days = (df['Freq_Spike_Ratio'] > freq_threshold).sum()
                st.metric(f"Hari Spike (> {freq_threshold}x)", f"{spike_days:,}")
            
            with col4:
                if 'Final Signal' in df.columns:
                    strong_buy = (df['Final Signal'] == 'Strong Akumulasi').sum()
                    st.metric("Sinyal Akumulasi Kuat", f"{strong_buy:,}")
            
            st.divider()
            
            # Top frequency spike stocks
            st.subheader("🚀 Top 10 Saham dengan Frekuensi Spike Tertinggi")
            
            # Get latest data for each stock
            latest_df = df.sort_values('Last Trading Date').groupby('Stock Code').last().reset_index()
            
            top_spikes = latest_df.nlargest(10, 'Freq_Spike_Ratio')[
                ['Stock Code', 'Company Name', 'Frequency', 'Freq_MA20', 
                 'Freq_Spike_Ratio', 'Close', 'Position_Category', 'Final Signal']
            ].copy()
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                fig = px.bar(
                    top_spikes,
                    x='Stock Code',
                    y='Freq_Spike_Ratio',
                    color='Position_Category',
                    color_discrete_map={'Bottom': '#10B981', 'Middle': '#F59E0B', 'Top': '#EF4444'},
                    title='Ratio Frekuensi vs MA20',
                    labels={'Freq_Spike_Ratio': 'Frekuensi / MA20', 'Position_Category': 'Posisi Harga'}
                )
                fig.update_layout(height=400, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Format the dataframe
                display_df = top_spikes.copy()
                display_df['Freq_Spike_Ratio'] = display_df['Freq_Spike_Ratio'].round(2).astype(str) + 'x'
                display_df['Frequency'] = display_df['Frequency'].apply(lambda x: f"{x:,.0f}")
                display_df['Close'] = display_df['Close'].apply(lambda x: f"Rp {x:,.0f}")
                
                st.dataframe(
                    display_df[['Stock Code', 'Freq_Spike_Ratio', 'Position_Category', 'Final Signal']],
                    use_container_width=True,
                    height=400
                )
            
            st.divider()
            
            # Sector analysis
            if 'Sector' in df.columns:
                st.subheader("🏭 Analisis per Sektor")
                
                sector_stats = df.groupby('Sector').agg({
                    'Frequency': 'mean',
                    'Volume': 'mean',
                    'Freq_Spike_Ratio': 'mean',
                    'Stock Code': 'nunique'
                }).round(2).reset_index()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.bar(
                        sector_stats.sort_values('Freq_Spike_Ratio', ascending=False).head(10),
                        x='Sector',
                        y='Freq_Spike_Ratio',
                        color='Freq_Spike_Ratio',
                        color_continuous_scale='RdYlGn',
                        title='Top 10 Sektor dengan Frekuensi Spike Tertinggi'
                    )
                    fig.update_layout(height=400, xaxis_tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.dataframe(
                        sector_stats.sort_values('Freq_Spike_Ratio', ascending=False).style.format({
                            'Frequency': '{:,.0f}',
                            'Volume': '{:,.0f}',
                            'Freq_Spike_Ratio': '{:.2f}x',
                            'Stock Code': '{:.0f}'
                        }).background_gradient(
                            subset=['Freq_Spike_Ratio'],
                            cmap='RdYlGn'
                        ),
                        use_container_width=True,
                        height=400
                    )
        
        with tab2:
            st.markdown("<h2 class='sub-header'>Stock Scanner</h2>", unsafe_allow_html=True)
            
            # Scanner filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                scan_freq_threshold = st.number_input(
                    "Min Freq Spike Ratio:",
                    min_value=1.0,
                    max_value=10.0,
                    value=2.0,
                    step=0.1
                )
            
            with col2:
                scan_price_position = st.selectbox(
                    "Posisi Harga:",
                    ['Semua', 'Bottom', 'Middle', 'Top']
                )
            
            with col3:
                scan_signal = st.selectbox(
                    "Sinyal:",
                    ['Semua', 'Strong Akumulasi', 'Akumulasi', 'Netral', 'Distribusi', 'Strong Distribusi']
                )
            
            # Apply scanner filters
            scan_df = latest_df.copy()
            
            # Filter by frequency spike
            scan_df = scan_df[scan_df['Freq_Spike_Ratio'] >= scan_freq_threshold]
            
            # Filter by price position
            if scan_price_position != 'Semua':
                scan_df = scan_df[scan_df['Position_Category'] == scan_price_position]
            
            # Filter by signal
            if scan_signal != 'Semua' and 'Final Signal' in scan_df.columns:
                scan_df = scan_df[scan_df['Final Signal'] == scan_signal]
            
            # Filter by minimum frequency
            scan_df = scan_df[scan_df['Frequency'] >= min_freq]
            
            # Display results
            st.subheader(f"🔍 {len(scan_df)} Saham Terdeteksi")
            
            if len(scan_df) > 0:
                # Select columns to display
                display_cols = ['Stock Code', 'Company Name', 'Last Trading Date', 
                              'Close', 'Frequency', 'Freq_Spike_Ratio', 'Volume_Spike_Ratio',
                              'Position_Category']
                
                # Add optional columns if available
                optional_cols = ['Final Signal', 'Avg_Order_Value', 'Big_Player_Anomaly', 
                               'Bid/Offer Imbalance', 'Net Foreign Flow']
                
                for col in optional_cols:
                    if col in scan_df.columns:
                        display_cols.append(col)
                
                # Format the dataframe
                display_df = scan_df[display_cols].copy()
                
                # Format numeric columns
                if 'Close' in display_df.columns:
                    display_df['Close'] = display_df['Close'].apply(lambda x: f"Rp {x:,.0f}")
                
                if 'Frequency' in display_df.columns:
                    display_df['Frequency'] = display_df['Frequency'].apply(lambda x: f"{x:,.0f}")
                
                if 'Freq_Spike_Ratio' in display_df.columns:
                    display_df['Freq Spike'] = display_df['Freq_Spike_Ratio'].apply(lambda x: f"{x:.2f}x")
                
                if 'Volume_Spike_Ratio' in display_df.columns:
                    display_df['Volume Spike'] = display_df['Volume_Spike_Ratio'].apply(lambda x: f"{x:.2f}x")
                
                # Convert date to string
                if 'Last Trading Date' in display_df.columns:
                    display_df['Last Trading Date'] = display_df['Last Trading Date'].dt.strftime('%Y-%m-%d')
                
                # Reorder columns for display
                final_display_cols = []
                col_order = ['Stock Code', 'Company Name', 'Last Trading Date', 'Close', 
                           'Frequency', 'Freq Spike', 'Volume Spike', 'Position_Category']
                
                for col in col_order:
                    if col in display_df.columns:
                        final_display_cols.append(col)
                
                # Add optional columns
                for col in display_df.columns:
                    if col not in final_display_cols and col != 'Freq_Spike_Ratio' and col != 'Volume_Spike_Ratio':
                        final_display_cols.append(col)
                
                st.dataframe(
                    display_df[final_display_cols].sort_values('Freq Spike', ascending=False, key=lambda x: x.str.replace('x', '').astype(float)),
                    use_container_width=True,
                    height=500
                )
                
                # Allow selection for detailed view
                selected_scanner_stocks = st.multiselect(
                    "Pilih saham untuk analisis detail:",
                    options=scan_df['Stock Code'].unique(),
                    max_selections=3,
                    key="scanner_select"
                )
                
                if selected_scanner_stocks:
                    st.session_state.selected_stocks = selected_scanner_stocks
                    st.info(f"📈 Saham terpilih: {', '.join(selected_scanner_stocks)} - Buka tab 'Detail Saham'")
            
            else:
                st.warning("Tidak ada saham yang memenuhi kriteria filter.")
        
        with tab3:
            st.markdown("<h2 class='sub-header'>Detail Analisis Saham</h2>", unsafe_allow_html=True)
            
            # Get stocks to analyze
            if 'selected_stocks' in st.session_state and len(st.session_state.selected_stocks) > 0:
                analyze_stocks = st.session_state.selected_stocks
            else:
                # Let user select from all stocks
                analyze_stocks = st.multiselect(
                    "Pilih saham untuk dianalisis:",
                    options=df['Stock Code'].unique(),
                    default=df['Stock Code'].unique()[:1] if len(df) > 0 else [],
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
                        
                        # Key metrics row
                        col1, col2, col3, col4, col5 = st.columns(5)
                        
                        with col1:
                            st.metric("Harga", f"Rp {latest['Close']:,.0f}")
                        
                        with col2:
                            st.metric("Frekuensi", f"{latest['Frequency']:,.0f}")
                        
                        with col3:
                            if 'Freq_Spike_Ratio' in latest:
                                spike_val = latest['Freq_Spike_Ratio']
                                st.metric("Freq Spike", f"{spike_val:.2f}x", 
                                         "↑ Tinggi" if spike_val > 2 else "↓ Normal" if spike_val > 1 else "↓ Rendah")
                        
                        with col4:
                            if 'Position_Category' in latest:
                                pos = latest['Position_Category']
                                color = "🟢" if pos == 'Bottom' else ("🟡" if pos == 'Middle' else "🔴")
                                st.metric("Posisi", f"{color} {pos}")
                        
                        with col5:
                            if 'Final Signal' in latest:
                                signal = latest['Final Signal']
                                color_class = "signal-buy" if 'Akumulasi' in signal else "signal-sell" if 'Distribusi' in signal else "signal-neutral"
                                st.markdown(f"<p class='{color_class}'>{signal}</p>", unsafe_allow_html=True)
                        
                        # Create chart
                        fig = create_stock_chart(stock_data, stock_code, company_name)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Additional metrics
                        st.subheader("📊 Metrics Tambahan")
                        
                        metrics_cols = st.columns(4)
                        
                        with metrics_cols[0]:
                            if 'Volume_Spike_Ratio' in latest:
                                vol_spike = latest['Volume_Spike_Ratio']
                                st.metric("Volume Spike", f"{vol_spike:.2f}x")
                        
                        with metrics_cols[1]:
                            if 'Bid/Offer Imbalance' in latest:
                                imbalance = latest['Bid/Offer Imbalance']
                                st.metric("Bid/Offer Imb", f"{imbalance:.2%}")
                        
                        with metrics_cols[2]:
                            if 'Avg_Order_Value' in latest:
                                aov = latest['Avg_Order_Value']
                                st.metric("Avg Order Value", f"Rp {aov:,.0f}")
                        
                        with metrics_cols[3]:
                            if 'Big_Player_Anomaly' in latest:
                                anomaly = latest['Big_Player_Anomaly']
                                status = "✅ Ya" if anomaly else "❌ Tidak"
                                st.metric("Big Player", status)
            
            else:
                st.info("Pilih saham untuk melihat analisis detail")
        
        with tab4:
            st.markdown("<h2 class='sub-header'>Data Insights & Statistics</h2>", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Date coverage
                date_coverage = df.groupby(df['Last Trading Date'].dt.date).agg({
                    'Stock Code': 'nunique'
                }).reset_index()
                
                fig = px.line(
                    date_coverage,
                    x='Last Trading Date',
                    y='Stock Code',
                    title='Coverage Saham per Hari',
                    markers=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Frequency distribution
                fig = px.histogram(
                    df,
                    x='Frequency',
                    nbins=50,
                    title='Distribusi Frekuensi Transaksi',
                    log_y=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            
            # Correlation analysis
            st.subheader("📊 Correlation Matrix")
            
            # Select numeric columns for correlation
            numeric_cols = ['Frequency', 'Volume', 'Close', 'Freq_Spike_Ratio']
            if 'Volume_Spike_Ratio' in df.columns:
                numeric_cols.append('Volume_Spike_Ratio')
            if 'Avg_Order_Value' in df.columns:
                numeric_cols.append('Avg_Order_Value')
            if 'Net Foreign Flow' in df.columns:
                numeric_cols.append('Net Foreign Flow')
            
            # Filter to existing columns
            corr_cols = [col for col in numeric_cols if col in df.columns]
            
            if len(corr_cols) > 1:
                corr_matrix = df[corr_cols].corr().round(2)
                
                fig = px.imshow(
                    corr_matrix,
                    text_auto=True,
                    color_continuous_scale='RdBu',
                    title='Korelasi antara Variabel'
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            
            # Data quality check
            st.subheader("✅ Data Quality Check")
            
            quality_cols = st.columns(3)
            
            with quality_cols[0]:
                total_rows = len(df)
                st.metric("Total Rows", f"{total_rows:,}")
            
            with quality_cols[1]:
                missing_freq = df['Frequency'].isna().sum()
                st.metric("Missing Frequency", f"{missing_freq:,}", 
                         f"{(missing_freq/total_rows*100):.1f}%" if total_rows > 0 else "0%")
            
            with quality_cols[2]:
                unique_stocks = df['Stock Code'].nunique()
                st.metric("Unique Stocks", f"{unique_stocks:,}")

# ==============================================================================
# 🚀 RUN THE APP
# ==============================================================================

if __name__ == "__main__":
    main()
