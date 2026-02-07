import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

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
</style>
""", unsafe_allow_html=True)

# Title
st.markdown("<h1 class='main-header'>📈 Frequency Analyzer - Deteksi Gerak Bandar</h1>", unsafe_allow_html=True)
st.markdown("**Dashboard untuk mendeteksi anomali frekuensi transaksi sebagai indikator akumulasi/distribusi bandar**")

# Sidebar
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/1006/1006771.png", width=100)
    st.title("Navigasi")
    
    # Upload file atau load from GDrive
    st.subheader("📂 Sumber Data")
    data_source = st.radio("Pilih sumber data:", ["File Upload", "Google Drive"])
    
    df = None
    
    if data_source == "File Upload":
        uploaded_file = st.file_uploader("Upload Kompilasi_Data_1Tahun.csv", type="csv")
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            st.success(f"✅ Data berhasil diunggah: {len(df)} baris")
    else:
        # Untuk Google Drive integration
        st.info("Untuk Google Drive, konfigurasikan secrets.toml")
        use_sample = st.checkbox("Gunakan data sample untuk demo", value=True)
        if use_sample:
            # Create sample data for demo
            st.warning("Mode demo dengan data sample")
            # In production, you would load from GDrive using service account
    
    st.divider()
    
    # Filter parameters
    st.subheader("🔍 Filter Analisis")
    
    if df is not None and 'Last Trading Date' in df.columns:
        df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
        min_date = df['Last Trading Date'].min().date()
        max_date = df['Last Trading Date'].max().date()
        
        date_range = st.date_input(
            "Rentang Tanggal:",
            value=(max_date - timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date
        )
    
    # Sector filter
    if df is not None and 'Sector' in df.columns:
        sectors = ['Semua'] + sorted(df['Sector'].unique().tolist())
        selected_sector = st.selectbox("Sektor:", sectors)
    
    st.divider()
    
    # Analysis parameters
    st.subheader("⚙️ Parameter Analisis")
    freq_spike_threshold = st.slider(
        "Threshold Spike Frekuensi (x dari MA20):",
        min_value=1.5,
        max_value=5.0,
        value=2.0,
        step=0.5,
        help="Frekuensi hari ini dibandingkan rata-rata 20 hari"
    )
    
    volume_threshold = st.slider(
        "Threshold Volume Spike:",
        min_value=1.5,
        max_value=5.0,
        value=2.0,
        step=0.5
    )
    
    min_frequency = st.number_input(
        "Frekuensi Minimum Harian:",
        min_value=10,
        max_value=1000,
        value=100,
        step=10
    )

# Main content
if df is None:
    st.info("👈 Silakan unggah data atau konfigurasikan koneksi Google Drive di sidebar")
    
    # Show sample analysis for demo
    st.subheader("📊 Contoh Analisis Frekuensi")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Saham", "800+")
    with col2:
        st.metric("Periode Data", "1 Tahun")
    with col3:
        st.metric("Indikator", "15+")
    
    st.divider()
    
    # Show analysis steps
    st.subheader("🔍 Cara Kerja Analisis Frekuensi")
    
    steps = """
    1. **Identifikasi Anomali Frekuensi**: Deteksi hari dengan frekuensi transaksi > 200% dari rata-rata 20 hari
    2. **Analisis Konteks Harga**: Lihat posisi harga (support/resistance) saat anomali terjadi
    3. **Filter Manipulasi**: Gunakan Avg_Order_Value untuk bedakan transaksi asli vs robot spam
    4. **Konfirmasi Volume**: Pastikan volume juga mendukung sinyal frekuensi
    5. **Monitor Foreign Flow**: Perhatikan aktivitas investor asing sebagai konfirmasi tambahan
    """
    st.markdown(steps)
    
else:
    # Data processing
    df['Last Trading Date'] = pd.to_datetime(df['Last Trading Date'])
    
    # Apply date filter
    if 'date_range' in locals() and len(date_range) == 2:
        mask = (df['Last Trading Date'].dt.date >= date_range[0]) & (df['Last Trading Date'].dt.date <= date_range[1])
        df = df[mask]
    
    # Apply sector filter
    if 'selected_sector' in locals() and selected_sector != 'Semua':
        df = df[df['Sector'] == selected_sector]
    
    # Calculate additional metrics
    df = df.sort_values(['Stock Code', 'Last Trading Date'])
    
    # Calculate frequency metrics
    df['Freq_MA20'] = df.groupby('Stock Code')['Frequency'].transform(
        lambda x: x.rolling(20, min_periods=5).mean()
    )
    df['Freq_Spike_Ratio'] = df['Frequency'] / df['Freq_MA20']
    df['Freq_Spike'] = df['Freq_Spike_Ratio'] > freq_spike_threshold
    
    # Calculate volume metrics
    df['Volume_MA20'] = df.groupby('Stock Code')['Volume'].transform(
        lambda x: x.rolling(20, min_periods=5).mean()
    )
    df['Volume_Spike_Ratio'] = df['Volume'] / df['Volume_MA20']
    df['Volume_Spike'] = df['Volume_Spike_Ratio'] > volume_threshold
    
    # Calculate price position
    df['Price_Position'] = (df['Close'] - df.groupby('Stock Code')['Close'].transform('min')) / \
                          (df.groupby('Stock Code')['Close'].transform('max') - df.groupby('Stock Code')['Close'].transform('min'))
    
    # Categorize price position
    df['Position_Category'] = pd.cut(
        df['Price_Position'],
        bins=[0, 0.3, 0.7, 1],
        labels=['Bottom', 'Middle', 'Top']
    )
    
    # Identify potential accumulation signals
    df['Accumulation_Signal'] = (
        (df['Freq_Spike'] == True) &
        (df['Volume_Spike'] == True) &
        (df['Position_Category'] == 'Bottom') &
        (df['Frequency'] > min_frequency)
    )
    
    # Tabs for different analyses
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Dashboard Overview", 
        "🔍 Stock Scanner", 
        "📈 Detail Analysis", 
        "📋 Data Quality"
    ])
    
    with tab1:
        st.markdown("<h2 class='sub-header'>Dashboard Overview</h2>", unsafe_allow_html=True)
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_stocks = df['Stock Code'].nunique()
            st.metric("Total Saham", f"{total_stocks}")
        
        with col2:
            avg_freq = df['Frequency'].mean()
            st.metric("Rata-rata Frekuensi", f"{avg_freq:,.0f}")
        
        with col3:
            spike_days = df['Freq_Spike'].sum()
            st.metric("Hari dengan Spike", f"{spike_days}")
        
        with col4:
            accumulation_signals = df['Accumulation_Signal'].sum()
            st.metric("Sinyal Akumulasi", f"{accumulation_signals}")
        
        st.divider()
        
        # Top stocks with frequency spikes
        st.subheader("🏆 Top Saham dengan Frekuensi Spike Terbesar")
        
        # Get latest date for each stock
        latest_data = df.sort_values('Last Trading Date').groupby('Stock Code').last().reset_index()
        
        top_spike_stocks = latest_data.nlargest(10, 'Freq_Spike_Ratio')[['Stock Code', 'Company Name', 'Frequency', 'Freq_MA20', 'Freq_Spike_Ratio', 'Close', 'Position_Category']]
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(
                top_spike_stocks,
                x='Stock Code',
                y='Freq_Spike_Ratio',
                title='Ratio Frekuensi vs Rata-rata 20 Hari',
                color='Position_Category',
                color_discrete_map={'Bottom': 'green', 'Middle': 'orange', 'Top': 'red'},
                labels={'Freq_Spike_Ratio': 'Frekuensi / MA20', 'Position_Category': 'Posisi Harga'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(
                top_spike_stocks.style.format({
                    'Frequency': '{:,.0f}',
                    'Freq_MA20': '{:,.0f}',
                    'Freq_Spike_Ratio': '{:.2f}x',
                    'Close': 'Rp {:,.0f}'
                }).applymap(
                    lambda x: 'color: green' if 'Bottom' in str(x) else ('color: red' if 'Top' in str(x) else 'color: orange'),
                    subset=['Position_Category']
                ),
                use_container_width=True
            )
        
        st.divider()
        
        # Sector analysis
        st.subheader("📊 Analisis per Sektor")
        
        if 'Sector' in df.columns:
            sector_stats = df.groupby('Sector').agg({
                'Frequency': 'mean',
                'Volume': 'mean',
                'Freq_Spike_Ratio': 'mean',
                'Stock Code': 'nunique'
            }).round(2).reset_index()
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.treemap(
                    sector_stats,
                    path=['Sector'],
                    values='Stock Code',
                    color='Freq_Spike_Ratio',
                    color_continuous_scale='RdYlGn',
                    title='Distribusi Saham & Frekuensi per Sektor'
                )
                fig.update_layout(height=500)
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
                    use_container_width=True
                )
    
    with tab2:
        st.markdown("<h2 class='sub-header'>Stock Scanner - Deteksi Anomali</h2>", unsafe_allow_html=True)
        
        # Scanner filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            min_spike_ratio = st.number_input("Min Freq Spike Ratio", min_value=1.0, max_value=10.0, value=2.0, step=0.1)
        
        with col2:
            price_position = st.selectbox("Posisi Harga", ['Semua', 'Bottom', 'Middle', 'Top'])
        
        with col3:
            min_volume_ratio = st.number_input("Min Volume Spike", min_value=1.0, max_value=10.0, value=1.5, step=0.1)
        
        with col4:
            signal_type = st.selectbox("Sinyal", ['Semua', 'Akumulasi', 'Distribusi', 'Netral'])
        
        # Apply filters
        filtered_scans = latest_data.copy()
        
        filtered_scans = filtered_scans[filtered_scans['Freq_Spike_Ratio'] >= min_spike_ratio]
        filtered_scans = filtered_scans[filtered_scans['Volume_Spike_Ratio'] >= min_volume_ratio]
        
        if price_position != 'Semua':
            filtered_scans = filtered_scans[filtered_scans['Position_Category'] == price_position]
        
        if signal_type != 'Semua':
            filtered_scans = filtered_scans[filtered_scans['Final Signal'] == signal_type]
        
        # Display results
        st.subheader(f"🔍 {len(filtered_scans)} Saham Terdeteksi")
        
        if len(filtered_scans) > 0:
            display_cols = ['Stock Code', 'Company Name', 'Last Trading Date', 'Close', 'Frequency', 
                          'Freq_MA20', 'Freq_Spike_Ratio', 'Volume_Spike_Ratio', 'Position_Category',
                          'Avg_Order_Value', 'Big_Player_Anomaly', 'Final Signal']
            
            display_cols = [col for col in display_cols if col in filtered_scans.columns]
            
            st.dataframe(
                filtered_scans[display_cols].sort_values('Freq_Spike_Ratio', ascending=False).style.format({
                    'Close': 'Rp {:,.0f}',
                    'Frequency': '{:,.0f}',
                    'Freq_MA20': '{:,.0f}',
                    'Freq_Spike_Ratio': '{:.2f}x',
                    'Volume_Spike_Ratio': '{:.2f}x',
                    'Avg_Order_Value': 'Rp {:,.0f}'
                }).applymap(
                    lambda x: 'background-color: #D1FAE5' if x == 'Bottom' else 
                             ('background-color: #FEF3C7' if x == 'Middle' else 
                              'background-color: #FEE2E2' if x == 'Top' else ''),
                    subset=['Position_Category']
                ).applymap(
                    lambda x: 'color: green; font-weight: bold' if x == 'Strong Akumulasi' else 
                             ('color: red; font-weight: bold' if x == 'Strong Distribusi' else 
                              ('color: orange' if x == 'Akumulasi' else 
                               ('color: purple' if x == 'Distribusi' else ''))),
                    subset=['Final Signal']
                ),
                use_container_width=True,
                height=400
            )
            
            # Allow user to select a stock for detailed view
            selected_stocks = st.multiselect(
                "Pilih saham untuk analisis detail:",
                options=filtered_scans['Stock Code'].unique(),
                max_selections=3
            )
            
            if selected_stocks:
                st.session_state['selected_stocks'] = selected_stocks
                st.info(f"📈 Saham terpilih: {', '.join(selected_stocks)} - Lihat analisis detail di tab 'Detail Analysis'")
        else:
            st.warning("Tidak ada saham yang memenuhi kriteria filter.")
    
    with tab3:
        st.markdown("<h2 class='sub-header'>Detail Analysis - Per Saham</h2>", unsafe_allow_html=True)
        
        # Check if stocks are selected from scanner
        if 'selected_stocks' in st.session_state and len(st.session_state['selected_stocks']) > 0:
            analyze_stocks = st.session_state['selected_stocks']
        else:
            # Let user select stocks
            analyze_stocks = st.multiselect(
                "Pilih saham untuk dianalisis:",
                options=df['Stock Code'].unique(),
                default=df['Stock Code'].unique()[:3] if len(df) > 0 else []
            )
        
        if analyze_stocks:
            for stock_code in analyze_stocks:
                stock_data = df[df['Stock Code'] == stock_code].sort_values('Last Trading Date')
                
                if len(stock_data) > 0:
                    st.divider()
                    
                    # Stock header
                    company_name = stock_data['Company Name'].iloc[0] if 'Company Name' in stock_data.columns else stock_code
                    latest_data = stock_data.iloc[-1]
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Saham", stock_code)
                    with col2:
                        st.metric("Harga Terakhir", f"Rp {latest_data['Close']:,.0f}")
                    with col3:
                        freq_change = ((latest_data['Frequency'] - latest_data['Freq_MA20']) / latest_data['Freq_MA20'] * 100) if latest_data['Freq_MA20'] > 0 else 0
                        st.metric("Frekuensi", f"{latest_data['Frequency']:,.0f}", f"{freq_change:.1f}% vs MA20")
                    with col4:
                        signal_color = "signal-buy" if 'Akumulasi' in str(latest_data.get('Final Signal', '')) else "signal-sell" if 'Distribusi' in str(latest_data.get('Final Signal', '')) else "signal-neutral"
                        st.markdown(f"<p class='{signal_color}'>Sinyal: {latest_data.get('Final Signal', 'N/A')}</p>", unsafe_allow_html=True)
                    
                    # Create subplots
                    fig = make_subplots(
                        rows=3, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.05,
                        subplot_titles=('Harga dan Volume', 'Frekuensi Transaksi', 'Avg Order Value'),
                        row_heights=[0.4, 0.3, 0.3]
                    )
                    
                    # Price and volume
                    fig.add_trace(
                        go.Candlestick(
                            x=stock_data['Last Trading Date'],
                            open=stock_data['Open Price'],
                            high=stock_data['High'],
                            low=stock_data['Low'],
                            close=stock_data['Close'],
                            name='Harga'
                        ),
                        row=1, col=1
                    )
                    
                    # Add volume bars
                    fig.add_trace(
                        go.Bar(
                            x=stock_data['Last Trading Date'],
                            y=stock_data['Volume'],
                            name='Volume',
                            marker_color='lightblue',
                            opacity=0.6,
                            yaxis='y2'
                        ),
                        row=1, col=1
                    )
                    
                    # Frequency
                    fig.add_trace(
                        go.Scatter(
                            x=stock_data['Last Trading Date'],
                            y=stock_data['Frequency'],
                            mode='lines+markers',
                            name='Frekuensi',
                            line=dict(color='green', width=2)
                        ),
                        row=2, col=1
                    )
                    
                    # Add MA20 frequency
                    fig.add_trace(
                        go.Scatter(
                            x=stock_data['Last Trading Date'],
                            y=stock_data['Freq_MA20'],
                            mode='lines',
                            name='MA20 Frekuensi',
                            line=dict(color='orange', width=1, dash='dash')
                        ),
                        row=2, col=1
                    )
                    
                    # Highlight frequency spikes
                    spike_dates = stock_data[stock_data['Freq_Spike']]['Last Trading Date']
                    spike_values = stock_data[stock_data['Freq_Spike']]['Frequency']
                    
                    fig.add_trace(
                        go.Scatter(
                            x=spike_dates,
                            y=spike_values,
                            mode='markers',
                            name='Spike Frekuensi',
                            marker=dict(color='red', size=10, symbol='triangle-up'),
                            hovertemplate='Spike: %{y:.0f} transaksi<extra></extra>'
                        ),
                        row=2, col=1
                    )
                    
                    # Avg Order Value
                    if 'Avg_Order_Value' in stock_data.columns:
                        fig.add_trace(
                            go.Scatter(
                                x=stock_data['Last Trading Date'],
                                y=stock_data['Avg_Order_Value'],
                                mode='lines',
                                name='Avg Order Value',
                                line=dict(color='purple', width=2)
                            ),
                            row=3, col=1
                        )
                        
                        # Highlight big player anomalies
                        if 'Big_Player_Anomaly' in stock_data.columns:
                            anomaly_dates = stock_data[stock_data['Big_Player_Anomaly']]['Last Trading Date']
                            anomaly_values = stock_data[stock_data['Big_Player_Anomaly']]['Avg_Order_Value']
                            
                            fig.add_trace(
                                go.Scatter(
                                    x=anomaly_dates,
                                    y=anomaly_values,
                                    mode='markers',
                                    name='Big Player',
                                    marker=dict(color='gold', size=12, symbol='star'),
                                    hovertemplate='Big Player Activity<extra></extra>'
                                ),
                                row=3, col=1
                            )
                    
                    # Update layout
                    fig.update_layout(
                        height=800,
                        showlegend=True,
                        hovermode='x unified',
                        title=f"Analisis Detil - {company_name} ({stock_code})"
                    )
                    
                    # Update y-axes
                    fig.update_yaxes(title_text="Harga (Rp)", row=1, col=1)
                    fig.update_yaxes(title_text="Frekuensi", row=2, col=1)
                    fig.update_yaxes(title_text="Avg Order Value", row=3, col=1)
                    
                    # Secondary y-axis for volume
                    fig.update_layout(
                        yaxis2=dict(
                            title="Volume",
                            overlaying="y",
                            side="right",
                            showgrid=False
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Additional metrics
                    st.subheader("📊 Metrics Tambahan")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        if 'Bid/Offer Imbalance' in latest_data:
                            imbalance = latest_data['Bid/Offer Imbalance']
                            st.metric("Bid/Offer Imbalance", f"{imbalance:.2%}")
                    
                    with col2:
                        if 'Volume Spike (x)' in latest_data:
                            vol_spike = latest_data['Volume Spike (x)']
                            st.metric("Volume Spike", f"{vol_spike:.2f}x")
                    
                    with col3:
                        if 'Net Foreign Flow' in latest_data:
                            nff = latest_data['Net Foreign Flow']
                            st.metric("Net Foreign Flow", f"Rp {nff:,.0f}")
                    
                    with col4:
                        if 'Position_Category' in latest_data:
                            pos = latest_data['Position_Category']
                            color = "🟢" if pos == 'Bottom' else ("🟡" if pos == 'Middle' else "🔴")
                            st.metric("Posisi Harga", f"{color} {pos}")
                    
                    # Trading statistics
                    st.subheader("📈 Statistik Trading 20 Hari Terakhir")
                    
                    if len(stock_data) >= 20:
                        recent_data = stock_data.tail(20)
                        
                        stats_cols = ['Frequency', 'Volume', 'Value', 'Avg_Order_Value', 'Close']
                        stats = {}
                        
                        for col in stats_cols:
                            if col in recent_data.columns:
                                stats[f"Avg {col}"] = recent_data[col].mean()
                                stats[f"Max {col}"] = recent_data[col].max()
                                stats[f"Min {col}"] = recent_data[col].min()
                                stats[f"Last {col}"] = recent_data[col].iloc[-1]
                        
                        stats_df = pd.DataFrame([stats])
                        st.dataframe(stats_df.T.style.format("{:,.2f}"), use_container_width=True)
        
        else:
            st.info("Pilih saham untuk melihat analisis detail")
    
    with tab4:
        st.markdown("<h2 class='sub-header'>Data Quality & Statistics</h2>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📅 Coverage Timeline")
            
            # Date coverage
            date_coverage = df.groupby('Last Trading Date').agg({
                'Stock Code': 'nunique'
            }).reset_index()
            
            fig = px.line(
                date_coverage,
                x='Last Trading Date',
                y='Stock Code',
                title='Jumlah Saham per Hari',
                markers=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📊 Data Completeness")
            
            completeness = {}
            total_rows = len(df)
            
            for col in df.columns:
                non_null = df[col].notna().sum()
                completeness[col] = {
                    'Non-Null': non_null,
                    'Null': total_rows - non_null,
                    'Completeness': (non_null / total_rows * 100) if total_rows > 0 else 0
                }
            
            completeness_df = pd.DataFrame(completeness).T.sort_values('Completeness', ascending=False)
            
            fig = px.bar(
                completeness_df.head(20),
                x=completeness_df.index,
                y='Completeness',
                title='Top 20 Kolom dengan Kelengkapan Data'
            )
            fig.update_layout(height=400, xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        # Data statistics
        st.subheader("📈 Statistical Summary")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if numeric_cols:
            summary_stats = df[numeric_cols].describe().T
            st.dataframe(
                summary_stats.style.format("{:,.2f}"),
                use_container_width=True,
                height=400
            )
        
        # Missing data analysis
        st.subheader("🔍 Missing Data Analysis")
        
        missing_data = df.isnull().sum().reset_index()
        missing_data.columns = ['Column', 'Missing_Count']
        missing_data['Missing_Percentage'] = (missing_data['Missing_Count'] / len(df)) * 100
        missing_data = missing_data[missing_data['Missing_Count'] > 0].sort_values('Missing_Percentage', ascending=False)
        
        if len(missing_data) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    missing_data.head(10),
                    x='Column',
                    y='Missing_Percentage',
                    title='Top 10 Kolom dengan Data Hilang'
                )
                fig.update_layout(xaxis_tickangle=45, height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.dataframe(
                    missing_data.style.format({'Missing_Percentage': '{:.1f}%'}),
                    use_container_width=True,
                    height=400
                )
        else:
            st.success("✅ Tidak ada data yang hilang (Missing Values)!")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #6B7280;'>
    <p>Dashboard Frequency Analyzer - Deteksi Gerak Bandar</p>
    <p>⚠️ Disclaimer: Analisis ini untuk edukasi dan penelitian. Bukan rekomendasi investasi.</p>
</div>
""", unsafe_allow_html=True)
