# ==============================================================================
# UPDATE 1: LOGIC PERHITUNGAN (Letakkan setelah load data)
# ==============================================================================
df = df_raw.sort_values(by=['Stock Code', 'Last Trading Date']).copy()

# 1. Hitung Rata-rata Historis (20 Hari)
df['MA20_Freq'] = df.groupby('Stock Code')['Frequency'].transform(lambda x: x.rolling(20).mean())
df['MA20_Vol']  = df.groupby('Stock Code')['Volume'].transform(lambda x: x.rolling(20).mean())

# 2. Hitung Spike Ratio (Seberapa gila kenaikannya?)
# Tambah +1 agar tidak error division by zero
df['Freq_Spike'] = df['Frequency'] / (df['MA20_Freq'] + 1)
df['Vol_Spike']  = df['Volume'] / (df['MA20_Vol'] + 1)

# 3. METRIK KUNCI: Lot Per Trade (LPT)
# Ini logika Bapak: Volume / Frequency
df['LPT'] = np.where(df['Frequency'] > 0, df['Volume'] / df['Frequency'], 0)
df['MA20_LPT'] = df.groupby('Stock Code')['LPT'].transform(lambda x: x.rolling(20).mean())

# 4. BANDAR SCORE (THE FILTER)
# Kita anggap VALID jika:
# a. Freq Spike Tinggi (> 2x)
# b. Vol Spike TIDAK setinggi Freq (Indikasi Split) -> Ratio > 1
# c. Total Value Transaksi cukup besar (misal > 100jt) -> Filter Noise Bot

# Menghitung Value Transaksi (Estimasi)
df['Tx_Value'] = df['Close'] * df['Volume'] * 100 # Asumsi 1 lot = 100 lembar

# Logic Pewarnaan Chart:
# Anomaly = (Freq Spike / Vol Spike). 
# Semakin tinggi angka ini, semakin "Terpecah" ordernya (Ciri Khas Bandar).
df['Anomaly_Intensity'] = np.where(df['Vol_Spike'] > 0, df['Freq_Spike'] / df['Vol_Spike'], 0)

# ==============================================================================
# UPDATE 2: VISUALISASI CHART (Ganti bagian fig plotly)
# ==============================================================================

# ... (Kode candlestick bagian atas tetap sama) ...

# LOGIC VISUALISASI BARU
# Sumbu Y = Frequency Spike (Supaya visualnya tetap "Tiang Listrik" seperti Stockbit)
# WARNA = Ditentukan oleh kualitas Volume/LPT (Logic Bapak)

colors = []
hover_texts = []

for index, row in df_chart.iterrows():
    # Syarat 1: Frekuensi harus meledak (Minimal 2x lipat rata-rata)
    is_freq_spike = row['Freq_Spike'] >= min_freq_spike 
    
    # Syarat 2: Anomaly Intensity Tinggi (Frekuensi lari lebih kencang dari Volume)
    # Ini menjawab: "Apakah ini split order?"
    is_split_order = row['Anomaly_Intensity'] >= 1.5 
    
    # Syarat 3: Value Filter (Optional) - Hapus noise saham gocap yg transaksi kecil
    # Misal minimal transaksi 500 Juta sehari
    is_significant_value = row['Tx_Value'] >= 500_000_000 

    # --- PENENTUAN WARNA ---
    if is_freq_spike and is_split_order and is_significant_value:
        colors.append('red') # 🚨 MURNI BANDAR (Split Order + Ada Duitnya)
        label = "🚨 BANDAR SPLIT"
    elif is_freq_spike and not is_split_order:
        colors.append('orange') # ⚠️ RAMAI NORMAL (Volume ikut naik, misal Breakout)
        label = "⚠️ RAMAI (Retail/News)"
    elif is_freq_spike and not is_significant_value:
        colors.append('gray') # 🗑️ NOISE (Frekuensi tinggi tapi duitnya dikit)
        label = "🗑️ BOT/NOISE"
    else:
        colors.append('lightgray') # Normal Day
        label = "Normal"

    # Info untuk tooltip saat mouse hover
    hover_texts.append(
        f"<b>{label}</b><br>" +
        f"Freq Spike: {row['Freq_Spike']:.2f}x<br>" +
        f"Vol Spike: {row['Vol_Spike']:.2f}x<br>" +
        f"LPT (Lot/Trade): {row['LPT']:.1f} (Avg: {row['MA20_LPT']:.1f})<br>" +
        f"Anomaly Score: {row['Anomaly_Intensity']:.2f}"
    )

# PLOT FREQUENCY BAR (Tiang Listrik)
fig.add_trace(go.Bar(
    x=df_chart['Last Trading Date'],
    y=df_chart['Freq_Spike'], # Tetap visualkan Spike Ratio agar terlihat menjulang
    marker_color=colors,
    name='Activity Anomaly',
    hovertext=hover_texts,
    hoverinfo="text"
), row=2, col=1)

# Tambah Garis Rata-rata LPT (Secondary Y-Axis di bawah? Agak rumit, mending di tooltip aja)
# Kita kasih garis batas threshold di chart frekuensi
fig.add_hline(y=min_freq_spike, line_dash="dot", row=2, col=1, line_color="red", annotation_text="Threshold Area")

# ... (Update layout & tampilkan chart)
