import os
import time
import bz2
import zlib
import lz4.frame
import zstandard as zstd
import pandas as pd

# File path 
file_path = '/kaggle/input/sample/eco-counter-data.csv'

# Function to measure compression and decompression
def measure_compression(data, compressor, decompressor, algo_name):
    # Measure compression time
    start_time = time.time()
    if algo_name == 'LZ4':
        compressed_data = lz4.frame.compress(data)
    else:
        compressed_data = compressor.compress(data)
        if hasattr(compressor, "flush"):
            compressed_data += compressor.flush()  # Finalize compression for chunk-based algorithms
    compression_time = time.time() - start_time

    # Measure decompression time
    start_time = time.time()
    if algo_name == 'LZ4':
        decompressed_data = lz4.frame.decompress(compressed_data)
    else:
        decompressed_data = decompressor.decompress(compressed_data)
    decompression_time = time.time() - start_time

    # Validate decompression
    if decompressed_data != data:
        raise AssertionError(f"Decompressed data mismatch for {algo_name}")

    return {
        'Compression Time (s)': compression_time,
        'Decompression Time (s)': decompression_time,
        'Compressed Size (bytes)': len(compressed_data),
        'Compression Ratio': len(data) / len(compressed_data)
    }

# Read CSV data
with open(file_path, 'rb') as f:
    csv_data = f.read()

# Initialize compressors and decompressors
compressors = {
    'bzip2': (bz2.BZ2Compressor(), bz2.BZ2Decompressor()),
    'zlib': (zlib.compressobj(), zlib.decompressobj()),
    'LZ4': (None, None),  # LZ4 has built-in compression and decompression
    'Zstandard': (zstd.ZstdCompressor(), zstd.ZstdDecompressor())
}

# Results dictionary
results = {}

# Measure performance for each algorithm
for name, (compressor, decompressor) in compressors.items():
    if name == 'LZ4':
        results[name] = measure_compression(csv_data, None, None, name)
    else:
        results[name] = measure_compression(csv_data, compressor, decompressor, name)

# Create DataFrame with results
results_df = pd.DataFrame.from_dict(results, orient='index')
results_df['Compression Speed (MB/s)'] = (len(csv_data) / (1024 * 1024)) / results_df['Compression Time (s)']
results_df['Decompression Speed (MB/s)'] = (len(csv_data) / (1024 * 1024)) / results_df['Decompression Time (s)']

# Display results
results_df
