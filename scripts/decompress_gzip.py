import gzip
import shutil

def decompress_gzip(input_path, output_path):
    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Decompressed {input_path} to {output_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: decompress_gzip.py <input_gz_file> <output_json_file>")
        sys.exit(1)
    
    input_gz_file = sys.argv[1]
    output_json_file = sys.argv[2]
    
    decompress_gzip(input_gz_file, output_json_file)
