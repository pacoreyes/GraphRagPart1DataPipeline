import polars as pl
from pathlib import Path

def main():
    # Define the path to the genres.parquet file
    # Based on the project structure provided
    file_path = Path("data_volume/datasets/genres.parquet")

    if not file_path.exists():
        print(f"Error: File not found at {file_path}")
        return

    # Load the parquet file
    print(f"Reading {file_path}...")
    df = pl.read_parquet(file_path)

    # Show the first few rows
    print("\nFirst 5 rows of genres:")
    print(df.head(5))

    # Show some basic info
    print(f"\nTotal rows: {len(df)}")
    print(f"Columns: {df.columns}")

if __name__ == "__main__":
    main()

