import pandas as pd

def parquet_to_csv(input_parquet: str, output_csv: str):
    """
    Reads a Parquet file and exports selected columns to a CSV file.
    This needed pandas, pyarrow, huggingface-hub, and fastparquet to
    be installed before running to work successfully.

    Parameters:
        input_parquet (str): Path to the input .parquet file.
        output_csv (str): Path where the output .csv file will be saved.
    """

    # Load the parquet file
    df = pd.read_parquet(input_parquet)

    # Select and rename the required columns
    column_map = {
        "id": "REPOID",
        "language": "LANG",
        "stars": "STARS",
        "url": "REPOURL"
    }

    # Filter + rename only the keys that exist in the Parquet
    available = {k: v for k, v in column_map.items() if k in df.columns}

    if not available:
        raise ValueError("None of the required columns were found in the parquet file.")

    df_out = df[list(available.keys())].rename(columns=available)

    # Save to CSV
    df_out.to_csv(output_csv, index=False)
    print(f"CSV file created successfully: {output_csv}")


if __name__ == "__main__":
    # Example usage
    parquet_to_csv("hf://datasets/hao-li/AIDev/all_repository.parquet", "output2.csv")
