from pathlib import Path

from pyspark.sql import DataFrame


def extract_filename(batch: DataFrame) -> str:
    """Extract the base filename (without extension) from a dataframe source path."""
    first_row = batch.select("filename").first()
    if first_row is None:
        raise ValueError("Cannot extract filename from an empty dataframe")

    source_path = str(first_row["filename"])
    return Path(source_path).name.split(".")[0]
