import os
import time

from config.const import LISTINGS_PATH
from processing.const import bronze_bucket, gold_bucket, silver_bucket
from processing.data_loader import DataLoader
from processing.session import SparkSessionManager
from processing.storage.s3 import S3StorageHandler


def main() -> None:
    debug_count = os.getenv("BRONZE_DEBUG_COUNT", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }
    output_mode = os.getenv("BRONZE_OUTPUT_MODE", "s3").strip().lower()
    local_output_dir = os.getenv("BRONZE_LOCAL_OUTPUT_DIR", "/tmp/bronze-debug")

    if output_mode not in {"s3", "local"}:
        raise ValueError(
            f"Invalid BRONZE_OUTPUT_MODE='{output_mode}'. Use 's3' or 'local'."
        )

    print(
        "[bronze_to_silver] "
        f"debug_count={debug_count}, output_mode={output_mode}, local_output_dir={local_output_dir}"
    )

    with SparkSessionManager("bronze-upload") as spark:
        data_loader = DataLoader("bronze-upload", spark=spark)
        batches = data_loader.load_batch()

        storage = None
        if output_mode == "s3":
            storage = S3StorageHandler(spark)
            storage.create_buckets([bronze_bucket, silver_bucket, gold_bucket])

        for i, batch in enumerate(batches):
            source_path = LISTINGS_PATH[i]
            output_key = f"bronze_listings_{i}.parquet"
            s3_path = f"s3a://{bronze_bucket}/{output_key}"
            local_path = f"{local_output_dir}/{output_key}"
            target_path = s3_path if output_mode == "s3" else local_path

            print(f"[batch={i}] source={source_path}")
            print(f"[batch={i}] target={target_path}")
            print(f"[batch={i}] partitions={batch.rdd.getNumPartitions()}")

            if debug_count:
                t_count = time.perf_counter()
                rows = batch.count()
                print(
                    f"[batch={i}] count={rows} "
                    f"(took {time.perf_counter() - t_count:.2f}s)"
                )

            t_write = time.perf_counter()
            if output_mode == "s3":
                storage.bucket_upload(bronze_bucket, output_key, batch)  # type: ignore[union-attr]
            else:
                batch.write.mode("overwrite").parquet(local_path)

            print(
                f"[batch={i}] write completed in {time.perf_counter() - t_write:.2f}s -> {target_path}"
            )


if __name__ == "__main__":
    main()
