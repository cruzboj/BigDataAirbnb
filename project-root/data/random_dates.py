from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def main():
    file_path = "./data/late_arrival/adsProviders.csv"

    print("Loading data...")
    df = pd.read_csv(file_path)

    now = datetime.now()

    def generate_random_date():
        rand = np.random.rand()
        if rand < 0.90:
            random_days = np.random.uniform(0, 4)
        else:
            random_days = np.random.uniform(700, 1000)

        return now - timedelta(days=random_days)

    print("Generating random dates...")
    df["last_updated"] = [generate_random_date() for _ in range(len(df))]

    df["last_updated"] = df["last_updated"].dt.strftime("%Y-%m-%d %H:%M:%S")

    temp_dates = pd.to_datetime(df["last_updated"])
    two_days_ago = now - timedelta(days=2)

    total_rows = len(df)
    in_last_two_days = len(df[temp_dates >= two_days_ago])
    older_than_four_days = len(df[temp_dates < (now - timedelta(days=4))])

    print("\n" + "=" * 30)
    print("📊 DATA DISTRIBUTION SUMMARY")
    print("=" * 30)
    print(f"Total rows updated: {total_rows}")
    print(
        f"Rows in the last 2 days: {in_last_two_days} ({(in_last_two_days / total_rows) * 100:.1f}%)"
    )
    print(
        f"Rows older than 4 days (Simulating 2023): {older_than_four_days} ({(older_than_four_days / total_rows) * 100:.1f}%)"
    )
    print("=" * 30 + "\n")

    df.to_csv(file_path, index=False)
    print(f"Successfully saved updated dates to {file_path}")


if __name__ == "__main__":
    main()
