import argparse
from twitter_api import TwitterMySQLAPI

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to follows.csv")
    args = parser.parse_args()

    api = TwitterMySQLAPI(host="localhost", user="root", password="twitter", database="twitter_rdb")
    n = api.load_follows_csv(args.csv, has_header=True)
    print(f"Inserted (or ignored duplicates): {n}")

if __name__ == "__main__":
    main()
