import argparse
import subprocess
import sys
from pathlib import Path


def load_csv_to_hadoop(local_file: Path, hdfs_destination: str, overwrite: bool = False) -> None:
    """Copy *local_file* into HDFS path *hdfs_destination*."""
    if not local_file.is_file():
        sys.exit(f"❌ Local file not found: {local_file}")

    cmd = ["hadoop", "fs", "-put"]
    if overwrite:
        cmd.append("-f")           # overwrite existing file
    cmd.extend([str(local_file), hdfs_destination])

    print(f"🚚  Uploading {local_file} → {hdfs_destination} ...")
    try:
        subprocess.run(cmd, check=True)   # shell=False by default
        print("✅  File successfully loaded into Hadoop HDFS!")
    except subprocess.CalledProcessError as e:
        sys.exit(f"❌ Hadoop put failed with exit code {e.returncode}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a CSV file to Hadoop HDFS.")
    parser.add_argument("local_csv_file", type=Path, help="Path to the local CSV file")
    parser.add_argument("hdfs_destination", help="Destination path in HDFS")
    parser.add_argument(
        "--overwrite", "-f",
        action="store_true",
        help="Overwrite the file in HDFS if it already exists"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_csv_to_hadoop(args.local_csv_file, args.hdfs_destination, args.overwrite)


if __name__ == "__main__":
    main()
