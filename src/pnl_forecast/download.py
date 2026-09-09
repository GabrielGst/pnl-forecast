#!/usr/bin/env python3

"""download.py - fetch Favorita en Olist data from Kaggle to a local raw/ directory.

Requires ~/.kaggle/credentials.json to be set up with your Kaggle API credentials. Idempotent: re-running skips files if already present unless --force is passed.

Usage:
    uv run download.py --dest data/raw
    uv run download.py --dest data/raw --force
"""

from __future__ import annotations

import argparse
import logging
import zipfile
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

logger = logging.getLogger("download")

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download kaggle sources for the pnl forecast")
    p.add_argument("--dest", type=Path, default=Path("data/raw"), help="Destination directory for downloaded files")
    p.add_argument("--force", action="store_true", help="Force re-download of files even if they already exist")
    p.add_argument("-v", "--verbose", action="store_true")
    
    return p.parse_args(argv)

def download(api: KaggleApi, kind: str, identifier: str, dest: Path, force: bool) -> None:
    marker = dest / f".{identifier}.downloaded"
    if marker.exists() and not force:
        logger.info(f"Skipping {identifier} (already downloaded, use --force to re-download)")
        return

    dest.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {identifier} {{kind}} -> {dest}...")

    if kind == "competition":
        api.competition_download_files(identifier, path=str(dest), quiet=False)
    elif kind == "dataset":
        api.dataset_download_files(identifier, path=str(dest), quiet=False)
    else:
        raise ValueError(f"Unknown kind: {kind}")
    
    for zf in dest.glob("*.zip"):
        logger.info(f"Extracting {zf}...")
        with zipfile.ZipFile(zf, "r") as zip_ref:
            zip_ref.extractall(dest)
        zf.unlink()  # remove the zip file after extraction

    # Create a marker file to indicate that the file has been downloaded
    marker.touch()

def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO)

    api = KaggleApi()
    api.config_file = '/home/gabri/.kaggle/kaggle.json'
    api.authenticate()
    
    download(api, "competition", "fmcg-sales-forecasting-challengess", args.dest / "fmcg-sales-forecasting", args.force)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
