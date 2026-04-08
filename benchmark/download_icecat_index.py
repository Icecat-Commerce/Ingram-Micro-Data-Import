"""Download the Icecat full product index (files.index.csv.gz).

Mirrors what `_prefilter_against_index` does, but as a standalone
script so it can be chained into the prefilter test workflow:

    python -m icecat_integration -c config/config.yaml ftp-download-assortment && \\
    python benchmark/download_icecat_index.py && \\
    python -m icecat_integration -c config/config.yaml prepare-sync \\
        -f data/assortment/DatasheetSKUGlobal_Coverage.txt --mode full && \\
    python benchmark/prefilter_diagnostic.py

Authentication uses the FrontOffice credentials from `config/config.yaml`
(`icecat.front_office_username` / `icecat.front_office_password`).

Output: `data/downloads/files.index.csv.gz` (~947 MB).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx

# Allow running as `python benchmark/download_icecat_index.py`
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from icecat_integration.config import AppConfig  # noqa: E402


INDEX_URL = "https://data.icecat.biz/export/level4/EN/files.index.csv.gz"
INDEX_PATH = PROJECT_ROOT / "data" / "downloads" / "files.index.csv.gz"
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def download_icecat_index(
    url: str = INDEX_URL,
    output_path: Path = INDEX_PATH,
    config_path: Path = CONFIG_PATH,
    timeout: float = 600.0,
) -> Path:
    """Download the Icecat full product index to the given path.

    Streams the response to disk in 64 KiB chunks so memory stays
    bounded regardless of file size.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")

    cfg = AppConfig.from_yaml(config_path)
    auth = (
        cfg.icecat.front_office_username,
        cfg.icecat.front_office_password,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Downloading Icecat index")
    print(f"  URL:    {url}")
    print(f"  Output: {output_path}")

    t0 = time.perf_counter()
    bytes_written = 0

    with httpx.Client(timeout=timeout, auth=auth) as client:
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)
                    bytes_written += len(chunk)

    dur = time.perf_counter() - t0
    size_mb = bytes_written / 1e6
    rate = (size_mb / dur) if dur > 0 else 0
    print(f"  Done:   {size_mb:.0f} MB in {dur:.1f}s ({rate:.1f} MB/s)")
    return output_path


def main() -> int:
    try:
        download_icecat_index()
        return 0
    except httpx.HTTPStatusError as e:
        print(f"ERROR: HTTP {e.response.status_code} from Icecat", file=sys.stderr)
        return 1
    except httpx.RequestError as e:
        print(f"ERROR: network failure: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
