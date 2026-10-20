# runner.py
import json
import re
import subprocess
import sys
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Optional

SCRAPY_PROJECT_DIR = Path(__file__).resolve().parent
_OUTPUT_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


def scrape_with_cli(
    url: str,
    id: Optional[str] = None,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    if not isinstance(url, str) or not url.strip():
        raise ValueError("A non-empty URL is required.")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero.")

    output_id = str(id) if id is not None else f"scrape-{uuid4().hex}"
    if not _OUTPUT_ID_PATTERN.fullmatch(output_id) or output_id in {".", ".."}:
        raise ValueError("The scrape output ID contains unsupported characters.")

    output_path = SCRAPY_PROJECT_DIR / f"{output_id}.json"
    command = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "text",
        "-a",
        f"url={url}",
        "-O",
        output_path.name,
    ]
    completed = subprocess.run(
        command,
        cwd=SCRAPY_PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Scrapy crawl failed with exit code {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )

    try:
        with output_path.open(encoding="utf-8") as output_file:
            result = json.load(output_file)
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(
            f"Scrapy did not produce valid JSON at {output_path}."
        ) from error
    if not isinstance(result, list):
        raise RuntimeError(f"Scrapy output must be a JSON array: {output_path}")
    return result


if __name__ == "__main__":
    url = (
        "https://esportsinsider.com/2025/06/valorant-masters-toronto-champion-paper-rex"
    )
    items = scrape_with_cli(url)
    for it in items:
        print(it.get("text"))
