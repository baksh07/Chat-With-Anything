# runner.py
import subprocess
import sys
import json
import time
def scrape_with_cli(url, id=time.time()):
    try:
        project_dir = "/mnt/data2/vocally_batch/myproject"
        cmd = [
            sys.executable, "-m", "scrapy",
            "crawl", "text",
            "-a", f"url={url}",
            "-o", f"{id}.json",
        ]
        proc = subprocess.run(
            cmd,
            cwd=project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if proc.returncode != 0:
            raise RuntimeError(f"Scrapy error:\n{proc.stderr}")
        return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
    except Exception as e:
        print(f"error in scrape with cli: {str(e)}")
        return []

if __name__ == "__main__":
    url = "https://esportsinsider.com/2025/06/valorant-masters-toronto-champion-paper-rex"
    items = scrape_with_cli(url)
    for it in items:
        print(it.get("text"))
