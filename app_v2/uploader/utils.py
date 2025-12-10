from pathlib import Path
from datetime import datetime

def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def today_iso():
    return datetime.today().isoformat()
