from pathlib import Path
import os

PARENT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = PARENT_DIR / 'data'

if not Path(DATA_DIR).exists():
    os.makedirs(DATA_DIR)

    
