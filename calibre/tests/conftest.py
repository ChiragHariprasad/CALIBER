import sys
from pathlib import Path

# Ensure root workspace containing 'calibre' package is on sys.path
root_workspace = Path(__file__).resolve().parent.parent.parent
if str(root_workspace) not in sys.path:
    sys.path.insert(0, str(root_workspace))

calibre_root = Path(__file__).resolve().parent.parent
if str(calibre_root) not in sys.path:
    sys.path.insert(0, str(calibre_root))
