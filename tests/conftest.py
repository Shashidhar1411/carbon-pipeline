import os
import sys
import tempfile

os.environ.setdefault("DATA_DIR", tempfile.mkdtemp())      # isolate SQLite + reservoir files from real data
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
