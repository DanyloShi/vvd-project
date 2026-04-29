import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

surnames = [ "shelep", "vulchyn", "zimnov", "kormyliuk", "khomyshyn", "kysil",]

for surname in surnames:
    script_path = BASE_DIR / f"business_questions_{surname}.py"
    print(f"Running {script_path.name} ...")
    subprocess.run([sys.executable, str(script_path)], check=True, cwd=str(PROJECT_ROOT))
