import subprocess
import sys
import os

BASE_DIR = r"C:\Coding\ACOS"

scripts = [
    "vendor_matcher_02.py",
    "expiry_decision_03.py",
    "email_sending_04.py"
]

for script in scripts:
    script_path = os.path.join(BASE_DIR, script)
    result = subprocess.run([sys.executable, script_path])
    if result.returncode != 0:
        print(f"Error running {script}")
        break
