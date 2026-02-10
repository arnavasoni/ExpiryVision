import sys
import time
import subprocess
import shutil
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from contextlib import contextmanager

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

WATCH_FOLDER = Path(
    r"C:\Users\SONIARN\OneDrive - Mercedes-Benz (corpdir.onmicrosoft.com)\DWT_ExpiryVision - Documents\Labels"
)

PROCESSED_FOLDER = WATCH_FOLDER / "Processed"
RETRY_FOLDER = WATCH_FOLDER / "Retry"

PYTHON_EXE = sys.executable
EXPIRY_VISION_SCRIPT = r"C:\Coding\ACOS\expiry_vision_01.py"

SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png"}

MAX_RETRIES = 3
LOCK_SUFFIX = ".lock"

# ---------------------------------------------------------
# SAFETY
# ---------------------------------------------------------

for folder in (PROCESSED_FOLDER, RETRY_FOLDER):
    folder.mkdir(exist_ok=True)

def is_image_file(path: Path) -> bool:
    return path.suffix.lower() in SUPPORTED_EXTENSIONS


def wait_until_file_stable(path: Path, timeout=10):
    """Wait until file size stops changing (SharePoint sync safety)"""
    last_size = -1
    for _ in range(timeout):
        current_size = path.stat().st_size
        if current_size == last_size:
            return True
        last_size = current_size
        time.sleep(1)
    return False


def get_retry_count(path: Path) -> int:
    """Extract retry count from filename: image__retry2.jpg"""
    if "__retry" in path.stem:
        try:
            return int(path.stem.split("__retry")[-1])
        except ValueError:
            return 0
    return 0


def increment_retry(path: Path) -> Path:
    count = get_retry_count(path) + 1
    base = path.stem.split("__retry")[0]
    return path.with_name(f"{base}__retry{count}{path.suffix}")


@contextmanager
def file_lock(path: Path):
    """Simple lock using a .lock file"""
    lock_file = path.with_suffix(path.suffix + LOCK_SUFFIX)

    try:
        lock_file.touch(exist_ok=False)
    except FileExistsError:
        raise RuntimeError("File is already locked")

    try:
        yield
    finally:
        if lock_file.exists():
            lock_file.unlink()

# ---------------------------------------------------------
# EVENT HANDLER
# ---------------------------------------------------------

class LabelHandler(FileSystemEventHandler):

    def on_created(self, event):
        if event.is_directory:
            return

        path = Path(event.src_path)

        # Ignore internal folders
        if any(folder in path.parents for folder in
               (PROCESSED_FOLDER, RETRY_FOLDER)):
            return

        if not is_image_file(path):
            return

        print(f"🆕 New label detected: {path.name}")

        if not wait_until_file_stable(path):
            print(f"⚠ File not stable, skipping: {path.name}")
            return

        try:
            with file_lock(path):
                self.process_file(path)

        except RuntimeError:
            print(f"🔒 File locked, skipping: {path.name}")

        except Exception as e:
            print(f"❌ Unexpected error: {e}")

    def process_file(self, path: Path):
        print("▶ Running expiry_vision_01.py")

        result = subprocess.run(
            [PYTHON_EXE, EXPIRY_VISION_SCRIPT, str(path)],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            destination = PROCESSED_FOLDER / path.name
            shutil.move(str(path), destination)
            print(f"✅ Processed and moved to: {destination}")
            return

        # ---- Retry handling ----
        retry_count = get_retry_count(path)
        print(f"❌ Processing failed (attempt {retry_count + 1})")
        print(result.stderr)

        if retry_count < MAX_RETRIES:
            new_path = RETRY_FOLDER / increment_retry(path).name
            shutil.move(str(path), new_path)
            print(f"🔁 Moved to retry folder: {new_path}")
        else:
            print(
                f"🚫 Max retries reached for {path.name}. "
                f"File remains in Retry folder."
            )

# ---------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------

def main():
    observer = Observer()
    handler = LabelHandler()

    observer.schedule(handler, str(WATCH_FOLDER), recursive=False)
    observer.start()

    print(f"👀 Watching folder: {WATCH_FOLDER}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

# ---------------------------------------------------------

if __name__ == "__main__":
    main()
