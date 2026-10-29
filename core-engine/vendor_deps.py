import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path

ENGINE_DIR = Path(__file__).resolve().parent
VENDOR_DIR = ENGINE_DIR / "vendor"
ARCHIVE = VENDOR_DIR / "node_modules.tar.gz"


def main() -> None:
    npm = shutil.which("npm")
    tar = shutil.which("tar")
    if npm is None:
        raise SystemExit("npm not found")
    if tar is None:
        raise SystemExit("tar not found")

    VENDOR_DIR.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as temporary:
        temporary_path = Path(temporary)
        shutil.copy2(ENGINE_DIR / "package.json", temporary_path / "package.json")
        shutil.copy2(
            ENGINE_DIR / "package-lock.json",
            temporary_path / "package-lock.json",
        )
        subprocess.check_call(
            f'"{npm}" ci --omit=dev --no-audit --no-fund',
            shell=True,
            cwd=temporary_path,
        )
        subprocess.check_call(
            [
                tar,
                "-czf",
                str(ARCHIVE),
                "-C",
                str(temporary_path),
                "node_modules",
            ]
        )

    digest = hashlib.sha256(ARCHIVE.read_bytes()).hexdigest()
    (VENDOR_DIR / "SHA256SUMS").write_text(
        f"{digest}  node_modules.tar.gz\n",
        encoding="ascii",
        newline="\n",
    )

    print(f"created {ARCHIVE} ({ARCHIVE.stat().st_size} bytes)")
    print(f"sha256 {digest}")


if __name__ == "__main__":
    main()
