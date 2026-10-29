import hashlib
import re
import subprocess
import sys
from pathlib import Path

AGENT_DIR = Path(__file__).resolve().parent
DESTINATION = AGENT_DIR / "vendor" / "wheels"
ARM64_DESTINATION = AGENT_DIR / "vendor" / "wheels-arm64"
LOCK_FILE = AGENT_DIR / "requirements.lock"
DESTINATION.mkdir(parents=True, exist_ok=True)
ARM64_DESTINATION.mkdir(parents=True, exist_ok=True)

for wheel in DESTINATION.glob("*.whl"):
    wheel.unlink()

download = [
    sys.executable,
    "-m",
    "pip",
    "download",
    "--requirement",
    str(LOCK_FILE),
    "--dest",
    str(DESTINATION),
    "--only-binary=:all:",
    "--no-deps",
    "--require-hashes",
    "--platform",
    "manylinux2014_x86_64",
    "--python-version",
    "3.12",
    "--implementation",
    "cp",
    "--abi",
    "cp312",
]

subprocess.check_call(download)

# The arm64 wheel is downloaded separately because pip can only target one
# platform per invocation. Keep it in wheels-arm64 and list its hash in
# requirements.lock; both wheel directories are copied into the image.
if not any(ARM64_DESTINATION.glob("pydantic_core-*-aarch64.whl")):
    raise SystemExit(
        "wheels-arm64/pydantic_core-*-aarch64.whl is missing; "
        "download it with pip --platform manylinux2014_aarch64"
    )


def normalize(value: str) -> str:
    return value.replace("_", "-").lower()


lock_pattern = re.compile(r"^([A-Za-z0-9_.-]+)==([^ ]+)\s+(.+)$")
hash_pattern = re.compile(r"--hash=sha256:([0-9a-f]{64})")

expected = {}
for line in LOCK_FILE.read_text(encoding="utf-8").splitlines():
    match = lock_pattern.match(line.strip())
    if match is None:
        raise SystemExit(f"invalid lock line: {line}")
    hashes = set(hash_pattern.findall(match.group(3)))
    if not hashes:
        raise SystemExit(f"lock line has no hashes: {line}")
    expected[(normalize(match.group(1)), match.group(2))] = hashes

actual = {}
for directory in (DESTINATION, ARM64_DESTINATION):
    for wheel in sorted(directory.glob("*.whl")):
        parts = wheel.name.split("-")
        key = (normalize(parts[0]), parts[1])
        digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
        actual.setdefault(key, set()).add(digest)

if set(actual) != set(expected):
    raise SystemExit(
        f"vendor set mismatch: expected={sorted(expected)} actual={sorted(actual)}"
    )

for key, digests in expected.items():
    if not actual[key].issubset(digests):
        raise SystemExit(f"hash mismatch for {key[0]}=={key[1]}")

print(f"verified {sum(len(v) for v in actual.values())} wheels")
