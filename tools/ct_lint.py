#!/usr/bin/env python3
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SEARCH_DIRS = [
    Path("crow-tree/src"),
    Path("crow-tree/include"),
    Path("crow-tree/tests"),
    Path("crow-tree/bench"),
    Path("crow-common/cpp"),
]
EXTENSIONS = {".cpp", ".h"}
DEFAULT_BATCH_SIZE = 3
DEFAULT_JOBS = 10

# Files that are Linux-only (guarded by CROW_TREE_HAVE_LIBURING). clang-tidy
# cannot process them on macOS (reactor.h has a #error when liburing is
# absent), so they are skipped when liburing is not found by CMake.
LIBURING_GATED_FILES = {
    "crow-tree/include/crow-tree/reactor.h",
    "crow-tree/src/reactor.cpp",
    "crow-tree/src/block_async_page_store.cpp",
    "crow-tree/tests/unit/reactor_test.cpp",
}


def liburing_available() -> bool:
    """Check the CMake cache for liburing (set by crow-tree/CMakeLists.txt)."""
    cache = Path("crow-tree/build/CMakeCache.txt")
    if not cache.exists():
        return True  # no build dir — don't skip (let clang-tidy report the real error)
    text = cache.read_text()
    return "LIBURING_INCLUDE_DIR:PATH=LIBURING_INCLUDE_DIR-NOTFOUND" not in text


def collect_files() -> list[str]:
    skip_liburing = not liburing_available()
    files: list[str] = []
    for root in SEARCH_DIRS:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in EXTENSIONS:
                posix = path.as_posix()
                if skip_liburing and posix in LIBURING_GATED_FILES:
                    continue
                files.append(posix)
    files.sort()
    return files


def run_batch(batch: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["clang-tidy", "-p", "crow-tree/build", "--quiet", *batch],
        text=True,
        capture_output=True,
    )


def main() -> int:
    batch_size = max(1, int(os.environ.get("CT_LINT_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))))
    jobs = max(1, int(os.environ.get("CT_LINT_JOBS", str(DEFAULT_JOBS))))
    files = collect_files()
    if not files:
        return 0

    batches = [files[i : i + batch_size] for i in range(0, len(files), batch_size)]
    exit_code = 0

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = [executor.submit(run_batch, batch) for batch in batches]
        for future in as_completed(futures):
            result = future.result()
            if result.stdout:
                sys.stdout.write(result.stdout)
            if result.stderr:
                sys.stderr.write(result.stderr)
            if result.returncode != 0 and exit_code == 0:
                exit_code = result.returncode

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
