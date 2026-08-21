#!/usr/bin/env python3
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Each entry: (source dirs, build dir for compile_commands.json).
# The build dir must have CMAKE_EXPORT_COMPILE_COMMANDS=ON so
# clang-tidy can find the compile flags.
SOURCE_TREES = [
    (
        [
            Path("lib/crow-tree/src"),
            Path("lib/crow-tree/include"),
            Path("lib/crow-tree/tests"),
            Path("lib/crow-tree/bench"),
            Path("lib/crow-common/cpp"),
        ],
        "lib/crow-tree/build",
    ),
    (
        [
            Path("lib/crow-rpc/src"),
            Path("lib/crow-rpc/include"),
            Path("lib/crow-rpc/tests"),
        ],
        "lib/crow-rpc/build",
    ),
]
EXTENSIONS = {".cpp", ".h"}
DEFAULT_BATCH_SIZE = 3
DEFAULT_JOBS = 10

# Files that are Linux-only (guarded by CROW_TREE_HAVE_LIBURING). clang-tidy
# cannot process them on macOS (reactor.h has a #error when liburing is
# absent), so they are skipped when liburing is not found by CMake.
LIBURING_GATED_FILES = {
    "lib/crow-tree/include/crow-tree/reactor.h",
    "lib/crow-tree/src/reactor.cpp",
    "lib/crow-tree/src/block_async_page_store.cpp",
    "lib/crow-tree/tests/unit/reactor_test.cpp",
}

# Files that are Linux-only (guarded by CROW_RPC_HAVE_RDMA). clang-tidy
# cannot process them on macOS (ibverbs headers absent), so they are
# skipped when RDMA is not found by CMake.
RDMA_GATED_FILES = {
    "lib/crow-rpc/include/crow-rpc/rdma_transport.h",
    "lib/crow-rpc/src/rdma_buffer_pool.cpp",
    "lib/crow-rpc/src/rdma_transport.cpp",
}


def liburing_available() -> bool:
    """Check the CMake cache for liburing (set by lib/crow-tree/CMakeLists.txt)."""
    cache = Path("lib/crow-tree/build/CMakeCache.txt")
    if not cache.exists():
        return True  # no build dir — don't skip (let clang-tidy report the real error)
    text = cache.read_text()
    return "LIBURING_INCLUDE_DIR:PATH=LIBURING_INCLUDE_DIR-NOTFOUND" not in text


def rdma_available() -> bool:
    """Check the CMake cache for RDMA (set by lib/crow-rpc/CMakeLists.txt)."""
    cache = Path("lib/crow-rpc/build/CMakeCache.txt")
    if not cache.exists():
        return True  # no build dir — don't skip
    text = cache.read_text()
    return "CROW_RPC_HAVE_RDMA:INTERNAL=TRUE" in text


def collect_files() -> list[tuple[str, str]]:
    """Return list of (filepath, build_dir) pairs."""
    skip_liburing = not liburing_available()
    skip_rdma = not rdma_available()
    files: list[tuple[str, str]] = []
    for dirs, build_dir in SOURCE_TREES:
        for root in dirs:
            if not root.exists():
                continue
            for path in root.rglob("*"):
                if path.is_file() and path.suffix in EXTENSIONS:
                    posix = path.as_posix()
                    if skip_liburing and posix in LIBURING_GATED_FILES:
                        continue
                    if skip_rdma and posix in RDMA_GATED_FILES:
                        continue
                    files.append((posix, build_dir))
    files.sort()
    return files


def run_batch(batch: list[tuple[str, str]]) -> subprocess.CompletedProcess[str]:
    # All files in a batch share the same build dir (grouped by caller).
    build_dir = batch[0][1]
    filepaths = [f for f, _ in batch]
    return subprocess.run(
        ["clang-tidy", "-p", build_dir, "--quiet", *filepaths],
        text=True,
        capture_output=True,
    )


def main() -> int:
    batch_size = max(1, int(os.environ.get("CT_LINT_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))))
    jobs = max(1, int(os.environ.get("CT_LINT_JOBS", str(DEFAULT_JOBS))))
    files = collect_files()
    if not files:
        return 0

    # Group by build dir so each batch uses the correct compile_commands.json.
    by_build: dict[str, list[tuple[str, str]]] = {}
    for f in files:
        by_build.setdefault(f[1], []).append(f)

    batches: list[list[tuple[str, str]]] = []
    for file_list in by_build.values():
        for i in range(0, len(file_list), batch_size):
            batches.append(file_list[i : i + batch_size])

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
