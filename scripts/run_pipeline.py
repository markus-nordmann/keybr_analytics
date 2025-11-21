#!/usr/bin/env python3
"""
Run the full KeyBR analytics pipeline.

Steps:
1. Update SQLite DB from raw/typing-data.json
2. Rebuild metrics CSVs (daily_metrics.csv, key_metrics.csv, weak_keys.csv)
3. Commit & push changes to GitHub (if output/*.csv changed)

You can run this script from:
- repo root:      python3 scripts/run_pipeline.py
- scripts folder: python3 run_pipeline.py
- any location:   python3 /full/path/to/keybr_analytics/scripts/run_pipeline.py
"""

import subprocess
import sys
from pathlib import Path
from datetime import date

# --------------------------------------------------------------------
# Paths (independent of the working directory where you call the script)
# --------------------------------------------------------------------
# .../keybr_analytics/scripts/run_pipeline.py -> parents[1] = .../keybr_analytics
ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"
OUTPUT_DIR = ROOT_DIR / "output"


def run(cmd, cwd: Path = ROOT_DIR, check: bool = True) -> int:
    """Helper: print and run a command."""
    print(">>", " ".join(str(c) for c in cmd), f"(cwd={cwd})")
    result = subprocess.run(cmd, cwd=cwd)
    if check and result.returncode != 0:
        raise SystemExit(
            f"Command failed with exit code {result.returncode}: "
            + " ".join(str(c) for c in cmd)
        )
    return result.returncode


# --------------------------------------------------------------------
# Pipeline steps
# --------------------------------------------------------------------
def update_database() -> None:
    """Step 1: JSON → SQLite DB via update_keybr.py."""
    print("\n[1/3] Updating SQLite DB from raw/typing-data.json ...")
    run([sys.executable, str(SCRIPTS_DIR / "update_keybr.py")])


def rebuild_metrics() -> None:
    """Step 2: DB → CSVs via build_metrics.py."""
    print("\n[2/3] Rebuilding metrics CSVs ...")
    run([sys.executable, str(SCRIPTS_DIR / "build_metrics.py")])


def git_has_output_changes() -> bool:
    """
    Check whether there are changes under output/.

    'git diff --quiet -- output' returns:
      0 → no changes
      1 → there are changes
    """
    rc = subprocess.run(
        ["git", "diff", "--quiet", "--", "output"],
        cwd=ROOT_DIR,
    ).returncode
    return rc != 0


def git_commit_and_push() -> None:
    """Step 3: commit CSVs and push to GitHub if there are changes."""
    print("\n[3/3] Committing and pushing changes to GitHub ...")

    if not git_has_output_changes():
        print("No changes in output/*.csv – skipping commit & push.")
        return

    # Stage CSV files
    run(
        [
            "git",
            "add",
            "output/daily_metrics.csv",
            "output/key_metrics.csv",
            "output/weak_keys.csv",
        ]
    )

    # Commit message with current date
    msg = f"Update metrics from {date.today().isoformat()}"
    rc = run(["git", "commit", "-m", msg], check=False)

    if rc != 0:
        # e.g. nothing to commit
        print("git commit returned non-zero exit code, skipping push.")
        return

    # Push to remote
    run(["git", "push"])
    print("Git push done.")


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def main() -> None:
    print(f"Project root: {ROOT_DIR}")
    update_database()
    rebuild_metrics()
    git_commit_and_push()
    print("\nAll done ✅")


if __name__ == "__main__":
    main()
