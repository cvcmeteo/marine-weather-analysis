#!/usr/bin/env python3
"""One-off migration of the output folder to the year/month/week layout.

Reports emitted before the year/month/week change live flat in the output root,
next to their two source files (chart image and Meteomar bulletin). The current
code files every new report under ``<year>/<month>/W<isoweek>/``. Both layouts
are read correctly by ``write_index()``, so this migration is optional and only
tidies the folder up.

Moving a report is not just a rename: the old reports embed their sources as
bare filenames (``![...](chart_<stamp>.gif)``) because the sources used to sit
in the same directory as the site root. The Markdown is fetched and rendered
client-side against that root, so once a report moves into a subfolder those
links must be rewritten to ``<subdir>/chart_<stamp>.gif`` or the chart 404s.
This script does both, in the right order.

``latest.md`` stays in the root (the app keeps rewriting it there) and is
refreshed from the newest report so its links stay valid too.

Dry run by default; nothing is touched without ``--apply``.

    python3 tools/migrate_output_layout.py                 # preview
    python3 tools/migrate_output_layout.py --apply         # do it

Exit code is 1 if any report was skipped because of a destination conflict.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Report filenames carry the emission timestamp; it is the join key between a
# report and the two source files saved alongside it.
REPORT_PREFIX = "analisi_meteo_"
STAMP_FORMAT = "%Y%m%d_%H%M_UTC"


def parse_stamp(name: str) -> Optional[str]:
    """Return the timestamp stamp of a report filename, or None if it is not one."""
    if not (name.startswith(REPORT_PREFIX) and name.endswith(".md")):
        return None
    stamp = name[len(REPORT_PREFIX):-len(".md")]
    try:
        datetime.strptime(stamp, STAMP_FORMAT)
    except ValueError:
        return None
    return stamp


def subdir_for(stamp: str) -> Path:
    """Return the <year>/<month>/W<isoweek> path the current code would use.

    Mirrors ``_report_subdir()`` in main.py; keep the two in sync.
    """
    dt = datetime.strptime(stamp, STAMP_FORMAT)
    return Path(str(dt.year)) / f"{dt.month:02d}" / f"W{dt.isocalendar()[1]:02d}"


def companions(root: Path, stamp: str) -> list[Path]:
    """Return the source files saved next to a report, in the output root."""
    found = list(root.glob(f"chart_{stamp}.*"))
    bulletin = root / f"meteomar_{stamp}.txt"
    if bulletin.exists():
        found.append(bulletin)
    return found


def rewrite_links(text: str, subdir: Path, names: list[str]) -> tuple[str, int]:
    """Prefix the Markdown links to ``names`` with ``subdir``.

    Only the exact ``](<name>)`` occurrences are touched, so nothing else in the
    report body can be rewritten by accident. Already-prefixed links are left
    alone, which makes the migration safe to re-run.
    """
    prefix = subdir.as_posix()
    count = 0
    for name in names:
        old = f"]({name})"
        new = f"]({prefix}/{name})"
        count += text.count(old)
        text = text.replace(old, new)
    return text, count


def make_subdir(root: Path, subdir: Path) -> None:
    """Create ``root/subdir``, giving new folders the owner and mode of ``root``.

    This matters when the migration is run as root while the app runs as UID
    1000: root-owned week folders would be readable but not writable by the
    container, and the first report of the *next* week would fail with a
    PermissionError while trying to create a sibling folder. Copying owner and
    mode from the output directory keeps the tree writable by whoever owns it.
    Ownership is only changed when running as root; otherwise chown is not
    permitted and is not needed, since the creating user already owns the dirs.
    """
    info = root.stat()
    can_chown = os.geteuid() == 0
    current = root
    for part in subdir.parts:
        current = current / part
        if current.exists():
            continue
        current.mkdir()
        os.chmod(current, info.st_mode & 0o7777)
        if can_chown:
            os.chown(current, info.st_uid, info.st_gid)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Move flat reports into <year>/<month>/W<week> subfolders."
    )
    parser.add_argument(
        "--output", default="output", type=Path,
        help="Path to the output directory (default: ./output).",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually move files. Without it, only a preview is printed.",
    )
    args = parser.parse_args()

    root: Path = args.output
    if not root.is_dir():
        print(f"ERROR: {root} is not a directory.", file=sys.stderr)
        return 2

    # Only look at the root: reports already filed in subfolders are done.
    flat = sorted(
        (p for p in root.iterdir() if p.is_file() and parse_stamp(p.name)),
        key=lambda p: p.name,
    )
    if not flat:
        print("Nothing to migrate: no flat reports in the output root.")
        return 0

    mode = "APPLY" if args.apply else "DRY RUN (use --apply to move anything)"
    print(f"=== Output layout migration - {mode} ===")
    print(f"Output dir: {root.resolve()}")
    print(f"Flat reports found: {len(flat)}\n")

    moved = skipped = links_fixed = 0
    newest_report: Optional[Path] = None

    for report in flat:
        stamp = parse_stamp(report.name)
        subdir = subdir_for(stamp)
        sources = companions(root, stamp)
        targets = [report, *sources]

        # Never overwrite: if anything is already there, leave this report alone.
        clash = [t for t in targets if (root / subdir / t.name).exists()]
        if clash:
            print(f"SKIP {report.name} -> {subdir}/ "
                  f"(already present: {', '.join(t.name for t in clash)})")
            skipped += 1
            continue

        names = [s.name for s in sources]
        detail = f"+ {len(names)} source(s)" if names else "no sources found"
        print(f"MOVE {report.name} -> {subdir}/ ({detail})")

        if not args.apply:
            continue

        make_subdir(root, subdir)

        # Move first, then rewrite in place: writing to the existing file keeps
        # its ownership, which matters because the app runs as UID 1000 while
        # this script is typically run as root.
        dest = root / subdir / report.name
        shutil.move(str(report), str(dest))
        text, n = rewrite_links(dest.read_text(encoding="utf-8"), subdir, names)
        if n:
            dest.write_text(text, encoding="utf-8")
            links_fixed += n

        for src in sources:
            shutil.move(str(src), str(root / subdir / src.name))

        moved += 1
        newest_report = dest

    # latest.md lives in the root and is a copy of the newest report, so it
    # carries the same source links and needs the same rewrite. Refresh it from
    # the newest report we just moved rather than patching it by hand.
    latest = root / "latest.md"
    if args.apply and moved and latest.exists() and newest_report is not None:
        latest.write_text(newest_report.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"\nRefreshed latest.md from {newest_report.relative_to(root)}")

    print(f"\nReports moved: {moved} | skipped: {skipped} | links rewritten: {links_fixed}")
    if not args.apply:
        print("Nothing was changed. Re-run with --apply to perform the migration.")
    else:
        print("index.html is regenerated by the app at startup; no action needed.")
    return 1 if skipped else 0


if __name__ == "__main__":
    sys.exit(main())
