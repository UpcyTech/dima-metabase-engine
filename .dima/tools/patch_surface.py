#!/usr/bin/env python3
"""Emit DIMA_PATCH_SURFACE for the thin Metabase engine fork."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Iterable

DEFAULT_BASE = "2ba2485c78d7e00a9a25f82c00fc201da71590c4"


def run(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()


def lines(text: str) -> list[str]:
    return [x for x in text.splitlines() if x.strip()]


def parse_numstat(base: str, head: str) -> dict[str, tuple[int | None, int | None]]:
    out: dict[str, tuple[int | None, int | None]] = {}
    for row in lines(run("git", "diff", "--numstat", base, head)):
        add, delete, path = row.split("\t", 2)
        out[path] = (
            None if add == "-" else int(add),
            None if delete == "-" else int(delete),
        )
    return out


def parse_status(base: str, head: str) -> list[tuple[str, str, str | None]]:
    result = []
    raw = run("git", "diff", "--name-status", "-M", base, head)
    for row in lines(raw):
        parts = row.split("\t")
        status = parts[0]
        if status.startswith("R") and len(parts) == 3:
            result.append((status, parts[2], parts[1]))
        else:
            result.append((status, parts[1], None))
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--json-out", type=Path, required=True)
    ap.add_argument("--md-out", type=Path, required=True)
    ap.add_argument("--assert-c0", action="store_true")
    ap.add_argument("--sync-conflicts", type=int, default=0)
    ap.add_argument("--manual-conflicts", type=int, default=0)
    ap.add_argument("--upstream-test-failures", type=int, default=0)
    ap.add_argument("--dima-test-failures", type=int, default=0)
    args = ap.parse_args()

    base = run("git", "rev-parse", f"{args.base}^{{commit}}")
    head = run("git", "rev-parse", f"{args.head}^{{commit}}")
    upstream_files = set(lines(run("git", "ls-tree", "-r", "--name-only", base)))
    status = parse_status(base, head)
    numstat = parse_numstat(base, head)

    modified_existing: list[str] = []
    added_files: list[str] = []
    deleted_upstream: list[str] = []
    renamed_existing: list[dict[str, str]] = []

    for code, path, previous in status:
        kind = code[0]
        if kind == "A":
            added_files.append(path)
        elif kind == "D":
            if path in upstream_files:
                deleted_upstream.append(path)
        elif kind == "R":
            if previous in upstream_files:
                modified_existing.append(previous or path)
                renamed_existing.append({"from": previous or "", "to": path})
        else:
            if path in upstream_files:
                modified_existing.append(path)

    modified_existing = sorted(set(modified_existing))
    added_files = sorted(set(added_files))
    deleted_upstream = sorted(set(deleted_upstream))

    upstream_added = 0
    upstream_deleted = 0
    dima_added_loc = 0
    dima_deleted_loc = 0
    for path, (add, delete) in numstat.items():
        add_i = add or 0
        del_i = delete or 0
        if path in upstream_files:
            upstream_added += add_i
            upstream_deleted += del_i
        else:
            dima_added_loc += add_i
            dima_deleted_loc += del_i

    payload = {
        "schema_version": "dima_patch_surface_v1",
        "upstream_base_sha": base,
        "engine_head": head,
        "modified_preexisting_upstream_files": modified_existing,
        "modified_preexisting_upstream_file_count": len(modified_existing),
        "added_dima_specific_files": added_files,
        "added_dima_specific_file_count": len(added_files),
        "deleted_upstream_files": deleted_upstream,
        "deleted_upstream_file_count": len(deleted_upstream),
        "renamed_preexisting_upstream_files": renamed_existing,
        "upstream_owned_lines_added": upstream_added,
        "upstream_owned_lines_deleted": upstream_deleted,
        "dima_integration_lines_added": dima_added_loc,
        "dima_integration_lines_deleted": dima_deleted_loc,
        "sync_conflict_count": args.sync_conflicts,
        "manual_conflict_interventions": args.manual_conflicts,
        "upstream_test_failures": args.upstream_test_failures,
        "dima_test_failures": args.dima_test_failures,
    }

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    md = [
        "# DIMA_PATCH_SURFACE",
        "",
        f"- upstream base SHA: `{base}`",
        f"- engine HEAD: `{head}`",
        f"- modified pre-existing upstream files: **{len(modified_existing)}**",
        f"- added Dima-specific files: **{len(added_files)}**",
        f"- deleted upstream files: **{len(deleted_upstream)}**",
        f"- upstream-owned line delta: **+{upstream_added} / -{upstream_deleted}**",
        f"- Dima integration line delta: **+{dima_added_loc} / -{dima_deleted_loc}**",
        f"- sync conflicts: **{args.sync_conflicts}**",
        f"- manual conflict interventions: **{args.manual_conflicts}**",
        f"- upstream test failures: **{args.upstream_test_failures}**",
        f"- Dima test failures: **{args.dima_test_failures}**",
        "",
        "## Modified pre-existing upstream files",
        "",
        *([f"- `{p}`" for p in modified_existing] or ["- none"]),
        "",
        "## Added Dima-specific files",
        "",
        *([f"- `{p}`" for p in added_files] or ["- none"]),
        "",
        "## Deleted upstream files",
        "",
        *([f"- `{p}`" for p in deleted_upstream] or ["- none"]),
        "",
    ]
    args.md_out.write_text("\n".join(md))

    print(json.dumps(payload, indent=2, sort_keys=True))

    if args.assert_c0:
        if modified_existing or deleted_upstream or upstream_added or upstream_deleted:
            raise SystemExit("C0 patch-surface invariant violated: existing upstream source changed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
