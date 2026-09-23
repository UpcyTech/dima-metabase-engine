# Files to add to UpcyTech/dima-metabase-engine at C0

All files in this overlay are **additions**. C0 must not modify or delete an upstream-owned file.

Suggested target mapping:
```text
engine-overlay/.dima/release.json
engine-overlay/.dima/tools/patch_surface.py
engine-overlay/.dima/UPSTREAM_SYNC_POLICY.md
engine-overlay/.github/workflows/dima-engine-c0.yml
engine-overlay/.github/workflows/dima-engine-upstream-sync-candidate.yml
```

After copying, run `patch_surface.py --assert-c0`. Expected:
```text
modified pre-existing upstream files = 0
deleted upstream files               = 0
upstream-owned lines changed         = 0
```
