# Upstream sync policy

## Immutable rules

- Production never follows `metabase:latest`.
- `upstream/base-v0.63.18` is immutable and contains no Dima patch.
- Published Dima release history is never automatically rebased.
- Every newer upstream release produces a **candidate branch only**.
- Candidate integration must report conflicts, build from source, run engine tests, run native
  capability regression, emit `DIMA_PATCH_SURFACE`, and require manual promotion.
- A successful candidate workflow does not modify `main`.
- Customer/sector-specific engine forks are forbidden.

## Candidate state machine

```text
upstream stable ref detected/selected
→ sync/<ref> candidate
→ merge attempt
→ conflict report
→ source build
→ upstream-focused tests
→ Dima engine tests
→ native capability sentinel
→ DIMA_PATCH_SURFACE
→ candidate image
→ human/manual promotion decision
```

The template workflow included here establishes candidate creation, conflict reporting, source build,
patch-surface generation and no-auto-promotion. Native capability sentinel and Dima bridge tests are
added in the engine repository as C1/C2 become GREEN; until then a sync candidate is not promotable.
