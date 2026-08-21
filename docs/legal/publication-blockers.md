# Publication status

Status date: 2026-08-21

The repository content now records approved team-code republication authorization
dated 2026-08-21 in `NOTICE` and `provenance/source-files.jsonl`. The provenance
manifest keeps source repository, commit, path, hash, derivation, and validator
fields intact while changing only the publication authorization status and
authorization reason.

Repo-local publication gates PASS:

- Apache-2.0 `LICENSE` and truthful `NOTICE` attribution are present.
- `provenance/source-files.jsonl` contains public republication authorization for
  tracked repository content.
- Hosted-only CI has no self-hosted route.
- The disabled manual no-op deploy workflow is safe, hosted-only, and inert.
- The prior audit point recorded 0 GitHub Releases, 0 downloadable Release
  artifacts, 121 GitHub Actions logs scanned clean, and a passed reachable-object
  scan.

GitHub external visibility cutover remains PENDING until fresh operational
readbacks are collected:

- fresh public-readiness preflight;
- repository variables disabled/public;
- exact offline runner removal;
- PR CI and merge;
- public visibility readback;
- post-public branch protection readback;
- fresh delta/full scan immediately before visibility.

This document records status only; it does not copy source content, expose
secrets, claim public visibility has been applied, or claim branch protection has
already been applied after public visibility.
