# Publication blockers

Status date: 2026-08-21

The repository content now records approved team-code republication authorization
dated 2026-08-21 in `NOTICE` and `provenance/source-files.jsonl`. The provenance
manifest keeps source repository, commit, path, hash, derivation, and validator
fields intact while changing only the publication authorization status and
authorization reason.

Remaining blockers before any visibility change:

- Legacy self-hosted workflow routes remain in `.github/workflows/ci.yml` and
  `.github/workflows/deploy-main.yml`.
- The deploy-main workflow remains present and gated by repository variables.
- Full Git history and GitHub Release scans still require separate evidence.
- Public branch governance readbacks still require separate evidence.

This document records status only; it does not copy source content, expose
secrets, or authorize a GitHub visibility change.
