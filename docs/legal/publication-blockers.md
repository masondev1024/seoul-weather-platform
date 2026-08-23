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

GitHub external visibility cutover is COMPLETE. Readbacks collected on
2026-08-22 confirmed:

- repository visibility is `PUBLIC`;
- repository variables are deployment `disabled` and governance `public`;
- 0 registered repository runners;
- `CI / required` and pull-request review protect `main`;
- 0 GitHub Releases and 0 downloadable Release assets.

This document records status only; it does not copy source content, expose
secrets, or authorize any future visibility, release, workflow, runner, or
runtime mutation.
