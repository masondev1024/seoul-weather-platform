"""Publish daily public reports to a generated-only gh-pages branch."""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

REPOSITORY = 'masondev1024/seoul-weather-platform'
BRANCH = 'gh-pages'
AUTHOR_EMAIL = '269997727+masondev1024@users.noreply.github.com'
INDEX = '''<!doctype html><html lang="ko"><meta charset="utf-8">
<title>서울 기상 수집 품질 기록</title>
<h1>서울 기상 수집 품질 기록</h1>
<p>기존 R2 수집 기록을 읽어 생성한 일일 운영 리포트입니다.</p>
<p>Raw 수집 근거와 Bronze 행 품질은 측정 범위가 다릅니다.</p>
<p><a href="https://github.com/masondev1024/seoul-weather-platform/tree/gh-pages/reports">날짜별 JSON·Markdown 리포트 보기</a></p>
</html>\n'''


def report_day(value: str | None = None) -> str:
    if value is None:
        return (datetime.now(ZoneInfo('Asia/Seoul')).date() - timedelta(days=1)).isoformat()
    day = date.fromisoformat(value).isoformat()
    if day != value:
        raise ValueError('invalid_report_date')
    return day


def _git(checkout: Path, *args: str, check: bool = True):
    env = os.environ.copy()
    # Use the owner's local date without backdating: Git still records the
    # actual commit instant, with KST as its timestamp offset.
    env['TZ'] = 'Asia/Seoul'
    token = env.get('GITHUB_TOKEN')
    if token:
        auth = base64.b64encode(f'x-access-token:{token}'.encode()).decode()
        env.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
                   GIT_CONFIG_VALUE_0=f'AUTHORIZATION: basic {auth}')
    # Never persist or return credential headers, remote errors, or command output on failure.
    result = subprocess.run(['git', '-C', str(checkout), *args], env=env,
                            capture_output=True, text=True, timeout=90, check=False)
    if check and result.returncode:
        raise ValueError('report_git_operation_failed')
    return result


def prepare_branch(checkout: Path, remote_url: str) -> None:
    checkout.mkdir(parents=True, exist_ok=False)
    _git(checkout, 'init', '--initial-branch=' + BRANCH)
    _git(checkout, 'remote', 'add', 'origin', remote_url)
    refs = _git(checkout, 'ls-remote', '--exit-code', '--heads', 'origin', BRANCH, check=False)
    if refs.returncode == 0:
        _git(checkout, 'fetch', '--depth=1', 'origin', BRANCH)
        _git(checkout, 'reset', '--hard', 'FETCH_HEAD')
    elif refs.returncode != 2:
        raise ValueError('report_remote_unavailable')
    _git(checkout, 'config', 'user.name', 'masondev1024')
    _git(checkout, 'config', 'user.email', AUTHOR_EMAIL)


def _assert_report_branch(checkout: Path) -> None:
    if _git(checkout, 'symbolic-ref', '--short', 'HEAD').stdout.strip() != BRANCH:
        raise ValueError('unexpected_report_branch')
    for path in _git(checkout, 'ls-files').stdout.splitlines():
        if path not in {'README.md', 'index.html', '.nojekyll'} and not re.fullmatch(
                r'reports/\d{4}-\d{2}-\d{2}\.(json|md)', path):
            raise ValueError('unexpected_report_branch_content')
    if _git(checkout, 'status', '--porcelain').stdout:
        raise ValueError('dirty_report_checkout')


def _write_index(checkout: Path) -> None:
    (checkout / '.nojekyll').write_text('')
    (checkout / 'index.html').write_text(INDEX, encoding='utf-8')
    (checkout / 'README.md').write_text(
        '# 서울 기상 수집 일일 품질 기록\n\n'
        'GitHub Actions가 기존 수집 기록을 읽어 생성한 공개 집계 리포트입니다.\n'
        '누락·최신성은 검증된 Raw manifest 기준이며 Bronze 행 품질과 구분합니다.\n'
        '미측정과 실패를 정상으로 표시하지 않습니다.\n\n'
        '[날짜별 리포트](reports/) · '
        '[생성 코드와 운영 문서](https://github.com/' + REPOSITORY + '/blob/main/docs/operations/weather-daily-data-quality-report.md)\n',
        encoding='utf-8')


def bootstrap(checkout: Path) -> None:
    _assert_report_branch(checkout)
    if _git(checkout, 'rev-parse', '--verify', 'HEAD', check=False).returncode == 0:
        raise ValueError('report_branch_already_exists')
    _write_index(checkout)
    _git(checkout, 'add', 'README.md', 'index.html', '.nojekyll')
    _git(checkout, 'commit', '-m', 'docs(weather): initialize public quality report archive')
    _git(checkout, 'push', 'origin', 'HEAD:refs/heads/' + BRANCH)



STATUSES = {'pass', 'warn', 'fail', 'unmeasured'}
REASONS = {
    'credentials_missing', 'evidence_unavailable', 'r2_evidence_unavailable',
    'listing_budget_exceeded', 'listing_invalid', 'manifest_budget_exceeded',
    'manifest_invalid', 'no_complete_manifests', 'raw_objects_present',
    'no_raw_collection_objects', 'no_report_day_evidence', 'contract_slots_missing',
    'contract_coverage_complete', 'no_manifest_evidence', 'manifest_duplicate_references',
    'manifest_references_unique', 'grid_evidence_missing', 'source_issue_stale',
    'source_issue_current',
}


def _validate_public_report(report: dict, day: str) -> None:
    expected = {'schema_version', 'report_date_kst', 'evaluation_as_of', 'source_revision',
                'status', 'contract', 'measurement_window', 'checks', 'evidence'}
    if (set(report) != expected or report['schema_version'] != 'weather-raw-quality/v1'
            or report['report_date_kst'] != day or report['status'] not in STATUSES
            or not re.fullmatch(r'(?:local|[0-9a-f]{40})', report['source_revision'])):
        raise ValueError('public_report_identity_invalid')
    evaluation = datetime.fromisoformat(report['evaluation_as_of'])
    next_day = date.fromisoformat(day) + timedelta(days=1)
    if evaluation.tzinfo is None or evaluation.astimezone(ZoneInfo('Asia/Seoul')).date() != next_day:
        raise ValueError('public_report_time_invalid')
    if report['measurement_window'] != {'load_date_start_kst': day, 'load_date_end_kst': next_day.isoformat()}:
        raise ValueError('public_report_window_invalid')
    if report['contract'] != {
        'source': 'kma_vilage_fcst', 'grid_count': 80, 'issue_cycle_count': 8,
        'freshness_error_minutes': 360, 'coverage_basis': 'configured_forecast_contract',
        'duplicate_basis': 'manifest_object_references',
    }:
        raise ValueError('public_report_contract_invalid')
    fields = {
        'collection': {'raw_objects'},
        'missing': {'expected_slots', 'observed_slots', 'missing_slots', 'scheduled_population_verified'},
        'duplicates': {'duplicate_references'},
        'freshness': {'measured_grids', 'stale_grids', 'missing_grids', 'latest_issue_at', 'latest_collected_at'},
    }
    if set(report['checks']) != set(fields):
        raise ValueError('public_report_checks_invalid')
    for name, metrics in fields.items():
        check = report['checks'][name]
        if (set(check) != metrics | {'status', 'reason_code'} or check['status'] not in STATUSES
                or check['reason_code'] not in REASONS):
            raise ValueError('public_report_check_invalid')
        for key in metrics:
            value = check[key]
            if key == 'scheduled_population_verified':
                if value is not False:
                    raise ValueError('public_report_denominator_invalid')
            elif key in {'latest_issue_at', 'latest_collected_at'}:
                if value is not None and datetime.fromisoformat(value).tzinfo is None:
                    raise ValueError('public_report_time_invalid')
            elif value is not None and (type(value) is not int or value < 0):
                raise ValueError('public_report_metric_invalid')
    evidence_fields = {'listed_objects', 'manifest_count', 'accepted_manifests', 'invalid_manifests',
                       'verified_raw_objects', 'unverified_raw_objects'}
    if set(report['evidence']) != evidence_fields:
        raise ValueError('public_report_evidence_invalid')
    if any(value is not None and (type(value) is not int or value < 0)
           for value in report['evidence'].values()):
        raise ValueError('public_report_metric_invalid')


def publish_report(output: Path, checkout: Path, day: str) -> bool:
    day = report_day(day)
    _assert_report_branch(checkout)
    destination = checkout / 'reports'
    if (destination / f'{day}.json').exists():
        if not (destination / f'{day}.md').is_file():
            raise ValueError('incomplete_existing_report')
        return False
    source_json, source_md = (output / f'{day}.{suffix}' for suffix in ('json', 'md'))
    if any(p.is_symlink() or not p.is_file() or p.stat().st_size > 128 * 1024
           for p in (source_json, source_md)):
        raise ValueError('public_report_files_invalid')
    report = json.loads(source_json.read_text(encoding='utf-8'))
    _validate_public_report(report, day)
    from tools.weather_daily_quality_cloud import render_markdown

    destination.mkdir(exist_ok=True)
    (destination / source_json.name).write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + '\n',
        encoding='utf-8')
    # Rebuild the public projection from validated fields; never copy arbitrary Markdown.
    (destination / source_md.name).write_text(render_markdown(report), encoding='utf-8')
    _write_index(checkout)
    _git(checkout, 'add', f'reports/{day}.json', f'reports/{day}.md',
         'README.md', 'index.html', '.nojekyll')
    _git(checkout, 'commit', '-m', f'docs(weather): record daily collection quality {day}')
    # A non-fast-forward rejection is a real conflict; never force-push report history.
    _git(checkout, 'push', 'origin', 'HEAD:refs/heads/' + BRANCH)
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument('--prepare', action='store_true')
    actions.add_argument('--publish', action='store_true')
    actions.add_argument('--bootstrap', action='store_true')
    parser.add_argument('--checkout', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args(argv)
    try:
        day = report_day()
        if args.prepare:
            if os.environ.get('GITHUB_REPOSITORY') != REPOSITORY:
                raise ValueError('unexpected_repository')
            prepare_branch(args.checkout, f'https://github.com/{REPOSITORY}.git')
            already = (args.checkout / f'reports/{day}.json').is_file()
            with Path(os.environ['GITHUB_OUTPUT']).open('a') as handle:
                handle.write(f'report_date={day}\nalready_published={str(already).lower()}\n')
        elif args.bootstrap:
            bootstrap(args.checkout)
        elif args.output_dir is None:
            raise ValueError('output_directory_required')
        else:
            published = publish_report(args.output_dir, args.checkout, day)
            print('daily_quality_publication status=' + ('published' if published else 'already_published'))
        return 0
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
        print('daily_quality_publication reason=publication_failed')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
