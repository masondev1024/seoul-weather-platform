import json
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from tools.weather_daily_quality_publish import prepare_branch, publish_report, report_day


def git(path, *args):
    return subprocess.run(['git', '-C', str(path), *args], check=True, text=True, capture_output=True).stdout.strip()


def bare_repo(tmp_path):
    remote = tmp_path / 'remote.git'
    subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
    return remote


def sample(output, day='2026-10-12'):
    output.mkdir()
    from tools.weather_daily_quality_cloud import build_report
    target = date.fromisoformat(day)
    from datetime import timedelta
    as_of = datetime.combine(target + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    report = build_report(None, '', target, as_of, unavailable_reason='credentials_missing')
    (output / f'{day}.json').write_text(json.dumps(report))
    (output / f'{day}.md').write_text('# Data quality\n\nUnmeasured.\n')
    return output


def test_publish_preserves_history_and_skips_same_day(tmp_path):
    remote = bare_repo(tmp_path)
    first = tmp_path / 'first'
    prepare_branch(first, str(remote))
    assert publish_report(sample(tmp_path / 'output'), first, '2026-10-12') is True
    original = git(first, 'rev-parse', 'HEAD')
    second = tmp_path / 'second'
    prepare_branch(second, str(remote))
    assert publish_report(tmp_path / 'output', second, '2026-10-12') is False
    assert git(second, 'rev-parse', 'HEAD') == original
    third = tmp_path / 'third'
    prepare_branch(third, str(remote))
    assert publish_report(sample(tmp_path / 'next', '2026-10-13'), third, '2026-10-13') is True
    assert git(third, 'rev-list', '--count', 'HEAD') == '2'
    assert git(third, 'log', '-1', '--format=%ae') == '269997727+masondev1024@users.noreply.github.com'
    assert git(third, 'log', '-1', '--format=%aI').endswith('+09:00')
    assert git(third, 'log', '-1', '--format=%cI').endswith('+09:00')
    assert git(third, 'show', 'HEAD:reports/2026-10-12.md').startswith('# 서울 기상 수집 품질')


def test_rejects_non_report_branch_content(tmp_path):
    remote = bare_repo(tmp_path)
    checkout = tmp_path / 'checkout'
    prepare_branch(checkout, str(remote))
    (checkout / 'unrelated.txt').write_text('other work')
    git(checkout, 'add', 'unrelated.txt')
    git(checkout, '-c', 'user.name=test', '-c', 'user.email=test@example.com', 'commit', '-m', 'Other work')
    with pytest.raises(ValueError, match='unexpected_report_branch_content'):
        publish_report(sample(tmp_path / 'output'), checkout, '2026-10-12')


def test_invalid_date_and_missing_pair_fail_before_commit(tmp_path):
    checkout = tmp_path / 'checkout'
    prepare_branch(checkout, str(bare_repo(tmp_path)))
    output = sample(tmp_path / 'output')
    (output / '2026-10-12.md').unlink()
    with pytest.raises((ValueError, OSError)):
        publish_report(output, checkout, '2026-10-12')
    with pytest.raises(ValueError):
        report_day('2026-10-12/../2026-10-13')
    assert git(checkout, 'ls-files') == ''


def test_publication_rejects_private_fields_and_rebuilds_markdown(tmp_path):
    checkout = tmp_path / 'checkout'
    prepare_branch(checkout, str(bare_repo(tmp_path)))
    output = sample(tmp_path / 'output')
    file = output / '2026-10-12.json'
    report = json.loads(file.read_text())
    report['bucket'] = 'must-not-publish'
    file.write_text(json.dumps(report))
    with pytest.raises(ValueError, match='public_report_identity_invalid'):
        publish_report(output, checkout, '2026-10-12')
    del report['bucket']
    file.write_text(json.dumps(report))
    (output / '2026-10-12.md').write_text('private string must never be copied')
    publish_report(output, checkout, '2026-10-12')
    assert 'private string' not in (checkout / 'reports/2026-10-12.md').read_text()


def test_explicit_recheck_appends_real_evidence_without_rewriting_daily_report(tmp_path):
    checkout = tmp_path/'checkout'
    prepare_branch(checkout,str(bare_repo(tmp_path)))
    output = sample(tmp_path/'output')
    assert publish_report(output,checkout,'2026-10-12') is True
    original = (checkout/'reports/2026-10-12.json').read_bytes()
    file = output/'2026-10-12.json'
    report = json.loads(file.read_text())
    report['evaluation_as_of'] = '2026-10-13T03:00:00+00:00'
    file.write_text(json.dumps(report))
    assert publish_report(output,checkout,'2026-10-12',refresh=True) is True
    assert (checkout/'reports/2026-10-12.json').read_bytes() == original
    recheck = checkout/'reports/2026-10-12/rechecks/20261013T030000000000Z.json'
    assert json.loads(recheck.read_text()) == report
    assert publish_report(output,checkout,'2026-10-12',refresh=True) is False
    assert git(checkout,'rev-list','--count','HEAD') == '2'
    assert publish_report(output,checkout,'2026-10-12') is False
