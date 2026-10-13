from pathlib import Path
import plistlib
import subprocess

from tools.weather_daily_quality_schedule import build_plist, run_report


def test_schedule_runs_at_kst_morning_with_bounded_independent_process(tmp_path):
    plist = build_plist(Path('/repo'), Path('/repo/.venv/bin/python'), tmp_path)
    decoded = plistlib.loads(plistlib.dumps(plist))
    assert decoded['StartCalendarInterval'] == {'Hour': 9, 'Minute': 17}
    assert decoded['RunAtLoad'] is True
    assert 'KeepAlive' not in decoded
    assert decoded['Nice'] == 10
    assert decoded['Umask'] == 0o077
    assert decoded['ProgramArguments'][-1] == '--run'
    assert decoded['WorkingDirectory'] == '/repo'
    assert 'tools.weather_daily_quality_schedule' in decoded['ProgramArguments']
    assert 'docker' not in str(decoded['ProgramArguments'])


def test_runner_preserves_unmeasured_exit_without_retrying(monkeypatch):
    calls = []
    def execute(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 2)
    monkeypatch.setattr(subprocess, 'run', execute)
    assert run_report() == 2
    assert len(calls) == 1
    assert calls[0][1]['timeout'] == 240
    assert calls[0][0][-1] == 'tools.weather_daily_quality'


def test_runner_bounds_hanging_report_without_emitting_details(monkeypatch, capsys):
    def execute(args, **kwargs):
        raise subprocess.TimeoutExpired(args, kwargs['timeout'], output='sensitive-source')
    monkeypatch.setattr(subprocess, 'run', execute)
    assert run_report() == 3
    assert 'sensitive-source' not in capsys.readouterr().err
