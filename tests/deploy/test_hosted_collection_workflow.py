"""Collection stages keep separate budgets without bypassing protected inputs."""
from pathlib import Path
import re

import pytest
import yaml


WORKFLOW = Path(__file__).resolve().parents[2] / '.github/workflows/weather-collection.yml'


def workflow():
    return yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)


def repair_allowed(**overrides):
    values = {
        'github.repository': 'masondev1024/seoul-weather-platform',
        'github.ref': 'refs/heads/main',
        'vars.WEATHER_COLLECTION_ENABLED': 'enabled',
        'inputs.only_latest': False,
        'inputs.source_issue': '',
        'needs.collect.result': 'success',
    }
    cancelled = overrides.pop('cancelled', False)
    values.update(overrides)
    expression = workflow()['jobs']['repair']['if']
    expression = expression.replace('always()', 'True').replace('cancelled()', repr(cancelled))
    expression = re.sub(r'(?:github|vars|inputs|needs)\.[A-Za-z_]+(?:\.[A-Za-z_]+)?',
                        lambda match: repr(values[match[0]]), expression)
    expression = re.sub(r'!(?!=)', ' not ', expression).replace('&&', ' and ').replace('||', ' or ')
    return eval(expression.strip(), {'__builtins__': {}}, {})


@pytest.mark.parametrize('outcome', ['success', 'failure'])
def test_repair_has_its_own_budget_after_latest_finishes(outcome):
    jobs = workflow()['jobs']
    assert jobs['repair']['needs'] == 'collect'
    assert repair_allowed(**{'needs.collect.result': outcome})
    for job in jobs.values():
        assert job['timeout-minutes'] == '25'
        assert job['runs-on'] == 'ubuntu-latest'


@pytest.mark.parametrize('overrides', [
    {'cancelled': True}, {'needs.collect.result': 'cancelled'},
    {'github.repository': 'someone/fork'}, {'github.ref': 'refs/heads/dev'},
    {'vars.WEATHER_COLLECTION_ENABLED': 'disabled'},
    {'inputs.only_latest': True}, {'inputs.source_issue': '2026-10-17T11:00:00+09:00'},
])
def test_repair_respects_cancellation_protection_and_explicit_single_issue(overrides):
    assert not repair_allowed(**overrides)


def test_only_r2_validation_is_parallel_and_each_stage_requests_one_issue():
    doc = workflow()
    assert doc['concurrency']['cancel-in-progress'] == 'false'
    assert doc['permissions'] == {'contents': 'read'}
    assert set(doc['jobs']) == {'collect', 'repair'}
    commands = {name: '\n'.join(step.get('run', '') for step in job['steps'])
                for name, job in doc['jobs'].items()}
    assert '--only-latest' in commands['collect']
    assert '--repair-oldest' in commands['repair']
    for job in doc['jobs'].values():
        assert 'strategy' not in job
        assert any(step.get('if') == 'always()' and 'GITHUB_STEP_SUMMARY' in step.get('run', '')
                   for step in job['steps'])
