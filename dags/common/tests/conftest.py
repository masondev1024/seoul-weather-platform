"""Make the dags root importable so ``import common.*`` resolves.

이 디렉터리에는 지금까지 conftest 가 없어서, 경로를 넣어 주는 다른 테스트
(``test_admin_dong``)가 **먼저 수집될 때만** 우연히 import 가 성공했다. 파일 하나만
지정해 돌리면 ``ModuleNotFoundError: common`` 로 깨진다. 수집 순서에 기대지 않도록
``dags/common/serving/tests/conftest.py`` 와 같은 방식으로 여기서 못박는다.
"""

from __future__ import annotations

import sys
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parents[2]  # tests -> common -> dags root
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))
