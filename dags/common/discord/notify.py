"""공통 Discord 전송 모듈 (#161) — 전송·env·안전장치만 공통, 메시지 내용은 도메인 소유.

culture `Notifier`(domains/culture/culture_ingest/common/notify.py) 패턴의 승격판.
설계 합의(#161 댓글 통합):

* **webhook 폴백 체인** — `<DOMAIN>_DISCORD_WEBHOOK_URL`(도메인별 채널, 있으면 우선)
  → `DISCORD_WEBHOOK_URL`(공통 폴백). 도메인 채널 유지 의견(3표)과 단일 수렴
  절충안(culture)을 모두 수용한다. 마이그레이션 무중단(기존 env 그대로 동작).
* **best-effort** — 전송 실패는 삼킨다(예외 타입명만 로그). 알림 실패가 태스크
  상태·다른 콜백을 오염시키지 않는다. URL 은 어떤 로그에도 남기지 않는다.
* **전송 직전 redaction** — `refresh_env_secrets()` → `redact()` (sink.py:81 선례).
  webhook URL 은 `_SECRET_NAME_DENY`(`_URL$`)로 자동 등록에서 빠지므로
  `register_secret()` 으로 명시 등록한다(culture 검토 의견).
* **stdlib urllib** — 외부 의존성 없음. Discord 제한에 맞춰 truncate 내장
  (title 256 / description 4096 / footer 2048).
"""
from __future__ import annotations

import json
import logging
import os
import socket
import urllib.request

from common.security import redact, refresh_env_secrets, register_secret

LOGGER = logging.getLogger(__name__)

# 표준 색 (culture COLOR_PASS/FAIL 승격 + WARN 추가)
COLOR_OK = 3066993     # 0x2ECC71 green
COLOR_FAIL = 15158332  # 0xE74C3C red
COLOR_WARN = 15844367  # 0xF1C40F yellow

FALLBACK_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"

# Discord 필드 길이 제한 (https://discord.com/developers/docs/resources/webhook)
_MAX_TITLE = 256
_MAX_DESCRIPTION = 4096
_MAX_FOOTER = 2048
_MAX_CONTENT = 2000

#: 전송 타임아웃(초). 정상 왕복은 1초 안쪽이고, 초과는 대개 DNS 해석·Cloudflare 앞단 지연·
#: 컨테이너 네트워크 경로 때문이다(레이트리밋 429 는 즉시 응답이라 타임아웃 원인이 아니다).
#:
#: **짧게 잡으면 알림이 조용히 사라진다** — 이 모듈은 best-effort 라 실패를 삼키므로, 타임아웃은
#: 곧 "아무도 모르는 유실"이다. 반대로 길면 Discord 가 멎었을 때 태스크 콜백이 그만큼 붙잡힌다.
#: 유실을 줄이는 쪽으로 15초를 기본값으로 둔다(ASAC-DAG#692). 운영에서 조정할 수 있게 env 노브.
DEFAULT_TIMEOUT_SECONDS = 15.0
TIMEOUT_ENV = "DISCORD_TIMEOUT_SECONDS"


def timeout_seconds(env: dict | None = None) -> float:
    """전송 타임아웃. 값이 없거나 해석 불가·비양수면 기본값으로 떨어진다(fail-safe)."""
    environ = os.environ if env is None else env
    try:
        value = float(str(environ.get(TIMEOUT_ENV, "")).strip())
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT_SECONDS
    return value if value > 0 else DEFAULT_TIMEOUT_SECONDS


def resolve_webhook(domain: str | None = None, *, env: dict | None = None) -> str:
    """도메인별 env 우선, 없으면 공통 `DISCORD_WEBHOOK_URL` 폴백. 둘 다 없으면 ""."""
    environ = os.environ if env is None else env
    if domain:
        domain_env = f"{domain.upper()}_DISCORD_WEBHOOK_URL"
        url = (environ.get(domain_env) or "").strip()
        if url:
            return url
    return (environ.get(FALLBACK_WEBHOOK_ENV) or "").strip()


def notify_environment(env: dict | None = None) -> str:
    """이 메시지를 만든 실행의 환경. **모르면 추측하지 않고 ``unknown``.**

    여러 인스턴스(로컬·맥미니)가 같은 팀 채널을 쓰고, 웹훅만 설정돼 있으면 환경과 무관하게
    전송된다. 그래서 받는 사람이 "이게 운영 결과인가 누가 로컬에서 돌린 건가"를 메시지만 보고
    가릴 수 있어야 한다.

    ``ASK_SEOUL_TARGET``·``DBT_TARGET`` 을 보고, 둘이 엇갈리면 **감추지 않고 그대로 드러낸다** —
    조용히 한쪽을 고르면 그 순간 잘못된 환경으로 표시된다. 미설정을 ``prod`` 로 채우지 않는 것도
    같은 이유다(없는 정보를 운영이라고 단정하면 로컬 실행이 운영으로 보인다).
    """
    environ = os.environ if env is None else env
    seen = {
        str(environ.get(name, "")).strip().lower()
        for name in ("ASK_SEOUL_TARGET", "DBT_TARGET")
    }
    seen.discard("")
    if not seen:
        return "unknown"
    if len(seen) > 1:
        return "conflict(" + ",".join(sorted(seen)) + ")"
    return seen.pop()


def _env_badge(environment: str) -> str:
    """제목 앞 표식 — **모든 환경에 붙인다.**

    운영만 표식을 빼면 "표식 없음"이 운영인지, 이 변경 이전 메시지인지, 표기가 누락된 것인지
    구분되지 않는다. **없음으로부터 운영을 추론하게 만들지 않는다** — 미설정을 ``prod`` 로
    채우지 않는 것과 같은 이유다. 읽는 사람이 매번 같은 자리에서 같은 형식을 보는 편이,
    소음을 줄이려고 한쪽만 비우는 것보다 낫다.
    """
    if environment == "unknown":
        return "[환경 미상] "
    return f"[{environment.upper()}] "


def _provenance(environment: str) -> str:
    """footer 에 항상 붙는 출처 한 줄 — 운영 메시지도 근거를 갖는다.

    호스트명을 같이 싣는 이유: 이 프로젝트는 여러 Airflow 인스턴스가 같은 저장소·채널을
    공유해서, 환경만으로는 "어느 인스턴스인지"가 안 갈린다.
    """
    try:
        host = socket.gethostname()
    except Exception:  # noqa: BLE001 - 출처 표기 실패가 알림을 막지 않는다
        host = "?"
    return f"env={environment} · host={host}"


#: 전송 주체 식별자. 제품 토큰은 **하나로 고정**하고(수신측 allowlist·차단 규칙이 이걸 본다),
#: 어느 도메인이 보냈는지는 표준 UA 주석 문법으로 괄호에 싣는다 — `asac-elt-notify/1.0 (traffic)`.
#: 전송 계층을 합치면서 도메인 식별까지 잃을 이유는 없다(ASAC-DAG#692).
USER_AGENT_PRODUCT = "asac-elt-notify/1.0"


def user_agent(domain: str | None = None) -> str:
    """UA 한 줄. 도메인이 없으면 제품 토큰만.

    urllib 기본 UA(`Python-urllib/x.y`)는 Discord 앞단 Cloudflare 가 403(error 1010)으로
    막으므로 식별 가능한 UA 가 **필수**다. 그 위에 도메인을 얹어, 수신측 로그·레이트리밋에서
    어느 파이프라인이 보낸 것인지 갈리게 한다.
    """
    name = (domain or "").strip().lower()
    return f"{USER_AGENT_PRODUCT} ({safe_ua_token(name)})" if name else USER_AGENT_PRODUCT


def safe_ua_token(value: str) -> str:
    """UA 주석에 넣어도 안전한 문자만 남긴다 — 괄호·개행이 섞이면 헤더가 깨진다."""
    return "".join(ch for ch in value if ch.isalnum() or ch in "-_.") or "unknown"


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _post(webhook: str, payload: dict, *, domain: str | None = None) -> bool:
    """payload 를 webhook 으로 POST. 실패는 삼키고 False (URL 비로그)."""
    register_secret(webhook)  # 전송 실패 로그 등 어떤 경로로도 URL 이 안 새게 명시 등록
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        webhook,
        data=body,
        headers={
            "Content-Type": "application/json",
            # 식별 가능한 UA 필수(기본 UA 는 Cloudflare 가 403 으로 차단) + 도메인 식별.
            "User-Agent": user_agent(domain),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds()):
            pass
        return True
    except Exception as exc:  # noqa: BLE001 -- best-effort: 알림 실패가 run 을 못 막게
        LOGGER.warning("[discord] 전송 실패(무시): %s", type(exc).__name__)
        return False


def send_embed(title: str, description: str, *, color: int = COLOR_OK,
               footer: str | None = None, domain: str | None = None,
               webhook: str | None = None) -> bool:
    """embed 1건 전송. webhook 미해석(env 미설정) 시 조용히 스킵하고 False.

    반환값은 "전송 성공 여부" — 호출측이 분기하라는 뜻이 아니라 테스트/로그용이다.
    """
    url = webhook or resolve_webhook(domain)
    if not url:
        LOGGER.info("[discord] webhook 미설정 — 전송 스킵: %s", _truncate(title, 80))
        return False
    refresh_env_secrets()
    environment = notify_environment()
    # 표식은 truncate **전에** 붙인다 — 뒤에 붙이면 긴 제목에서 표식이 잘려 나간다.
    embed: dict = {
        "title": _truncate(_env_badge(environment) + redact(title), _MAX_TITLE),
        "description": _truncate(redact(description), _MAX_DESCRIPTION),
        "color": color,
    }
    # 출처는 기존 footer 를 밀어내지 않고 뒤에 잇는다(도메인이 쓰던 문구 보존).
    provenance = _provenance(environment)
    text = f"{redact(footer)} · {provenance}" if footer else provenance
    embed["footer"] = {"text": _truncate(text, _MAX_FOOTER)}
    return _post(url, {"embeds": [embed]}, domain=domain)


def stamp_payload(payload: dict, *, env: dict | None = None) -> dict:
    """**호출측이 만든 payload 를 그대로 두고** 환경 표식·출처만 주입한다.

    도메인마다 embed 모양이 다르다(traffic·weather 는 ``fields``, culture 는 자체 footer).
    공용 ``send_embed`` 로 갈아끼우면 그 모양이 바뀌므로, **메시지 조립은 도메인이 그대로 하고
    전송만 공용을 쓰게** 하는 통로다. 바뀌는 것은 첫 embed 의 제목 앞 표식과 footer 뒤 출처뿐.

    - 제목: 표식을 **앞에** 붙이고 256자로 자른다(뒤에 붙이면 긴 제목에서 표식이 잘린다).
    - footer: 기존 문구를 밀어내지 않고 ``· env=… · host=…`` 를 뒤에 잇는다. 없으면 새로 만든다.
    - ``embeds`` 가 없는 payload(평문 ``content``)는 content 앞에 표식만 붙인다.

    원본을 변형하지 않는다 — 호출측이 같은 dict 를 재사용해도 표식이 겹쳐 붙지 않는다.
    """
    import copy

    environment = notify_environment(env)
    badge = _env_badge(environment)
    provenance = _provenance(environment)
    stamped = copy.deepcopy(payload)

    embeds = stamped.get("embeds")
    if isinstance(embeds, list) and embeds and isinstance(embeds[0], dict):
        head = embeds[0]
        head["title"] = _truncate(badge + str(head.get("title") or ""), _MAX_TITLE)
        footer = head.get("footer")
        existing = str((footer or {}).get("text") or "").strip()
        text = f"{existing} · {provenance}" if existing else provenance
        head["footer"] = {**(footer or {}), "text": _truncate(text, _MAX_FOOTER)}
    elif stamped.get("content") is not None:
        stamped["content"] = _truncate(badge + str(stamped["content"]), _MAX_CONTENT)
    return stamped


def send_payload(payload: dict, *, domain: str | None = None,
                 webhook: str | None = None) -> bool:
    """도메인이 조립한 payload 를 공용 전송 경로로 보낸다(표식·출처 주입 + redaction).

    ``send_embed`` 가 못 담는 모양(``fields`` 등)을 쓰는 도메인용. 전송 계층만 공용이 되고
    메시지 내용은 도메인 소유라는 이 모듈의 원칙(#161)을 그대로 지킨다.
    """
    url = webhook or resolve_webhook(domain)
    if not url:
        LOGGER.info("[discord] webhook 미설정 — 전송 스킵")
        return False
    refresh_env_secrets()
    stamped = stamp_payload(payload)
    # 전송 직전 일괄 redaction — 도메인이 만든 값에 자격증명이 섞여도 밖으로 나가지 않는다.
    return _post(url, json.loads(redact(json.dumps(stamped, ensure_ascii=False))),
                 domain=domain)


def send_text(content: str, *, domain: str | None = None,
              webhook: str | None = None) -> bool:
    """평문 메시지 1건 전송 (population 일일 리포트 등 기존 평문 사용처 이전용)."""
    url = webhook or resolve_webhook(domain)
    if not url:
        LOGGER.info("[discord] webhook 미설정 — 전송 스킵")
        return False
    refresh_env_secrets()
    environment = notify_environment()
    # 평문에는 footer 자리가 없어 앞에 표식만 붙인다(운영이면 표식 없음 — 기존 형태 그대로).
    body = _env_badge(environment) + redact(content)
    return _post(url, {"content": _truncate(body, _MAX_CONTENT)}, domain=domain)
