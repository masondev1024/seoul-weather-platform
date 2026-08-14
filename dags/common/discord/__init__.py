"""공통 Discord 전송 (#161) — 전송·env·안전장치 공통화, 메시지 내용은 도메인 소유."""
from common.discord.guard import first_notice_for_run  # noqa: F401
from common.discord.notify import (  # noqa: F401
    COLOR_FAIL,
    COLOR_OK,
    COLOR_WARN,
    FALLBACK_WEBHOOK_ENV,
    resolve_webhook,
    send_embed,
    send_text,
)
