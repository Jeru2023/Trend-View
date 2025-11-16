"""Client helpers for Coze agent API."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from ..config.settings import CozeSettings

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 90.0
_CONNECT_TIMEOUT = 10.0
_CHAT_POLL_INTERVAL_SECONDS = 1.0
_CHAT_POLL_MAX_ATTEMPTS = 120


def run_coze_agent(
    query: str,
    *,
    settings: CozeSettings,
    conversation_id: Optional[str] = None,
    stream: bool = False,
    timeout: Optional[float] = None,
) -> Optional[dict[str, Any]]:
    """Call the Coze chat API with the aggregated JSON payload."""
    if not query:
        logger.debug("Coze skipped empty query")
        return None

    base_url = settings.base_url.rstrip("/")
    url = base_url + "/v3/chat"

    payload: dict[str, Any] = {
        "bot_id": settings.bot_id,
        "user_id": settings.user_id,
        "additional_messages": [
            {
                "role": "user",
                "type": "question",
                "content": query,
                "content_type": "text",
            }
        ],
        "auto_save_history": True,
        "stream": bool(stream),
    }
    params: Dict[str, Any] = {}
    if conversation_id or settings.conversation_id:
        params["conversation_id"] = conversation_id or settings.conversation_id

    headers = {
        "Authorization": f"Bearer {settings.token}",
        "Content-Type": "application/json",
    }

    request_timeout = float(timeout or settings.request_timeout_seconds or _DEFAULT_TIMEOUT)
    request_timeout = max(request_timeout, 5.0)

    try:
        response = requests.post(
            url,
            headers=headers,
            params=params or None,
            json=payload,
            timeout=(_CONNECT_TIMEOUT, request_timeout),
        )
        response.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - external API
        detail = None
        try:
            detail = exc.response.text if exc.response is not None else None
        except Exception:  # pragma: no cover - defensive
            detail = None
        if detail:
            logger.warning("Coze request failed: %s | body=%s", exc, detail)
        else:
            logger.warning("Coze request failed: %s", exc)
        return None

    try:
        data = response.json()
    except ValueError as exc:  # pragma: no cover - invalid JSON
        logger.warning("Coze returned non-JSON payload: %s", exc)
        return None

    chat_data = data.get("data") if isinstance(data, dict) else None
    if not isinstance(chat_data, dict):
        logger.debug("Coze response missing chat data: %s", data)
        return None

    chat_id = chat_data.get("id")
    conversation_id = chat_data.get("conversation_id")
    if not chat_id or not conversation_id:
        logger.debug("Coze chat response missing identifiers: %s", chat_data)
        return None

    chat_detail = chat_data
    status = (chat_data.get("status") or "").lower()
    usage = chat_data.get("usage")

    retrieve_url = base_url + "/v3/chat/retrieve"
    retrieve_params = {"conversation_id": conversation_id, "chat_id": chat_id}
    if status in {"created", "in_progress"}:
        for attempt in range(_CHAT_POLL_MAX_ATTEMPTS):
            if attempt:
                time.sleep(_CHAT_POLL_INTERVAL_SECONDS)
            try:
                retrieve_resp = requests.get(
                    retrieve_url,
                    headers=headers,
                    params=retrieve_params,
                    timeout=(_CONNECT_TIMEOUT, request_timeout),
                )
                retrieve_resp.raise_for_status()
                retrieve_data = retrieve_resp.json()
            except requests.RequestException as exc:  # pragma: no cover - external
                logger.warning("Coze retrieve failed: %s", exc)
                break
            except ValueError:
                logger.warning("Coze retrieve returned non-JSON payload")
                break
            detail = retrieve_data.get("data") if isinstance(retrieve_data, dict) else None
            if isinstance(detail, dict):
                chat_detail = detail
                usage = detail.get("usage") or usage
                status = (detail.get("status") or status).lower()
            if status not in {"created", "in_progress"}:
                break
        else:
            logger.warning("Coze chat polling timed out after %s attempts", _CHAT_POLL_MAX_ATTEMPTS)

    messages_url = base_url + "/v3/chat/message/list"
    try:
        messages_resp = requests.get(
            messages_url,
            headers=headers,
            params=retrieve_params,
            timeout=(_CONNECT_TIMEOUT, request_timeout),
        )
        messages_resp.raise_for_status()
        messages_payload = messages_resp.json()
    except requests.RequestException as exc:  # pragma: no cover - external
        logger.warning("Coze message list request failed: %s", exc)
        return None
    except ValueError:
        logger.warning("Coze message list returned non-JSON payload")
        return None

    messages = messages_payload.get("data") if isinstance(messages_payload, dict) else None
    if not isinstance(messages, list):
        logger.debug("Coze message list missing data: %s", messages_payload)
        return None

    answer_parts: List[str] = []
    fallback_parts: List[str] = []
    def _extract_message_content(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            parts: List[str] = []
            for item in value:
                if isinstance(item, dict):
                    text = item.get("text") or item.get("content") or item.get("value")
                    if isinstance(text, str) and text.strip():
                        parts.append(text.strip())
                elif isinstance(item, str) and item.strip():
                    parts.append(item.strip())
            return "\n".join(parts).strip()
        if isinstance(value, dict):
            for key in ("text", "content", "value"):
                text = value.get(key)
                if isinstance(text, str) and text.strip():
                    return text.strip()
        return str(value).strip()

    for message in messages:
        if not isinstance(message, dict):
            continue
        if message.get("role") != "assistant":
            continue
        content = _extract_message_content(message.get("content"))
        if not content:
            continue
        message_type = (message.get("type") or "").lower()
        if message_type == "answer":
            answer_parts.append(content)
        elif message_type not in {"verbose"}:
            fallback_parts.append(content)

    content_parts = answer_parts or fallback_parts
    if not content_parts:
        logger.debug("Coze conversation contained no assistant answers: %s", messages)
        return None

    combined = "\n".join(content_parts).strip()
    if not combined:
        return None

    result: dict[str, Any] = {
        "content": combined,
        "model": "coze-agent",
        "raw": {"chat": chat_detail, "messages": messages},
    }
    if usage:
        result["usage"] = usage
    return result


__all__ = ["run_coze_agent"]
