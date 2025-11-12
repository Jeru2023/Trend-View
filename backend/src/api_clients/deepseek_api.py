"""
Client helpers for DeepSeek large language model API.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

import requests

from ..config.settings import DeepseekSettings

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 90.0
_CONNECT_TIMEOUT = 10.0
_BACKOFF_INITIAL_SECONDS = 2.0
_BACKOFF_MAX_SECONDS = 10.0


def generate_finance_analysis(
    news_content: str,
    *,
    settings: DeepseekSettings,
    prompt_template: str,
    timeout: Optional[float] = None,
    temperature: float = 0.3,
    model_override: Optional[str] = None,
    response_format: Optional[dict] = None,
    max_output_tokens: Optional[int] = None,
    return_usage: bool = False,
) -> Optional[object]:
    """Call the DeepSeek chat completion endpoint to analyse finance news content."""
    if not news_content:
        logger.debug("DeepSeek skipped empty news content")
        return None

    prompt_content = news_content.strip()
    if not prompt_content:
        logger.debug("DeepSeek skipped whitespace-only news content")
        return None

    prompt = prompt_template.replace("{news_content}", prompt_content)

    url = settings.base_url.rstrip("/") + "/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {settings.token}",
        "Content-Type": "application/json",
    }
    payload: dict[str, object] = {
        "model": model_override or settings.model,
        "messages": [
            {
                "role": "system",
                "content": "You are an experienced A-share market analyst. Respond in JSON only.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": temperature,
    }

    if response_format is not None:
        payload["response_format"] = response_format
    else:
        payload["response_format"] = {"type": "json_object"}

    if max_output_tokens is not None:
        payload["max_output_tokens"] = int(max_output_tokens)

    request_timeout = float(timeout or getattr(settings, "request_timeout_seconds", _DEFAULT_TIMEOUT))
    request_timeout = max(request_timeout, 5.0)
    retry_attempts = max(int(getattr(settings, "max_retries", 0)), 0)
    attempts = retry_attempts + 1

    backoff_seconds = _BACKOFF_INITIAL_SECONDS
    response: Optional[requests.Response] = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=(_CONNECT_TIMEOUT, request_timeout),
            )
            response.raise_for_status()
            break
        except requests.Timeout as exc:  # pragma: no cover - external API
            logger.warning(
                "DeepSeek request timed out after %.1fs (attempt %s/%s): %s",
                request_timeout,
                attempt,
                attempts,
                exc,
            )
            if attempt >= attempts:
                return None
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, _BACKOFF_MAX_SECONDS)
        except requests.RequestException as exc:  # pragma: no cover - external API
            error_detail = None
            if isinstance(exc, requests.HTTPError) and exc.response is not None:
                try:
                    error_detail = exc.response.text
                except Exception:  # pragma: no cover - defensive
                    error_detail = None
            if error_detail:
                logger.warning("DeepSeek request failed: %s | body=%s", exc, error_detail)
            else:
                logger.warning("DeepSeek request failed: %s", exc)
            return None
    else:
        return None

    if response is None:
        return None

    try:
        data = response.json()
    except ValueError as exc:  # pragma: no cover - invalid JSON
        logger.warning("DeepSeek returned non-JSON payload: %s", exc)
        return None

    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:  # pragma: no cover - unexpected format
        logger.warning("DeepSeek response missing content: %s", exc)
        return None

    if not content:
        logger.debug("DeepSeek returned empty content")
        return None

    text = content.strip()
    if not text:
        return None

    normalized = None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("DeepSeek response is not valid JSON; storing raw text")
    else:
        try:
            normalized = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            normalized = None

    result_text = normalized or text

    if return_usage:
        model_name = data.get("model")
        usage = data.get("usage") if isinstance(data, dict) else None
        return {
            "content": result_text,
            "usage": usage or {},
            "raw": text,
            "model": model_name,
        }

    return result_text


__all__ = ["generate_finance_analysis"]
