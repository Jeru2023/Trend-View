#!/usr/bin/env python3
"""
Quick DeepSeek prompt tester.

Usage:
    python scripts/deepseek_prompt_runner.py --prompt "分析博俊在手订单是否充足。"

You can also pass --prompt-file path/to/file.txt or pipe content via STDIN.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.src.config.settings import load_settings


def _read_prompt(args: argparse.Namespace) -> str:
    if args.prompt:
        return args.prompt.strip()
    if args.prompt_file:
        return Path(args.prompt_file).read_text(encoding="utf-8").strip()
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    try:
        user_input = input("请输入要测试的 Prompt（直接回车退出）: ").strip()
    except EOFError:
        user_input = ""
    if not user_input:
        raise SystemExit("Prompt 为空，已取消。")
    return user_input


def run_prompt(
    prompt: str,
    *,
    settings_path: Optional[str],
    system_prompt: Optional[str],
    model_override: Optional[str],
    temperature: float,
) -> None:
    app_settings = load_settings(settings_path)
    deepseek = app_settings.deepseek
    if deepseek is None:
        raise SystemExit("DeepSeek settings not configured. Populate backend/config/settings.local.json (deepseek.token, etc.).")

    url = deepseek.base_url.rstrip("/") + "/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {deepseek.token}",
        "Content-Type": "application/json",
    }
    payload: dict[str, object] = {
        "model": model_override or deepseek.model,
        "messages": [
            {
                "role": "system",
                "content": system_prompt
                or "You are an experienced China A-share market analyst. Respond in Chinese unless asked otherwise.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=(10.0, getattr(deepseek, "request_timeout_seconds", 90.0)),
    )
    response.raise_for_status()
    data = response.json()
    choice = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {})

    print("=== DeepSeek response ===")
    print(choice.strip())
    if usage:
        print("\n--- Usage ---")
        print(json.dumps(usage, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="DeepSeek Reasoner prompt tester")
    parser.add_argument("--prompt", help="Prompt text to send.")
    parser.add_argument("--prompt-file", help="Read prompt from file path.")
    parser.add_argument("--settings", help="Path to settings file (defaults to backend/config/settings.local.json if present).")
    parser.add_argument("--system", help="Override the default system prompt.")
    parser.add_argument("--model", help="Override the model name (defaults to settings.deepseek.model).")
    parser.add_argument("--temperature", type=float, default=0.3, help="Generation temperature (default: 0.3).")
    parser.add_argument("--save-prompt", help="If provided, save the resolved prompt text to this file before sending.")
    args = parser.parse_args()

    # prompt = _read_prompt(args)
    prompt = '分析博俊科技在手订单是否充足。'
    if not prompt:
        raise SystemExit("Prompt is empty.")

    if args.save_prompt:
        Path(args.save_prompt).write_text(prompt, encoding="utf-8")

    run_prompt(
        prompt,
        settings_path=args.settings,
        system_prompt=args.system,
        model_override=args.model,
        temperature=args.temperature,
    )


if __name__ == "__main__":
    main()
