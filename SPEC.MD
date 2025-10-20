Session Boot Rules
Always run chcp 65001 in PowerShell as the first command so the console uses UTF-8. Apply UTF-8 encoding for all file and console read/write/print operations.
Immediately set [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 and export PYTHONIOENCODING=utf-8 (plus similar flags for other runtimes) so downstream tools avoid mojibake.

Workflow
After user approval, commit changes using Git.
Log every new requirement in this SPEC.MD file for future reference.
Whenever a Python package is installed or upgraded, update requirements.txt to keep dependency pins in sync.

Documentation Standards
Keep all Markdown files (e.g., README.md, SPEC.MD) written entirely in English, even if requirements are delivered in another language.

## Dependency Log
- 2025-10-20: Installed `pandas==1.5.3`, `psycopg2-binary==2.9.6`, and `tushare==1.4.24` in the trend environment and recorded them in `requirements.txt`.
