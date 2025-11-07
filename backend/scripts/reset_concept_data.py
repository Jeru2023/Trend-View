"""Utility script to clear concept fund flow and index history tables."""

from __future__ import annotations

from backend.src.config.settings import load_settings
from backend.src.dao import ConceptFundFlowDAO, ConceptIndexHistoryDAO


def main() -> None:
    settings = load_settings()
    ConceptFundFlowDAO(settings.postgres).truncate()
    ConceptIndexHistoryDAO(settings.postgres).truncate()
    print("Concept fund flow and concept index history tables have been truncated.")


if __name__ == "__main__":
    main()
