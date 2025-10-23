import importlib.util
import json
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SERVICE_PATH = PROJECT_ROOT / "backend" / "src" / "services" / "finance_breakfast_service.py"


def _u(hex_str: str) -> str:
    return bytes.fromhex(hex_str.replace(" ", "")).decode("utf-8")


# Provide stubs for packages the service depends on so we avoid heavy imports.
def _install_backend_stubs() -> None:
    backend_pkg = types.ModuleType("backend")
    backend_pkg.__path__ = [str(PROJECT_ROOT / "backend")]
    sys.modules["backend"] = backend_pkg

    backend_src_pkg = types.ModuleType("backend.src")
    backend_src_pkg.__path__ = [str(PROJECT_ROOT / "backend" / "src")]
    backend_pkg.src = backend_src_pkg
    sys.modules["backend.src"] = backend_src_pkg

    api_clients_stub = types.ModuleType("backend.src.api_clients")

    def _fetch_finance_breakfast_stub(*args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    api_clients_stub.fetch_finance_breakfast = _fetch_finance_breakfast_stub

    class _DetailStub:  # pragma: no cover
        def __init__(self):
            self.title = None
            self.content = None
            self.url = ""

    def _fetch_eastmoney_detail_stub(*args, **kwargs):  # pragma: no cover
        return _DetailStub()

    def _generate_finance_analysis_stub(*args, **kwargs):  # pragma: no cover
        return None

    api_clients_stub.fetch_eastmoney_detail = _fetch_eastmoney_detail_stub
    api_clients_stub.generate_finance_analysis = _generate_finance_analysis_stub
    sys.modules["backend.src.api_clients"] = api_clients_stub

    config_pkg = types.ModuleType("backend.src.config")
    config_pkg.__path__ = [str(PROJECT_ROOT / "backend" / "src" / "config")]
    sys.modules["backend.src.config"] = config_pkg

    settings_stub = types.ModuleType("backend.src.config.settings")

    def _load_settings_stub(*args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    settings_stub.load_settings = _load_settings_stub
    sys.modules["backend.src.config.settings"] = settings_stub

    dao_stub = types.ModuleType("backend.src.dao")

    class _PlaceholderDAO:  # pragma: no cover
        pass

    dao_stub.FinanceBreakfastDAO = _PlaceholderDAO
    sys.modules["backend.src.dao"] = dao_stub


_install_backend_stubs()


# Provide a lightweight pandas shim to satisfy module-level imports.
if "pandas" not in sys.modules:
    fake_pandas = types.ModuleType("pandas")

    def _shim_to_datetime(value, *_, **__):
        return value

    class _FakeDataFrame:  # pragma: no cover
        pass

    fake_pandas.to_datetime = _shim_to_datetime
    fake_pandas.DataFrame = _FakeDataFrame
    sys.modules["pandas"] = fake_pandas


def _load_service_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "backend.src.services.finance_breakfast_service", SERVICE_PATH
    )
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load finance_breakfast_service module")
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "backend.src.services"
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


service_module = _load_service_module()
list_finance_breakfast = service_module.list_finance_breakfast

FIELD_MARKET_IMPACT = service_module.FIELD_MARKET_IMPACT
FIELD_MARKET_INTENSITY = service_module.FIELD_MARKET_INTENSITY
FIELD_FOCUS_SECTORS = service_module.FIELD_FOCUS_SECTORS
FIELD_OPPORTUNITIES = service_module.FIELD_OPPORTUNITIES
FIELD_RISKS = service_module.FIELD_RISKS
FIELD_ANALYSIS_SUMMARY = service_module.FIELD_ANALYSIS_SUMMARY
FIELD_INVESTMENT_SUMMARY = service_module.FIELD_INVESTMENT_SUMMARY


class FinanceBreakfastDisplayTests(unittest.TestCase):
    @patch.object(service_module, "FinanceBreakfastDAO")
    @patch.object(service_module, "load_settings")
    def test_summary_uses_comprehensive_assessment(self, mock_load_settings, mock_dao_cls) -> None:
        mock_load_settings.return_value = type("Settings", (), {"postgres": object()})()

        ai_payload = {
            service_module.SECTION_COMPREHENSIVE_ASSESS: {
                FIELD_MARKET_IMPACT: _u("E6ADA3E99DA2"),
                FIELD_MARKET_INTENSITY: 58,
                FIELD_FOCUS_SECTORS: [
                    _u("E696B0E883BDE6BA90"),
                    _u("E7A791E68A80"),
                    _u("E58CBBE88DAF"),
                    _u("E7A880E59C9F"),
                ],
                FIELD_OPPORTUNITIES: [
                    _u("E694BFE7AD96E9A9B1E58AA8E79A84E7A791E68A80E5928CE696B0E883BDE6BA90E69DBFE59D97"),
                    _u("E58F97E79B8AE4BA8EE587BAE58FA3E5A29EE995BFE79A84E588B6E980A0E4B89A"),
                    _u("E7A880E59C9FE7AD89E68898E795A5E8B584E6BA90E69DBFE59D97"),
                ],
                FIELD_RISKS: [
                    _u("E4B8ADE7BE8EE8B4B8E69893E8B088E588A4E4B88DE7A1AEE5AE9AE680A7"),
                    _u("E59CB0E7BC98E694BFE6B2BBE9A38EE999A9"),
                    _u("E5AE8FE8A782E7BB8FE6B58EE4B88BE8A18CE58E8BE58A9B"),
                ],
                FIELD_ANALYSIS_SUMMARY: _u(
                    "E695B4E4BD93E69DA5E79C8BEFBC8CE694BFE7AD96E99DA2E5928CE68A80E69CAFE5889BE696B0E9A9B1E58AA8E59BA0E7B4A0E8BE83E5A49AEFBC8CE5B882E59CBAE78EAFE5A283E5818FE7A7AFE69E81E38082"
                    "E6B7B1E59CB3E5B9B6E8B4ADE9878DE7BB84E694BFE7AD96E38081E7AE97E58A9BE6A087E58786E5BBBAE8AEBEE38081E6B5B7E4B88AE9A38EE794B5E79BAEE6A087E7AD89E4B8BAE79BB8E585B3E69DBFE59D97E68F90E4BE9BE6988EE7A1AEE694AFE69291EFBC8CE4B8ADE7BE8EE8B4B8E69893E585B3E7B3BBE7BC93E5928CE9A284E69C9FE4B99FE69C89E588A9E4BA8EE5B882E59CBAE9A38EE999A9E5818FE5A5BDE68F90E58D87E38082"
                ),
                FIELD_INVESTMENT_SUMMARY: _u(
                    "E5BBBAE8AEAEE9878DE782B9E585B3E6B3A8E694BFE7AD96E58F97E79B8AE79A84E7A791E68A80E38081E696B0E883BDE6BA90E5928CE68898E795A5E8B584E6BA90E69DBFE59D97E38082"
                ),
            },
            _u("E5BDB1E5938DE58886E69E90"): [
                {"event": "placeholder"},
            ],
        }
        raw_ai = json.dumps(ai_payload, ensure_ascii=False)
        summary_json = json.dumps(ai_payload[service_module.SECTION_COMPREHENSIVE_ASSESS], ensure_ascii=False)
        detail_json = json.dumps(ai_payload[_u("E5BDB1E5938DE58886E69E90")], ensure_ascii=False)

        mock_dao_cls.return_value.list_recent.return_value = [
            {
                "title": _u("E6AF8FE697A5E7BBBCE8BFB0"),
                "summary": raw_ai,
                "content": _u("E58E9FE5A78BE58685E5AEB9"),
                "ai_extract": raw_ai,
                "ai_extract_summary": summary_json,
                "ai_extract_detail": detail_json,
                "published_at": datetime(2024, 10, 27, 7, 0, 0),
                "url": "https://example.com",
            }
        ]

        results = list_finance_breakfast(limit=1)
        self.assertEqual(len(results), 1)
        summary = results[0]["summary"]
        expected = (
            f"{FIELD_MARKET_IMPACT}：{_u('E6ADA3E99DA2')}（58分）；"
            f"{FIELD_FOCUS_SECTORS}：{_u('E696B0E883BDE6BA90')}、{_u('E7A791E68A80')}、{_u('E58CBBE88DAF')}、{_u('E7A880E59C9F')}；"
            f"{FIELD_OPPORTUNITIES}：{_u('E694BFE7AD96E9A9B1E58AA8E79A84E7A791E68A80E5928CE696B0E883BDE6BA90E69DBFE59D97')}、"
            f"{_u('E58F97E79B8AE4BA8EE587BAE58FA3E5A29EE995BFE79A84E588B6E980A0E4B89A')}、"
            f"{_u('E7A880E59C9FE7AD89E68898E795A5E8B584E6BA90E69DBFE59D97')}；"
            f"{FIELD_RISKS}：{_u('E4B8ADE7BE8EE8B4B8E69893E8B088E588A4E4B88DE7A1AEE5AE9AE680A7')}、"
            f"{_u('E59CB0E7BC98E694BFE6B2BBE9A38EE999A9')}、{_u('E5AE8FE8A782E7BB8FE6B58EE4B88BE8A18CE58E8BE58A9B')}；"
            f"{FIELD_ANALYSIS_SUMMARY}：{_u('E695B4E4BD93E69DA5E79C8BEFBC8CE694BFE7AD96E99DA2E5928CE68A80E69CAFE5889BE696B0E9A9B1E58AA8E59BA0E7B4A0E8BE83E5A49AEFBC8CE5B882E59CBAE78EAFE5A283E5818FE7A7AFE69E81E38082E6B7B1E59CB3E5B9B6E8B4ADE9878DE7BB84E694BFE7AD96E38081E7AE97E58A9BE6A087E58786E5BBBAE8AEBEE38081E6B5B7E4B88AE9A38EE794B5E79BAEE6A087E7AD89E4B8BAE79BB8E585B3E69DBFE59D97E68F90E4BE9BE6988EE7A1AEE694AFE69291EFBC8CE4B8ADE7BE8EE8B4B8E69893E585B3E7B3BBE7BC93E5928CE9A284E69C9FE4B99FE69C89E588A9E4BA8EE5B882E59CBAE9A38EE999A9E5818FE5A5BDE68F90E58D87E38082')}；"
            f"{FIELD_INVESTMENT_SUMMARY}：{_u('E5BBBAE8AEAEE9878DE782B9E585B3E6B3A8E694BFE7AD96E58F97E79B8AE79A84E7A791E68A80E38081E696B0E883BDE6BA90E5928CE68898E795A5E8B584E6BA90E69DBFE59D97E38082')}"
        )
        self.assertEqual(summary, expected)
        self.assertEqual(results[0]["ai_extract_summary"], json.loads(summary_json))
        self.assertEqual(results[0]["ai_extract_detail"], json.loads(detail_json))


if __name__ == "__main__":
    unittest.main()


