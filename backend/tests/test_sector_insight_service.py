import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


class _DummySQLStatement:
    def __init__(self, template):
        self.template = template

    def format(self, *_, **__):
        return self


class _DummySQLModule:
    def SQL(self, template):
        return _DummySQLStatement(template)

    def Identifier(self, value):
        return value


def _ensure_psycopg2_stub() -> None:
    if "psycopg2" in sys.modules:
        return
    dummy = SimpleNamespace(sql=_DummySQLModule())
    sys.modules["psycopg2"] = dummy


def _prepare_package(name: str, path: Path) -> ModuleType:
    module = sys.modules.get(name)
    if isinstance(module, ModuleType):
        module.__path__ = [str(path)]
        return module
    module = ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module
    return module


def _load_service_module():
    _ensure_psycopg2_stub()

    backend_pkg = _prepare_package("backend", ROOT / "backend")
    src_pkg = _prepare_package("backend.src", ROOT / "backend" / "src")
    setattr(backend_pkg, "src", src_pkg)

    services_pkg = _prepare_package("backend.src.services", ROOT / "backend" / "src" / "services")
    setattr(src_pkg, "services", services_pkg)

    if "backend.src.api_clients" not in sys.modules:
        api_clients_stub = ModuleType("backend.src.api_clients")

        def _fake_generate(*_args, **_kwargs):  # pragma: no cover - defensive
            raise RuntimeError("generate_finance_analysis should be patched in tests")

        api_clients_stub.generate_finance_analysis = _fake_generate
        sys.modules["backend.src.api_clients"] = api_clients_stub
        sys.modules["backend.src.services.api_clients"] = api_clients_stub
        setattr(src_pkg, "api_clients", api_clients_stub)
        setattr(services_pkg, "api_clients", api_clients_stub)

    if "backend.src.dao" not in sys.modules:
        dao_stub = ModuleType("backend.src.dao")

        class _PlaceholderDAO:  # pragma: no cover - simple stub
            def __init__(self, *_args, **_kwargs):
                pass

        dao_stub.NewsArticleDAO = _PlaceholderDAO
        dao_stub.NewsInsightDAO = _PlaceholderDAO
        dao_stub.NewsSectorInsightDAO = _PlaceholderDAO
        sys.modules["backend.src.dao"] = dao_stub
        sys.modules["backend.src.services.dao"] = dao_stub
        setattr(src_pkg, "dao", dao_stub)
        setattr(services_pkg, "dao", dao_stub)

    if "backend.src.config.settings" not in sys.modules:
        config_pkg = ModuleType("backend.src.config")
        config_pkg.__path__ = [str(ROOT / "backend" / "src" / "config")]
        settings_stub = ModuleType("backend.src.config.settings")

        def _placeholder_load_settings(*_args, **_kwargs):  # pragma: no cover - patched in tests
            raise RuntimeError("load_settings should be patched in tests")

        settings_stub.load_settings = _placeholder_load_settings
        config_pkg.settings = settings_stub
        sys.modules["backend.src.config"] = config_pkg
        sys.modules["backend.src.config.settings"] = settings_stub
        sys.modules["backend.src.services.config"] = config_pkg
        sys.modules["backend.src.services.config.settings"] = settings_stub
        setattr(src_pkg, "config", config_pkg)
        setattr(services_pkg, "config", config_pkg)

    module_name = "backend.src.services.sector_insight_service"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(
        module_name,
        ROOT / "backend" / "src" / "services" / "sector_insight_service.py",
        submodule_search_locations=[str(ROOT / "backend" / "src" / "services")],
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError("Unable to load sector_insight_service module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


service = _load_service_module()


def _sample_article(**overrides):
    base = {
        "article_id": overrides.get("article_id", "a1"),
        "title": overrides.get("title", "新能源车补贴政策利好"),
        "impact_summary": overrides.get("impact_summary", "政策利好新能源车板块"),
        "impact_analysis": overrides.get(
            "impact_analysis",
            "政策延续有利于整车与锂电，但需关注补贴退坡与供需错配风险。",
        ),
        "impact_confidence": overrides.get("impact_confidence", 0.82),
        "impact_industries": overrides.get("impact_industries", ["新能源车"]),
        "impact_sectors": overrides.get("impact_sectors", ["锂电池"]),
        "impact_themes": overrides.get("impact_themes", ["特斯拉供应链"]),
        "extra_metadata": overrides.get(
            "extra_metadata",
            {
                "impact_severity": "high",
                "severity_score": 0.88,
                "event_type": "policy",
                "time_sensitivity": "中期/阶段性",
                "focus_topics": ["补贴", "新能源"],
            },
        ),
        "published_at": overrides.get("published_at", datetime(2025, 11, 4, 9, 30)),
        "source": overrides.get("source", "global_flash"),
        "url": overrides.get("url", "https://example.com/a1"),
        "impact_levels": overrides.get("impact_levels", ["industry", "sector", "theme"]),
    }
    base.update(overrides)
    return base


def test_build_sector_group_snapshot_creates_group_rankings():
    now = datetime(2025, 11, 4, 10, 0)
    articles = [
        _sample_article(article_id="a1", published_at=now - timedelta(minutes=5)),
        _sample_article(
            article_id="a2",
            title="算力产业链加码资本开支",
            impact_summary="服务器板块受益，关注散热与液冷公司",
            impact_analysis="多家云厂商提升资本开支，IDC、服务器及液冷链条受益，但需警惕订单兑现节奏",
            impact_confidence=0.76,
            impact_industries=["算力基础设施"],
            impact_sectors=["服务器"],
            impact_themes=["AI 算力"],
            extra_metadata={
                "impact_severity": "medium",
                "severity_score": 0.62,
                "event_type": "market_liquidity",
                "time_sensitivity": "短期",
                "focus_topics": ["液冷", "IDC"],
            },
            published_at=now - timedelta(hours=1),
        ),
    ]

    snapshot = service.build_sector_group_snapshot(articles, lookback_hours=24, reference_time=now)

    assert snapshot["groupCount"] == 6
    names = [group["name"] for group in snapshot["groups"]]
    assert "新能源车" in names
    assert "AI 算力" in names

    first_group = snapshot["groups"][0]
    assert first_group["sampleArticles"], "expected sample articles for top group"
    assert first_group["sampleArticles"][0]["articleId"] in {"a1", "a2"}


def test_generate_sector_insight_summary_without_llm(monkeypatch):
    articles = [
        _sample_article(article_id="g1", impact_confidence=0.9),
        _sample_article(
            article_id="g2",
            impact_industries=["光伏"],
            impact_sectors=["光伏设备"],
            impact_themes=["TOPCon"],
            extra_metadata={
                "impact_severity": "high",
                "severity_score": 0.92,
                "event_type": "macro_policy",
                "time_sensitivity": "短期",
                "focus_topics": ["出货", "新增装机"],
            },
            published_at=datetime(2025, 11, 4, 8, 45),
        ),
    ]

    monkeypatch.setattr(service, "collect_recent_sector_headlines", lambda **_: articles)

    dummy_settings = SimpleNamespace(deepseek=None, postgres=SimpleNamespace())
    monkeypatch.setattr(service, "load_settings", lambda *_args, **_kwargs: dummy_settings)

    captures: dict[str, object] = {}

    class DummyDAO:
        def __init__(self, *_args, **_kwargs):
            self.payload = None

        def insert_summary(self, payload):
            self.payload = payload
            captures["payload"] = payload
            return "sector-summary-001"

    monkeypatch.setattr(service, "NewsSectorInsightDAO", lambda *_args, **_kwargs: DummyDAO())

    result = service.generate_sector_insight_summary(run_llm=False, lookback_hours=12, limit=20)

    assert result["summary_id"] == "sector-summary-001"
    assert result["group_snapshot"]["groupCount"] >= 3
    assert "sector-summary-001" in result["summary_id"]

    persisted = captures.get("payload")
    assert persisted is not None
    assert persisted["group_count"] == result["group_snapshot"]["groupCount"]
    assert persisted["summary_json"] is None
    assert persisted["raw_response"] is None
