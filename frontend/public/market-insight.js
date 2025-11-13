console.info("Market Insight bundle v20261110");

const translations = getTranslations("marketInsight");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const ARTICLE_LIMIT = 40;
const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  summary: document.getElementById("market-insight-summary"),
  stages: document.getElementById("market-insight-stages"),
  refreshButton: document.getElementById("market-insight-refresh"),
  langButtons: document.querySelectorAll(".lang-btn"),
  status: document.getElementById("market-insight-status"),
};

const state = {
  data: null,
};

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch (error) {
    /* ignore */
  }
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
  }
  const htmlLang = document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) {
    return htmlLang;
  }
  const browserLang = (navigator.language || "").toLowerCase();
  return translations[browserLang] ? browserLang : "zh";
}

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    /* ignore */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

let currentLang = getInitialLanguage();
persistLanguage(currentLang);

function getDict() {
  return translations[currentLang] || translations.en;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const dateValue = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(dateValue.getTime())) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return `${dateValue.toLocaleDateString(locale)} ${dateValue.toLocaleTimeString(locale, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })}`;
}

function formatNumber(value, digits = 0) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return numeric.toFixed(digits);
}

function formatPercent(value, digits = 0) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const percentage = Math.max(0, Math.min(numeric, 1)) * 100;
  return `${percentage.toFixed(digits)}%`;
}

function formatInteger(value) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return numeric.toLocaleString(locale);
}

function setRefreshLoading(isLoading) {
  const button = elements.refreshButton;
  if (!button) return;
  const label = button.querySelector(".btn__label");
  const dict = getDict();
  if (isLoading) {
    button.dataset.loading = "1";
    button.disabled = true;
    if (label) label.textContent = dict.refreshing || "Refreshing";
  } else {
    delete button.dataset.loading;
    button.disabled = false;
    if (label) label.textContent = dict.refreshButton || "Refresh";
  }
}

function setStatus(message, tone) {
  const node = elements.status;
  if (!node) return;
  if (!message) {
    node.textContent = "";
    node.removeAttribute("data-tone");
    return;
  }
  node.textContent = message;
  if (tone) {
    node.setAttribute("data-tone", tone);
  } else {
    node.removeAttribute("data-tone");
  }
}

function clearContainer(node, message) {
  if (!node) return;
  node.innerHTML = "";
  if (message) {
    const placeholder = document.createElement("div");
    placeholder.className = "empty-placeholder";
    placeholder.textContent = message;
    node.appendChild(placeholder);
  }
}

function renderStageResults(stageResults) {
  const container = elements.stages;
  if (!container) return;
  container.innerHTML = "";
  if (!Array.isArray(stageResults) || !stageResults.length) {
    container.style.display = "none";
    return;
  }
  container.style.display = "";
  const dict = getDict();
  const heading = document.createElement("h2");
  heading.className = "stage-list__heading";
  heading.textContent = dict.stageSectionTitle || "Staged Reasoning";
  container.appendChild(heading);

  stageResults.forEach((stage) => {
    if (!stage || typeof stage !== "object") return;
    const card = document.createElement("article");
    card.className = "stage-card";

    const header = document.createElement("header");
    header.className = "stage-card__header";
    const title = document.createElement("h3");
    title.className = "stage-card__title";
    title.textContent = stage.title || stage.stage || dict.stageSectionTitle || "Stage";
    header.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "stage-card__meta";
    const sentimentLabelMap = {
      bullish: dict.sentimentBullish || "Bullish",
      bearish: dict.sentimentBearish || "Bearish",
      neutral: dict.sentimentNeutral || "Neutral",
    };
    const biasValue = (stage.bias || "neutral").toLowerCase();
    const biasChip = document.createElement("span");
    biasChip.className = `stage-chip stage-chip--${["bullish", "bearish", "neutral"].includes(biasValue) ? biasValue : "neutral"}`;
    biasChip.textContent = sentimentLabelMap[biasValue] || sentimentLabelMap.neutral;
    meta.appendChild(biasChip);

    if (stage.confidence !== undefined && stage.confidence !== null && Number.isFinite(Number(stage.confidence))) {
      const confidence = document.createElement("span");
      confidence.className = "stage-card__confidence";
      confidence.textContent = `${dict.stageConfidenceLabel || "Confidence"}: ${formatPercent(stage.confidence, 0)}`;
      meta.appendChild(confidence);
    }

    header.appendChild(meta);
    card.appendChild(header);

    if (stage.analysis) {
      const analysis = document.createElement("p");
      analysis.className = "stage-card__analysis";
      analysis.textContent = stage.analysis;
      card.appendChild(analysis);
    }

    const highlights = Array.isArray(stage.highlights) ? stage.highlights.filter(Boolean) : [];
    if (highlights.length) {
      const highlightsHeading = document.createElement("h4");
      highlightsHeading.className = "stage-card__subheading";
      highlightsHeading.textContent = dict.stageHighlightsLabel || "Highlights";
      card.appendChild(highlightsHeading);
      const list = document.createElement("ul");
      list.className = "stage-card__list";
      highlights.forEach((item) => {
        const li = document.createElement("li");
        li.textContent = item;
        list.appendChild(li);
      });
      card.appendChild(list);
    }

    const metrics = Array.isArray(stage.key_metrics) ? stage.key_metrics.filter(Boolean) : [];
    if (metrics.length) {
      const metricsHeading = document.createElement("h4");
      metricsHeading.className = "stage-card__subheading";
      metricsHeading.textContent = dict.stageMetricsLabel || "Key Metrics";
      card.appendChild(metricsHeading);
      const metricList = document.createElement("dl");
      metricList.className = "stage-card__metrics";
      metrics.forEach((metric) => {
        if (!metric || typeof metric !== "object") return;
        const dt = document.createElement("dt");
        dt.textContent = metric.label || "--";
        const dd = document.createElement("dd");
        const value = document.createElement("strong");
        value.textContent = metric.value || "--";
        dd.appendChild(value);
        if (metric.insight) {
          const insight = document.createElement("span");
          insight.textContent = ` · ${metric.insight}`;
          dd.appendChild(insight);
        }
        metricList.appendChild(dt);
        metricList.appendChild(dd);
      });
      card.appendChild(metricList);
    }

    container.appendChild(card);
  });
}

function renderSummary() {
  const dict = getDict();
  const container = elements.summary;
  if (!container) return;

  const summaryWrapper = state.data?.summary;
  container.innerHTML = "";

  const llmSummary = summaryWrapper?.summary;
  if (!summaryWrapper || !llmSummary) {
    const message = container.dataset[`empty${currentLang.toUpperCase()}`] || dict.emptySummary || "--";
    clearContainer(container, message);
    renderStageResults([]);
    return;
  }

  const stageResults = Array.isArray(llmSummary.stage_results) ? llmSummary.stage_results : [];
  renderStageResults(stageResults);

  const comprehensive = llmSummary.comprehensive_conclusion || {};
  const intermediate = llmSummary.intermediate_analysis || {};

  const card = document.createElement("article");
  card.className = "insight-card";

  const header = document.createElement("div");
  header.className = "insight-card__header";
  const title = document.createElement("h2");
  title.className = "insight-card__title";
  title.textContent = dict.summarySectionTitle || "Market Insight";
  header.appendChild(title);

  const sentimentLabelMap = {
    bullish: dict.sentimentBullish || "Bullish",
    bearish: dict.sentimentBearish || "Bearish",
    neutral: dict.sentimentNeutral || "Neutral",
  };
  const sentimentValueRaw = (comprehensive.bias || "").toLowerCase();
  const sentimentValue = ["bullish", "bearish", "neutral"].includes(sentimentValueRaw)
    ? sentimentValueRaw
    : "neutral";
  const sentimentChip = document.createElement("span");
  sentimentChip.className = `sentiment-chip sentiment-chip--${sentimentValue}`;
  sentimentChip.textContent = sentimentLabelMap[sentimentValue] || dict.sentimentUnknown || "Unknown";

  const tokenHintParts = [];
  const promptTokens = summaryWrapper.promptTokens;
  const completionTokens = summaryWrapper.completionTokens;
  const totalTokens = summaryWrapper.totalTokens;
  if (promptTokens !== null && promptTokens !== undefined) {
    tokenHintParts.push(`${dict.promptTokensLabel || "Prompt"} ${formatInteger(promptTokens)}`);
  }
  if (completionTokens !== null && completionTokens !== undefined) {
    tokenHintParts.push(`${dict.completionTokensLabel || "Completion"} ${formatInteger(completionTokens)}`);
  }
  const tokenHint = tokenHintParts.length ? tokenHintParts.join(" · ") : null;

  const stats = document.createElement("div");
  stats.className = "insight-card__stats";
  const statsItems = [
    {
      label: dict.statsSentimentLabel || "市场情绪",
      content: (() => {
        const wrapper = document.createElement("div");
        wrapper.className = "insight-stat__sentiment";
        wrapper.appendChild(sentimentChip);
        return wrapper;
      })(),
    },
    {
      label: dict.statsConfidenceLabel || "置信度",
      value:
        comprehensive.confidence !== null && comprehensive.confidence !== undefined
          ? formatPercent(comprehensive.confidence, 0)
          : "--",
    },
  ];

  if (totalTokens !== null && totalTokens !== undefined) {
    statsItems.push({
      label: dict.statsTokenLabel || "Token",
      value: formatInteger(totalTokens),
      hint: tokenHint,
    });
  }

  statsItems.forEach((item) => {
    const stat = document.createElement("div");
    stat.className = "insight-stat";

    const label = document.createElement("div");
    label.className = "insight-stat__label";
    label.textContent = item.label;
    stat.appendChild(label);

    if (item.content) {
      const valueWrapper = document.createElement("div");
      valueWrapper.className = "insight-stat__value insight-stat__value--content";
      valueWrapper.appendChild(item.content);
      stat.appendChild(valueWrapper);
    } else {
      const value = document.createElement("div");
      value.className = "insight-stat__value";
      value.textContent = item.value ?? "--";
      stat.appendChild(value);
    }

    if (item.hint) {
      const hint = document.createElement("div");
      hint.className = "insight-stat__hint";
      hint.textContent = item.hint;
      stat.appendChild(hint);
    }

    stats.appendChild(stat);
  });

  const meta = document.createElement("div");
  meta.className = "insight-card__meta";
  meta.innerHTML = `
    <div>${dict.generatedAtLabel || "Generated"}: <strong>${formatDateTime(summaryWrapper.generatedAt)}</strong></div>
    <div>${dict.windowRangeLabel || "Window"}: ${formatDateTime(summaryWrapper.windowStart)} - ${formatDateTime(summaryWrapper.windowEnd)}</div>
    <div>${dict.elapsedLabel || "Latency"}: ${
      summaryWrapper.elapsedSeconds !== null && summaryWrapper.elapsedSeconds !== undefined
        ? `${summaryWrapper.elapsedSeconds.toFixed(2)}s`
        : "--"
    }</div>
    <div>${dict.modelLabel || "Model"}: ${summaryWrapper.modelUsed || "-"}</div>
  `;

  header.appendChild(meta);
  card.appendChild(header);
  card.appendChild(stats);

  const overviewBlock = document.createElement("section");
  overviewBlock.className = "insight-card__section";
  const overviewHeading = document.createElement("h3");
  overviewHeading.textContent =
    dict.comprehensiveSummaryLabel || dict.marketOverviewLabel || dict.summarySectionTitle || "Overview";
  overviewBlock.appendChild(overviewHeading);
  const overviewPara = document.createElement("p");
  overviewPara.textContent = comprehensive.summary || "--";
  overviewBlock.appendChild(overviewPara);
  card.appendChild(overviewBlock);

  const keySignals = Array.isArray(comprehensive.key_signals)
    ? comprehensive.key_signals.filter((item) => item && (item.title || item.detail))
    : [];
  if (keySignals.length) {
    const signalsSection = document.createElement("section");
    signalsSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.signalsSectionTitle || "Key Signals";
    signalsSection.appendChild(heading);
    const list = document.createElement("ul");
    list.className = "insight-card__detail-list";
    keySignals.forEach((signal) => {
      const li = document.createElement("li");
      const strong = document.createElement("strong");
      strong.textContent = signal.title || dict.signalLabel || "Signal";
      li.appendChild(strong);
      if (signal.detail) {
        const detail = document.createElement("p");
        detail.textContent = signal.detail;
        li.appendChild(detail);
      }
      if (Array.isArray(signal.supporting_analyses) && signal.supporting_analyses.length) {
        const sources = document.createElement("div");
        sources.className = "insight-card__detail-confidence";
        sources.textContent = `${dict.signalSupportLabel || "Based on"}: ${signal.supporting_analyses.join(", ")}`;
        li.appendChild(sources);
      }
      list.appendChild(li);
    });
    signalsSection.appendChild(list);
    card.appendChild(signalsSection);
  }

  const position = comprehensive.position_suggestion || {};
  const positionItems = [
    { label: dict.positionShortLabel || "Short term", value: position.short_term },
    { label: dict.positionMediumLabel || "Medium term", value: position.medium_term },
    { label: dict.positionRiskLabel || "Risk control", value: position.risk_control },
  ].filter((item) => item.value);
  if (positionItems.length) {
    const positionSection = document.createElement("section");
    positionSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.positionSectionTitle || "Position Suggestion";
    positionSection.appendChild(heading);
    const list = document.createElement("ul");
    list.className = "insight-card__list";
    positionItems.forEach((item) => {
      const li = document.createElement("li");
      li.textContent = `${item.label}: ${item.value}`;
      list.appendChild(li);
    });
    positionSection.appendChild(list);
    card.appendChild(positionSection);
  }

  const scenarios = Array.isArray(comprehensive.scenario_analysis)
    ? comprehensive.scenario_analysis.filter((item) => item && (item.scenario || item.conditions || item.target))
    : [];
  if (scenarios.length) {
    const scenarioSection = document.createElement("section");
    scenarioSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.scenarioSectionTitle || "Scenario Analysis";
    scenarioSection.appendChild(heading);
    const list = document.createElement("ul");
    list.className = "insight-card__detail-list insight-card__scenario-list";
    scenarios.forEach((scenario) => {
      const li = document.createElement("li");
      if (scenario.scenario) {
        const strong = document.createElement("strong");
        strong.textContent = scenario.scenario;
        li.appendChild(strong);
      }
      if (scenario.conditions) {
        const conditions = document.createElement("p");
        conditions.className = "insight-card__scenario-meta";
        conditions.textContent = `${dict.scenarioConditionsLabel || "Conditions"}: ${scenario.conditions}`;
        li.appendChild(conditions);
      }
      if (scenario.target) {
        const target = document.createElement("p");
        target.className = "insight-card__detail-analysis";
        target.textContent = `${dict.scenarioTargetLabel || "Outcome"}: ${scenario.target}`;
        li.appendChild(target);
      }
      list.appendChild(li);
    });
    scenarioSection.appendChild(list);
    card.appendChild(scenarioSection);
  }

  const intermediateDefs = [
    { key: "index_analysis", label: dict.indexAnalysisLabel || "Index & Trend" },
    { key: "fund_flow_analysis", label: dict.fundFlowAnalysisLabel || "Funds & Leverage" },
    { key: "sentiment_analysis", label: dict.sentimentAnalysisLabel || "Market Sentiment" },
    { key: "macro_analysis", label: dict.macroAnalysisLabel || "Macro Backdrop" },
  ];
  const hasIntermediate = intermediateDefs.some((def) => typeof intermediate[def.key] === "string" && intermediate[def.key]);
  if (hasIntermediate) {
    const intermediateSection = document.createElement("section");
    intermediateSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.intermediateSectionTitle || "Detailed Analyses";
    intermediateSection.appendChild(heading);
    intermediateDefs.forEach((def) => {
      const text = intermediate[def.key];
      if (!text) return;
      const subsection = document.createElement("div");
      subsection.className = "insight-card__subsection";
      const subheading = document.createElement("h4");
      subheading.textContent = def.label;
      subsection.appendChild(subheading);
      const paragraph = document.createElement("p");
      paragraph.textContent = text;
      subsection.appendChild(paragraph);
      intermediateSection.appendChild(subsection);
    });
    card.appendChild(intermediateSection);
  }

  container.appendChild(card);
}

async function fetchMarketInsight() {
  const dict = getDict();
  clearContainer(elements.summary, dict.loading || "Loading...");
  renderStageResults([]);
  try {
    const response = await fetch(`${API_BASE}/market/market-insight`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    state.data = data;
    renderSummary();
  } catch (error) {
    console.error("Failed to fetch market insight", error);
    clearContainer(elements.summary, error?.message || "Failed to load insight");
    setStatus(error?.message || dict.statusFailed || dict.refreshFailed || "Request failed", "error");
  }
}

async function triggerManualSync() {
  if (elements.refreshButton?.dataset.loading === "1") {
    return;
  }
  setRefreshLoading(true);
  const dict = getDict();
  setStatus(dict.statusGenerating || "Generating...", "info");
  try {
    const response = await fetch(`${API_BASE}/control/sync/market-insight`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lookbackHours: 24, articleLimit: ARTICLE_LIMIT }),
    });
    if (!response.ok) {
      let message = response.statusText || `HTTP ${response.status}`;
      try {
        const payload = await response.json();
        if (payload?.detail) {
          message = payload.detail;
        }
      } catch (parseError) {
        /* ignore */
      }
      if (response.status === 409) {
        setStatus(dict.statusRunning || message, "warn");
        return;
      }
      throw new Error(message);
    }
    await fetchMarketInsight();
    setStatus(dict.statusGenerated || "Insight updated.", "success");
  } catch (error) {
    console.error("Manual market insight generation failed", error);
    setStatus(error?.message || dict.statusFailed || dict.refreshFailed || "Request failed", "error");
  } finally {
    setRefreshLoading(false);
  }
}

function bindLanguageButtons() {
  elements.langButtons.forEach((btn) => {
    btn.onclick = () => {
      const lang = btn.dataset.lang;
      if (lang && translations[lang]) {
        currentLang = lang;
        persistLanguage(lang);
        applyTranslations();
        fetchMarketInsight();
      }
    };
  });
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.title = dict.title || "Trend View - Market Insight";
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (key && dict[key]) {
      node.textContent = dict[key];
    }
  });
  document.querySelectorAll("[data-i18n-option]").forEach((node) => {
    const key = node.dataset.i18nOption;
    if (key && dict[key]) {
      node.textContent = dict[key];
    }
  });
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });
  if (elements.summary?.dataset) {
    elements.summary.dataset.emptyEn = dict.emptySummary || "No insight yet.";
    elements.summary.dataset.emptyZh = dict.emptySummaryZh || "暂无推理结果。";
  }
  renderSummary();
}

function initialize() {
  setStatus("", "");
  bindLanguageButtons();
  applyTranslations();
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => triggerManualSync());
  }
  fetchMarketInsight();
}

document.addEventListener("DOMContentLoaded", initialize);

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
