console.info("Market News Overview bundle v20270514");

const translations = getTranslations("marketNewsOverview");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const ARTICLE_LIMIT = 40;
const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  summary: document.getElementById("market-news-overview-summary"),
  articles: document.getElementById("market-news-overview-articles"),
  refreshButton: document.getElementById("market-news-overview-refresh"),
  langButtons: document.querySelectorAll(".lang-btn"),
  status: document.getElementById("market-news-overview-status"),
  history: document.getElementById("market-news-overview-history"),
};

const state = {
  data: null,
  history: [],
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

function formatHeadlineTime(value) {
  if (!value) {
    return "--";
  }
  const dateValue = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(dateValue.getTime())) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  const now = new Date();
  const sameDay = dateValue.toDateString() === now.toDateString();
  const timeString = dateValue.toLocaleTimeString(locale, {
    hour: "2-digit",
    minute: "2-digit",
  });
  if (sameDay) {
    return timeString;
  }
  const dateString = dateValue.toLocaleDateString(locale, {
    month: "2-digit",
    day: "2-digit",
  });
  return `${dateString} ${timeString}`;
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

function renderSummary() {
  const dict = getDict();
  const container = elements.summary;
  if (!container) return;

  const summaryData = state.data?.summary;
  container.innerHTML = "";

  if (!summaryData) {
    const message = container.dataset[`empty${currentLang.toUpperCase()}`] || dict.emptySummary || "--";
    clearContainer(container, message);
    return;
  }

  const summary = summaryData.summary || {};
  const card = document.createElement("article");
  card.className = "insight-card";

  const header = document.createElement("div");
  header.className = "insight-card__header";
  const title = document.createElement("h2");
  title.className = "insight-card__title";
  title.textContent = dict.summaryHeader || "Market Insight";
  header.appendChild(title);

  const sentimentLabelMap = {
    bullish: dict.sentimentBullish || "Bullish",
    bearish: dict.sentimentBearish || "Bearish",
    neutral: dict.sentimentNeutral || "Neutral",
  };
  const sentimentValueRaw = (summary.sentiment || "").toLowerCase();
  const sentimentValue = ["bullish", "bearish", "neutral"].includes(sentimentValueRaw)
    ? sentimentValueRaw
    : "neutral";
  const sentimentChip = document.createElement("span");
  sentimentChip.className = `sentiment-chip sentiment-chip--${sentimentValue}`;
  sentimentChip.textContent = sentimentLabelMap[sentimentValue] || dict.sentimentUnknown || "Unknown";

  const tokenHintParts = [];
  const promptTokens = summaryData.promptTokens;
  const completionTokens = summaryData.completionTokens;
  const totalTokens = summaryData.totalTokens;
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
        summary.confidence !== null && summary.confidence !== undefined
          ? formatPercent(summary.confidence, 0)
          : "--",
    },
    {
      label: dict.statsHeadlineLabel || "参与新闻",
      value: formatInteger(summaryData.headlineCount),
    },
    {
      label: dict.statsTokenLabel || "Token",
      value: formatInteger(totalTokens),
      hint: tokenHint,
    },
  ];

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
    <div>${dict.generatedAtLabel || "Generated"}: <strong>${formatDateTime(summaryData.generatedAt)}</strong></div>
    <div>${dict.windowRangeLabel || "Window"}: ${formatDateTime(summaryData.windowStart)} - ${formatDateTime(summaryData.windowEnd)}</div>
    <div>${dict.elapsedLabel || "Latency"}: ${summaryData.elapsedSeconds !== null && summaryData.elapsedSeconds !== undefined ? `${summaryData.elapsedSeconds.toFixed(2)}s` : "--"}</div>
    <div>${dict.modelLabel || "Model"}: ${summaryData.modelUsed || "-"}</div>
  `;

  header.appendChild(meta);
  card.appendChild(header);
  card.appendChild(stats);

  const overviewBlock = document.createElement("section");
  overviewBlock.className = "insight-card__section";
  const overviewHeading = document.createElement("h3");
  overviewHeading.textContent = dict.marketOverviewLabel || "Overview";
  overviewBlock.appendChild(overviewHeading);
  const overviewPara = document.createElement("p");
  overviewPara.textContent = summary.market_overview || "--";
  overviewBlock.appendChild(overviewPara);

  if (summary.recommended_actions) {
    const actionHeading = document.createElement("h4");
    actionHeading.className = "insight-subheading";
    actionHeading.textContent = dict.recommendedActionsLabel || "Actions";
    overviewBlock.appendChild(actionHeading);
    const actionPara = document.createElement("p");
    actionPara.className = "insight-actions";
    actionPara.textContent = summary.recommended_actions;
    overviewBlock.appendChild(actionPara);
  }

  card.appendChild(overviewBlock);

  const sectionDefs = [
    { key: "key_drivers", label: dict.keyDriversLabel || "Key Drivers", type: "list" },
    { key: "sectors_to_watch", label: dict.sectorsToWatchLabel || "Sectors to Watch", type: "list", horizontal: true },
    { key: "indices_to_watch", label: dict.indicesToWatchLabel || "Indices to Watch", type: "list", horizontal: true },
    { key: "risk_factors", label: dict.riskFactorsLabel || "Risk Factors", type: "list" },
  ];

  sectionDefs.forEach((section) => {
    const value = summary[section.key];
    if (!value || (Array.isArray(value) && !value.length)) {
      return;
    }
    const block = document.createElement("section");
    block.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = section.label;
    block.appendChild(heading);
    if (section.type === "list" && Array.isArray(value)) {
      const list = document.createElement("ul");
      list.className = section.horizontal ? "insight-card__list insight-card__list--horizontal" : "insight-card__list";
      value.forEach((item) => {
        if (!item) return;
        const li = document.createElement("li");
        li.textContent = item;
        list.appendChild(li);
      });
      block.appendChild(list);
    } else {
      const paragraph = document.createElement("p");
      paragraph.textContent = value;
      block.appendChild(paragraph);
    }
    card.appendChild(block);
  });

  const notes = Array.isArray(summary.detailed_notes) ? summary.detailed_notes : [];
  if (notes.length) {
    const notesSection = document.createElement("section");
    notesSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.detailNotesLabel || "Detailed Notes";
    notesSection.appendChild(heading);
    const list = document.createElement("ul");
    list.className = "insight-card__detail-list";
    notes.forEach((item) => {
      if (!item || typeof item !== "object") return;
      const li = document.createElement("li");
      const noteTitle = document.createElement("strong");
      noteTitle.textContent = item.title || "--";
      li.appendChild(noteTitle);
      if (item.timestamp) {
        const timestamp = document.createElement("div");
        timestamp.className = "insight-card__detail-timestamp";
        const timeText = formatDateTime(item.timestamp);
        timestamp.textContent =
          timeText && timeText !== "--"
            ? `${dict.publishedAt || "Published"}: ${timeText}`
            : `${dict.publishedAt || "Published"}: ${item.timestamp}`;
        li.appendChild(timestamp);
      }
      if (item.impact_summary) {
        const summaryPara = document.createElement("p");
        summaryPara.textContent = item.impact_summary;
        li.appendChild(summaryPara);
      }
      if (item.analysis) {
        const analysisPara = document.createElement("p");
        analysisPara.className = "insight-card__detail-analysis";
        analysisPara.textContent = item.analysis;
        li.appendChild(analysisPara);
      }
      if (item.confidence !== undefined) {
        const confidenceMeta = document.createElement("div");
        confidenceMeta.className = "insight-card__detail-confidence";
        confidenceMeta.textContent = `${dict.confidenceLabel || "Confidence"}: ${formatNumber(item.confidence, 2)}`;
        li.appendChild(confidenceMeta);
      }
      list.appendChild(li);
    });
    notesSection.appendChild(list);
    card.appendChild(notesSection);
  }

  container.appendChild(card);
}

function createArticleCard(article) {
  const dict = getDict();
  const card = document.createElement("article");
  card.className = "news-card";

  const header = document.createElement("div");
  header.className = "news-card__header";
  const title = document.createElement("h2");
  title.className = "news-card__title";
  title.textContent = article.title || "--";
  header.appendChild(title);
  const formattedHeadlineTime = formatHeadlineTime(article.publishedAt);
  if (formattedHeadlineTime !== "--") {
    const timestamp = document.createElement("time");
    timestamp.className = "news-card__timestamp";
    const rawValue =
      article.publishedAt instanceof Date ? article.publishedAt.toISOString() : article.publishedAt;
    if (typeof rawValue === "string") {
      timestamp.dateTime = rawValue;
    }
    const tooltip = formatDateTime(article.publishedAt);
    if (tooltip !== "--") {
      timestamp.title = `${dict.publishedAt || "Published"}: ${tooltip}`;
    }
    timestamp.textContent = formattedHeadlineTime;
    header.appendChild(timestamp);
  }
  if (article.impactConfidence !== undefined && article.impactConfidence !== null) {
    const badge = document.createElement("span");
    badge.className = "news-card__badge";
    badge.textContent = `${dict.confidenceLabel || "Confidence"}: ${formatNumber(article.impactConfidence, 2)}`;
    header.appendChild(badge);
  }
  card.appendChild(header);

  if (article.impactSummary) {
    const summarySection = document.createElement("div");
    summarySection.className = "news-card__section";
    const label = document.createElement("div");
    label.className = "news-card__section-label";
    label.textContent = dict.impactSummaryLabel || "Key takeaway";
    const summary = document.createElement("p");
    summary.className = "news-card__summary";
    summary.textContent = article.impactSummary;
    summarySection.appendChild(label);
    summarySection.appendChild(summary);
    card.appendChild(summarySection);
  }

  if (article.impactAnalysis) {
    const analysisSection = document.createElement("div");
    analysisSection.className = "news-card__section";
    const label = document.createElement("div");
    label.className = "news-card__section-label";
    label.textContent = dict.impactAnalysisLabel || "Analysis";
    const analysis = document.createElement("p");
    analysis.className = "news-card__impact-analysis";
    analysis.textContent = article.impactAnalysis;
    analysisSection.appendChild(label);
    analysisSection.appendChild(analysis);
    card.appendChild(analysisSection);
  }

  if (article.markets && article.markets.length) {
    const chipsSection = document.createElement("div");
    chipsSection.className = "news-card__section news-card__section--chips";
    const label = document.createElement("div");
    label.className = "news-card__section-label";
    label.textContent = dict.marketsLabel || "Markets";
    const list = document.createElement("div");
    list.className = "news-card__chip-list";
    article.markets.forEach((market) => {
      const chip = document.createElement("span");
      chip.className = "news-card__chip";
      chip.textContent = market;
      list.appendChild(chip);
    });
    chipsSection.appendChild(label);
    chipsSection.appendChild(list);
    card.appendChild(chipsSection);
  }

  const metaParts = [];
  if (article.source) {
    const sourceMap = {
      global_flash: dict.sourceGlobalFlash || "Global Flash",
      finance_breakfast: dict.sourceFinanceBreakfast || "Daily Finance",
    };
    const sourceName = sourceMap[article.source] || article.source;
    metaParts.push(sourceName);
  }
  metaParts.push(`${dict.publishedAt || "Published"}: ${formatDateTime(article.publishedAt)}`);

  const metaRow = document.createElement("div");
  metaRow.className = "news-card__footer";
  const meta = document.createElement("span");
  meta.className = "news-card__meta";
  meta.textContent = metaParts.join(" · ");
  metaRow.appendChild(meta);
  if (article.url) {
    const link = document.createElement("a");
    link.className = "news-card__link";
    link.href = article.url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = dict.readMore || "View source";
    metaRow.appendChild(link);
  }
  card.appendChild(metaRow);

  return card;
}

function renderArticles() {
  const dict = getDict();
  const container = elements.articles;
  if (!container) return;
  container.innerHTML = "";

  const articles = Array.isArray(state.data?.articles) ? state.data.articles : [];
  if (!articles.length) {
    const message = container.dataset[`empty${currentLang.toUpperCase()}`] || dict.emptyArticles || "--";
    clearContainer(container, message);
    return;
  }

  const fragment = document.createDocumentFragment();
  articles.forEach((article) => {
    fragment.appendChild(createArticleCard(article));
  });
  container.appendChild(fragment);
}

function renderHistory() {
  const container = elements.history;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const dict = getDict();
  const items = Array.isArray(state.history) ? state.history : [];
  if (!items.length) {
    const message =
      dict.historyEmpty ||
      (currentLang === "zh" ? container.dataset.emptyZh : container.dataset.emptyEn) ||
      "No history.";
    const empty = document.createElement("p");
    empty.className = "insight-history__empty";
    empty.textContent = message;
    container.appendChild(empty);
    return;
  }

  items.forEach((item) => {
    if (!item?.summaryJson) {
      return;
    }
    const card = document.createElement("article");
    card.className = "concept-history-card";

    const header = document.createElement("header");
    header.className = "concept-history-card__header";
    const time = document.createElement("time");
    time.textContent = formatDateTime(item.generatedAt || item.windowStart || item.windowEnd);
    header.appendChild(time);
    card.appendChild(header);

    const summary = item.summaryJson;
    const sentimentValue = (summary.sentiment || "").toLowerCase();
    if (sentimentValue) {
      const sentiment = document.createElement("p");
      sentiment.className = "macro-history-bias";
      const sentimentMap = {
        bullish: dict.sentimentBullish || "Bullish",
        bearish: dict.sentimentBearish || "Bearish",
        neutral: dict.sentimentNeutral || "Neutral",
      };
      sentiment.textContent = `${dict.statsSentimentLabel || "Sentiment"}: ${
        sentimentMap[sentimentValue] || summary.sentiment || "--"
      }`;
      card.appendChild(sentiment);
    }

    if (summary.market_overview) {
      const overview = document.createElement("p");
      overview.className = "macro-history-overview";
      overview.textContent = summary.market_overview;
      card.appendChild(overview);
    }

    if (Array.isArray(summary.key_drivers) && summary.key_drivers.length) {
      const list = document.createElement("ul");
      list.className = "concept-history-card__concepts";
      summary.key_drivers.slice(0, 3).forEach((driver) => {
        const li = document.createElement("li");
        li.textContent = driver || "--";
        list.appendChild(li);
      });
      card.appendChild(list);
    }

    container.appendChild(card);
  });
}

async function fetchMarketInsight() {
  const dict = getDict();
  clearContainer(elements.summary, dict.loading || "Loading...");
  clearContainer(elements.articles, "");
  if (elements.history) {
    elements.history.innerHTML = "";
  }
  try {
    const [insightResp, historyResp] = await Promise.all([
      fetch(`${API_BASE}/news/market-insight?articleLimit=${ARTICLE_LIMIT}`),
      fetch(`${API_BASE}/news/market-insight/history?limit=6`),
    ]);
    if (!insightResp.ok) {
      throw new Error(`HTTP ${insightResp.status}`);
    }
    if (!historyResp.ok) {
      throw new Error(`HTTP ${historyResp.status}`);
    }
    state.data = await insightResp.json();
    const historyJson = await historyResp.json();
    state.history = Array.isArray(historyJson?.items) ? historyJson.items : [];
    renderSummary();
    renderArticles();
    renderHistory();
  } catch (error) {
    console.error("Failed to fetch market insight", error);
    state.data = null;
    state.history = [];
    clearContainer(elements.summary, error?.message || "Failed to load insight");
    clearContainer(elements.articles, "");
    if (elements.history) {
      elements.history.innerHTML = "";
      const empty = document.createElement("p");
      empty.className = "insight-history__empty";
      empty.textContent = error?.message || dict.historyEmpty || "Failed to load history.";
      elements.history.appendChild(empty);
    }
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
  if (elements.articles?.dataset) {
    elements.articles.dataset.emptyEn = dict.emptyArticles || "No referenced headlines.";
    elements.articles.dataset.emptyZh = dict.emptyArticlesZh || "暂无相关新闻。";
  }
  if (elements.history?.dataset) {
    elements.history.dataset.emptyEn = dict.historyEmpty || "No history.";
    elements.history.dataset.emptyZh = dict.historyEmpty || "暂无历史记录。";
  }
  renderSummary();
  renderArticles();
  renderHistory();
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
