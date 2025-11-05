console.info("Industry insight module v20270438");

const translations = getTranslations("industryInsight");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  refreshButton: document.getElementById("industry-insight-refresh"),
  status: document.getElementById("industry-insight-status"),
  generatedAt: document.getElementById("industry-insight-generated-at"),
  lookback: document.getElementById("industry-insight-lookback"),
  summary: document.getElementById("industry-insight-summary"),
  grid: document.getElementById("industry-insight-grid"),
  history: document.getElementById("industry-insight-history"),
  langButtons: document.querySelectorAll(".lang-btn"),
};

const state = {
  lang: null,
  loading: false,
  insight: null,
  snapshot: null,
  history: [],
  fallbackSummary: null,
  fallbackGeneratedAt: null,
};

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) return stored;
  } catch (error) {
    /* ignore */
  }
  const htmlLang = document.documentElement.getAttribute("data-pref-lang") || document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) return htmlLang;
  const browserLang = (navigator.language || navigator.userLanguage || "").toLowerCase();
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

function getDict() {
  return translations[state.lang] || translations.zh || translations.en;
}

function setLoading(isLoading) {
  state.loading = isLoading;
  const button = elements.refreshButton;
  if (!button) return;
  const dict = getDict();
  const label = button.querySelector(".btn__label");
  if (isLoading) {
    button.disabled = true;
    button.dataset.loading = "1";
    if (label) label.textContent = dict.refreshing || "Refreshing...";
  } else {
    button.disabled = false;
    delete button.dataset.loading;
    if (label) label.textContent = dict.refreshButton || "Refresh";
  }
}

function formatDateTime(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit" })}`;
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return numeric.toFixed(digits);
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return `${numeric.toFixed(digits)}%`;
}

function formatMoney(value) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  const absValue = Math.abs(numeric);
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  if (absValue >= 1e8) {
    const unit = state.lang === "zh" ? "亿" : "B";
    return `${(numeric / 1e8).toFixed(2)}${unit}`;
  }
  if (absValue >= 1e4) {
    const unit = state.lang === "zh" ? "万" : "K";
    return `${(numeric / 1e4).toFixed(1)}${unit}`;
  }
  return numeric.toLocaleString(locale, { maximumFractionDigits: 2 });
}

function formatList(values) {
  if (!Array.isArray(values) || !values.length) return [];
  return values
    .map((item) => {
      if (item === null || item === undefined) return null;
      if (typeof item === "string") return item.trim();
      if (typeof item === "number") return item.toString();
      if (typeof item === "object") {
        if (item.label) return item.label;
        if (item.name) return item.name;
        if (item.title) return item.title;
      }
      return String(item);
    })
    .filter(Boolean);
}

function clearContainer(node, message) {
  if (!node) return;
  node.innerHTML = "";
  if (!message) return;
  const placeholder = document.createElement("div");
  placeholder.className = "concept-placeholder";
  placeholder.textContent = message;
  node.appendChild(placeholder);
}

function renderMeta(snapshot, insight) {
  const fallbackTime = state.fallbackSummary ? state.fallbackGeneratedAt : null;
  if (elements.generatedAt) {
    const generated = insight?.generatedAt || fallbackTime || snapshot?.generatedAt;
    elements.generatedAt.textContent = formatDateTime(generated);
  }
  if (elements.lookback) {
    let hours = snapshot?.lookbackHours;
    if ((!hours || !Number.isFinite(hours)) && insight?.windowEnd && insight?.windowStart) {
      hours = Math.round((new Date(insight.windowEnd) - new Date(insight.windowStart)) / (1000 * 60 * 60));
    }
    if ((!hours || !Number.isFinite(hours)) && fallbackTime && snapshot?.generatedAt) {
      const fallbackDate = new Date(fallbackTime);
      const snapDate = new Date(snapshot.generatedAt);
      const diff = snapDate - fallbackDate;
      if (Number.isFinite(diff)) {
        hours = Math.max(1, Math.round(diff / (1000 * 60 * 60)));
      }
    }
    if (hours && Number.isFinite(hours)) {
      elements.lookback.textContent = state.lang === "zh" ? `${hours} 小时` : `${hours}h`;
    } else {
      elements.lookback.textContent = "--";
    }
  }
}

function buildSummarySection(title, content, type = "text") {
  if (!content) return null;
  const block = document.createElement("section");
  block.className = "concept-summary__block";
  const heading = document.createElement("h3");
  heading.className = "concept-summary__block-title";
  heading.textContent = title;
  block.appendChild(heading);

  if (type === "list") {
    const list = document.createElement("ul");
    list.className = "concept-summary__list";
    formatList(content).forEach((item) => {
      const li = document.createElement("li");
      li.textContent = item;
      list.appendChild(li);
    });
    block.appendChild(list);
  } else if (type === "html") {
    const para = document.createElement("div");
    para.className = "concept-summary__text";
    para.textContent = content;
    block.appendChild(para);
  }

  return block;
}

function createTopIndustryCard(industry, dict) {
  if (!industry) return null;
  const stanceMap = dict.stanceMap || { bullish: "Bullish", watch: "Watch", bearish: "Bearish" };
  const card = document.createElement("article");
  card.className = "concept-summary__top-card";

  const header = document.createElement("header");
  header.className = "concept-summary__top-header";
  const title = document.createElement("h4");
  title.textContent = industry.name || "--";
  header.appendChild(title);

  if (industry.stance) {
    const stance = document.createElement("span");
    stance.className = `concept-summary__stance concept-summary__stance--${industry.stance || "watch"}`;
    stance.textContent = stanceMap[industry.stance] || industry.stance;
    header.appendChild(stance);
  }

  if (industry.confidence !== undefined && industry.confidence !== null) {
    const confidence = document.createElement("span");
    confidence.className = "concept-summary__confidence";
    confidence.textContent = `${dict.confidenceLabel || "Confidence"} ${formatPercent(industry.confidence * 100, 1)}`;
    header.appendChild(confidence);
  }

  card.appendChild(header);

  if (industry.drivers) {
    const drivers = document.createElement("p");
    drivers.className = "concept-summary__drivers";
    drivers.textContent = industry.drivers;
    card.appendChild(drivers);
  }

  const metrics = formatList(industry.key_metrics);
  if (metrics.length) {
    const metricsList = document.createElement("ul");
    metricsList.className = "concept-summary__chips";
    metrics.slice(0, 4).forEach((metric) => {
      const li = document.createElement("li");
      li.textContent = metric;
      metricsList.appendChild(li);
    });
    card.appendChild(metricsList);
  }

  const stocks = formatList(industry.leading_stocks);
  if (stocks.length) {
    const stocksLine = document.createElement("p");
    stocksLine.className = "concept-summary__meta";
    stocksLine.textContent = `${dict.leadingStocks || "Leading Stocks"}: ${stocks.join(" · ")}`;
    card.appendChild(stocksLine);
  }

  const risks = formatList(industry.risk_flags);
  if (risks.length) {
    const riskTitle = document.createElement("h5");
    riskTitle.className = "concept-summary__subheading";
    riskTitle.textContent = dict.riskFlags || "Risks";
    card.appendChild(riskTitle);
    const riskList = document.createElement("ul");
    riskList.className = "concept-summary__list";
    risks.forEach((risk) => {
      const li = document.createElement("li");
      li.textContent = risk;
      riskList.appendChild(li);
    });
    card.appendChild(riskList);
  }

  const actions = formatList(industry.suggested_actions);
  if (actions.length) {
    const actionTitle = document.createElement("h5");
    actionTitle.className = "concept-summary__subheading";
    actionTitle.textContent = dict.suggestedActions || "Actions";
    card.appendChild(actionTitle);
    const actionList = document.createElement("ul");
    actionList.className = "concept-summary__list concept-summary__list--actions";
    actions.forEach((action) => {
      const li = document.createElement("li");
      li.textContent = action;
      actionList.appendChild(li);
    });
    card.appendChild(actionList);
  }

  return card;
}

function renderSummary(insight) {
  const container = elements.summary;
  if (!container) return;
  const dict = getDict();

  const summaryJson = insight?.summaryJson || state.fallbackSummary;
  container.innerHTML = "";

  if (!summaryJson) {
    const emptyMessage = dict.noInsight || container.dataset[`empty${state.lang === "zh" ? "Zh" : "En"}`];
    clearContainer(container, emptyMessage || "No insight available.");
    return;
  }

  if (summaryJson.headline) {
    const headline = document.createElement("h2");
    headline.className = "concept-summary__headline";
    headline.textContent = summaryJson.headline;
    container.appendChild(headline);
  }

  if (summaryJson.market_view) {
    const marketBlock = buildSummarySection(dict.marketView || "Market View", summaryJson.market_view, "html");
    if (marketBlock) container.appendChild(marketBlock);
  }

  if (Array.isArray(summaryJson.top_industries) && summaryJson.top_industries.length) {
    const topContainer = document.createElement("section");
    topContainer.className = "concept-summary__top";
    const topTitle = document.createElement("h3");
    topTitle.className = "concept-summary__block-title";
    topTitle.textContent = dict.topIndustries || "Top Industries";
    topContainer.appendChild(topTitle);

    const grid = document.createElement("div");
    grid.className = "concept-summary__top-grid";
    summaryJson.top_industries.forEach((item) => {
      const card = createTopIndustryCard(item, dict);
      if (card) grid.appendChild(card);
    });
    topContainer.appendChild(grid);
    container.appendChild(topContainer);
  }

  if (summaryJson.rotation_notes) {
    const rotationBlock = buildSummarySection(dict.rotationNotes || "Rotation Notes", summaryJson.rotation_notes, "html");
    if (rotationBlock) container.appendChild(rotationBlock);
  }

  const risks = formatList(summaryJson.risk_summary);
  if (risks.length) {
    const riskBlock = buildSummarySection(dict.riskSummary || "Risk Summary", risks, "list");
    if (riskBlock) container.appendChild(riskBlock);
  }

  const nextSteps = formatList(summaryJson.next_steps);
  if (nextSteps.length) {
    const nextBlock = buildSummarySection(dict.nextSteps || "Next Steps", nextSteps, "list");
    if (nextBlock) container.appendChild(nextBlock);
  }

  if (summaryJson.data_timestamp) {
    const dataBlock = buildSummarySection(dict.dataTimestamp || "Data Timestamp", summaryJson.data_timestamp, "html");
    if (dataBlock) container.appendChild(dataBlock);
  }
}

function buildStageTable(stages, dict) {
  if (!Array.isArray(stages) || !stages.length) return null;
  const table = document.createElement("table");
  table.className = "concept-card__table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  [
    dict.tableSymbol || "Stage",
    dict.tableRank || "Rank",
    dict.tableNet || "Net",
    dict.tableInflow || "Inflow",
    dict.tableOutflow || "Outflow",
    dict.tablePrice || "Price %",
    dict.tableStage || "Stage %",
    dict.tableLeader || "Leader",
  ].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  stages.forEach((stage) => {
    const row = document.createElement("tr");
    const cells = [
      stage.symbol,
      stage.rank ?? "--",
      formatMoney(stage.netAmount),
      formatMoney(stage.inflow),
      formatMoney(stage.outflow),
      formatPercent(stage.priceChangePercent, 1),
      formatPercent(stage.stageChangePercent, 1),
      stage.leadingStock
        ? `${stage.leadingStock}${
            stage.leadingStockChangePercent ? ` (${formatPercent(stage.leadingStockChangePercent, 1)})` : ""
          }`
        : "--",
    ];
    cells.forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value === undefined ? "--" : value;
      row.appendChild(td);
    });
    tbody.appendChild(row);
  });
  table.appendChild(tbody);
  return table;
}

function buildNewsList(news, dict) {
  if (!Array.isArray(news) || !news.length) return null;
  const list = document.createElement("ul");
  list.className = "concept-card__news";
  news.forEach((item) => {
    const li = document.createElement("li");
    const title = document.createElement("a");
    title.className = "concept-card__news-title";
    title.textContent = item.title || dict.untitledArticle || "Untitled";
    if (item.url) {
      title.href = item.url;
      title.target = "_blank";
      title.rel = "noopener noreferrer";
    } else {
      title.href = "javascript:void(0)";
    }
    li.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "concept-card__news-meta";
    const parts = [];
    if (item.source) parts.push(item.source);
    if (item.published_at) parts.push(formatDateTime(item.published_at));
    if (item.impact_summary) parts.push(item.impact_summary);
    if (parts.length) meta.textContent = parts.join(" · ");
    li.appendChild(meta);
    list.appendChild(li);
  });
  return list;
}

function renderIndustryGrid(snapshot) {
  const container = elements.grid;
  if (!container) return;
  const dict = getDict();
  const industries = snapshot?.industries || [];
  if (!industries.length) {
    const message = dict.noIndustryData || container.dataset[`empty${state.lang === "zh" ? "Zh" : "En"}`];
    clearContainer(container, message || "No industry snapshot.");
    return;
  }

  container.innerHTML = "";
  industries.forEach((entry) => {
    const card = document.createElement("article");
    card.className = "concept-card";

    const header = document.createElement("header");
    header.className = "concept-card__header";
    const title = document.createElement("h3");
    title.textContent = entry.name || "--";
    header.appendChild(title);
    const lastUpdate = entry.latestUpdatedAt || (entry.fundFlow?.stages?.[0]?.updatedAt ?? null);
    if (lastUpdate) {
      const tag = document.createElement("span");
      tag.className = "concept-card__tag";
      tag.textContent = `${dict.latestUpdated || "Updated"}: ${formatDateTime(lastUpdate)}`;
      header.appendChild(tag);
    }
    card.appendChild(header);

    const metrics = document.createElement("div");
    metrics.className = "concept-card__metrics";
    const flow = entry.fundFlow || {};
    const metricItems = [
      { label: dict.score || "Score", value: formatNumber(flow.score, 3) },
      { label: dict.bestRank || "Best Rank", value: flow.bestRank ?? "--" },
      { label: dict.bestSymbol || "Stage", value: flow.bestSymbol || "--" },
      { label: dict.totalNet || "Net", value: formatMoney(flow.totalNetAmount) },
      { label: dict.totalInflow || "Inflow", value: formatMoney(flow.totalInflow) },
      { label: dict.totalOutflow || "Outflow", value: formatMoney(flow.totalOutflow) },
    ];
    metricItems.forEach(({ label, value }) => {
      const item = document.createElement("div");
      item.className = "concept-card__metric";
      const labelNode = document.createElement("span");
      labelNode.className = "concept-card__metric-label";
      labelNode.textContent = label;
      const valueNode = document.createElement("strong");
      valueNode.className = "concept-card__metric-value";
      valueNode.textContent = value;
      item.appendChild(labelNode);
      item.appendChild(valueNode);
      metrics.appendChild(item);
    });

    const stageMetrics = entry.stageMetrics || {};
    const stageBlock = document.createElement("div");
    stageBlock.className = "concept-card__metrics concept-card__metrics--secondary";
    [
      { label: dict.latestIndex || "Index", value: formatNumber(stageMetrics.latestIndex, 2) },
      { label: dict.change1d || "1D %", value: formatPercent(stageMetrics.change1d, 2) },
      { label: dict.change3d || "3D %", value: formatPercent(stageMetrics.change3d, 2) },
      { label: dict.change5d || "5D %", value: formatPercent(stageMetrics.change5d, 2) },
      { label: dict.change10d || "10D %", value: formatPercent(stageMetrics.change10d, 2) },
      { label: dict.change20d || "20D %", value: formatPercent(stageMetrics.change20d, 2) },
    ].forEach(({ label, value }) => {
      const item = document.createElement("div");
      item.className = "concept-card__metric concept-card__metric--compact";
      const labelNode = document.createElement("span");
      labelNode.className = "concept-card__metric-label";
      labelNode.textContent = label;
      const valueNode = document.createElement("strong");
      valueNode.className = "concept-card__metric-value";
      valueNode.textContent = value;
      item.appendChild(labelNode);
      item.appendChild(valueNode);
      stageBlock.appendChild(item);
    });

    card.appendChild(metrics);
    card.appendChild(stageBlock);

    const stageTable = buildStageTable(flow.stages, dict);
    if (stageTable) {
      const tableWrapper = document.createElement("div");
      tableWrapper.className = "concept-card__table-wrapper";
      tableWrapper.appendChild(stageTable);
      card.appendChild(tableWrapper);
    }

    const newsList = buildNewsList(entry.news, dict);
    if (newsList) {
      const newsTitle = document.createElement("h4");
      newsTitle.className = "concept-card__section-title";
      newsTitle.textContent = dict.relatedNews || "Related News";
      card.appendChild(newsTitle);
      card.appendChild(newsList);
    }

    container.appendChild(card);
  });
}

function renderHistory(items) {
  const container = elements.history;
  if (!container) return;
  const dict = getDict();
  if (!Array.isArray(items) || !items.length) {
    const message = dict.noHistory || container.dataset[`empty${state.lang === "zh" ? "Zh" : "En"}`];
    clearContainer(container, message || "No history.");
    return;
  }

  container.innerHTML = "";
  items.forEach((item) => {
    const card = document.createElement("article");
    card.className = "concept-history-card";

    const header = document.createElement("header");
    header.className = "concept-history-card__header";
    const time = document.createElement("time");
    time.textContent = formatDateTime(item.generatedAt);
    header.appendChild(time);
    card.appendChild(header);

    const summaryJson = item.summaryJson;
    if (summaryJson?.headline) {
      const headline = document.createElement("h4");
      headline.className = "concept-history-card__headline";
      headline.textContent = summaryJson.headline;
      card.appendChild(headline);
    }

    if (summaryJson?.top_industries?.length) {
      const list = document.createElement("ul");
      list.className = "concept-history-card__concepts";
      summaryJson.top_industries.slice(0, 3).forEach((industry) => {
        const li = document.createElement("li");
        li.textContent = industry.name || "--";
        list.appendChild(li);
      });
      card.appendChild(list);
    }

    if (summaryJson?.rotation_notes) {
      const notes = document.createElement("p");
      notes.className = "concept-history-card__notes";
      notes.textContent = summaryJson.rotation_notes;
      card.appendChild(notes);
    }

    container.appendChild(card);
  });
}

function renderAll() {
  renderMeta(state.snapshot, state.insight);
  renderSummary(state.insight);
  renderIndustryGrid(state.snapshot);
  renderHistory(state.history);
}

async function fetchInsight() {
  setLoading(true);
  const dict = getDict();
  setStatus(dict.loading || "Loading...", "info");
  try {
    const [insightResp, historyResp] = await Promise.all([
      fetch(`${API_BASE}/market/industry-insight`).then((res) => {
        if (!res.ok) throw new Error(`Failed to load industry insight: ${res.status}`);
        return res.json();
      }),
      fetch(`${API_BASE}/market/industry-insight/history?limit=6`).then((res) => {
        if (!res.ok) throw new Error(`Failed to load industry insight history: ${res.status}`);
        return res.json();
      }),
    ]);

    state.insight = insightResp.insight || null;
    state.snapshot = insightResp.snapshot || null;
    const latestId = state.insight?.summaryId;
    let historyItems = Array.isArray(historyResp.items) ? historyResp.items : [];
    if (latestId) {
      historyItems = historyItems.filter((item) => item.summaryId !== latestId);
    }
    const historyWithSummary = historyItems.filter((item) => item?.summaryJson);
    state.history = historyWithSummary;
    state.fallbackSummary = null;
    state.fallbackGeneratedAt = null;
    if (!state.insight?.summaryJson) {
      const fallback = historyWithSummary.find((item) => item.summaryJson);
      if (fallback) {
        state.fallbackSummary = fallback.summaryJson;
        state.fallbackGeneratedAt = fallback.generatedAt;
      }
    }

    renderAll();
    if (state.insight?.generatedAt) {
      elements.summary?.setAttribute("data-generated-from", "live");
      setStatus(`${dict.updatedAt || "Updated"}: ${formatDateTime(state.insight.generatedAt)}`);
    } else if (state.snapshot?.generatedAt) {
      elements.summary?.setAttribute("data-generated-from", "snapshot");
      setStatus(`${dict.snapshotUpdated || "Snapshot"}: ${formatDateTime(state.snapshot.generatedAt)}`);
    } else if (state.fallbackGeneratedAt) {
      setStatus(
        (dict.fallbackNoticeWithTime || "Showing cached reasoning from {time}.").replace(
          "{time}",
          formatDateTime(state.fallbackGeneratedAt)
        ),
        "warning"
      );
    } else {
      setStatus(dict.noInsightShort || "No cached insight.", "warning");
    }
  } catch (error) {
    console.error(error);
    setStatus(dict.loadFailed || "Failed to load industry insight.", "error");
  } finally {
    setLoading(false);
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
    node.dataset.tone = tone;
  } else {
    node.removeAttribute("data-tone");
  }
}

function handleLanguageSwitch(event) {
  const button = event.currentTarget;
  const lang = button?.dataset?.lang;
  if (!lang || lang === state.lang || !translations[lang]) return;
  state.lang = lang;
  persistLanguage(lang);
  elements.langButtons.forEach((node) => {
    if (node.dataset.lang === lang) {
      node.classList.add("lang-btn--active");
    } else {
      node.classList.remove("lang-btn--active");
    }
  });
  renderAll();
}

function initLanguage() {
  state.lang = getInitialLanguage();
  elements.langButtons.forEach((button) => {
    if (button.dataset.lang === state.lang) {
      button.classList.add("lang-btn--active");
    }
    button.addEventListener("click", handleLanguageSwitch);
  });
  persistLanguage(state.lang);
}

function init() {
  initLanguage();
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => {
      if (!state.loading) fetchInsight();
    });
  }
  fetchInsight();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
