console.info("Industry insight module v20270460");

const translations = getTranslations("industryInsight");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";
const EXPORT_SCALE = window.devicePixelRatio > 1 ? 2 : 1.4;
const EXPORT_MAX_WIDTH = 1080;
const EXPORT_MOBILE_MAX_WIDTH = 720;
const EXPORT_MOBILE_MIN_WIDTH = 540;

const elements = {
  refreshButton: document.getElementById("industry-insight-refresh"),
  status: document.getElementById("industry-insight-status"),
  exportImageButton: document.getElementById("industry-insight-export-image"),
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
let exportInProgress = false;

function formatNarrativeText(value) {
  if (value === null || value === undefined) return "";
  let text = String(value);
  if (!text.trim()) return "";
  text = text.replace(/；/g, "；\n\n").replace(/;/g, ";\n\n");
  text = text.replace(/([。！？])(?!\s|\n)/g, "$1\n");
  text = text.replace(/(\.)(?=\s|$)/g, ".\n");
  text = text.replace(/\n{3,}/g, "\n\n");
  return text.trim();
}

function setNarrativeText(node, value, fallback = "--") {
  if (!node) return;
  const formatted = formatNarrativeText(value);
  node.textContent = formatted || fallback || "";
}

function getExportBannerInfo() {
  const dict = getDict();
  const generated =
    state.insight?.generatedAt || state.fallbackGeneratedAt || state.snapshot?.generatedAt || null;
  const title = dict.pageTitle || "Industry Reasoning";
  const dateLabel = dict.generatedAtLabel || "Generated";
  const dateValue = generated ? `${dateLabel}: ${formatDateTime(generated)}` : "";
  const footer =
    dict.exportDisclaimer ||
    (state.lang === "zh" ? "以上内容由AI推理，仅供参考" : "AI-generated content. For reference only.");
  return { title, date: dateValue, footer };
}

function injectExportBanners(root, info) {
  if (!root || !info) return;
  const top = document.createElement("div");
  top.className = "insight-export-banner insight-export-banner--top";
  const titleEl = document.createElement("strong");
  titleEl.textContent = info.title || "";
  top.appendChild(titleEl);
  if (info.date) {
    const dateEl = document.createElement("span");
    dateEl.textContent = info.date;
    top.appendChild(dateEl);
  }
  root.insertBefore(top, root.firstChild);

  const bottom = document.createElement("div");
  bottom.className = "insight-export-banner insight-export-banner--bottom";
  bottom.textContent = info.footer || "";
  root.appendChild(bottom);
}

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

function setLoading(isLoading, labelOverride) {
  state.loading = isLoading;
  const button = elements.refreshButton;
  if (!button) return;
  const dict = getDict();
  const label = button.querySelector(".btn__label");
  if (isLoading) {
    button.disabled = true;
    button.dataset.loading = "1";
    if (label) label.textContent = labelOverride || dict.refreshing || "Refreshing...";
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

function getExportFilename() {
  const now = new Date();
  const pad = (value) => String(value).padStart(2, "0");
  const timestamp = [
    now.getFullYear(),
    pad(now.getMonth() + 1),
    pad(now.getDate()),
    pad(now.getHours()),
    pad(now.getMinutes()),
    pad(now.getSeconds()),
  ].join("");
  return `industry-insight-${timestamp}`;
}

function isMobileDevice() {
  return /Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent || "");
}

function openMobilePreview(dataUrl) {
  const previewWindow = window.open("", "_blank");
  if (!previewWindow) {
    return false;
  }
  const dict = getDict();
  previewWindow.document.write(`<!DOCTYPE html>
<html lang="${state.lang || "zh"}">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${dict.pageTitle || "Industry Insight"}</title>
    <style>
      body{margin:0;padding:16px;background:#0f172a;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}
      img{width:100%;height:auto;display:block;border-radius:16px;box-shadow:0 20px 70px rgba(15,23,42,0.45);}
    </style>
  </head>
  <body>
    <img src="${dataUrl}" alt="Industry insight export" />
  </body>
</html>`);
  previewWindow.document.close();
  return true;
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
      setNarrativeText(li, item);
      list.appendChild(li);
    });
    block.appendChild(list);
  } else {
    const tag = type === "html" ? "div" : "p";
    const para = document.createElement(tag);
    para.className = "concept-summary__text";
    setNarrativeText(para, content, "--");
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
    setNarrativeText(drivers, industry.drivers);
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
      setNarrativeText(li, risk);
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
      setNarrativeText(li, action);
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
    metrics.className = "industry-card__metrics industry-card__metrics--primary";
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
      setNarrativeText(notes, summaryJson.rotation_notes);
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

async function fetchInsight(options = {}) {
  const { skipButtonState = false, skipInitialStatus = false } = options;
  if (!skipButtonState) {
    setLoading(true);
  }
  const dict = getDict();
  if (!skipInitialStatus) {
    setStatus(dict.loading || "Loading...", "info");
  }
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
    if (!skipButtonState) {
      setLoading(false);
    }
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

function triggerImageDownload(dataUrl) {
  const link = document.createElement("a");
  link.href = dataUrl;
  link.download = `${getExportFilename()}.png`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

function resolveExportWidth() {
  const viewportWidth = Math.max(
    window.innerWidth || 0,
    document.documentElement?.clientWidth || 0,
    EXPORT_MOBILE_MIN_WIDTH
  );
  if (viewportWidth > 900) {
    return EXPORT_MOBILE_MAX_WIDTH;
  }
  return Math.min(Math.max(viewportWidth, EXPORT_MOBILE_MIN_WIDTH), EXPORT_MOBILE_MAX_WIDTH);
}

function prepareExportClone(target, width) {
  const wrapper = document.createElement("div");
  wrapper.style.position = "fixed";
  wrapper.style.left = "-10000px";
  wrapper.style.top = "0";
  wrapper.style.width = `${width}px`;
  wrapper.style.background = "#ffffff";
  wrapper.style.padding = "0";
  wrapper.style.zIndex = "-1";

  const clone = target.cloneNode(true);
  clone.style.width = "100%";
  clone.setAttribute("data-export-mode", "mobile");
  clone.classList.add("insight-export-clone");
  stripHistorySectionForExport(clone);
  injectExportBanners(clone, getExportBannerInfo());
  wrapper.appendChild(clone);
  document.body.appendChild(wrapper);

  return {
    node: clone,
    cleanup: () => {
      document.body.removeChild(wrapper);
    },
  };
}

function stripHistorySectionForExport(root) {
  if (!root || typeof root.querySelector !== "function") return;
  const historyNode = root.querySelector("#industry-insight-history");
  if (!historyNode) return;
  const section = historyNode.closest(".insight-section");
  if (section && section.parentNode) {
    section.remove();
  } else {
    historyNode.remove();
  }
}

async function exportIndustryInsightImage() {
  if (exportInProgress) return;
  const dict = getDict();
  if (!window.html2canvas) {
    setStatus(dict.exportFailed || "Export failed", "error");
    return;
  }
  const target = document.getElementById("industry-insight-root");
  if (!target) {
    setStatus(dict.exportFailed || "Export failed", "error");
    return;
  }
  exportInProgress = true;
  setStatus(dict.exporting || "Preparing export...", "info");
  let cleanupExport = null;
  try {
    const exportWidth = resolveExportWidth();
    const { node: exportNode, cleanup } = prepareExportClone(target, exportWidth);
    cleanupExport = cleanup;
    const canvas = await window.html2canvas(exportNode, {
      scale: EXPORT_SCALE,
      useCORS: true,
      backgroundColor: "#ffffff",
      scrollY: 0,
      scrollX: 0,
      width: exportWidth,
      windowWidth: exportWidth,
    });
    let finalCanvas = canvas;
    if (canvas.width > EXPORT_MAX_WIDTH) {
      const scaleFactor = EXPORT_MAX_WIDTH / canvas.width;
      const scaledCanvas = document.createElement("canvas");
      scaledCanvas.width = EXPORT_MAX_WIDTH;
      scaledCanvas.height = Math.round(canvas.height * scaleFactor);
      const ctx = scaledCanvas.getContext("2d");
      if (ctx) {
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = "high";
        ctx.drawImage(canvas, 0, 0, scaledCanvas.width, scaledCanvas.height);
        finalCanvas = scaledCanvas;
      }
    }
    const dataUrl = finalCanvas.toDataURL("image/png");
    if (isMobileDevice()) {
      const opened = openMobilePreview(dataUrl);
      if (!opened) {
        triggerImageDownload(dataUrl);
      }
    } else {
      triggerImageDownload(dataUrl);
    }
    setStatus(dict.exportReady || "Export ready.", "success");
  } catch (error) {
    console.error("Industry insight export failed:", error);
    setStatus(dict.exportFailed || "Export failed", "error");
  } finally {
    if (typeof cleanupExport === "function") {
      cleanupExport();
    }
    exportInProgress = false;
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

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchIndustryJobStatus() {
  try {
    const response = await fetch(`${API_BASE}/control/status`, { cache: "no-cache" });
    if (!response.ok) {
      throw new Error(`Failed to fetch job status: ${response.status}`);
    }
    const payload = await response.json();
    return payload?.jobs?.industry_insight || null;
  } catch (error) {
    console.error("Failed to fetch industry insight job status:", error);
    return null;
  }
}

async function waitForIndustryJobCompletion(notBefore) {
  const dict = getDict();
  const timeoutMs = 4 * 60 * 1000;
  const pollIntervalMs = 2500;
  const startTime = Date.now();
  let observedStart = null;

  while (Date.now() - startTime < timeoutMs) {
    const status = await fetchIndustryJobStatus();
    if (status) {
      const jobStartedAt = status.startedAt ? Date.parse(status.startedAt) : null;
      const isCurrentJob =
        !notBefore ||
        !jobStartedAt ||
        jobStartedAt >= notBefore - 2000; /* allow small clock drift */

      if (!isCurrentJob) {
        await delay(pollIntervalMs);
        continue;
      }

      if (status.status === "running") {
        observedStart = jobStartedAt || observedStart || Date.now();
        const message = status.message || dict.jobRunning || "Reasoning in progress...";
        setStatus(message, "info");
      } else if (status.status === "success" || status.status === "idle") {
        return status;
      } else if (status.status === "error") {
        const errorMessage = status.error || dict.jobFailed || "Reasoning job failed.";
        const error = new Error(errorMessage);
        error.jobStatus = status;
        throw error;
      }
    }
    await delay(pollIntervalMs);
  }

  throw new Error(dict.jobTimeout || "Reasoning timed out. Check the control panel.");
}

async function triggerIndustryInsightJob() {
  const dict = getDict();
  const payload = {
    lookbackHours: Math.min(Math.max(Number(state.snapshot?.lookbackHours) || 48, 1), 168),
    industryLimit: Math.min(Math.max(Number(state.snapshot?.industryCount) || 5, 1), 10),
    runLLM: true,
  };

  const response = await fetch(`${API_BASE}/control/sync/industry-insight`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    throw new Error(dict.jobFailed || `Failed to trigger reasoning (${response.status}).`);
  }

  return Date.now();
}

async function handleRefreshClick() {
  if (state.loading) {
    return;
  }
  const dict = getDict();
  try {
    setLoading(true, dict.syncing || dict.refreshing || "Refreshing...");
    setStatus(dict.jobStarting || "Triggering reasoning job...", "info");
    const requestedAt = await triggerIndustryInsightJob();
    setStatus(dict.jobRunning || "Reasoning in progress...", "info");
    await waitForIndustryJobCompletion(requestedAt);
    await fetchInsight({ skipButtonState: true, skipInitialStatus: true });
  } catch (error) {
    console.error("Failed to refresh industry insight:", error);
    const message = error?.message || dict.jobFailed || "Failed to refresh insight.";
    setStatus(message, "error");
  } finally {
    setLoading(false);
  }
}

function init() {
  initLanguage();
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", handleRefreshClick);
  }
  if (elements.exportImageButton) {
    elements.exportImageButton.addEventListener("click", exportIndustryInsightImage);
  }
  fetchInsight();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
