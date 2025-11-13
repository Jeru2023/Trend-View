const translations = getTranslations("peripheralInsight");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();
const REFRESH_POLL_INTERVAL_MS = 4000;
const REFRESH_POLL_TIMEOUT_MS = 120000;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("peripheral-insight-refresh"),
  generatedAt: document.getElementById("peripheral-insight-generated"),
  modelBadge: document.getElementById("peripheral-insight-model"),
  summaryContainer: document.getElementById("peripheral-insight-content"),
  indicesTable: document.querySelector("#peripheral-indices-table tbody"),
  dollarTable: document.querySelector("#peripheral-dollar-table tbody"),
  rmbTable: document.querySelector("#peripheral-rmb-table tbody"),
  commoditiesTable: document.querySelector("#peripheral-commodities-table tbody"),
  warnings: document.getElementById("peripheral-insight-warnings"),
  historyList: document.getElementById("peripheral-history-list"),
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch (error) {
    /* no-op */
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
  return "zh";
}

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    /* no-op */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

function getDict() {
  return translations[currentLang] || translations.en;
}

function formatNumber(value, options = {}) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(numeric);
}

function formatPercent(value) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}%`;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    })}`;
  } catch (error) {
    return String(value);
  }
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return date.toLocaleDateString(locale, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  } catch (error) {
    return String(value);
  }
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = dict.title || document.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      if (key === "refreshButton" && elements.refreshButton?.dataset.loading === "1") {
        el.textContent = dict.refreshing || value;
      } else {
        el.textContent = value;
      }
    }
  });

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });
}

function setLang(lang) {
  if (!translations[lang] || lang === currentLang) {
    return;
  }
  currentLang = lang;
  persistLanguage(lang);
  applyTranslations();
  renderLatestInsight(latestInsightData);
}

function setRefreshLoading(loading) {
  if (!elements.refreshButton) {
    return;
  }
  elements.refreshButton.disabled = loading;
  if (loading) {
    elements.refreshButton.dataset.loading = "1";
    elements.refreshButton.textContent = getDict().refreshing || "Reasoning...";
  } else {
    delete elements.refreshButton.dataset.loading;
    elements.refreshButton.textContent = getDict().refreshButton || "Run reasoning";
  }
}

let latestInsightData = null;
let historyInsightData = [];

function parseSummaryPayload(summary) {
  if (!summary) {
    return null;
  }
  if (typeof summary === "object") {
    return summary;
  }
  if (typeof summary === "string") {
    try {
      return JSON.parse(summary);
    } catch (error) {
      return { summary };
    }
  }
  return { summary: String(summary) };
}

function renderEmptyRow(container, message) {
  if (!container) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = container.parentElement?.querySelectorAll("th").length || 1;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  container.innerHTML = "";
  container.appendChild(row);
}

function resolveModelName(value) {
  if (!value) {
    return "deepseek-reasoner";
  }
  const lower = String(value).toLowerCase();
  if (lower === "deepseek-chat") {
    return "deepseek-reasoner";
  }
  return value;
}

function renderSummary(summary, rawResponse, modelValue) {
  const container = elements.summaryContainer;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const dict = getDict();
  if (!summary) {
    const message = container.dataset[`empty${currentLang.toUpperCase()}`] || dict.emptySummary || "No summary.";
    const empty = document.createElement("div");
    empty.className = "summary-empty";
    empty.textContent = message;
    container.appendChild(empty);
    return;
  }

  const parsed = parseSummaryPayload(summary);

  if (parsed && typeof parsed === "object") {
    if (parsed.summary) {
      const paragraph = document.createElement("p");
      paragraph.className = "summary-text";
      paragraph.textContent = parsed.summary;
      container.appendChild(paragraph);
    }
    if (parsed.a_share_bias) {
      const bias = document.createElement("div");
      bias.className = "summary-badge";
      bias.textContent = `${dict.biasLabel || "Bias"}: ${parsed.a_share_bias}`;
      container.appendChild(bias);
    }
    if (Array.isArray(parsed.drivers) && parsed.drivers.length) {
      const listTitle = document.createElement("h4");
      listTitle.className = "summary-subheading";
      listTitle.textContent = dict.driversLabel || "Drivers";
      container.appendChild(listTitle);
      const list = document.createElement("ul");
      parsed.drivers.forEach((item) => {
        const li = document.createElement("li");
        li.textContent = item;
        list.appendChild(li);
      });
      container.appendChild(list);
    }
    if (parsed.risk_level || parsed.confidence !== undefined) {
      const meta = document.createElement("div");
      meta.className = "summary-meta";
      if (parsed.risk_level) {
        const risk = document.createElement("span");
        risk.textContent = `${dict.riskLabel || "Risk"}: ${parsed.risk_level}`;
        meta.appendChild(risk);
      }
      if (parsed.confidence !== undefined) {
        const confidence = document.createElement("span");
        confidence.textContent = `${dict.confidenceLabel || "Confidence"}: ${parsed.confidence}`;
        meta.appendChild(confidence);
      }
      container.appendChild(meta);
    }
  } else {
    const paragraph = document.createElement("p");
    paragraph.className = "summary-text";
    paragraph.textContent = typeof summary === "string" ? summary : JSON.stringify(summary);
    container.appendChild(paragraph);
  }

  if (rawResponse && rawResponse !== summary) {
    const details = document.createElement("details");
    const summaryEl = document.createElement("summary");
    summaryEl.textContent = dict.rawResponseLabel || "Raw response";
    details.appendChild(summaryEl);
    const pre = document.createElement("pre");
    pre.textContent = rawResponse;
    details.appendChild(pre);
    container.appendChild(details);
  }
}

function renderTable(container, rows, formatter) {
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!rows || !rows.length) {
    const message = container.dataset[`empty${currentLang.toUpperCase()}`] || getDict().empty || "No data.";
    renderEmptyRow(container, message);
    return;
  }
  const fragment = document.createDocumentFragment();
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    formatter(row).forEach((cellValue) => {
      const td = document.createElement("td");
      td.innerHTML = cellValue;
      tr.appendChild(td);
    });
    fragment.appendChild(tr);
  });
  container.appendChild(fragment);
}

function renderWarnings(warnings = []) {
  if (!elements.warnings) {
    return;
  }
  elements.warnings.innerHTML = "";
  if (!warnings.length) {
    return;
  }
  const list = document.createElement("ul");
  list.className = "warning-list";
  warnings.forEach((text) => {
    const li = document.createElement("li");
    li.textContent = text;
    list.appendChild(li);
  });
  const title = document.createElement("h4");
  title.textContent = getDict().warningTitle || "Warnings";
  elements.warnings.appendChild(title);
  elements.warnings.appendChild(list);
}

function renderLatestInsight(data) {
  latestInsightData = data;
  if (!data) {
    setRefreshLoading(false);
    renderSummary(null, null);
    renderTable(elements.indicesTable, [], () => []);
    renderTable(elements.dollarTable, [], () => []);
    renderTable(elements.rmbTable, [], () => []);
    renderTable(elements.commoditiesTable, [], () => []);
    renderWarnings();
    if (elements.generatedAt) {
      elements.generatedAt.textContent = "--";
    }
    if (elements.modelBadge) {
      elements.modelBadge.textContent = "";
    }
    return;
  }

  const metrics = data.metrics || {};
  const dict = getDict();
  if (elements.generatedAt) {
    elements.generatedAt.textContent = formatDateTime(data.generated_at || data.generatedAt);
  }
  if (elements.modelBadge) {
    const resolvedModel = resolveModelName(data.model);
    elements.modelBadge.textContent = data.model ? `${dict.modelLabel || "Model"}: ${resolvedModel}` : "";
  }

  renderSummary(data.summary, data.raw_response || data.rawResponse, data.model);

  const indicesRows = (metrics.globalIndices || []).map((item) => {
    const changeClass = Number(item.changePercent) > 0 ? "text-up" : Number(item.changePercent) < 0 ? "text-down" : "";
    return [
      item.name || item.code || "--",
      formatNumber(item.last, { maximumFractionDigits: 2 }),
      changeClass
        ? `<span class="${changeClass}">${formatNumber(item.changeAmount, { maximumFractionDigits: 2 })}</span>`
        : formatNumber(item.changeAmount, { maximumFractionDigits: 2 }),
      changeClass ? `<span class="${changeClass}">${formatPercent(item.changePercent)}</span>` : formatPercent(item.changePercent),
      formatDateTime(item.asOf),
    ];
  });
  renderTable(elements.indicesTable, indicesRows, (row) => row);

  const dollar = metrics.dollarIndex ? [metrics.dollarIndex] : [];
  const dollarRows = dollar.map((item) => {
    const changeClass = Number(item.changePercent) > 0 ? "text-up" : Number(item.changePercent) < 0 ? "text-down" : "";
    return [
      item.name || item.code || "--",
      formatNumber(item.close, { maximumFractionDigits: 2 }),
      changeClass
        ? `<span class="${changeClass}">${formatNumber(item.changeAmount, { maximumFractionDigits: 2 })}</span>`
        : formatNumber(item.changeAmount, { maximumFractionDigits: 2 }),
      changeClass ? `<span class="${changeClass}">${formatPercent(item.changePercent)}</span>` : formatPercent(item.changePercent),
      formatDate(item.tradeDate),
    ];
  });
  renderTable(elements.dollarTable, dollarRows, (row) => row);

  const rmbRates = metrics.rmbMidpoint?.rates || {};
  const rmbRows = Object.entries(rmbRates).map(([currency, payload]) => {
    return [currency, formatNumber(payload.quotePer100, { maximumFractionDigits: 4 }), formatDate(metrics.rmbMidpoint?.tradeDate)];
  });
  renderTable(elements.rmbTable, rmbRows, (row) => row);

  const commoditiesRows = (metrics.commodities || []).map((item) => {
    const changeClass = Number(item.changePercent) > 0 ? "text-up" : Number(item.changePercent) < 0 ? "text-down" : "";
    const lastValue = item.unit
      ? `${formatNumber(item.last, { maximumFractionDigits: 2 })} <span class="table-unit">${item.unit}</span>`
      : formatNumber(item.last, { maximumFractionDigits: 2 });
    return [
      item.name || item.code || "--",
      lastValue,
      changeClass
        ? `<span class="${changeClass}">${formatNumber(item.changeAmount, { maximumFractionDigits: 2 })}</span>`
        : formatNumber(item.changeAmount, { maximumFractionDigits: 2 }),
      changeClass ? `<span class="${changeClass}">${formatPercent(item.changePercent)}</span>` : formatPercent(item.changePercent),
      formatDateTime(item.quoteTime),
    ];
  });
  renderTable(elements.commoditiesTable, commoditiesRows, (row) => row);

  renderWarnings(metrics.warnings || data.warnings || []);
  setRefreshLoading(false);
}

function renderHistory(items = []) {
  const container = elements.historyList;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!items.length) {
    const message =
      container.dataset[`empty${currentLang.toUpperCase()}`] ||
      getDict().historyEmpty ||
      "No historical records.";
    const empty = document.createElement("div");
    empty.className = "insight-history__empty";
    empty.textContent = message;
    container.appendChild(empty);
    return;
  }

  items.forEach((entry) => {
    const card = document.createElement("article");
    card.className = "history-card";

    const header = document.createElement("div");
    header.className = "history-card__header";
    header.textContent = `${formatDateTime(entry.generated_at || entry.generatedAt)} · ${getDict().modelLabel || "Model"}: ${
      entry.model || "--"
    }`;
    card.appendChild(header);

    const parsed = parseSummaryPayload(entry.summary);
    const summaryText = parsed?.summary || (typeof entry.summary === "string" ? entry.summary : "");
    if (summaryText) {
      const summaryEl = document.createElement("p");
      summaryEl.className = "history-card__summary";
      summaryEl.textContent = summaryText;
      card.appendChild(summaryEl);
    }

    if (Array.isArray(parsed?.drivers) && parsed.drivers.length) {
      const list = document.createElement("ul");
      list.className = "history-card__drivers";
      parsed.drivers.forEach((driver) => {
        const li = document.createElement("li");
        li.textContent = driver;
        list.appendChild(li);
      });
      card.appendChild(list);
    }

    const metaLine = document.createElement("div");
    metaLine.className = "history-card__meta";
    if (parsed?.a_share_bias) {
      metaLine.appendChild(document.createTextNode(`${getDict().biasLabel || "Bias"}: ${parsed.a_share_bias}`));
    }
    if (parsed?.risk_level) {
      metaLine.appendChild(document.createTextNode(`${getDict().riskLabel || "Risk"}: ${parsed.risk_level}`));
    }
    if (parsed?.confidence !== undefined) {
      metaLine.appendChild(document.createTextNode(`${getDict().confidenceLabel || "Confidence"}: ${parsed.confidence}`));
    }
    if (metaLine.childNodes.length) {
      card.appendChild(metaLine);
    }

    if (entry.raw_response || entry.rawResponse || entry.metrics) {
      const details = document.createElement("details");
      const summaryToggle = document.createElement("summary");
      summaryToggle.textContent = getDict().historyDetailsLabel || "Details";
      details.appendChild(summaryToggle);

      if (entry.raw_response || entry.rawResponse) {
        const rawTitle = document.createElement("strong");
        rawTitle.textContent = getDict().rawResponseLabel || "Raw response";
        details.appendChild(rawTitle);
        const pre = document.createElement("pre");
        pre.textContent = entry.raw_response || entry.rawResponse;
        details.appendChild(pre);
      }

      if (entry.metrics) {
        const metricsTitle = document.createElement("strong");
        metricsTitle.textContent = getDict().historyMetricsLabel || "Metrics snapshot";
        details.appendChild(metricsTitle);
        const pre = document.createElement("pre");
        pre.textContent = JSON.stringify(entry.metrics, null, 2);
        details.appendChild(pre);
      }
      card.appendChild(details);
    }

    container.appendChild(card);
  });
}

async function fetchLatestInsightSnapshot() {
  try {
    const response = await fetch(`${API_BASE}/peripheral/insights/latest`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    return payload.insight || null;
  } catch (error) {
    console.error("Failed to load peripheral insight", error);
    return null;
  }
}

async function loadInsight() {
  const snapshot = await fetchLatestInsightSnapshot();
  renderLatestInsight(snapshot);
}

async function loadHistory(limit = 10) {
  if (!elements.historyList) {
    return;
  }
  try {
    const response = await fetch(`${API_BASE}/peripheral/insights/history?limit=${limit}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    historyInsightData = payload.items || [];
    renderHistory(historyInsightData);
  } catch (error) {
    console.error("Failed to load peripheral insight history", error);
    historyInsightData = [];
    renderHistory(historyInsightData);
  }
}

async function pollForInsightUpdate(previousGeneratedAt) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < REFRESH_POLL_TIMEOUT_MS) {
    const snapshot = await fetchLatestInsightSnapshot();
    const currentGeneratedAt = snapshot?.generated_at || snapshot?.generatedAt;
    if (currentGeneratedAt && currentGeneratedAt !== previousGeneratedAt) {
      return true;
    }
    await sleep(REFRESH_POLL_INTERVAL_MS);
  }
  return false;
}

async function triggerRefresh() {
  if (elements.refreshButton?.disabled) {
    return;
  }
  const previousGeneratedAt = latestInsightData?.generated_at || latestInsightData?.generatedAt || null;
  setRefreshLoading(true);
  try {
    const response = await fetch(`${API_BASE}/control/sync/peripheral-summary`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ runLLM: true }),
    });
    if (!response.ok) {
      throw new Error(`Refresh failed with status ${response.status}`);
    }
  } catch (error) {
    console.error("Failed to trigger peripheral insight refresh", error);
    setRefreshLoading(false);
    return;
  }

  const completed = await pollForInsightUpdate(previousGeneratedAt);
  if (completed) {
    window.location.reload();
    return;
  }
  console.warn("Peripheral insight reasoning did not finish before timeout.");
  setRefreshLoading(false);
}

function initLanguageSwitch() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => setLang(btn.dataset.lang))
  );
}

function initActions() {
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", triggerRefresh);
  }
}

initLanguageSwitch();
initActions();
applyTranslations();
loadInsight();
loadHistory();


window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
