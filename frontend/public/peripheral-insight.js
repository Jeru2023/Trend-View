const translations = getTranslations("peripheralInsight");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();
let pendingRefresh = null;

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
};

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
    elements.refreshButton.textContent = getDict().refreshing || "Refreshing";
  } else {
    delete elements.refreshButton.dataset.loading;
    elements.refreshButton.textContent = getDict().refreshButton || "Refresh";
  }
}

let latestInsightData = null;

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

function renderSummary(summary, rawResponse) {
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

  let parsed = null;
  if (typeof summary === "string") {
    try {
      parsed = JSON.parse(summary);
    } catch (error) {
      parsed = null;
    }
  }

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
    elements.modelBadge.textContent = data.model ? `${dict.modelLabel || "Model"}: ${data.model}` : "";
  }

  renderSummary(data.summary, data.raw_response || data.rawResponse);

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

async function loadInsight() {
  try {
    const response = await fetch(`${API_BASE}/peripheral/insights/latest`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    renderLatestInsight(payload.insight || null);
  } catch (error) {
    console.error("Failed to load peripheral insight", error);
    renderLatestInsight(null);
  }
}

async function triggerRefresh() {
  if (elements.refreshButton?.disabled) {
    return;
  }
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

  if (pendingRefresh) {
    clearTimeout(pendingRefresh);
  }
  pendingRefresh = setTimeout(() => {
    loadInsight().finally(() => {
      setRefreshLoading(false);
      pendingRefresh = null;
    });
  }, 1500);
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


window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
