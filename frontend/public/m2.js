const translations = getTranslations("m2");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";
let echartsLoader = null;

function ensureEchartsLoaded() {
  if (window.echarts) {
    return Promise.resolve();
  }
  if (echartsLoader) {
    return echartsLoader;
  }
  echartsLoader = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = ECHARTS_CDN;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => {
      echartsLoader = null;
      reject(new Error("Failed to load chart library"));
    };
    document.head.appendChild(script);
  });
  return echartsLoader;
}

let currentLang = getInitialLanguage();
let latestItems = [];
let chartInstance = null;
let resizeListenerBound = false;

const SERIES_CONFIG = [
  { key: "m2_yoy", color: "#0f766e", labelKey: "seriesM2" },
  { key: "m1_yoy", color: "#2563eb", labelKey: "seriesM1" },
  { key: "m0_yoy", color: "#f97316", labelKey: "seriesM0" },
];

const TABLE_COLUMNS = [
  { key: "m0", formatter: (value) => formatAmount(value) },
  { key: "m0_yoy", formatter: (value) => formatPercent(value) },
  { key: "m0_mom", formatter: (value) => formatPercent(value) },
  { key: "m1", formatter: (value) => formatAmount(value) },
  { key: "m1_yoy", formatter: (value) => formatPercent(value) },
  { key: "m1_mom", formatter: (value) => formatPercent(value) },
  { key: "m2", formatter: (value) => formatAmount(value) },
  { key: "m2_yoy", formatter: (value) => formatPercent(value) },
  { key: "m2_mom", formatter: (value) => formatPercent(value) },
];

const TABLE_COLUMN_COUNT = 1 + TABLE_COLUMNS.length;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("m2-refresh"),
  lastSynced: document.getElementById("m2-last-synced"),
  chartContainer: document.getElementById("m2-chart"),
  chartEmpty: document.getElementById("m2-chart-empty"),
  tableBody: document.getElementById("m2-tbody"),
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
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed || /^nan$/i.test(trimmed) || /^null$/i.test(trimmed) || trimmed === "--") {
      return "--";
    }
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(numeric);
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
    return date.toLocaleDateString(locale, { year: "numeric", month: "short" });
  } catch (error) {
    return String(value);
  }
}

function formatAmount(value) {
  const numeric = parseNumericValue(value);
  if (numeric === null) {
    return "--";
  }
  return formatNumber(numeric, { maximumFractionDigits: 2 });
}

function formatPercent(value) {
  const numeric = parseNumericValue(value);
  if (numeric === null) {
    return "--";
  }
  return `${numeric.toFixed(2)}%`;
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
    })}`;
  } catch (error) {
    return String(value);
  }
}

function getPeriodLabel(item) {
  const rawLabel = item.period_label || item.periodLabel;
  if (rawLabel && typeof rawLabel === "string") {
    if (rawLabel.includes("-")) {
      return rawLabel.slice(0, 7);
    }
    if (rawLabel.length === 6) {
      return `${rawLabel.slice(0, 4)}-${rawLabel.slice(4, 6)}`;
    }
  }
  const dateValue = item.period_date || item.periodDate;
  if (dateValue) {
    return formatDate(dateValue);
  }
  return rawLabel || "--";
}

function ensureChartInstance() {
  if (!window.echarts || !elements.chartContainer) {
    return null;
  }
  if (!chartInstance) {
    chartInstance = window.echarts.init(elements.chartContainer);
  }
  if (!resizeListenerBound) {
    window.addEventListener("resize", () => chartInstance && chartInstance.resize());
    resizeListenerBound = true;
  }
  return chartInstance;
}

function clearChart() {
  if (chartInstance) {
    chartInstance.clear();
  }
  if (elements.chartContainer) {
    elements.chartContainer.classList.add("hidden");
  }
  if (elements.chartEmpty) {
    elements.chartEmpty.classList.remove("hidden");
  }
}

function parseNumericValue(raw) {
  if (raw === null || raw === undefined) {
    return null;
  }
  if (typeof raw === "number") {
    return Number.isFinite(raw) ? raw : null;
  }
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (!trimmed || /^nan$/i.test(trimmed) || /^null$/i.test(trimmed) || trimmed === "--") {
      return null;
    }
    const normalized = trimmed.replace(/,/g, "").replace(/%$/, "");
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function resolveFieldValue(item, key) {
  if (!item || typeof item !== "object") {
    return null;
  }
  if (key in item) {
    return item[key];
  }
  const camelKey = key.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
  if (camelKey in item) {
    return item[camelKey];
  }
  return null;
}

function renderChart(items = []) {
  if (!items.length) {
    clearChart();
    return;
  }

  const rows = items
    .map((item) => {
      const period = item.period_date || item.periodDate || item.period_label || item.periodLabel;
      const parsedDate = period ? new Date(period) : null;
      if (!parsedDate || Number.isNaN(parsedDate.getTime())) {
        return null;
      }
      return { date: parsedDate, record: item };
    })
    .filter(Boolean);

  if (!rows.length) {
    clearChart();
    return;
  }

  rows.sort((a, b) => a.date - b.date);
  const categories = rows.map((entry) => entry.date.toISOString().slice(0, 7));
  const dict = getDict();
  const chartSeries = SERIES_CONFIG.map((config) => {
    const label = dict[config.labelKey] || config.labelKey;
    const data = rows.map((entry) => parseNumericValue(resolveFieldValue(entry.record, config.key)));
    const hasData = data.some((value) => value !== null);
    if (!hasData) {
      return null;
    }
    return {
      name: label,
      type: "line",
      smooth: true,
      symbol: "circle",
      symbolSize: 4,
      lineStyle: { width: 2, color: config.color },
      itemStyle: { color: config.color },
      data: data.map((value) => (value === null ? null : Number(value.toFixed(2)))),
    };
  }).filter(Boolean);

  if (!chartSeries.length) {
    clearChart();
    return;
  }

  if (elements.chartContainer) {
    elements.chartContainer.classList.remove("hidden");
  }
  if (elements.chartEmpty) {
    elements.chartEmpty.classList.add("hidden");
  }

  const chart = ensureChartInstance();
  if (!chart) {
    clearChart();
    return;
  }

  chart.setOption(
    {
      tooltip: {
        trigger: "axis",
        valueFormatter: (value) =>
          `${formatNumber(value, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`,
      },
      legend: {
        data: chartSeries.map((series) => series.name),
      },
      grid: { left: "5%", right: "4%", top: 40, bottom: 60 },
      xAxis: {
        type: "category",
        data: categories,
        boundaryGap: false,
        axisLabel: {
          rotate: categories.length > 18 ? 45 : categories.length > 12 ? 30 : 0,
          margin: 16,
        },
      },
      yAxis: {
        type: "value",
        axisLabel: {
          formatter(value) {
            return `${value.toFixed(1)}%`;
          },
        },
        splitLine: { lineStyle: { color: "rgba(148, 163, 184, 0.2)" } },
      },
      series: chartSeries,
      animationDuration: 600,
    },
    true
  );
  chart.resize();
}

function renderTable(items = []) {
  if (!elements.tableBody) {
    return;
  }
  const fragment = document.createDocumentFragment();
  let validCount = 0;

  items.forEach((item) => {
    const periodLabel = getPeriodLabel(item);
    const row = document.createElement("tr");
    const periodCell = document.createElement("td");
    periodCell.textContent = periodLabel;
    row.appendChild(periodCell);

    TABLE_COLUMNS.forEach((column) => {
      const cell = document.createElement("td");
      const rawValue = resolveFieldValue(item, column.key);
      cell.textContent = column.formatter(rawValue);
      row.appendChild(cell);
    });

    fragment.appendChild(row);
    validCount += 1;
  });

  if (!validCount) {
    const message =
      elements.tableBody?.dataset?.[`empty${currentLang.toUpperCase()}`] || getDict().empty || "No data.";
    renderEmpty(message);
    return;
  }

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function renderEmpty(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = TABLE_COLUMN_COUNT;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(row);
}

function updateLastSynced(timestamp) {
  if (!elements.lastSynced) {
    return;
  }
  elements.lastSynced.textContent = timestamp ? formatDateTime(timestamp) : "--";
}

function setRefreshLoading(isLoading) {
  const button = elements.refreshButton;
  if (!button) {
    return;
  }
  const dict = getDict();
  if (isLoading) {
    button.dataset.loading = "1";
    button.disabled = true;
    button.textContent = dict.refreshing || dict.refreshButton || "Refreshing...";
  } else {
    delete button.dataset.loading;
    button.disabled = false;
    button.textContent = dict.refreshButton || "Refresh";
  }
}

async function fetchM2() {
  const dict = getDict();
  renderEmpty(dict.loading || "Loading...");

  try {
    await ensureEchartsLoaded();
    const response = await fetch(`${API_BASE}/macro/m2?limit=40`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderChart(latestItems);
    renderTable(latestItems);
    updateLastSynced(data.lastSyncedAt || data.last_synced_at || data.updated_at);
  } catch (error) {
    console.error("Failed to fetch M2 data:", error);
    latestItems = [];
    clearChart();
    renderEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
  }
}

async function triggerManualSync() {
  if (!elements.refreshButton || elements.refreshButton.dataset.loading === "1") {
    return;
  }
  setRefreshLoading(true);
  try {
    const response = await fetch(`${API_BASE}/control/sync/m2`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      let detail = response.statusText || `HTTP ${response.status}`;
      try {
        const payload = await response.json();
        if (payload && typeof payload.detail === "string") {
          detail = payload.detail;
        }
      } catch (parseError) {
        /* ignore */
      }
      throw new Error(detail);
    }
    setTimeout(fetchM2, 800);
  } catch (error) {
    console.error("Manual M2 sync failed:", error);
    latestItems = [];
    clearChart();
    renderEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
  } finally {
    setTimeout(() => setRefreshLoading(false), 400);
  }
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = dict.title;

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

  document.querySelectorAll(".lang-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });

  renderTable(latestItems);
  renderChart(latestItems);
}

function bindLanguageButtons() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const lang = btn.dataset.lang;
      if (lang && translations[lang]) {
        currentLang = lang;
        persistLanguage(lang);
        applyTranslations();
      }
    });
  });
}

function bindActions() {
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => triggerManualSync());
  }
}

async function initialize() {
  applyTranslations();
  bindLanguageButtons();
  bindActions();
  await fetchM2();
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

document.addEventListener("DOMContentLoaded", initialize);
