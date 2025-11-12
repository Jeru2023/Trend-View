const translations = getTranslations("shibor");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";
const SHIBOR_SERIES = [
  { field: "on_rate", labelKey: "chartLegendOn", color: "#0ea5e9" },
  { field: "rate_1w", labelKey: "chartLegend1W", color: "#16a34a" },
  { field: "rate_1m", labelKey: "chartLegend1M", color: "#f97316" },
  { field: "rate_3m", labelKey: "chartLegend3M", color: "#9333ea" },
  { field: "rate_1y", labelKey: "chartLegend1Y", color: "#0f172a" },
];
const TABLE_FIELDS = [
  "on_rate",
  "rate_1w",
  "rate_2w",
  "rate_1m",
  "rate_3m",
  "rate_6m",
  "rate_9m",
  "rate_1y",
];

let echartsLoader = null;
let chartInstance = null;
let resizeListenerBound = false;
let currentLang = getInitialLanguage();
let latestItems = [];

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("shibor-refresh"),
  lastSynced: document.getElementById("shibor-last-synced"),
  chartContainer: document.getElementById("shibor-chart"),
  chartEmpty: document.getElementById("shibor-chart-empty"),
  tableBody: document.getElementById("shibor-tbody"),
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

function parseDateLike(value) {
  if (!value) {
    return null;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    if (/^\d{8}$/.test(trimmed)) {
      const year = Number(trimmed.slice(0, 4));
      const month = Number(trimmed.slice(4, 6)) - 1;
      const day = Number(trimmed.slice(6, 8));
      const candidate = new Date(year, month, day);
      if (
        !Number.isNaN(candidate.getTime()) &&
        candidate.getFullYear() === year &&
        candidate.getMonth() === month &&
        candidate.getDate() === day
      ) {
        return candidate;
      }
    }
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }
  return null;
}

function buildKeyVariants(field) {
  const variants = new Set([field]);
  const noUnderscoreDigits = field.replace(/_(\d)/g, "$1");
  variants.add(noUnderscoreDigits);
  const camelized = noUnderscoreDigits.replace(/_([a-zA-Z])/g, (_, letter) => letter.toUpperCase());
  variants.add(camelized);
  const digitCamelized = camelized.replace(/(\d)([a-zA-Z])/g, (_, digit, letter) => `${digit}${letter.toUpperCase()}`);
  variants.add(digitCamelized);
  return Array.from(variants).filter(Boolean);
}

function resolveField(item, field) {
  const keys = buildKeyVariants(field);
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(item, key)) {
      return item[key];
    }
  }
  return undefined;
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
    const normalized = trimmed.replace(/,/g, "");
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function formatNumber(value, options = {}) {
  if (value === null || value === undefined) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(value);
}

function formatPercent(value) {
  const formatted = formatNumber(value, { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  return formatted === "--" ? formatted : `${formatted}%`;
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return date.toLocaleDateString(locale, { year: "numeric", month: "short", day: "numeric" });
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, {
    hour: "2-digit",
    minute: "2-digit",
  })}`;
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

function normalizeRecords(records = []) {
  return records
    .map((item) => {
      const periodValue =
        item.period_date || item.periodDate || item.period_label || item.periodLabel;
      const periodDate = parseDateLike(periodValue);
      if (!periodDate) {
        return null;
      }
      const values = {};
      let hasValue = false;
      TABLE_FIELDS.forEach((field) => {
        const parsed = parseNumericValue(resolveField(item, field));
        values[field] = parsed;
        if (parsed !== null) {
          hasValue = true;
        }
      });
      if (!hasValue) {
        return null;
      }
      return {
        periodDate,
        label: periodDate.toISOString().slice(0, 10),
        displayLabel: formatDate(periodDate),
        values,
      };
    })
    .filter(Boolean);
}

function renderChart(items = []) {
  const normalized = normalizeRecords(items);
  if (!normalized.length) {
    clearChart();
    return;
  }

  normalized.sort((a, b) => a.periodDate - b.periodDate);
  const categories = normalized.map((entry) => entry.label);
  const dict = getDict();

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

  const legend = SHIBOR_SERIES.map((series) => dict[series.labelKey]);
  const seriesConfig = SHIBOR_SERIES.map((series) => ({
    name: dict[series.labelKey],
    type: "line",
    smooth: true,
    symbolSize: 3,
    data: normalized.map((entry) => entry.values[series.field]),
    lineStyle: { width: 2 },
  }));
  const colors = SHIBOR_SERIES.map((series) => series.color);

  chart.setOption(
    {
      color: colors,
      tooltip: {
        trigger: "axis",
        valueFormatter(value) {
          if (value === null || value === undefined || Number.isNaN(value)) {
            return "--";
          }
          return `${Number(value).toFixed(2)}%`;
        },
      },
      legend: {
        data: legend,
        top: 10,
        left: "center",
      },
      grid: { left: 48, right: 24, top: 80, bottom: 60 },
      xAxis: {
        type: "category",
        data: categories,
        boundaryGap: false,
        axisLabel: {
          rotate: categories.length > 14 ? 45 : 0,
          margin: 18,
        },
      },
      yAxis: {
        type: "value",
        axisLabel: {
          formatter(value) {
            return `${Number(value).toFixed(2)}%`;
          },
        },
        splitLine: { lineStyle: { color: "rgba(148, 163, 184, 0.2)" } },
      },
      series: seriesConfig,
      animationDuration: 600,
    },
    true
  );
  chart.resize();
}

function renderEmpty(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = TABLE_FIELDS.length + 1;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(row);
}

function renderTable(items = []) {
  if (!elements.tableBody) {
    return;
  }
  const dict = getDict();
  const normalized = normalizeRecords(items);
  if (!normalized.length) {
    renderEmpty(dict.empty || "No SHIBOR data.");
    return;
  }

  normalized.sort((a, b) => b.periodDate - a.periodDate);
  const fragment = document.createDocumentFragment();
  normalized.forEach((entry) => {
    const row = document.createElement("tr");
    const dateCell = document.createElement("td");
    dateCell.textContent = entry.displayLabel;
    row.appendChild(dateCell);
    TABLE_FIELDS.forEach((field) => {
      const cell = document.createElement("td");
      cell.textContent = formatPercent(entry.values[field]);
      row.appendChild(cell);
    });
    fragment.appendChild(row);
  });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function updateLastSynced(timestamp) {
  if (!elements.lastSynced) {
    return;
  }
  elements.lastSynced.textContent = timestamp ? formatDateTime(timestamp) : "--";
}

function setRefreshLoading(isLoading) {
  if (!elements.refreshButton) {
    return;
  }
  const dict = getDict();
  if (isLoading) {
    elements.refreshButton.dataset.loading = "1";
    elements.refreshButton.disabled = true;
    elements.refreshButton.textContent = dict.refreshing || dict.refreshButton || "Refreshing...";
  } else {
    delete elements.refreshButton.dataset.loading;
    elements.refreshButton.disabled = false;
    elements.refreshButton.textContent = dict.refreshButton || "Refresh";
  }
}

async function fetchShiborRates() {
  const dict = getDict();
  renderEmpty(dict.loading || "Loading...");
  try {
    await ensureEchartsLoaded();
    const response = await fetch(`${API_BASE}/macro/shibor?limit=500`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderChart(latestItems);
    renderTable(latestItems);
    updateLastSynced(data.lastSyncedAt || data.updated_at || data.last_synced_at);
  } catch (error) {
    console.error("Failed to fetch SHIBOR data:", error);
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
    const response = await fetch(`${API_BASE}/control/sync/shibor`, {
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
    setTimeout(fetchShiborRates, 800);
  } catch (error) {
    console.error("Manual SHIBOR sync failed:", error);
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
  await fetchShiborRates();
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

document.addEventListener("DOMContentLoaded", initialize);
