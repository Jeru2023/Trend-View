const translations = getTranslations("pbcRate");

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

let currentLang = getInitialLanguage();
let latestItems = [];
let chartInstance = null;
let resizeListenerBound = false;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("pbc-rate-refresh"),
  lastSynced: document.getElementById("pbc-rate-last-synced"),
  chartContainer: document.getElementById("pbc-rate-chart"),
  chartEmpty: document.getElementById("pbc-rate-chart-empty"),
  tableBody: document.getElementById("pbc-rate-tbody"),
};

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

function formatPercent(value, options = {}) {
  const formatted = formatNumber(value, options);
  return formatted === "--" ? formatted : `${formatted}`;
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
    return date.toLocaleDateString(locale, { year: "numeric", month: "short", day: "numeric" });
  } catch (error) {
    return String(value);
  }
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

function renderChart(items = []) {
  if (!items.length) {
    clearChart();
    return;
  }

  const parsed = items
    .map((item) => {
      const period =
        item.period_date || item.periodDate || item.period_label || item.periodLabel || null;
      const periodDate = period ? new Date(period) : null;
      if (!periodDate || Number.isNaN(periodDate.getTime())) {
        return null;
      }
      const actual = parseNumericValue(item.actual_value ?? item.actualValue);
      const forecast = parseNumericValue(item.forecast_value ?? item.forecastValue);
      const previous = parseNumericValue(item.previous_value ?? item.previousValue);
      if (actual === null && forecast === null && previous === null) {
        return null;
      }
      return {
        periodDate,
        label: periodDate.toISOString().slice(0, 10),
        actual,
        forecast,
        previous,
      };
    })
    .filter(Boolean);

  if (!parsed.length) {
    clearChart();
    return;
  }

  parsed.sort((a, b) => a.periodDate - b.periodDate);
  const categories = parsed.map((entry) => entry.label);
  const actualSeries = parsed.map((entry) =>
    entry.actual !== null ? Number(entry.actual.toFixed(2)) : null
  );
  const forecastSeries = parsed.map((entry) =>
    entry.forecast !== null ? Number(entry.forecast.toFixed(2)) : null
  );
  const previousSeries = parsed.map((entry) =>
    entry.previous !== null ? Number(entry.previous.toFixed(2)) : null
  );

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

  const dict = getDict();
  chart.setOption(
    {
      tooltip: {
        trigger: "axis",
        formatter(params = []) {
          if (!Array.isArray(params) || !params.length) {
            return "";
          }
          const rows = params
            .filter((param) => param.value !== null && param.value !== undefined)
            .map((param) => `${param.marker}${param.seriesName}: ${param.value?.toFixed?.(2) ?? param.value}`)
            .join("<br/>");
          return rows ? `${params[0].axisValue}<br/>${rows}` : params[0].axisValue;
        },
      },
      legend: {
        data: [dict.chartLegendActual, dict.chartLegendForecast, dict.chartLegendPrevious],
        bottom: 10,
      },
      grid: { left: "5%", right: "5%", top: 24, bottom: 80 },
      xAxis: {
        type: "category",
        data: categories,
        boundaryGap: false,
        axisLabel: {
          rotate: categories.length > 10 ? 45 : 0,
          margin: 16,
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
      series: [
        {
          name: dict.chartLegendActual,
          type: "line",
          smooth: true,
          symbol: "circle",
          symbolSize: 4,
          data: actualSeries,
          lineStyle: { width: 2, color: "#1d4ed8" },
        },
        {
          name: dict.chartLegendForecast,
          type: "line",
          smooth: true,
          symbol: "triangle",
          symbolSize: 4,
          lineStyle: { width: 2, color: "#f97316", type: "dashed" },
          data: forecastSeries,
        },
        {
          name: dict.chartLegendPrevious,
          type: "line",
          smooth: true,
          symbol: "diamond",
          symbolSize: 4,
          lineStyle: { width: 2, color: "#0f766e", type: "dotted" },
          data: previousSeries,
        },
      ],
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
  cell.colSpan = 4;
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
  const displayItems = items
    .map((item) => {
      const actual = parseNumericValue(item.actual_value ?? item.actualValue);
      const forecast = parseNumericValue(item.forecast_value ?? item.forecastValue);
      const previous = parseNumericValue(item.previous_value ?? item.previousValue);
      if (actual === null && forecast === null && previous === null) {
        return null;
      }
      const label =
        item.period_label || item.periodLabel || formatDate(item.period_date || item.periodDate);
      return {
        label,
        actual,
        forecast,
        previous,
      };
    })
    .filter(Boolean);

  if (!displayItems.length) {
    renderEmpty(dict.empty || "No interest rate decisions.");
    return;
  }

  const fragment = document.createDocumentFragment();
  displayItems.forEach((entry) => {
    const row = document.createElement("tr");
    const cells = [
      entry.label,
      entry.actual !== null ? `${formatNumber(entry.actual, { maximumFractionDigits: 2 })}%` : "--",
      entry.forecast !== null ? `${formatNumber(entry.forecast, { maximumFractionDigits: 2 })}%` : "--",
      entry.previous !== null ? `${formatNumber(entry.previous, { maximumFractionDigits: 2 })}%` : "--",
    ];
    cells.forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = typeof value === "string" ? value : String(value ?? "--");
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

async function fetchPbcRate() {
  const dict = getDict();
  renderEmpty(dict.loading || "Loading...");

  try {
    await ensureEchartsLoaded();
    const response = await fetch(`${API_BASE}/macro/pbc-rate?limit=500`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderChart(latestItems);
    renderTable(latestItems);
    updateLastSynced(data.lastSyncedAt || data.last_synced_at || data.updated_at);
  } catch (error) {
    console.error("Failed to fetch PBC rate data:", error);
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
    const response = await fetch(`${API_BASE}/control/sync/pbc-rate`, {
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
    setTimeout(fetchPbcRate, 800);
  } catch (error) {
    console.error("Manual PBC rate sync failed:", error);
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
  await fetchPbcRate();
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

document.addEventListener("DOMContentLoaded", initialize);
