const translations = getTranslations("dollarIndex");

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

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tableBody: document.getElementById("dollar-index-tbody"),
  lastSynced: document.getElementById("dollar-index-last-synced"),
  refreshButton: document.getElementById("dollar-index-refresh"),
  chartContainer: document.getElementById("dollar-index-chart"),
  chartEmpty: document.getElementById("dollar-index-chart-empty"),
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
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric.toFixed(2)}%`;
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
    return date.toLocaleDateString(locale);
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

  renderChart(latestItems);
}

function renderEmpty(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 8;
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
  if (!items.length) {
    const message =
      elements.tableBody.dataset[`empty${currentLang.toUpperCase()}`] || getDict().empty || "No data.";
    renderEmpty(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((item) => {
    const row = document.createElement("tr");
    const cells = [
      formatDate(item.trade_date || item.tradeDate),
      item.code || "--",
      item.name || "--",
      formatNumber(item.open_price ?? item.openPrice, { maximumFractionDigits: 2 }),
      formatNumber(item.close_price ?? item.closePrice, { maximumFractionDigits: 2 }),
      formatNumber(item.high_price ?? item.highPrice, { maximumFractionDigits: 2 }),
      formatNumber(item.low_price ?? item.lowPrice, { maximumFractionDigits: 2 }),
      formatPercent(item.amplitude),
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

function handleResize() {
  if (chartInstance) {
    chartInstance.resize();
  }
}

function ensureChartInstance() {
  if (!window.echarts || !elements.chartContainer) {
    return null;
  }
  if (!chartInstance) {
    chartInstance = window.echarts.init(elements.chartContainer);
  }
  if (!resizeListenerBound) {
    window.addEventListener("resize", handleResize);
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
      const rawDate = item.trade_date || item.tradeDate;
      if (!rawDate) {
        return null;
      }
      const parsedDate = new Date(rawDate);
      if (Number.isNaN(parsedDate.getTime())) {
        return null;
      }
      const close = Number(item.close_price ?? item.closePrice);
      if (!Number.isFinite(close)) {
        return null;
      }
      return { date: parsedDate, close };
    })
    .filter(Boolean);

  if (!parsed.length) {
    clearChart();
    return;
  }

  parsed.sort((a, b) => a.date - b.date);
  const latestPoint = parsed[parsed.length - 1];
  const cutoffTime = latestPoint.date.getTime() - 365 * 24 * 60 * 60 * 1000;
  const timeRange = parsed.filter((point) => point.date.getTime() >= cutoffTime);
  const dataPoints = timeRange.length ? timeRange : parsed;

  const categories = dataPoints.map((point) => point.date.toISOString().slice(0, 10));
  const seriesData = dataPoints.map((point) => Number(point.close.toFixed(4)));

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
        valueFormatter: (value) => formatNumber(value, { maximumFractionDigits: 2 }),
      },
      grid: {
        left: "4%",
        right: "2%",
        top: 32,
        bottom: 32,
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: categories,
        axisLabel: {
          formatter(value) {
            return value;
          },
        },
        axisLine: {
          lineStyle: { color: "rgba(148, 163, 184, 0.4)" },
        },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: {
          formatter(value) {
            return formatNumber(value, { maximumFractionDigits: 2 });
          },
        },
        splitLine: {
          lineStyle: { color: "rgba(148, 163, 184, 0.25)" },
        },
      },
      series: [
        {
          name: dict.tableClose || "Close",
          type: "line",
          smooth: true,
          showSymbol: false,
          data: seriesData,
          lineStyle: {
            width: 2,
            color: "#2563eb",
          },
          areaStyle: {
            opacity: 0.12,
            color: "#60a5fa",
          },
        },
      ],
      animationDuration: 600,
    },
    true
  );
  chart.resize();
}

function updateLastSynced(timestamp) {
  if (!elements.lastSynced) {
    return;
  }
  elements.lastSynced.textContent = timestamp ? formatDateTime(timestamp) : "--";
}

async function fetchDollarIndex() {
  const dict = getDict();
  renderEmpty(dict.loading || "Loading...");

  try {
    await ensureEchartsLoaded();
    const response = await fetch(`${API_BASE}/macro/dollar-index?limit=500`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderChart(latestItems);
    renderTable(latestItems);
    updateLastSynced(data.lastSyncedAt || data.last_synced_at || data.updated_at);
  } catch (error) {
    console.error("Failed to fetch dollar index:", error);
    latestItems = [];
    clearChart();
    renderEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
  }
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

async function triggerManualSync() {
  if (!elements.refreshButton || elements.refreshButton.dataset.loading === "1") {
    return;
  }
  setRefreshLoading(true);
  try {
    const response = await fetch(`${API_BASE}/control/sync/dollar-index`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      let detail = response.statusText || `HTTP ${response.status}`;
      try {
        const data = await response.json();
        if (data && typeof data.detail === "string") {
          detail = data.detail;
        }
      } catch (parseError) {
        /* swallow */
      }
      throw new Error(detail);
    }
    setTimeout(fetchDollarIndex, 800);
  } catch (error) {
    console.error("Manual dollar index sync failed:", error);
    latestItems = [];
    clearChart();
    renderEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
  } finally {
    setTimeout(() => setRefreshLoading(false), 400);
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
        renderTable(latestItems);
      }
    };
  });
}

function bindControls() {
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", (event) => {
      event.preventDefault();
      triggerManualSync();
    });
  }
}

function initialize() {
  applyTranslations();
  bindLanguageButtons();
  bindControls();
  fetchDollarIndex();
}

document.addEventListener("DOMContentLoaded", initialize);

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
