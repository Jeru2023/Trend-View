const translations = getTranslations("futuresRealtime");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();
let latestItems = [];
let chartInstance = null;
let resizeBound = false;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tableBody: document.getElementById("futures-tbody"),
  lastSynced: document.getElementById("futures-last-synced"),
  refreshButton: document.getElementById("futures-refresh"),
  chartContainer: document.getElementById("futures-chart"),
  chartEmpty: document.getElementById("futures-chart-empty"),
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

function readValue(item, snakeKey, camelKey) {
  if (!item) {
    return undefined;
  }
  if (snakeKey && Object.prototype.hasOwnProperty.call(item, snakeKey) && item[snakeKey] !== null && item[snakeKey] !== undefined) {
    return item[snakeKey];
  }
  if (camelKey && Object.prototype.hasOwnProperty.call(item, camelKey) && item[camelKey] !== null && item[camelKey] !== undefined) {
    return item[camelKey];
  }
  return snakeKey ? item[snakeKey] ?? item[camelKey] : item[camelKey];
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

  renderTable(latestItems);
  renderChart(latestItems);
}

function renderEmpty(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 15;
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
    const lastPrice = readValue(item, "last_price", "lastPrice");
    const priceCny = readValue(item, "price_cny", "priceCny");
    const changeValue = readValue(item, "change_amount", "changeAmount");
    const changePercentValue = readValue(item, "change_percent", "changePercent");
    const openPrice = readValue(item, "open_price", "openPrice");
    const highPrice = readValue(item, "high_price", "highPrice");
    const lowPrice = readValue(item, "low_price", "lowPrice");
    const prevSettlement = readValue(item, "prev_settlement", "prevSettlement");
    const openInterest = readValue(item, "open_interest", "openInterest");
    const bidPrice = readValue(item, "bid_price", "bidPrice");
    const askPrice = readValue(item, "ask_price", "askPrice");
    const quoteTime = readValue(item, "quote_time", "quoteTime");
    const tradeDate = readValue(item, "trade_date", "tradeDate");

    const change = Number(changeValue);
    const changePercent = Number(changePercentValue);
    const changeClass = change > 0 ? "text-up" : change < 0 ? "text-down" : "";

    const cells = [
      readValue(item, "name", "name") || "--",
      readValue(item, "code", "code") || "--",
      formatNumber(lastPrice, { maximumFractionDigits: 4 }),
      formatNumber(priceCny, { maximumFractionDigits: 2 }),
      changeClass
        ? `<span class="${changeClass}">${formatNumber(change, { maximumFractionDigits: 2 })}</span>`
        : formatNumber(change, { maximumFractionDigits: 2 }),
      changeClass
        ? `<span class="${changeClass}">${formatPercent(changePercent)}</span>`
        : formatPercent(changePercent),
      formatNumber(openPrice, { maximumFractionDigits: 2 }),
      formatNumber(highPrice, { maximumFractionDigits: 2 }),
      formatNumber(lowPrice, { maximumFractionDigits: 2 }),
      formatNumber(prevSettlement, { maximumFractionDigits: 2 }),
      formatNumber(openInterest, { maximumFractionDigits: 0 }),
      formatNumber(bidPrice, { maximumFractionDigits: 2 }),
      formatNumber(askPrice, { maximumFractionDigits: 2 }),
      quoteTime || "--",
      formatDate(tradeDate),
    ];

    cells.forEach((value, index) => {
      const cell = document.createElement("td");
      if (index === 4 || index === 5) {
        cell.innerHTML = value;
      } else {
        cell.textContent = typeof value === "string" ? value : String(value ?? "--");
      }
      row.appendChild(cell);
    });

    fragment.appendChild(row);
  });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function ensureChartInstance() {
  if (!window.echarts || !elements.chartContainer) {
    return null;
  }
  if (!chartInstance) {
    chartInstance = window.echarts.init(elements.chartContainer);
  }
  if (!resizeBound) {
    window.addEventListener("resize", () => chartInstance && chartInstance.resize());
    resizeBound = true;
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

  const dataPoints = items
    .map((item) => {
      const name = readValue(item, "name", "name");
      const percent = Number(readValue(item, "change_percent", "changePercent"));
      return {
        name,
        value: Number.isFinite(percent) ? Number(percent.toFixed(2)) : null,
      };
    })
    .filter((entry) => entry.name && entry.value !== null);

  if (!dataPoints.length) {
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
        axisPointer: { type: "shadow" },
        valueFormatter: (value) => formatPercent(value),
      },
      grid: { left: "6%", right: "4%", top: 16, bottom: 60 },
      xAxis: {
        type: "category",
        data: dataPoints.map((entry) => entry.name),
        axisLabel: {
          interval: 0,
          rotate: dataPoints.length > 10 ? 30 : 0,
        },
        axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.4)" } },
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
      series: [
        {
          type: "bar",
          data: dataPoints.map((entry) => entry.value),
          itemStyle: {
            color: (params) => (params.value >= 0 ? "#16a34a" : "#dc2626"),
          },
          barWidth: "55%",
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

async function fetchFuturesRealtime() {
  const dict = getDict();
  renderEmpty(dict.loading || "Loading...");

  try {
    const response = await fetch(`${API_BASE}/macro/futures-realtime`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderTable(latestItems);
    updateLastSynced(data.lastSyncedAt || data.last_synced_at || data.updated_at);
    renderChart(latestItems);
  } catch (error) {
    console.error("Failed to fetch futures realtime:", error);
    latestItems = [];
    renderEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
    clearChart();
  }
}

async function triggerManualSync() {
  if (!elements.refreshButton || elements.refreshButton.dataset.loading === "1") {
    return;
  }
  setRefreshLoading(true);
  try {
    const response = await fetch(`${API_BASE}/control/sync/futures-realtime`, {
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
    setTimeout(fetchFuturesRealtime, 800);
  } catch (error) {
    console.error("Manual futures sync failed:", error);
    latestItems = [];
    renderEmpty(error?.message || "Failed to load data");
    updateLastSynced(null);
    clearChart();
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
  fetchFuturesRealtime();
}

document.addEventListener("DOMContentLoaded", initialize);

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
