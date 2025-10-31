const translations = getTranslations("rmbMidpoint");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const CURRENCIES = [
  { key: "usd", labelKey: "tableUsd" },
  { key: "eur", labelKey: "tableEur" },
  { key: "jpy", labelKey: "tableJpy" },
  { key: "hkd", labelKey: "tableHkd" },
  { key: "gbp", labelKey: "tableGbp" },
  { key: "aud", labelKey: "tableAud" },
  { key: "cad", labelKey: "tableCad" },
  { key: "nzd", labelKey: "tableNzd" },
  { key: "sgd", labelKey: "tableSgd" },
  { key: "chf", labelKey: "tableChf" },
  { key: "myr", labelKey: "tableMyr" },
  { key: "rub", labelKey: "tableRub" },
  { key: "zar", labelKey: "tableZar" },
  { key: "krw", labelKey: "tableKrw" },
  { key: "aed", labelKey: "tableAed" },
  { key: "sar", labelKey: "tableSar" },
  { key: "huf", labelKey: "tableHuf" },
  { key: "pln", labelKey: "tablePln" },
  { key: "dkk", labelKey: "tableDkk" },
  { key: "sek", labelKey: "tableSek" },
  { key: "nok", labelKey: "tableNok" },
  { key: "try", labelKey: "tableTry" },
  { key: "mxn", labelKey: "tableMxn" },
  { key: "thb", labelKey: "tableThb" },
];

let currentLang = getInitialLanguage();
let latestItems = [];
let selectedCurrency = "usd";
let chartInstance = null;
let resizeBound = false;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tableBody: document.getElementById("rmb-mid-tbody"),
  lastSynced: document.getElementById("rmb-mid-last-synced"),
  refreshButton: document.getElementById("rmb-mid-refresh"),
  currencySelect: document.getElementById("rmb-mid-currency"),
  chartContainer: document.getElementById("rmb-mid-chart"),
  chartEmpty: document.getElementById("rmb-mid-chart-empty"),
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

  if (elements.currencySelect) {
    Array.from(elements.currencySelect.options).forEach((option) => {
      const found = CURRENCIES.find((item) => item.key === option.value);
      if (found) {
        option.textContent = getDict()[found.labelKey] || option.value.toUpperCase();
      }
    });
  }

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
  cell.colSpan = 25;
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
      formatNumber(item.usd),
      formatNumber(item.eur),
      formatNumber(item.jpy),
      formatNumber(item.hkd),
      formatNumber(item.gbp),
      formatNumber(item.aud),
      formatNumber(item.cad),
      formatNumber(item.nzd),
      formatNumber(item.sgd),
      formatNumber(item.chf),
      formatNumber(item.myr),
      formatNumber(item.rub),
      formatNumber(item.zar),
      formatNumber(item.krw),
      formatNumber(item.aed),
      formatNumber(item.sar),
      formatNumber(item.huf),
      formatNumber(item.pln),
      formatNumber(item.dkk),
      formatNumber(item.sek),
      formatNumber(item.nok),
      formatNumber(item["try"]),
      formatNumber(item.mxn),
      formatNumber(item.thb),
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

  const parsed = items
    .map((item) => {
      const dateValue = item.trade_date || item.tradeDate;
      if (!dateValue) {
        return null;
      }
      const parsedDate = new Date(dateValue);
      if (Number.isNaN(parsedDate.getTime())) {
        return null;
      }
      const rawValue = item[selectedCurrency];
      const numeric = Number(rawValue);
      if (!Number.isFinite(numeric)) {
        return null;
      }
      return { date: parsedDate, value: numeric };
    })
    .filter(Boolean)
    .sort((a, b) => a.date - b.date);

  if (!parsed.length) {
    clearChart();
    return;
  }

  const cutoff = parsed[parsed.length - 1].date.getTime() - 365 * 24 * 60 * 60 * 1000;
  const subset = parsed.filter((point) => point.date.getTime() >= cutoff);
  const dataPoints = subset.length ? subset : parsed;

  const categories = dataPoints.map((point) => point.date.toISOString().slice(0, 10));
  const seriesData = dataPoints.map((point) => Number(point.value.toFixed(4)));

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
  const label = dict[CURRENCIES.find((c) => c.key === selectedCurrency)?.labelKey || ""] ||
    selectedCurrency.toUpperCase();

  chart.setOption(
    {
      tooltip: {
        trigger: "axis",
        valueFormatter: (value) => formatNumber(value, { maximumFractionDigits: 4 }),
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
        axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.4)" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: {
          formatter(value) {
            return formatNumber(value, { maximumFractionDigits: 4 });
          },
        },
        splitLine: { lineStyle: { color: "rgba(148, 163, 184, 0.25)" } },
      },
      series: [
        {
          name: label,
          type: "line",
          smooth: true,
          showSymbol: false,
          data: seriesData,
          lineStyle: { width: 2, color: "#10b981" },
          areaStyle: { opacity: 0.12, color: "#34d399" },
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

async function fetchRmbMidpoint() {
  const dict = getDict();
  renderEmpty(dict.loading || "Loading...");

  try {
    const response = await fetch(`${API_BASE}/macro/rmb-midpoint?limit=500`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderTable(latestItems);
    renderChart(latestItems);
    updateLastSynced(data.lastSyncedAt || data.last_synced_at || data.updated_at);
  } catch (error) {
    console.error("Failed to fetch RMB midpoint data:", error);
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
    const response = await fetch(`${API_BASE}/control/sync/rmb-midpoint`, {
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
        /* ignore */
      }
      throw new Error(detail);
    }
    setTimeout(fetchRmbMidpoint, 800);
  } catch (error) {
    console.error("Manual RMB midpoint sync failed:", error);
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
  if (elements.currencySelect) {
    elements.currencySelect.addEventListener("change", (event) => {
      selectedCurrency = event.target.value || "usd";
      renderChart(latestItems);
    });
  }
}

function initialize() {
  applyTranslations();
  bindLanguageButtons();
  bindControls();
  fetchRmbMidpoint();
}

document.addEventListener("DOMContentLoaded", initialize);

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
