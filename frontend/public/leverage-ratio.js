const translations = getTranslations("leverageRatio");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();
let latestItems = [];
let chartInstance = null;
let resizeListenerBound = false;
let echartsLoader = null;
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";

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
    script.onload = () => {
      resolve();
    };
    script.onerror = (event) => {
      echartsLoader = null;
      reject(new Error("Failed to load ECharts library"));
    };
    document.head.appendChild(script);
  });
  return echartsLoader;
}

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("leverage-refresh"),
  lastSynced: document.getElementById("leverage-last-synced"),
  chartContainer: document.getElementById("leverage-chart"),
  chartEmpty: document.getElementById("leverage-chart-empty"),
  tableBody: document.getElementById("leverage-tbody"),
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
    return date.toLocaleDateString(locale, { year: "numeric", month: "short" });
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

function handleResize() {
  if (chartInstance) {
    chartInstance.resize();
  }
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
      const period = item.period_date || item.periodDate || item.period_label || item.periodLabel;
      const parsedDate = period ? new Date(period) : null;
      if (!parsedDate || Number.isNaN(parsedDate.getTime())) {
        return null;
      }
      return {
        date: parsedDate,
        values: {
          household_ratio: Number(item.household_ratio ?? item.householdRatio ?? NaN),
          non_financial_corporate_ratio: Number(
            item.non_financial_corporate_ratio ?? item.nonFinancialCorporateRatio ?? NaN
          ),
          government_ratio: Number(item.government_ratio ?? item.governmentRatio ?? NaN),
          real_economy_ratio: Number(item.real_economy_ratio ?? item.realEconomyRatio ?? NaN),
          financial_assets_ratio: Number(item.financial_assets_ratio ?? item.financialAssetsRatio ?? NaN),
          financial_liabilities_ratio: Number(
            item.financial_liabilities_ratio ?? item.financialLiabilitiesRatio ?? NaN
          ),
        },
      };
    })
    .filter(Boolean);

  if (!parsed.length) {
    clearChart();
    return;
  }

  parsed.sort((a, b) => a.date - b.date);
  const cutoff = parsed.length > 40 ? parsed.slice(-40) : parsed;
  const categories = cutoff.map((point) => point.date.toISOString().slice(0, 10));

  const seriesConfig = [
    { key: "household_ratio", labelKey: "tableHousehold" },
    { key: "non_financial_corporate_ratio", labelKey: "tableCorporate" },
    { key: "government_ratio", labelKey: "tableGovernment" },
    { key: "real_economy_ratio", labelKey: "tableRealEconomy" },
  ];

  const dict = getDict();
  const series = seriesConfig.map((config) => {
    const data = cutoff.map((entry) => entry.values[config.key]);
    return {
      name: dict[config.labelKey] || config.key,
      type: "line",
      smooth: true,
      showSymbol: false,
      data: data.map((value) => (Number.isFinite(value) ? Number(value.toFixed(2)) : null)),
    };
  });

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
        valueFormatter(value) {
          return formatNumber(value, { maximumFractionDigits: 1 });
        },
      },
      legend: {
        top: 0,
        textStyle: { color: "#52525b" },
      },
      grid: {
        left: "4%",
        right: "3%",
        top: 40,
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
            return formatNumber(value, { maximumFractionDigits: 1 });
          },
        },
        splitLine: {
          lineStyle: { color: "rgba(148, 163, 184, 0.25)" },
        },
      },
      series,
    },
    true
  );
}

function renderTable(items = []) {
  if (!elements.tableBody) {
    return;
  }
  if (!items.length) {
    const message =
      elements.tableBody?.dataset?.[`empty${currentLang.toUpperCase()}`] || getDict().empty || "No data.";
    renderEmpty(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((item) => {
    const row = document.createElement("tr");
    const cells = [
      item.period_label || item.periodLabel || formatDate(item.period_date || item.periodDate),
      formatNumber(item.household_ratio ?? item.householdRatio, { maximumFractionDigits: 1 }),
      formatNumber(item.non_financial_corporate_ratio ?? item.nonFinancialCorporateRatio, {
        maximumFractionDigits: 1,
      }),
      formatNumber(item.government_ratio ?? item.governmentRatio, { maximumFractionDigits: 1 }),
      formatNumber(item.central_government_ratio ?? item.centralGovernmentRatio, {
        maximumFractionDigits: 1,
      }),
      formatNumber(item.local_government_ratio ?? item.localGovernmentRatio, { maximumFractionDigits: 1 }),
      formatNumber(item.real_economy_ratio ?? item.realEconomyRatio, { maximumFractionDigits: 1 }),
      formatNumber(item.financial_assets_ratio ?? item.financialAssetsRatio, {
        maximumFractionDigits: 1,
      }),
      formatNumber(item.financial_liabilities_ratio ?? item.financialLiabilitiesRatio, {
        maximumFractionDigits: 1,
      }),
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

function renderEmpty(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 9;
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
  if (!timestamp) {
    elements.lastSynced.textContent = "--";
    return;
  }
  const value = typeof timestamp === "string" ? new Date(timestamp) : timestamp;
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    elements.lastSynced.textContent = `${value.toLocaleDateString(locale)} ${value.toLocaleTimeString(locale, {
      hour: "2-digit",
      minute: "2-digit",
    })}`;
  } else {
    elements.lastSynced.textContent = String(timestamp);
  }
}

async function fetchLeverageRatios() {
  try {
    const response = await fetch(`${API_BASE}/macro/leverage-ratio?limit=500`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    latestItems = Array.isArray(payload.items) ? payload.items : [];
    renderTable(latestItems);
    await ensureEchartsLoaded();
    renderChart(latestItems);
    updateLastSynced(payload.lastSyncedAt || payload.last_synced_at);
  } catch (error) {
    console.error("Failed to fetch macro leverage ratios", error);
    latestItems = [];
    renderTable([]);
    renderChart([]);
  }
}

async function triggerRefresh() {
  if (!elements.refreshButton) {
    return;
  }
  const dict = getDict();
  try {
    elements.refreshButton.dataset.loading = "1";
    elements.refreshButton.textContent = dict.refreshing || "Refreshing...";
    elements.refreshButton.disabled = true;
    const response = await fetch(`${API_BASE}/control/sync/leverage-ratio`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      throw new Error(`Sync failed with status ${response.status}`);
    }
    setTimeout(fetchLeverageRatios, 1500);
  } catch (error) {
    console.error("Failed to trigger leverage ratio sync", error);
  } finally {
    if (elements.refreshButton) {
      elements.refreshButton.disabled = false;
      delete elements.refreshButton.dataset.loading;
      elements.refreshButton.textContent = dict.refreshButton || "Refresh";
    }
  }
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
    elements.refreshButton.addEventListener("click", () => triggerRefresh());
  }
}

async function initialize() {
  applyTranslations();
  bindLanguageButtons();
  bindActions();
  await ensureEchartsLoaded();
  await fetchLeverageRatios();
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

document.addEventListener("DOMContentLoaded", initialize);
