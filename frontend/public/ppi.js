const translations = getTranslations("ppi");

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
  refreshButton: document.getElementById("ppi-refresh"),
  lastSynced: document.getElementById("ppi-last-synced"),
  chartContainer: document.getElementById("ppi-chart"),
  chartEmpty: document.getElementById("ppi-chart-empty"),
  tableBody: document.getElementById("ppi-tbody"),
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
  return formatted === "--" ? formatted : `${formatted}%`;
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
    const normalized = trimmed.replace(/,/g, "").replace(/%$/, "");
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

  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - 60);

  const parsed = items
    .map((item) => {
      const period =
        item.period_date || item.periodDate || item.period_label || item.periodLabel || null;
      const periodDate = period ? new Date(period) : null;
      if (!periodDate || Number.isNaN(periodDate.getTime())) {
        return null;
      }
      const currentIndex = parseNumericValue(item.current_index ?? item.currentIndex);
      const yoyChange = parseNumericValue(item.yoy_change ?? item.yoyChange);
      if (currentIndex === null) {
        return null;
      }
      return {
        periodDate,
        label: `${periodDate.getFullYear()}-${String(periodDate.getMonth() + 1).padStart(2, "0")}`,
        currentIndex,
        yoyChange,
      };
    })
    .filter(Boolean)
    .filter((entry) => entry.periodDate >= cutoff);

  if (!parsed.length) {
    clearChart();
    return;
  }

  parsed.sort((a, b) => a.periodDate - b.periodDate);
  const categories = parsed.map((entry) => entry.label);
  const currentSeries = parsed.map((entry) =>
    typeof entry.currentIndex === "number" ? Number(entry.currentIndex.toFixed(2)) : null
  );
  const yoySeries = parsed.map((entry) =>
    typeof entry.yoyChange === "number" ? Number(entry.yoyChange.toFixed(2)) : null
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
            .map((param) => {
              const value =
                param.seriesName === dict.chartLegendYoy
                  ? formatPercent(param.value, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    })
                  : formatNumber(param.value, {
                      minimumFractionDigits: 2,
                      maximumFractionDigits: 2,
                    });
              return `${param.marker}${param.seriesName}: ${value}`;
            })
            .join("<br/>");
          return `${params[0].axisValue}<br/>${rows}`;
        },
      },
      legend: {
        data: [dict.chartLegendCurrent, dict.chartLegendYoy],
        bottom: 10,
      },
      grid: { left: "5%", right: "8%", top: 24, bottom: 80 },
      xAxis: {
        type: "category",
        data: categories,
        boundaryGap: false,
        axisLabel: {
          rotate: categories.length > 18 ? 45 : categories.length > 12 ? 30 : 0,
          margin: 16,
        },
      },
      yAxis: [
        {
          type: "value",
          name: dict.chartLegendCurrent,
          axisLabel: {
            formatter(value) {
              return `${Number(value).toFixed(1)}`;
            },
          },
          splitLine: { lineStyle: { color: "rgba(148, 163, 184, 0.2)" } },
        },
        {
          type: "value",
          name: dict.chartLegendYoy,
          axisLabel: {
            formatter(value) {
              return `${Number(value).toFixed(1)}%`;
            },
          },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: dict.chartLegendCurrent,
          type: "line",
          smooth: true,
          symbol: "circle",
          symbolSize: 4,
          data: currentSeries,
          lineStyle: { width: 2, color: "#2563eb" },
          areaStyle: { color: "rgba(37, 99, 235, 0.12)" },
        },
        {
          name: dict.chartLegendYoy,
          type: "line",
          smooth: true,
          symbol: "circle",
          symbolSize: 4,
          yAxisIndex: 1,
          data: yoySeries,
          lineStyle: { width: 2, color: "#16a34a", type: "dashed" },
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
  const displayItems = items.filter(
    (item) => parseNumericValue(item.current_index ?? item.currentIndex) !== null
  );
  if (!displayItems.length) {
    const dict = getDict();
    const message = dict.empty || "No PPI data.";
    renderEmpty(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  displayItems.forEach((item) => {
    const current = parseNumericValue(item.current_index ?? item.currentIndex);
    if (current === null) {
      return;
    }
    const yoy = parseNumericValue(item.yoy_change ?? item.yoyChange);
    const cumulative = parseNumericValue(item.cumulative_index ?? item.cumulativeIndex);
    const periodLabel =
      item.period_label || item.periodLabel || formatDate(item.period_date || item.periodDate);
    const row = document.createElement("tr");
    const cells = [
      periodLabel,
      formatNumber(current, { maximumFractionDigits: 2 }),
      formatPercent(yoy, { maximumFractionDigits: 2 }),
      formatNumber(cumulative, { maximumFractionDigits: 2 }),
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

async function fetchPpi() {
  const dict = getDict();
  renderEmpty(dict.loading || "Loading...");

  try {
    await ensureEchartsLoaded();
    const response = await fetch(`${API_BASE}/macro/ppi?limit=600`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    latestItems = Array.isArray(data.items) ? data.items : [];
    renderChart(latestItems);
    renderTable(latestItems);
    updateLastSynced(data.lastSyncedAt || data.last_synced_at || data.updated_at);
  } catch (error) {
    console.error("Failed to fetch PPI data:", error);
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
    const response = await fetch(`${API_BASE}/control/sync/ppi`, {
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
    setTimeout(fetchPpi, 800);
  } catch (error) {
    console.error("Manual PPI sync failed:", error);
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
  await fetchPpi();
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

document.addEventListener("DOMContentLoaded", initialize);
