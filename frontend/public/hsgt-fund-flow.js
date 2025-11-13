const translations = getTranslations("hsgtFundFlow");
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";
const LANG_STORAGE_KEY = "trend-view-lang";
const PAGE_SIZE_STORAGE_KEY = "hsgt-fund-flow-page-size";
const PAGE_SIZE_OPTIONS = [100, 200, 500];
const DEFAULT_PAGE_SIZE = 200;
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
let echartsLoader = null;
let chartInstance = null;

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

let currentLang = getInitialLanguage();
let currentItems = [];

function getInitialPageSize() {
  try {
    const stored = window.localStorage.getItem(PAGE_SIZE_STORAGE_KEY);
    const parsed = Number.parseInt(stored, 10);
    if (PAGE_SIZE_OPTIONS.includes(parsed)) {
      return parsed;
    }
  } catch (error) {
    /* no-op */
  }
  return DEFAULT_PAGE_SIZE;
}

function normalizePageSize(value) {
  if (PAGE_SIZE_OPTIONS.includes(value)) {
    return value;
  }
  const numeric = Number.parseInt(value, 10);
  if (PAGE_SIZE_OPTIONS.includes(numeric)) {
    return numeric;
  }
  return DEFAULT_PAGE_SIZE;
}

const state = {
  year: "all",
  page: 0,
  pageSize: getInitialPageSize(),
  total: 0,
  availableYears: [],
  lastSyncedAt: null,
  syncing: false,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tableBody: document.getElementById("hsgt-fund-flow-tbody"),
  yearFilter: document.getElementById("hsgt-year-filter"),
  resetButton: document.getElementById("hsgt-reset"),
  pageSizeSelect: document.getElementById("hsgt-page-size"),
  paginationPrev: document.getElementById("hsgt-prev"),
  paginationNext: document.getElementById("hsgt-next"),
  pageInfo: document.getElementById("hsgt-page-info"),
  syncButton: document.getElementById("hsgt-sync-button"),
  lastUpdatedValue: document.getElementById("hsgt-last-updated"),
  chartContainer: document.getElementById("hsgt-chart"),
  chartEmpty: document.getElementById("hsgt-chart-empty"),
};

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

function getField(item, camelKey) {
  if (!item || typeof item !== "object") {
    return undefined;
  }
  if (camelKey in item) {
    return item[camelKey];
  }
  const snakeKey = camelKey.replace(/[A-Z]/g, (match) => `_${match.toLowerCase()}`);
  if (snakeKey in item) {
    return item[snakeKey];
  }
  return undefined;
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

function extractYear(value) {
  if (!value) {
    return "";
  }
  if (typeof value === "string") {
    return value.slice(0, 4);
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return String(date.getFullYear());
    }
  } catch (error) {
    /* no-op */
  }
  return "";
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString().slice(0, 10);
    }
  } catch (error) {
    /* no-op */
  }
  return String(value);
}

function loadEcharts() {
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
      console.error("Failed to load ECharts", event);
      echartsLoader = null;
      reject(new Error("Failed to load ECharts"));
    };
    document.body.appendChild(script);
  });
  return echartsLoader;
}

function disposeChart() {
  if (chartInstance) {
    chartInstance.dispose();
    chartInstance = null;
  }
}

function showChartEmpty(message) {
  if (elements.chartEmpty) {
    elements.chartEmpty.textContent = message;
    elements.chartEmpty.hidden = false;
  }
  if (elements.chartContainer) {
    elements.chartContainer.classList.add("hidden");
  }
  disposeChart();
}

function hideChartEmpty() {
  if (elements.chartEmpty) {
    elements.chartEmpty.hidden = true;
  }
  if (elements.chartContainer) {
    elements.chartContainer.classList.remove("hidden");
  }
}

async function renderChart() {
  if (!elements.chartContainer) {
    return;
  }
  const dict = getDict();
  const dataset = (Array.isArray(currentItems) ? currentItems : [])
    .map((item) => {
      const tradeDate = getField(item, "tradeDate");
      if (!tradeDate) {
        return null;
      }
      const dateObj = new Date(tradeDate);
      if (Number.isNaN(dateObj.getTime())) {
        return null;
      }
      const label = dateObj.toISOString().slice(0, 10);
      const netBuyRaw = Number(getField(item, "netBuyAmount"));
      const fundInflowRaw = Number(getField(item, "fundInflow"));
      const netBuy = Number.isFinite(netBuyRaw) ? Number(netBuyRaw.toFixed(2)) : null;
      const fundInflow = Number.isFinite(fundInflowRaw) ? Number(fundInflowRaw.toFixed(2)) : null;
      return {
        dateObj,
        label,
        netBuy,
        fundInflow,
      };
    })
    .filter((point) => point);

  const sorted = dataset.sort((a, b) => a.dateObj - b.dateObj);
  const recent = sorted.slice(-120);
  const hasSeries = recent.some((point) => point.netBuy !== null || point.fundInflow !== null);

  if (!recent.length || !hasSeries) {
    showChartEmpty(dict.chartEmpty || "Not enough data to render the chart.");
    return;
  }

  hideChartEmpty();
  await loadEcharts();
  if (!window.echarts) {
    showChartEmpty(dict.chartEmpty || "Not enough data to render the chart.");
    return;
  }

  if (!chartInstance) {
    chartInstance = window.echarts.init(elements.chartContainer);
  }

  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  const numberFormatter = new Intl.NumberFormat(locale, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  const legendNetBuy = dict.legendNetBuy || "Net buy (CNY 100M)";
  const legendFundInflow = dict.legendFundInflow || "Fund inflow (CNY 100M)";
  const labels = recent.map((point) => point.label);
  const seriesDefs = [
    {
      name: legendNetBuy,
      data: recent.map((point) => point.netBuy),
      color: "#2563eb",
    },
    {
      name: legendFundInflow,
      data: recent.map((point) => point.fundInflow),
      color: "#f97316",
    },
  ];

  const option = {
    color: seriesDefs.map((s) => s.color),
    tooltip: {
      trigger: "axis",
      valueFormatter: (value) =>
        typeof value === "number" ? `${numberFormatter.format(value)} 亿` : "--",
    },
    legend: {
      top: 0,
      data: seriesDefs.map((item) => item.name),
      textStyle: { color: "#4b5563" },
    },
    grid: {
      left: "4%",
      right: "3%",
      top: 48,
      bottom: 36,
    },
    xAxis: {
      type: "category",
      boundaryGap: false,
      data: labels,
      axisLabel: {
        formatter: (value) => value.slice(5),
        color: "#6b7280",
      },
      axisLine: { lineStyle: { color: "rgba(148, 163, 184, 0.4)" } },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value",
      axisLabel: {
        formatter: (value) => numberFormatter.format(value),
        color: "#6b7280",
      },
      splitLine: {
        lineStyle: {
          type: "dashed",
          color: "rgba(148, 163, 184, 0.35)",
        },
      },
    },
    series: seriesDefs.map((config) => ({
      name: config.name,
      type: "line",
      smooth: true,
      showSymbol: false,
      emphasis: { focus: "series" },
      lineStyle: { width: 2 },
      data: config.data,
      areaStyle: {
        opacity: 0.12,
        color: new window.echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: config.color },
          { offset: 1, color: "rgba(37, 99, 235, 0.03)" },
        ]),
      },
    })),
  };

  chartInstance.setOption(option, true);
}

function formatTimestamp(value) {
  const dict = getDict();
  if (!value) {
    return dict.lastUpdatedUnknown || "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      throw new Error("Invalid date");
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return new Intl.DateTimeFormat(locale, {
      dateStyle: "medium",
      timeStyle: "short",
    }).format(date);
  } catch (error) {
    return dict.lastUpdatedUnknown || "--";
  }
}

function updateLastUpdatedDisplay() {
  if (!elements.lastUpdatedValue) {
    return;
  }
  elements.lastUpdatedValue.textContent = formatTimestamp(state.lastSyncedAt);
}

function updateSyncButton() {
  if (!elements.syncButton) {
    return;
  }
  const dict = getDict();
  elements.syncButton.disabled = Boolean(state.syncing);
  elements.syncButton.textContent = state.syncing
    ? dict.syncing || "Syncing..."
    : dict.syncButton || "Update now";
}

function applyFilters(items) {
  return items.filter((item) => {
    if (state.year !== "all") {
      const tradeDate = getField(item, "tradeDate");
      const year = extractYear(tradeDate);
      if (year !== state.year) {
        return false;
      }
    }
    return true;
  });
}

function renderEmptyRow(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 5;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(row);
}

function renderTable(items = currentItems) {
  if (!elements.tableBody) {
    return;
  }
  const baseItems = Array.isArray(items) ? items : [];
  const rows =
    state.year === "all"
      ? baseItems
      : baseItems.filter((item) => extractYear(getField(item, "tradeDate")) === state.year);

  if (!rows.length) {
    const key = currentLang === "zh" ? "data-empty-zh" : "data-empty-en";
    const fallback = currentLang === "zh" ? "暂无沪港通资金流数据。" : "No HSGT fund flow data.";
    const message =
      (elements.tableBody.dataset && elements.tableBody.dataset[key]) || fallback;
    renderEmptyRow(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  rows.forEach((item) => {
    const row = document.createElement("tr");
    const symbol = getField(item, "symbol");
    const tradeDate = getField(item, "tradeDate");
    const netBuy = getField(item, "netBuyAmount");
    const cumulative = getField(item, "netBuyAmountCumulative");
    const fundInflow = getField(item, "fundInflow");

    const cells = [
      symbol || "--",
      formatDate(tradeDate),
      formatNumber(netBuy, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      formatNumber(fundInflow, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      formatNumber(cumulative, { minimumFractionDigits: 3, maximumFractionDigits: 3 }),
    ];

    cells.forEach((text) => {
      const cell = document.createElement("td");
      cell.textContent = text;
      row.appendChild(cell);
    });

    fragment.appendChild(row);
  });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function updatePagination() {
  if (!elements.pageInfo) {
    return;
  }

  const dict = getDict();
  const total = Number(state.total) || 0;
  if (total === 0 && currentItems.length === 0) {
    elements.pageInfo.textContent = "--";
    if (elements.paginationPrev) {
      elements.paginationPrev.disabled = true;
    }
    if (elements.paginationNext) {
      elements.paginationNext.disabled = true;
    }
    return;
  }
  const totalPages = total > 0 ? Math.ceil(total / state.pageSize) : 0;
  const displayTotalPages = totalPages > 0 ? totalPages : 0;
  const currentPage = displayTotalPages === 0 ? 0 : Math.min(state.page + 1, displayTotalPages);

  const template =
    dict.paginationInfo ||
    "Page {current} of {totalPages} · {total} results · {pageSize} / page";
  elements.pageInfo.textContent = template
    .replace("{current}", String(currentPage))
    .replace("{totalPages}", String(displayTotalPages))
    .replace("{total}", String(total))
    .replace("{pageSize}", String(state.pageSize));

  if (elements.paginationPrev) {
    elements.paginationPrev.disabled = currentPage <= 1;
  }

  if (elements.paginationNext) {
    elements.paginationNext.disabled = displayTotalPages === 0 || currentPage >= displayTotalPages;
  }
}

function populateFilters() {
  if (!elements.yearFilter) {
    return;
  }
  const dict = getDict();
  const years = state.availableYears.map((year) => String(year)).sort((a, b) => b.localeCompare(a));
  const currentValue = state.year;

  elements.yearFilter.innerHTML = "";
  const optionAll = document.createElement("option");
  optionAll.value = "all";
  optionAll.textContent = dict.filterAllYears || "All years";
  elements.yearFilter.appendChild(optionAll);

  years.forEach((year) => {
    const option = document.createElement("option");
    option.value = year;
    option.textContent = year;
    elements.yearFilter.appendChild(option);
  });

  if (years.includes(currentValue)) {
    elements.yearFilter.value = currentValue;
  } else {
    elements.yearFilter.value = "all";
    state.year = "all";
  }
}

function populatePageSizeSelect() {
  const select = elements.pageSizeSelect;
  if (!select) {
    return;
  }

  const activeValue = normalizePageSize(state.pageSize);

  if (!select.options.length || select.options.length !== PAGE_SIZE_OPTIONS.length) {
    select.innerHTML = "";
    PAGE_SIZE_OPTIONS.forEach((size) => {
      const option = document.createElement("option");
      option.value = String(size);
      option.textContent = String(size);
      select.appendChild(option);
    });
  }

  select.value = String(activeValue);
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.title = dict.title || document.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      el.textContent = value;
    }
  });

  updateSyncButton();
  updateLastUpdatedDisplay();
  populateFilters();
  populatePageSizeSelect();
  updatePagination();
  renderTable(currentItems);
  renderChart().catch((error) => {
    console.error("Failed to render HSGT chart", error);
  });
}

function onFilterChange() {
  if (!elements.yearFilter) {
    return;
  }
  const selectedYear = elements.yearFilter.value || "all";
  if (state.year !== selectedYear) {
    state.year = selectedYear;
    state.page = 0;
    loadHsgtFundFlow();
  }
}

function resetFilters() {
  state.year = "all";
  state.page = 0;
  if (elements.yearFilter) {
    elements.yearFilter.value = "all";
  }
  loadHsgtFundFlow();
}

async function loadHsgtFundFlow() {
  try {
    state.pageSize = normalizePageSize(state.pageSize);
    const params = new URLSearchParams();
    params.set("limit", String(state.pageSize));
    params.set("offset", String(state.page * state.pageSize));
    if (state.year !== "all") {
      params.set("startDate", `${state.year}-01-01`);
      params.set("endDate", `${state.year}-12-31`);
    }

    const response = await fetch(`${API_BASE}/fund-flow/hsgt?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }

    const payload = await response.json();
    currentItems = Array.isArray(payload.items) ? payload.items : [];
    state.total = Number(payload.total) || currentItems.length;

    if (Array.isArray(payload.availableYears)) {
      state.availableYears = payload.availableYears.map((year) => String(year));
    }
    const lastSyncedAt = payload.lastSyncedAt || payload.last_synced_at;
    state.lastSyncedAt = lastSyncedAt || null;

    const totalPages = state.total > 0 ? Math.ceil(state.total / state.pageSize) : 0;
    if (state.total > 0 && state.page >= totalPages) {
      state.page = Math.max(0, totalPages - 1);
      await loadHsgtFundFlow();
      return;
    }

    populateFilters();
    populatePageSizeSelect();
    renderTable(currentItems);
    updatePagination();
    updateLastUpdatedDisplay();
    await renderChart();
  } catch (error) {
    console.error("Failed to load HSGT fund flow data", error);
    currentItems = [];
    state.total = 0;
    populatePageSizeSelect();
    renderTable([]);
    updatePagination();
    updateLastUpdatedDisplay();
    await renderChart();
  }
}

async function triggerSync() {
  if (state.syncing) {
    return;
  }
  state.syncing = true;
  updateSyncButton();
  try {
    const response = await fetch(`${API_BASE}/control/sync/hsgt-fund-flow`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      throw new Error(`Sync request failed with status ${response.status}`);
    }
    await response.json().catch(() => ({}));
  } catch (error) {
    console.error("Failed to trigger HSGT sync", error);
  } finally {
    state.syncing = false;
    updateSyncButton();
    setTimeout(() => {
      loadHsgtFundFlow();
    }, 4000);
  }
}

function bindEvents() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      if (btn.dataset.lang && btn.dataset.lang !== currentLang) {
        currentLang = btn.dataset.lang;
        persistLanguage(currentLang);
        applyTranslations();
      }
    });
  });

  if (elements.resetButton) {
    elements.resetButton.addEventListener("click", resetFilters);
  }

  if (elements.yearFilter) {
    elements.yearFilter.addEventListener("change", onFilterChange);
  }

  if (elements.pageSizeSelect) {
    elements.pageSizeSelect.addEventListener("change", () => {
      const nextSize = normalizePageSize(elements.pageSizeSelect.value);
      if (nextSize !== state.pageSize) {
        state.pageSize = nextSize;
        state.page = 0;
        try {
          window.localStorage.setItem(PAGE_SIZE_STORAGE_KEY, String(nextSize));
        } catch (error) {
          /* no-op */
        }
        loadHsgtFundFlow();
      } else {
        elements.pageSizeSelect.value = String(state.pageSize);
      }
    });
  }

  if (elements.paginationPrev) {
    elements.paginationPrev.addEventListener("click", () => {
      if (state.page > 0) {
        state.page -= 1;
        loadHsgtFundFlow();
      }
    });
  }

  if (elements.paginationNext) {
    elements.paginationNext.addEventListener("click", () => {
      const totalPages = state.total > 0 ? Math.ceil(state.total / state.pageSize) : 0;
      if (state.page + 1 < totalPages) {
        state.page += 1;
        loadHsgtFundFlow();
      }
    });
  }

  if (elements.syncButton) {
    elements.syncButton.addEventListener("click", triggerSync);
  }
}

function initialize() {
  bindEvents();
  window.addEventListener("resize", () => {
    if (chartInstance) {
      chartInstance.resize();
    }
  });
  applyTranslations();
  loadHsgtFundFlow();
}

initialize();
