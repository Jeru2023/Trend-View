const translations = getTranslations("basicInfo");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const PAGE_SIZE = 20;

const exchangeLabels = {
  en: { SSE: "SSE", SZSE: "SZSE", BSE: "BSE" },
  zh: {
    SSE: "\u4e0a\u4ea4\u6240",
    SZSE: "\u6df1\u4ea4\u6240",
    BSE: "\u5317\u4ea4\u6240",
  },
};

const marketLabels = {
  en: {
    "\u4e3b\u677f": "Main Board",
    "\u521b\u4e1a\u677f": "ChiNext",
    "\u79d1\u521b\u677f": "STAR Market",
    "Main Board": "Main Board",
    ChiNext: "ChiNext",
    "STAR Market": "STAR Market",
  },
  zh: {
    "\u4e3b\u677f": "\u4e3b\u677f",
    "\u521b\u4e1a\u677f": "\u521b\u4e1a\u677f",
    "\u79d1\u521b\u677f": "\u79d1\u521b\u677f",
    "Main Board": "\u4e3b\u677f",
    ChiNext: "\u521b\u4e1a\u677f",
    "STAR Market": "\u79d1\u521b\u677f",
  },
};

const LANG_STORAGE_KEY = "trend-view-lang";

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
  return browserLang.startsWith("zh") ? "zh" : "en";
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

const state = {
  trading: {
    page: 1,
    total: 0,
    items: [],
    filters: { keyword: "", market: "all", exchange: "all" },
  },
  metrics: {
    page: 1,
    total: 0,
    items: [],
    filters: { keyword: "", market: "all", exchange: "all" },
  },
};

let activeTab = "tradingData";

const elements = {
  tables: {
    tradingData: document.getElementById("trading-data-table"),
    financialData: document.getElementById("financial-data-table"),
    tradingStats: document.getElementById("trading-stats-table"),
    financialStats: document.getElementById("financial-stats-table"),
  },
  bodies: {
    tradingData: document.getElementById("trading-data-body"),
    financialData: document.getElementById("financial-data-body"),
    tradingStats: document.getElementById("trading-stats-body"),
    financialStats: document.getElementById("financial-stats-body"),
  },
  tabs: document.querySelectorAll(".tab"),
  langButtons: document.querySelectorAll(".lang-btn"),
  searchBox: document.querySelector(".search-box"),
  keywordInput: document.getElementById("keyword"),
  marketSelect: document.getElementById("market"),
  exchangeSelect: document.getElementById("exchange"),
  applyButton: document.getElementById("apply-filters"),
  resetButton: document.getElementById("reset-filters"),
  prevPage: document.getElementById("prev-page"),
  nextPage: document.getElementById("next-page"),
  pageInfo: document.getElementById("page-info"),
};

const EMPTY_VALUE = "--";

function formatNumber(value) {
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 2 }).format(
    value ?? 0
  );
}

function formatOptionalNumber(value, options = {}) {
  if (value === null || value === undefined) {
    return EMPTY_VALUE;
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(value);
}

function formatOptionalDate(value) {
  if (!value) {
    return EMPTY_VALUE;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toISOString().slice(0, 10);
}

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.title = dict.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      el.textContent = value;
    }
  });

  document.querySelectorAll("[data-placeholder-en]").forEach((el) => {
    const placeholder = el.dataset[`placeholder${currentLang.toUpperCase()}`];
    if (typeof placeholder === "string") {
      el.placeholder = placeholder;
    }
  });
}

function formatPercent(value, { fromRatio = false } = {}) {
  if (value === null || value === undefined) {
    return EMPTY_VALUE;
  }
  const percentValue = fromRatio ? value * 100 : value;
  const formatted = percentValue.toFixed(2);
  return `${percentValue >= 0 ? "+" : ""}${formatted}%`;
}

function formatFinancialPercent(value) {
  if (value === null || value === undefined) {
    return EMPTY_VALUE;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return EMPTY_VALUE;
  }
  const treatAsRatio = Math.abs(numeric) <= 1;
  return formatPercent(numeric, { fromRatio: treatAsRatio });
}

function getTrendClass(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return value >= 0 ? "text-up" : "text-down";
}

function appendEmptyRow(body, colSpan) {
  if (!body) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = colSpan;
  cell.textContent = translations[currentLang].noData;
  cell.style.textAlign = "center";
  cell.style.color = "#6b7280";
  row.appendChild(cell);
  body.appendChild(row);
}

function renderTradingDataTable(items) {
  const body = elements.bodies.tradingData;
  if (!body) {
    return;
  }
  body.innerHTML = "";

  if (!items.length) {
    appendEmptyRow(body, 11);
    return;
  }

  const marketMap = marketLabels[currentLang] || {};
  const exchangeMap = exchangeLabels[currentLang] || {};

  items.forEach((item) => {
    const marketLabel =
      item.market && marketMap[item.market] ? marketMap[item.market] : item.market ?? EMPTY_VALUE;
    const exchangeLabel =
      item.exchange && exchangeMap[item.exchange]
        ? exchangeMap[item.exchange]
        : item.exchange ?? EMPTY_VALUE;
    const changeClass = getTrendClass(item.pct_change);

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.code}</td>
      <td>${item.name ?? EMPTY_VALUE}</td>
      <td>${item.industry ?? EMPTY_VALUE}</td>
      <td>${marketLabel ?? EMPTY_VALUE}</td>
      <td>${exchangeLabel ?? EMPTY_VALUE}</td>
      <td>${formatOptionalNumber(item.last_price, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}</td>
      <td class="${changeClass}">${formatPercent(item.pct_change)}</td>
      <td>${formatOptionalNumber(item.volume, { maximumFractionDigits: 0 })}</td>
      <td>${formatOptionalNumber(item.market_cap, { maximumFractionDigits: 0 })}</td>
      <td>${formatOptionalNumber(item.pe_ratio, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}</td>
      <td>${
        item.turnover_rate == null
          ? EMPTY_VALUE
          : `${formatOptionalNumber(item.turnover_rate, {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            })}%`
      }</td>
    `;
    body.appendChild(row);
  });
}

function renderFinancialDataTable(items) {
  const body = elements.bodies.financialData;
  if (!body) {
    return;
  }
  body.innerHTML = "";

  if (!items.length) {
    appendEmptyRow(body, 10);
    return;
  }

  items.forEach((item) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.code}</td>
      <td>${item.name ?? EMPTY_VALUE}</td>
      <td>${formatOptionalDate(item.ann_date)}</td>
      <td>${formatOptionalDate(item.end_date)}</td>
      <td>${formatOptionalNumber(item.basic_eps, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}</td>
      <td>${formatOptionalNumber(
        item.revenue === null || item.revenue === undefined ? null : item.revenue / 1_000_000,
        { maximumFractionDigits: 2 }
      )}</td>
      <td>${formatOptionalNumber(
        item.operate_profit === null || item.operate_profit === undefined
          ? null
          : item.operate_profit / 1_000_000,
        { maximumFractionDigits: 2 }
      )}</td>
      <td>${formatOptionalNumber(
        item.net_income === null || item.net_income === undefined ? null : item.net_income / 1_000_000,
        { maximumFractionDigits: 2 }
      )}</td>
      <td>${formatOptionalNumber(
        item.gross_margin === null || item.gross_margin === undefined
          ? null
          : item.gross_margin / 1_000_000,
        { maximumFractionDigits: 2 }
      )}</td>
      <td>${formatFinancialPercent(item.roe)}</td>
    `;
    body.appendChild(row);
  });
}

function renderTradingStatsTable(items) {
  const body = elements.bodies.tradingStats;
  if (!body) {
    return;
  }
  body.innerHTML = "";

  if (!items.length) {
    appendEmptyRow(body, 11);
    return;
  }

  items.forEach((item) => {
    const statsValues = [
      item.pct_change_1y,
      item.pct_change_6m,
      item.pct_change_3m,
      item.pct_change_1m,
      item.pct_change_2w,
      item.pct_change_1w,
    ];
    const maValues = [item.ma_20, item.ma_10, item.ma_5];

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.code}</td>
      <td>${item.name ?? EMPTY_VALUE}</td>
      ${statsValues
        .map((value) => {
          const trendClass = getTrendClass(value);
          return `<td class="${trendClass}">${formatPercent(value, { fromRatio: true })}</td>`;
        })
        .join("")}
      ${maValues
        .map((value) =>
          `<td>${formatOptionalNumber(value, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}</td>`
        )
        .join("")}
    `;
    body.appendChild(row);
  });
}

function renderFinancialStatsTable(items) {
  const body = elements.bodies.financialStats;
  if (!body) {
    return;
  }
  body.innerHTML = "";

  if (!items.length) {
    appendEmptyRow(body, 11);
    return;
  }

  items.forEach((item) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.code}</td>
      <td>${item.name ?? EMPTY_VALUE}</td>
      <td>${formatOptionalDate(item.reportingPeriod)}</td>
      <td class="${getTrendClass(item.netIncomeYoyLatest)}">${formatPercent(
      item.netIncomeYoyLatest,
      { fromRatio: true }
    )}</td>
      <td class="${getTrendClass(item.netIncomeYoyPrev1)}">${formatPercent(
      item.netIncomeYoyPrev1,
      { fromRatio: true }
    )}</td>
      <td class="${getTrendClass(item.netIncomeYoyPrev2)}">${formatPercent(
      item.netIncomeYoyPrev2,
      { fromRatio: true }
    )}</td>
      <td class="${getTrendClass(item.netIncomeQoqLatest)}">${formatPercent(
      item.netIncomeQoqLatest,
      { fromRatio: true }
    )}</td>
      <td class="${getTrendClass(item.revenueYoyLatest)}">${formatPercent(
      item.revenueYoyLatest,
      { fromRatio: true }
    )}</td>
      <td class="${getTrendClass(item.revenueQoqLatest)}">${formatPercent(
      item.revenueQoqLatest,
      { fromRatio: true }
    )}</td>
      <td class="${getTrendClass(item.roeYoyLatest)}">${formatPercent(item.roeYoyLatest, {
      fromRatio: true,
    })}</td>
      <td class="${getTrendClass(item.roeQoqLatest)}">${formatPercent(item.roeQoqLatest, {
      fromRatio: true,
    })}</td>
    `;
    body.appendChild(row);
  });
}

function renderActiveTab() {
  switch (activeTab) {
    case "financialData":
      renderFinancialDataTable(state.trading.items);
      break;
    case "tradingStats":
      renderTradingStatsTable(state.trading.items);
      break;
    case "financialStats":
      renderFinancialStatsTable(state.metrics.items);
      break;
    case "tradingData":
    default:
      renderTradingDataTable(state.trading.items);
      break;
  }
}

function collectFilters() {
  return {
    keyword: elements.keywordInput.value.trim(),
    market: elements.marketSelect.value,
    exchange: elements.exchangeSelect.value,
  };
}

function setActiveTab(tabName) {
  activeTab = tabName;

  elements.tabs.forEach((tab) => {
    const isActive = tab.dataset.tab === tabName;
    tab.classList.toggle("tab--active", isActive);
  });

  Object.entries(elements.tables).forEach(([key, table]) => {
    if (table) {
      table.classList.toggle("hidden", key !== tabName);
    }
  });

  if (tabName === "financialStats" && !state.metrics.items.length) {
    loadFinancialStats(1);
  } else {
    renderActiveTab();
    updatePaginationControls();
  }
}

function updateLanguage(lang) {
  persistLanguage(lang);
  currentLang = lang;
  elements.langButtons.forEach((btn) =>
    btn.classList.toggle("active", btn.dataset.lang === lang)
  );
  applyTranslations();
  renderActiveTab();
  updatePaginationControls();
}

function updatePaginationControls() {
  const currentState = activeTab === "financialStats" ? state.metrics : state.trading;
  const totalPages = Math.max(1, Math.ceil(currentState.total / PAGE_SIZE));
  const dict = translations[currentLang];
  const pageText = dict.paginationInfo
    .replace("{current}", currentState.page)
    .replace("{totalPages}", totalPages)
    .replace("{total}", formatNumber(currentState.total));
  if (elements.pageInfo) {
    elements.pageInfo.textContent = pageText;
  }
  if (elements.prevPage) {
    elements.prevPage.disabled = currentState.page <= 1;
  }
  if (elements.nextPage) {
    elements.nextPage.disabled = currentState.page >= totalPages;
  }
}

async function loadTradingData(page = 1) {
  state.trading.page = page;
  state.trading.filters = collectFilters();

  const params = new URLSearchParams();
  params.set("limit", PAGE_SIZE.toString());
  params.set("offset", ((state.trading.page - 1) * PAGE_SIZE).toString());

  const filters = state.trading.filters;
  if (filters.keyword) params.set("keyword", filters.keyword);
  if (filters.market && filters.market !== "all") params.set("market", filters.market);
  if (filters.exchange && filters.exchange !== "all") params.set("exchange", filters.exchange);

  try {
    const response = await fetch(`${API_BASE}/stocks?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    state.trading.total = data.total;
    state.trading.items = data.items.map((item) => ({
      code: item.code,
      name: item.name,
      industry: item.industry,
      market: item.market,
      exchange: item.exchange,
      last_price: item.lastPrice,
      pct_change: item.pctChange,
      volume: item.volume,
      trade_date: item.tradeDate,
      market_cap: item.marketCap,
      pe_ratio: item.peRatio,
      turnover_rate: item.turnoverRate,
      pct_change_1y: item.pctChange1Y,
      pct_change_6m: item.pctChange6M,
      pct_change_3m: item.pctChange3M,
      pct_change_1m: item.pctChange1M,
      pct_change_2w: item.pctChange2W,
      pct_change_1w: item.pctChange1W,
      ma_20: item.ma20,
      ma_10: item.ma10,
      ma_5: item.ma5,
      ann_date: item.annDate,
      end_date: item.endDate,
      basic_eps: item.basicEps,
      revenue: item.revenue,
      operate_profit: item.operateProfit,
      net_income: item.netIncome,
      gross_margin: item.grossMargin,
      roe: item.roe,
    }));
  } catch (error) {
    console.error("Failed to fetch stock data:", error);
    state.trading.total = 0;
    state.trading.items = [];
  }

  if (activeTab !== "financialStats") {
    renderActiveTab();
    updatePaginationControls();
  }
}

async function loadFinancialStats(page = 1) {
  state.metrics.page = page;
  state.metrics.filters = collectFilters();

  const params = new URLSearchParams();
  params.set("limit", PAGE_SIZE.toString());
  params.set("offset", ((state.metrics.page - 1) * PAGE_SIZE).toString());

  const filters = state.metrics.filters;
  if (filters.keyword) params.set("keyword", filters.keyword);
  if (filters.market && filters.market !== "all") params.set("market", filters.market);
  if (filters.exchange && filters.exchange !== "all") params.set("exchange", filters.exchange);

  try {
    const response = await fetch(
      `${API_BASE}/fundamental-metrics?${params.toString()}`
    );
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    state.metrics.total = data.total;
    state.metrics.items = data.items.map((item) => ({
      code: item.code,
      name: item.name,
      reportingPeriod: item.netIncomeEndDateLatest,
      netIncomeYoyLatest: item.netIncomeYoyLatest,
      netIncomeYoyPrev1: item.netIncomeYoyPrev1,
      netIncomeYoyPrev2: item.netIncomeYoyPrev2,
      netIncomeQoqLatest: item.netIncomeQoqLatest,
      revenueYoyLatest: item.revenueYoyLatest,
      revenueQoqLatest: item.revenueQoqLatest,
      roeYoyLatest: item.roeYoyLatest,
      roeQoqLatest: item.roeQoqLatest,
    }));
  } catch (error) {
    console.error("Failed to fetch fundamental metrics data:", error);
    state.metrics.total = 0;
    state.metrics.items = [];
  }

  if (activeTab === "financialStats") {
    renderFinancialStatsTable(state.metrics.items);
    updatePaginationControls();
  }
}

elements.langButtons.forEach((btn) =>
  btn.addEventListener("click", () => updateLanguage(btn.dataset.lang))
);

if (elements.applyButton) {
  elements.applyButton.addEventListener("click", () => {
    loadTradingData(1);
    if (activeTab === "financialStats") {
      loadFinancialStats(1);
    }
  });
}

function resetFilters() {
  if (elements.keywordInput) {
    elements.keywordInput.value = "";
  }
  if (elements.marketSelect) {
    elements.marketSelect.value = "all";
  }
  if (elements.exchangeSelect) {
    elements.exchangeSelect.value = "all";
  }
  if (elements.searchBox) {
    elements.searchBox.value = "";
  }
}

if (elements.resetButton) {
  elements.resetButton.addEventListener("click", () => {
    resetFilters();
    loadTradingData(1);
    if (activeTab === "financialStats") {
      loadFinancialStats(1);
    }
  });
}

elements.tabs.forEach((tab) =>
  tab.addEventListener("click", () => setActiveTab(tab.dataset.tab))
);

if (elements.prevPage) {
  elements.prevPage.addEventListener("click", () => {
    if (activeTab === "financialStats") {
      if (state.metrics.page > 1) {
        loadFinancialStats(state.metrics.page - 1);
      }
    } else if (state.trading.page > 1) {
      loadTradingData(state.trading.page - 1);
    }
  });
}

if (elements.nextPage) {
  elements.nextPage.addEventListener("click", () => {
    const currentState = activeTab === "financialStats" ? state.metrics : state.trading;
    const totalPages = Math.max(1, Math.ceil(currentState.total / PAGE_SIZE));

    if (currentState.page < totalPages) {
      if (activeTab === "financialStats") {
        loadFinancialStats(currentState.page + 1);
      } else {
        loadTradingData(currentState.page + 1);
      }
    }
  });
}

if (elements.searchBox) {
  elements.searchBox.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      if (elements.keywordInput) {
        elements.keywordInput.value = event.target.value.trim();
      }
      loadTradingData(1);
      if (activeTab === "financialStats") {
        loadFinancialStats(1);
      }
    }
  });
}

setActiveTab("tradingData");
updateLanguage(currentLang);
loadTradingData(1);
