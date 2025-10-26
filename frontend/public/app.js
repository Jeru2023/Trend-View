import { tradingDataTab } from "./tabs/trading-data.js";
import { financialDataTab } from "./tabs/financial-data.js";
import { tradingStatsTab } from "./tabs/trading-stats.js";
import { financialStatsTab } from "./tabs/financial-stats.js";

const TAB_MODULES = [tradingDataTab, financialDataTab, tradingStatsTab, financialStatsTab];

const translations = getTranslations("basicInfo");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const PAGE_SIZE = 20;
const LANG_STORAGE_KEY = "trend-view-lang";
const EMPTY_VALUE = "--";

const exchangeLabels = {
  en: { SSE: "SSE", SZSE: "SZSE", BSE: "BSE" },
  zh: { SSE: "上交所", SZSE: "深交所", BSE: "北交所" },
};

const marketLabels = {
  en: {
    主板: "Main Board",
    创业板: "ChiNext",
    科创板: "STAR Market",
    "Main Board": "Main Board",
    ChiNext: "ChiNext",
    "STAR Market": "STAR Market",
  },
  zh: {
    主板: "主板",
    创业板: "创业板",
    科创板: "科创板",
    "Main Board": "主板",
    ChiNext: "创业板",
    "STAR Market": "科创板",
  },
};

let currentLang = getInitialLanguage();
let activeTab = "tradingData";

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

const elements = {
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

const tabRegistry = TAB_MODULES.reduce((registry, module) => {
  registry[module.id] = {
    ...module,
    container: document.querySelector(`[data-tab-panel="${module.id}"]`),
    body: null,
    isLoaded: false,
    loadingPromise: null,
  };
  return registry;
}, {});

initialize().catch((error) => console.error("Failed to initialize basic info page:", error));

async function initialize() {
  bindEvents();
  await updateLanguage(currentLang);
  await setActiveTab(activeTab, { force: true });
  await loadTradingData(1);
}

function bindEvents() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      updateLanguage(btn.dataset.lang).catch((error) => console.error("Failed to switch language:", error));
    });
  });

  if (elements.applyButton) {
    elements.applyButton.addEventListener("click", () => {
      loadTradingData(1).catch((error) => console.error("Failed to reload trading data:", error));
      if (isMetricsTabActive()) {
        loadFinancialStats(1).catch((error) =>
          console.error("Failed to reload financial stats data:", error)
        );
      }
    });
  }

  if (elements.resetButton) {
    elements.resetButton.addEventListener("click", () => {
      resetFilters();
      loadTradingData(1).catch((error) => console.error("Failed to reload trading data:", error));
      if (isMetricsTabActive()) {
        loadFinancialStats(1).catch((error) =>
          console.error("Failed to reload financial stats data:", error)
        );
      }
    });
  }

  elements.tabs.forEach((tabButton) => {
    tabButton.addEventListener("click", () => {
      const targetTab = tabButton.dataset.tab;
      setActiveTab(targetTab).catch((error) => console.error("Failed to switch tab:", error));
    });
  });

  if (elements.prevPage) {
    elements.prevPage.addEventListener("click", () => {
      if (isMetricsTabActive()) {
        if (state.metrics.page > 1) {
          loadFinancialStats(state.metrics.page - 1).catch((error) =>
            console.error("Failed to load previous financial stats page:", error)
          );
        }
      } else if (state.trading.page > 1) {
        loadTradingData(state.trading.page - 1).catch((error) =>
          console.error("Failed to load previous trading page:", error)
        );
      }
    });
  }

  if (elements.nextPage) {
    elements.nextPage.addEventListener("click", () => {
      const currentState = getActiveDataState();
      const totalPages = Math.max(1, Math.ceil(currentState.total / PAGE_SIZE));
      if (currentState.page < totalPages) {
        if (isMetricsTabActive()) {
          loadFinancialStats(currentState.page + 1).catch((error) =>
            console.error("Failed to load next financial stats page:", error)
          );
        } else {
          loadTradingData(currentState.page + 1).catch((error) =>
            console.error("Failed to load next trading page:", error)
          );
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
        loadTradingData(1).catch((error) => console.error("Failed to reload trading data:", error));
        if (isMetricsTabActive()) {
          loadFinancialStats(1).catch((error) =>
            console.error("Failed to reload financial stats data:", error)
          );
        }
      }
    });
  }
}

async function setActiveTab(tabName, { force = false } = {}) {
  if (!tabRegistry[tabName]) {
    return;
  }
  if (!force && activeTab === tabName) {
    return;
  }

  activeTab = tabName;
  elements.tabs.forEach((tab) => {
    tab.classList.toggle("tab--active", tab.dataset.tab === tabName);
  });

  Object.values(tabRegistry).forEach((tab) => {
    if (tab.container) {
      tab.container.classList.toggle("hidden", tab.id !== tabName);
    }
  });

  if (tabName === "financialStats" && !state.metrics.items.length) {
    await loadFinancialStats(1);
    return;
  }

  await renderActiveTab();
  updatePaginationControls();
}

async function updateLanguage(lang) {
  if (!translations[lang]) {
    return;
  }
  persistLanguage(lang);
  currentLang = lang;

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === lang);
  });

  applyTranslations();
  await renderActiveTab();
  updatePaginationControls();
}

async function renderActiveTab() {
  const tab = tabRegistry[activeTab];
  if (!tab || typeof tab.render !== "function") {
    return;
  }

  await ensureTabReady(tab.id);
  const context = createTabContext(tab);
  const data = tab.dataSource === "metrics" ? state.metrics.items : state.trading.items;
  tab.render(data, context);
}

function createTabContext(tab) {
  const body = resolveTabBody(tab);
  return {
    body,
    emptyValue: EMPTY_VALUE,
    formatNumber,
    formatOptionalNumber,
    formatOptionalDate,
    formatPercent,
    formatFinancialPercent,
    getTrendClass,
    renderEmptyRow,
    getMarketLabel,
    getExchangeLabel,
  };
}

async function ensureTabReady(tabId) {
  const tab = tabRegistry[tabId];
  if (!tab) {
    return;
  }
  if (tab.isLoaded) {
    resolveTabBody(tab);
    return;
  }
  if (!tab.container) {
    tab.container = document.querySelector(`[data-tab-panel="${tabId}"]`);
  }
  if (!tab.container) {
    return;
  }

  if (!tab.loadingPromise) {
    tab.loadingPromise = (async () => {
      try {
        const response = await fetch(tab.template, { cache: "no-cache" });
        if (!response.ok) {
          throw new Error(`Failed to load template for ${tabId}`);
        }
        const markup = await response.text();
        tab.container.innerHTML = markup;
        tab.isLoaded = true;
        tab.body = tab.container.querySelector(`[data-tab-body="${tabId}"]`);
        applyTranslations();
      } catch (error) {
        console.error(error);
        tab.container.innerHTML = `<div class="tab-error">${translations[currentLang].noData}</div>`;
      } finally {
        tab.loadingPromise = null;
      }
    })();
  }

  await tab.loadingPromise;
}

function resolveTabBody(tab) {
  if (tab.body && tab.body.isConnected) {
    return tab.body;
  }
  if (tab.container) {
    tab.body = tab.container.querySelector(`[data-tab-body="${tab.id}"]`);
  }
  return tab.body;
}

function isMetricsTabActive() {
  return tabRegistry[activeTab]?.dataSource === "metrics";
}

function getActiveDataState() {
  return isMetricsTabActive() ? state.metrics : state.trading;
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

  if (!isMetricsTabActive()) {
    await renderActiveTab();
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
    const response = await fetch(`${API_BASE}/fundamental-metrics?${params.toString()}`);
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

  if (isMetricsTabActive()) {
    await renderActiveTab();
    updatePaginationControls();
  }
}

function collectFilters() {
  return {
    keyword: elements.keywordInput?.value?.trim() || "",
    market: elements.marketSelect?.value || "all",
    exchange: elements.exchangeSelect?.value || "all",
  };
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

function updatePaginationControls() {
  const currentState = getActiveDataState();
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

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.title = dict.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      el.innerHTML = value;
    }
  });

  document.querySelectorAll("[data-placeholder-en]").forEach((el) => {
    const placeholder = el.dataset[`placeholder${currentLang.toUpperCase()}`];
    if (typeof placeholder === "string") {
      el.placeholder = placeholder;
    }
  });
}

function formatNumber(value) {
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 2 }).format(value ?? 0);
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

function renderEmptyRow(body, colSpan) {
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

function getMarketLabel(value) {
  const map = marketLabels[currentLang] || {};
  return value && map[value] ? map[value] : value ?? EMPTY_VALUE;
}

function getExchangeLabel(value) {
  const map = exchangeLabels[currentLang] || {};
  return value && map[value] ? map[value] : value ?? EMPTY_VALUE;
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

window.applyTranslations = applyTranslations;
