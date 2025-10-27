import { tradingDataTab } from "./tabs/trading-data.js";
import { financialDataTab } from "./tabs/financial-data.js";
import { tradingStatsTab } from "./tabs/trading-stats.js";
import { financialStatsTab } from "./tabs/financial-stats.js";

const TAB_MODULES = [tradingDataTab, financialDataTab, tradingStatsTab, financialStatsTab];

const translations = getTranslations("marketIntelligence");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const PAGE_SIZE = 20;
const LANG_STORAGE_KEY = "trend-view-lang";
const EMPTY_VALUE = "--";
const FAVORITES_QUERY_PARAM = "favoritesOnly";
const FAVORITES_GROUP_QUERY_KEY = "favoriteGroup";
const FAVORITES_GROUP_ALL = "all";
const FAVORITES_GROUP_NONE = "__ungrouped__";
const DEFAULT_FILTERS = {
  keyword: "",
  market: "all",
  exchange: "all",
  volumeSpikeMin: 1.8,
  peMin: 0,
  roeMin: 3,
  netIncomeQoqMin: 0,
  netIncomeYoyMinPercent: 10,
};

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

function createDefaultFilterState() {
  return {
    keyword: DEFAULT_FILTERS.keyword,
    market: DEFAULT_FILTERS.market,
    exchange: DEFAULT_FILTERS.exchange,
    volumeSpikeMin: DEFAULT_FILTERS.volumeSpikeMin,
    peMin: DEFAULT_FILTERS.peMin,
    roeMin: DEFAULT_FILTERS.roeMin,
    netIncomeQoqMin: DEFAULT_FILTERS.netIncomeQoqMin,
    netIncomeYoyMin: DEFAULT_FILTERS.netIncomeYoyMinPercent / 100,
  };
}

let currentLang = getInitialLanguage();
let activeTab = "tradingData";

const state = {
  favoritesOnly: false,
  favoriteGroup: FAVORITES_GROUP_ALL,
  trading: {
    page: 1,
    total: 0,
    items: [],
    filters: createDefaultFilterState(),
  },
  metrics: {
    page: 1,
    total: 0,
    items: [],
    filters: createDefaultFilterState(),
  },
};

const favoriteGroupsState = {
  items: [],
  loaded: false,
  loading: null,
};

const elements = {
  tabs: document.querySelectorAll(".tab"),
  langButtons: document.querySelectorAll(".lang-btn"),
  searchBox: document.querySelector(".search-box"),
  keywordInput: document.getElementById("keyword"),
  marketSelect: document.getElementById("market"),
  exchangeSelect: document.getElementById("exchange"),
  volumeSpikeInput: document.getElementById("volume-spike-min"),
  peMinInput: document.getElementById("pe-min"),
  roeMinInput: document.getElementById("roe-min"),
  netIncomeQoqInput: document.getElementById("net-income-qoq-min"),
  netIncomeYoyInput: document.getElementById("net-income-yoy-min"),
  applyButton: document.getElementById("apply-filters"),
  resetButton: document.getElementById("reset-filters"),
  prevPage: document.getElementById("prev-page"),
  nextPage: document.getElementById("next-page"),
  pageInfo: document.getElementById("page-info"),
  favoritesBanner: document.getElementById("favorites-banner"),
  favoritesBannerGroup: document.getElementById("favorites-banner-group"),
  favoritesGroupSelect: document.getElementById("favorites-group-filter"),
  favoritesBannerClear: document.getElementById("favorites-banner-clear"),
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

function setNumericFilterInputs(filters) {
  if (elements.volumeSpikeInput) {
    elements.volumeSpikeInput.value = String(
      filters.volumeSpikeMin ?? DEFAULT_FILTERS.volumeSpikeMin
    );
  }
  if (elements.peMinInput) {
    elements.peMinInput.value = String(filters.peMin ?? DEFAULT_FILTERS.peMin);
  }
  if (elements.roeMinInput) {
    elements.roeMinInput.value = String(filters.roeMin ?? DEFAULT_FILTERS.roeMin);
  }
  if (elements.netIncomeQoqInput) {
    elements.netIncomeQoqInput.value = String(
      filters.netIncomeQoqMin ?? DEFAULT_FILTERS.netIncomeQoqMin
    );
  }
  if (elements.netIncomeYoyInput) {
    const ratio = filters.netIncomeYoyMin ?? (DEFAULT_FILTERS.netIncomeYoyMinPercent / 100);
    elements.netIncomeYoyInput.value = String((ratio * 100).toFixed(2).replace(/\.?0+$/, ""));
  }
}

function parseFavoritesFlag(value) {
  if (!value) {
    return false;
  }
  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "y"].includes(normalized);
}

function parseFavoriteGroupParam(value) {
  if (value === null || value === undefined) {
    return { value: FAVORITES_GROUP_ALL, specified: false };
  }
  const trimmed = String(value).trim();
  if (!trimmed) {
    return { value: FAVORITES_GROUP_NONE, specified: true };
  }
  if (trimmed === FAVORITES_GROUP_ALL) {
    return { value: FAVORITES_GROUP_ALL, specified: true };
  }
  if (trimmed === FAVORITES_GROUP_NONE) {
    return { value: FAVORITES_GROUP_NONE, specified: true };
  }
  return { value: trimmed, specified: true };
}

function normalizeFavoriteGroupForRequest(value) {
  if (!value || value === FAVORITES_GROUP_ALL) {
    return null;
  }
  if (value === FAVORITES_GROUP_NONE) {
    return FAVORITES_GROUP_NONE;
  }
  return value;
}

function getFavoriteGroupLabel(value) {
  const dict = translations[currentLang] || {};
  if (
    value === FAVORITES_GROUP_NONE ||
    value === null ||
    value === undefined ||
    value === ""
  ) {
    return dict.favoriteGroupNone || "Ungrouped";
  }
  if (value === FAVORITES_GROUP_ALL) {
    return dict.favoriteGroupAll || "All groups";
  }
  return value;
}

async function ensureFavoriteGroupsLoaded(force = false) {
  if (!force && favoriteGroupsState.loaded) {
    return false;
  }
  if (favoriteGroupsState.loading) {
    return favoriteGroupsState.loading;
  }
  favoriteGroupsState.loading = (async () => {
    try {
      const response = await fetch(`${API_BASE}/favorites/groups`);
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const data = await response.json();
      const rawItems = Array.isArray(data?.items) ? data.items : [];
      favoriteGroupsState.items = rawItems.map((entry) => {
        const name = entry?.name ?? null;
        const value = name === null ? FAVORITES_GROUP_NONE : String(name);
        const total = Number(entry?.total ?? 0);
        return { name, value, total };
      });
      favoriteGroupsState.loaded = true;
      const availableValues = new Set(
        favoriteGroupsState.items.map((entry) => entry.value)
      );
      let stateChanged = false;
      if (
        state.favoriteGroup !== FAVORITES_GROUP_ALL &&
        !availableValues.has(state.favoriteGroup)
      ) {
        state.favoriteGroup = FAVORITES_GROUP_ALL;
        stateChanged = true;
      }
      renderFavoriteGroupOptions();
      updateFavoritesUI();
      return stateChanged;
    } catch (error) {
      console.error("Failed to load favorite groups:", error);
      favoriteGroupsState.loaded = false;
      favoriteGroupsState.items = [];
      renderFavoriteGroupOptions();
      updateFavoritesUI();
      return false;
    } finally {
      favoriteGroupsState.loading = null;
    }
  })();
  return favoriteGroupsState.loading;
}

function renderFavoriteGroupOptions() {
  if (!elements.favoritesGroupSelect) {
    return;
  }
  const select = elements.favoritesGroupSelect;
  const dict = translations[currentLang] || {};
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  const formatter = new Intl.NumberFormat(locale, { maximumFractionDigits: 0 });

  const valueMap = new Map();
  (favoriteGroupsState.items || []).forEach((entry) => {
    valueMap.set(entry.value, entry);
  });
  if (
    state.favoriteGroup !== FAVORITES_GROUP_ALL &&
    !valueMap.has(state.favoriteGroup)
  ) {
    valueMap.set(state.favoriteGroup, {
      value: state.favoriteGroup,
      name: state.favoriteGroup === FAVORITES_GROUP_NONE ? null : state.favoriteGroup,
      total: 0,
    });
  }

  const options = [];
  options.push({
    value: FAVORITES_GROUP_ALL,
    label: dict.favoriteGroupAll || "All groups",
  });

  const groupedValues = Array.from(valueMap.values());
  const ungroupedEntry = groupedValues.find(
    (entry) => entry.value === FAVORITES_GROUP_NONE
  );
  if (ungroupedEntry) {
    const baseLabel = dict.favoriteGroupNone || "Ungrouped";
    options.push({
      value: FAVORITES_GROUP_NONE,
      label: `${baseLabel} (${formatter.format(ungroupedEntry.total || 0)})`,
    });
  }

  groupedValues
    .filter((entry) => entry.value !== FAVORITES_GROUP_NONE)
    .sort((a, b) => a.value.localeCompare(b.value))
    .forEach((entry) => {
      const label = `${entry.value} (${formatter.format(entry.total || 0)})`;
      options.push({
        value: entry.value,
        label,
      });
    });

  select.innerHTML = "";
  options.forEach((option) => {
    const node = document.createElement("option");
    node.value = option.value;
    node.textContent = option.label;
    select.appendChild(node);
  });

  if (!options.some((option) => option.value === state.favoriteGroup)) {
    state.favoriteGroup = FAVORITES_GROUP_ALL;
  }
  select.value = state.favoriteGroup;
}

function persistFavoritesQueryParam() {
  const url = new URL(window.location.href);
  if (state.favoritesOnly) {
    url.searchParams.set(FAVORITES_QUERY_PARAM, "1");
    const groupParam = normalizeFavoriteGroupForRequest(state.favoriteGroup);
    if (groupParam) {
      url.searchParams.set(FAVORITES_GROUP_QUERY_KEY, groupParam);
    } else {
      url.searchParams.delete(FAVORITES_GROUP_QUERY_KEY);
    }
  } else {
    url.searchParams.delete(FAVORITES_QUERY_PARAM);
    url.searchParams.delete(FAVORITES_GROUP_QUERY_KEY);
  }
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
}

function updateFavoritesUI() {
  const shouldShowBanner = state.favoritesOnly;
  if (elements.favoritesBanner) {
    elements.favoritesBanner.classList.toggle("hidden", !shouldShowBanner);
  }
  if (elements.favoritesBannerClear) {
    elements.favoritesBannerClear.disabled = !shouldShowBanner;
  }
  if (elements.favoritesBannerGroup) {
    elements.favoritesBannerGroup.classList.toggle("hidden", !shouldShowBanner);
  }
  if (elements.favoritesGroupSelect) {
    const select = elements.favoritesGroupSelect;
    const hasMatchingOption = Array.from(select.options || []).some(
      (option) => option.value === state.favoriteGroup
    );
    const effectiveValue = hasMatchingOption ? state.favoriteGroup : FAVORITES_GROUP_ALL;
    if (select.value !== effectiveValue) {
      select.value = effectiveValue;
    }
    select.disabled = !shouldShowBanner || !favoriteGroupsState.loaded;
  }
  const activeNavKey = state.favoritesOnly ? "portfolio" : "market-intelligence";
  if (document.body) {
    document.body.setAttribute("data-active-nav", activeNavKey);
  }
  const sidebarRoot = document.querySelector("[data-sidebar-container] .sidebar");
  if (sidebarRoot) {
    sidebarRoot.querySelectorAll(".nav__item").forEach((item) => {
      const key = item.dataset.navKey;
      item.classList.toggle("nav__item--active", key === activeNavKey);
    });
  }
}

function determineInitialFavoritesMode() {
  const params = new URLSearchParams(window.location.search);
  const favoritesFlag = parseFavoritesFlag(params.get(FAVORITES_QUERY_PARAM));
  const { value: initialGroup, specified } = parseFavoriteGroupParam(
    params.get(FAVORITES_GROUP_QUERY_KEY)
  );
  state.favoriteGroup = initialGroup;
  state.favoritesOnly = favoritesFlag || specified;
  renderFavoriteGroupOptions();
  updateFavoritesUI();
  persistFavoritesQueryParam();
}

function setFavoritesMode(enabled) {
  const previous = state.favoritesOnly;
  const next = Boolean(enabled);
  if (previous === next) {
    return;
  }
  state.favoritesOnly = next;
  if (!next) {
    state.favoriteGroup = FAVORITES_GROUP_ALL;
  } else if (!previous) {
    favoriteGroupsState.loaded = false;
  }
  renderFavoriteGroupOptions();
  updateFavoritesUI();
  persistFavoritesQueryParam();
}

function parseNumericInput(element, fallback) {
  if (!element) {
    return fallback;
  }
  const value = parseFloat(element.value);
  return Number.isFinite(value) ? value : fallback;
}

function syncMetricsFromTrading() {
  state.metrics.page = state.trading.page;
  state.metrics.total = state.trading.total;
  state.metrics.items = state.trading.items;
  state.metrics.filters = { ...state.trading.filters };
}

initialize().catch((error) => console.error("Failed to initialize basic info page:", error));

async function initialize() {
  bindEvents();
  setNumericFilterInputs(state.trading.filters);
  determineInitialFavoritesMode();
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
    });
  }

  if (elements.resetButton) {
    elements.resetButton.addEventListener("click", () => {
      resetFilters();
      loadTradingData(1).catch((error) => console.error("Failed to reload trading data:", error));
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
        if (state.trading.page > 1) {
          loadTradingData(state.trading.page - 1).catch((error) =>
            console.error("Failed to load previous trading page:", error)
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
        loadTradingData(state.trading.page + 1).catch((error) =>
          console.error("Failed to load next trading page:", error)
        );
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
      }
    });
  }

  if (elements.favoritesBannerClear) {
    elements.favoritesBannerClear.addEventListener("click", () => {
      if (!state.favoritesOnly) {
        return;
      }
      setFavoritesMode(false);
      loadTradingData(1).catch((error) => console.error("Failed to reload trading data:", error));
    });
  }

  if (elements.favoritesGroupSelect) {
    elements.favoritesGroupSelect.addEventListener("change", (event) => {
      const selectedValue = event.target.value || FAVORITES_GROUP_ALL;
      if (!state.favoritesOnly) {
        setFavoritesMode(true);
      }
      if (state.favoriteGroup === selectedValue) {
        return;
      }
      state.favoriteGroup = selectedValue;
      updateFavoritesUI();
      persistFavoritesQueryParam();
      loadTradingData(1).catch((error) =>
        console.error("Failed to reload trading data:", error)
      );
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

  if (tabName === "financialStats") {
    await loadFinancialStats();
  } else {
    await renderActiveTab();
    updatePaginationControls();
  }
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
    favoriteLabel: translations[currentLang]?.favoritesBadgeLabel ?? "",
    isFavoritesMode: state.favoritesOnly,
    getFavoriteGroupLabel,
    favoritesGroupNone: FAVORITES_GROUP_NONE,
    favoritesGroupAll: FAVORITES_GROUP_ALL,
    currentFavoriteGroup: state.favoriteGroup,
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
  let targetPage = page;
  if (state.favoritesOnly) {
    try {
      const groupsChanged = await ensureFavoriteGroupsLoaded();
      if (groupsChanged) {
        targetPage = 1;
        persistFavoritesQueryParam();
      }
    } catch (error) {
      console.error("Failed to refresh favorite groups:", error);
    }
  }

  state.trading.page = targetPage;
  state.trading.filters = collectFilters();

  const params = new URLSearchParams();
  params.set("limit", PAGE_SIZE.toString());
  params.set("offset", ((state.trading.page - 1) * PAGE_SIZE).toString());

  const filters = state.trading.filters;
  if (filters.keyword) params.set("keyword", filters.keyword);
  if (filters.market && filters.market !== "all") params.set("market", filters.market);
  if (filters.exchange && filters.exchange !== "all") params.set("exchange", filters.exchange);
  params.set("volumeSpikeMin", filters.volumeSpikeMin.toString());
  params.set("peMin", filters.peMin.toString());
  params.set("roeMin", filters.roeMin.toString());
  params.set("netIncomeQoqMin", filters.netIncomeQoqMin.toString());
  params.set("netIncomeYoyMin", filters.netIncomeYoyMin.toString());
  if (state.favoritesOnly) {
    params.set("favoritesOnly", "true");
    const groupParam = normalizeFavoriteGroupForRequest(state.favoriteGroup);
    if (groupParam) {
      params.set("favoriteGroup", groupParam);
    }
  }

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
        volume_spike: item.volumeSpike,
        ann_date: item.annDate,
        end_date: item.endDate,
        reportingPeriod: item.endDate,
        basic_eps: item.basicEps,
        revenue: item.revenue,
        operate_profit: item.operateProfit,
        net_income: item.netIncome,
        gross_margin: item.grossMargin,
        roe: item.roe,
        net_income_yoy_latest: item.netIncomeYoyLatest,
        net_income_yoy_prev1: item.netIncomeYoyPrev1,
        net_income_yoy_prev2: item.netIncomeYoyPrev2,
        net_income_qoq_latest: item.netIncomeQoqLatest,
        revenue_yoy_latest: item.revenueYoyLatest,
        revenue_qoq_latest: item.revenueQoqLatest,
        roe_yoy_latest: item.roeYoyLatest,
        roe_qoq_latest: item.roeQoqLatest,
        netIncomeYoyLatest: item.netIncomeYoyLatest,
        netIncomeYoyPrev1: item.netIncomeYoyPrev1,
        netIncomeYoyPrev2: item.netIncomeYoyPrev2,
        netIncomeQoqLatest: item.netIncomeQoqLatest,
        revenueYoyLatest: item.revenueYoyLatest,
        revenueQoqLatest: item.revenueQoqLatest,
        roeYoyLatest: item.roeYoyLatest,
        roeQoqLatest: item.roeQoqLatest,
        favorite_group: item.favoriteGroup,
        favoriteGroup: item.favoriteGroup,
        isFavorite: Boolean(item.isFavorite),
      }));
    syncMetricsFromTrading();
  } catch (error) {
    console.error("Failed to fetch stock data:", error);
    state.trading.total = 0;
    state.trading.items = [];
    syncMetricsFromTrading();
  }

  await renderActiveTab();
  updatePaginationControls();
}

async function loadFinancialStats() {
  syncMetricsFromTrading();
  if (isMetricsTabActive()) {
    await renderActiveTab();
    updatePaginationControls();
  }
}

function collectFilters() {
  const keyword = elements.keywordInput?.value?.trim() || "";
  const market = elements.marketSelect?.value || "all";
  const exchange = elements.exchangeSelect?.value || "all";
  const volumeSpikeMin = parseNumericInput(
    elements.volumeSpikeInput,
    DEFAULT_FILTERS.volumeSpikeMin
  );
  const peMin = parseNumericInput(elements.peMinInput, DEFAULT_FILTERS.peMin);
  const roeMin = parseNumericInput(elements.roeMinInput, DEFAULT_FILTERS.roeMin);
  const netIncomeQoqMin = parseNumericInput(
    elements.netIncomeQoqInput,
    DEFAULT_FILTERS.netIncomeQoqMin
  );
  const netIncomeYoyPercent = parseNumericInput(
    elements.netIncomeYoyInput,
    DEFAULT_FILTERS.netIncomeYoyMinPercent
  );

  return {
    keyword,
    market,
    exchange,
    volumeSpikeMin,
    peMin,
    roeMin,
    netIncomeQoqMin,
    netIncomeYoyMin: netIncomeYoyPercent / 100,
  };
}

function resetFilters() {
  if (elements.keywordInput) {
    elements.keywordInput.value = DEFAULT_FILTERS.keyword;
  }
  if (elements.marketSelect) {
    elements.marketSelect.value = DEFAULT_FILTERS.market;
  }
  if (elements.exchangeSelect) {
    elements.exchangeSelect.value = DEFAULT_FILTERS.exchange;
  }
  setNumericFilterInputs(createDefaultFilterState());
  if (elements.searchBox) {
    elements.searchBox.value = DEFAULT_FILTERS.keyword;
  }
  state.trading.filters = createDefaultFilterState();
  state.metrics.filters = createDefaultFilterState();
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

  renderFavoriteGroupOptions();
  updateFavoritesUI();
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


