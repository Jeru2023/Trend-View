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
const MARKET_CAP_UNIT = 100000000;
const LANG_STORAGE_KEY = "trend-view-lang";
const EMPTY_VALUE = "--";
const FAVORITES_QUERY_PARAM = "favoritesOnly";
const FAVORITES_GROUP_QUERY_KEY = "favoriteGroup";
const FAVORITES_GROUP_ALL = "all";
const FAVORITES_GROUP_NONE = "__ungrouped__";
const DEFAULT_FILTERS = {
  market: "all",
  industry: "all",
  marketCapMin: null,
  marketCapMax: null,
  volumeSpikeMin: 1.8,
  peMin: 0,
  peMax: null,
  roeMin: 3,
  netIncomeQoqMin: 0,
  netIncomeYoyMinPercent: 10,
};

function normalizeFavoriteGroupValue(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  return value;
}

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
    market: DEFAULT_FILTERS.market,
    industry: DEFAULT_FILTERS.industry,
    marketCapMin: DEFAULT_FILTERS.marketCapMin,
    marketCapMax: DEFAULT_FILTERS.marketCapMax,
    volumeSpikeMin: DEFAULT_FILTERS.volumeSpikeMin,
    peMin: DEFAULT_FILTERS.peMin,
    peMax: DEFAULT_FILTERS.peMax,
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

let industryOptions = [];

const elements = {
  tabs: document.querySelectorAll(".tab"),
  langButtons: document.querySelectorAll(".lang-btn"),
  searchBox: document.querySelector(".search-box"),
  contentHeader: document.querySelector(".content__header"),
  filtersSection: document.querySelector(".filters"),
  pageTitle: document.querySelector("[data-page-title]") || document.querySelector("[data-i18n='pageTitle']"),
  pageSubtitle: document.querySelector("[data-page-subtitle]") || document.querySelector("[data-i18n='pageSubtitle']"),
  marketSelect: document.getElementById("market"),
  industrySelect: document.getElementById("industry"),
  marketCapMinInput: document.getElementById("market-cap-min"),
  marketCapMaxInput: document.getElementById("market-cap-max"),
  volumeSpikeInput: document.getElementById("volume-spike-min"),
  peMinInput: document.getElementById("pe-min"),
  peMaxInput: document.getElementById("pe-max"),
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
  favoritesGroupTags: document.getElementById("favorites-group-tags"),
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
  if (elements.marketCapMinInput) {
    if (filters.marketCapMin !== null && filters.marketCapMin !== undefined) {
      elements.marketCapMinInput.value = formatNumberForInput(
        filters.marketCapMin / MARKET_CAP_UNIT,
        2
      );
    } else {
      elements.marketCapMinInput.value = "";
    }
  }
  if (elements.marketCapMaxInput) {
    if (filters.marketCapMax !== null && filters.marketCapMax !== undefined) {
      elements.marketCapMaxInput.value = formatNumberForInput(
        filters.marketCapMax / MARKET_CAP_UNIT,
        2
      );
    } else {
      elements.marketCapMaxInput.value = "";
    }
  }
  if (elements.volumeSpikeInput) {
    elements.volumeSpikeInput.value = formatNumberForInput(
      filters.volumeSpikeMin ?? DEFAULT_FILTERS.volumeSpikeMin,
      2
    );
  }
  if (elements.peMinInput) {
    elements.peMinInput.value = formatNumberForInput(
      filters.peMin ?? DEFAULT_FILTERS.peMin,
      2
    );
  }
  if (elements.peMaxInput) {
    if (filters.peMax !== null && filters.peMax !== undefined) {
      elements.peMaxInput.value = formatNumberForInput(filters.peMax, 2);
    } else {
      elements.peMaxInput.value = "";
    }
  }
  if (elements.roeMinInput) {
    elements.roeMinInput.value = formatNumberForInput(
      filters.roeMin ?? DEFAULT_FILTERS.roeMin,
      2
    );
  }
  if (elements.netIncomeQoqInput) {
    elements.netIncomeQoqInput.value = formatNumberForInput(
      filters.netIncomeQoqMin ?? DEFAULT_FILTERS.netIncomeQoqMin,
      2
    );
  }
  if (elements.netIncomeYoyInput) {
    const ratio =
      filters.netIncomeYoyMin ?? DEFAULT_FILTERS.netIncomeYoyMinPercent / 100;
    elements.netIncomeYoyInput.value = formatNumberForInput(ratio * 100, 2);
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
  const normalized = normalizeFavoriteGroupValue(value);
  if (!normalized || normalized === FAVORITES_GROUP_ALL) {
    return null;
  }
  if (normalized === FAVORITES_GROUP_NONE) {
    return FAVORITES_GROUP_NONE;
  }
  return normalized;
}

function getFavoriteGroupLabel(value) {
  const dict = translations[currentLang] || {};
  const normalized =
    typeof value === "string" ? value.trim() : value;
  if (
    normalized === FAVORITES_GROUP_NONE ||
    normalized === null ||
    normalized === undefined ||
    normalized === ""
  ) {
    return dict.favoriteGroupNone || "Ungrouped";
  }
  if (normalized === FAVORITES_GROUP_ALL) {
    return dict.favoriteGroupAll || "All groups";
  }
  return normalized;
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
        const normalizedName = normalizeFavoriteGroupValue(entry?.name ?? null);
        const value =
          normalizedName === null ? FAVORITES_GROUP_NONE : String(normalizedName);
        const total = Number(entry?.total ?? 0);
        return { name: normalizedName, value, total };
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
  if (!elements.favoritesGroupTags) {
    return;
  }
  const container = elements.favoritesGroupTags;
  const dict = translations[currentLang] || {};
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  const formatter = new Intl.NumberFormat(locale, { maximumFractionDigits: 0 });

  const groupedValues = Array.isArray(favoriteGroupsState.items)
    ? favoriteGroupsState.items.slice()
    : [];

  const totals = groupedValues.reduce(
    (acc, entry) => acc + Number(entry.total ?? 0),
    0
  );

  const options = [];
  options.push({
    value: FAVORITES_GROUP_ALL,
    label: `${dict.favoriteGroupAll || "All groups"} (${formatter.format(totals)})`,
  });

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

  if (!options.some((option) => option.value === state.favoriteGroup)) {
    state.favoriteGroup = FAVORITES_GROUP_ALL;
  }

  container.innerHTML = "";
  options.forEach((option) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "favorite-group-tag";
    button.dataset.groupValue = option.value;
    button.setAttribute("role", "tab");
    button.setAttribute(
      "aria-selected",
      option.value === state.favoriteGroup ? "true" : "false"
    );
    button.textContent = option.label;
    if (option.value === state.favoriteGroup) {
      button.classList.add("favorite-group-tag--active");
    }
    container.appendChild(button);
  });
  updateFavoriteGroupTagSelection();
}

function updateFavoriteGroupTagSelection() {
  if (!elements.favoritesGroupTags) {
    return;
  }
  const tags = elements.favoritesGroupTags.querySelectorAll("[data-group-value]");
  tags.forEach((tag) => {
    const value = tag.dataset.groupValue || FAVORITES_GROUP_ALL;
    const isActive = value === state.favoriteGroup;
    tag.classList.toggle("favorite-group-tag--active", isActive);
    tag.setAttribute("aria-selected", isActive ? "true" : "false");
  });
}

function renderIndustryOptions(options = industryOptions) {
  if (!elements.industrySelect) {
    return;
  }
  const select = elements.industrySelect;
  const normalized = Array.isArray(options)
    ? options
        .map((value) => (typeof value === "string" ? value.trim() : ""))
        .filter(Boolean)
    : [];
  const uniqueValues = Array.from(new Set(normalized));
  const locale = currentLang === "zh" ? "zh-CN" : "en";
  uniqueValues.sort((a, b) => a.localeCompare(b, locale, { sensitivity: "base" }));

  const currentFilterValue =
    state.trading.filters?.industry && state.trading.filters.industry !== ""
      ? state.trading.filters.industry
      : select.value || DEFAULT_FILTERS.industry;

  if (
    currentFilterValue &&
    currentFilterValue !== "all" &&
    !uniqueValues.includes(currentFilterValue)
  ) {
    uniqueValues.unshift(currentFilterValue);
  }

  industryOptions = uniqueValues;

  while (select.options.length > 1) {
    select.remove(1);
  }

  industryOptions.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });

  if (
    currentFilterValue &&
    currentFilterValue !== "all" &&
    industryOptions.includes(currentFilterValue)
  ) {
    select.value = currentFilterValue;
  } else {
    select.value = DEFAULT_FILTERS.industry;
  }
}

function updateSearchAreaVisibility() {
  const shouldHide = state.favoritesOnly;
  if (elements.contentHeader) {
    elements.contentHeader.classList.toggle("hidden", shouldHide);
    elements.contentHeader.setAttribute("aria-hidden", shouldHide ? "true" : "false");
  }
  if (elements.filtersSection) {
    elements.filtersSection.classList.toggle("hidden", shouldHide);
    elements.filtersSection.setAttribute("aria-hidden", shouldHide ? "true" : "false");
  }
}

function updateHeaderTexts() {
  const dict = translations[currentLang] || {};
  if (elements.pageTitle) {
    const titleKey = state.favoritesOnly ? "portfolioTitle" : "pageTitle";
    const titleText = dict[titleKey] || dict.pageTitle || elements.pageTitle.textContent;
    elements.pageTitle.textContent = titleText;
  }
  if (elements.pageSubtitle) {
    const subtitleKey = state.favoritesOnly ? "portfolioSubtitle" : "pageSubtitle";
    const subtitleText = (dict[subtitleKey] ?? "").trim();
    elements.pageSubtitle.textContent = subtitleText;
    const hasText = subtitleText.length > 0;
    elements.pageSubtitle.classList.toggle("hidden", !hasText);
    elements.pageSubtitle.setAttribute("aria-hidden", hasText ? "false" : "true");
  }
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
  if (elements.favoritesBannerGroup) {
    elements.favoritesBannerGroup.classList.toggle("hidden", !shouldShowBanner);
  }
  updateFavoriteGroupTagSelection();
  updateSearchAreaVisibility();
  updateHeaderTexts();
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

function parseOptionalNumericInput(element) {
  if (!element) {
    return null;
  }
  const raw = typeof element.value === "string" ? element.value.trim() : "";
  if (!raw) {
    return null;
  }
  const value = parseFloat(raw);
  return Number.isFinite(value) ? value : null;
}

function formatNumberForInput(value, fractionDigits = 2) {
  if (value === null || value === undefined) {
    return "";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "";
  }
  if (typeof fractionDigits === "number") {
    return numeric.toFixed(fractionDigits).replace(/\.?0+$/, "");
  }
  return String(numeric);
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
        event.target.value = event.target.value.trim();
        loadTradingData(1).catch((error) => console.error("Failed to reload trading data:", error));
      }
    });
  }

  if (elements.favoritesGroupTags) {
    elements.favoritesGroupTags.addEventListener("click", (event) => {
      const target = event.target.closest("[data-group-value]");
      if (!target) {
        return;
      }
      const selectedValue = target.dataset.groupValue || FAVORITES_GROUP_ALL;
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
  if (filters.market && filters.market !== "all") {
    params.set("market", filters.market);
  }
  if (filters.industry && filters.industry !== "all") {
    params.set("industry", filters.industry);
  }
  if (filters.marketCapMin !== null && filters.marketCapMin !== undefined) {
    params.set("marketCapMin", filters.marketCapMin.toString());
  }
  if (filters.marketCapMax !== null && filters.marketCapMax !== undefined) {
    params.set("marketCapMax", filters.marketCapMax.toString());
  }
  if (Number.isFinite(filters.volumeSpikeMin)) {
    params.set("volumeSpikeMin", filters.volumeSpikeMin.toString());
  }
  if (Number.isFinite(filters.peMin)) {
    params.set("peMin", filters.peMin.toString());
  }
  if (filters.peMax !== null && filters.peMax !== undefined && Number.isFinite(filters.peMax)) {
    params.set("peMax", filters.peMax.toString());
  }
  if (Number.isFinite(filters.roeMin)) {
    params.set("roeMin", filters.roeMin.toString());
  }
  if (Number.isFinite(filters.netIncomeQoqMin)) {
    params.set("netIncomeQoqMin", filters.netIncomeQoqMin.toString());
  }
  if (Number.isFinite(filters.netIncomeYoyMin)) {
    params.set("netIncomeYoyMin", filters.netIncomeYoyMin.toString());
  }
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
    if (Array.isArray(data.industries)) {
      renderIndustryOptions(data.industries);
    } else if (!industryOptions.length) {
      renderIndustryOptions();
    }
    state.trading.total = data.total;
    state.trading.items = data.items.map((item) => {
      const favoriteGroup = normalizeFavoriteGroupValue(
        item.favoriteGroup ?? item.favorite_group ?? null
      );
      return {
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
        favorite_group: favoriteGroup,
        favoriteGroup: favoriteGroup,
        isFavorite: Boolean(item.isFavorite),
      };
    });
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
  const market = elements.marketSelect?.value || "all";
  const industry = elements.industrySelect?.value || "all";
  let marketCapMin = parseOptionalNumericInput(elements.marketCapMinInput);
  let marketCapMax = parseOptionalNumericInput(elements.marketCapMaxInput);
  if (marketCapMin !== null) {
    marketCapMin *= MARKET_CAP_UNIT;
  }
  if (marketCapMax !== null) {
    marketCapMax *= MARKET_CAP_UNIT;
  }
  if (
    marketCapMin !== null &&
    marketCapMax !== null &&
    marketCapMax < marketCapMin
  ) {
    const temp = marketCapMin;
    marketCapMin = marketCapMax;
    marketCapMax = temp;
  }

  const volumeSpikeMin = parseNumericInput(
    elements.volumeSpikeInput,
    DEFAULT_FILTERS.volumeSpikeMin
  );
  const peMin = parseNumericInput(elements.peMinInput, DEFAULT_FILTERS.peMin);
  let peMax = parseOptionalNumericInput(elements.peMaxInput);
  if (peMax !== null && peMax < peMin) {
    peMax = peMin;
  }
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
    market,
    industry,
    marketCapMin,
    marketCapMax,
    volumeSpikeMin,
    peMin,
    peMax,
    roeMin,
    netIncomeQoqMin,
    netIncomeYoyMin: netIncomeYoyPercent / 100,
  };
}

function resetFilters() {
  if (elements.marketSelect) {
    elements.marketSelect.value = DEFAULT_FILTERS.market;
  }
  if (elements.industrySelect) {
    elements.industrySelect.value = DEFAULT_FILTERS.industry;
  }
  const defaults = createDefaultFilterState();
  setNumericFilterInputs(defaults);
  if (elements.searchBox) {
    elements.searchBox.value = "";
  }
  state.trading.filters = { ...defaults };
  state.metrics.filters = { ...defaults };
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

  if (elements.favoritesGroupTags) {
    const label = dict.favoritesGroupLabel || "Group";
    elements.favoritesGroupTags.setAttribute("aria-label", label);
  }

  renderIndustryOptions();
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


