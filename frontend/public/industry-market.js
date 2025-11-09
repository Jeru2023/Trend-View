console.info("Industry market module v20270449");

const translations = getTranslations("industryMarket");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";
const DEFAULT_LOOKBACK = 180;
const VOLUME_ANALYSIS_LOOKBACK = 90;
const SEARCH_DEBOUNCE_MS = 250;
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  chartContainer: document.getElementById("industry-market-chart"),
  searchInput: document.getElementById("industry-market-search"),
  searchDropdown: document.getElementById("industry-market-search-results"),
  statusMessage: document.getElementById("industry-market-status"),
  codeLabel: document.getElementById("industry-market-code"),
  latestDate: document.getElementById("industry-market-latest-date"),
  lastSync: document.getElementById("industry-market-last-sync"),
  watchlist: document.getElementById("industry-market-watchlist"),
  tabButtons: Array.from(document.querySelectorAll(".concept-market__tab-btn")),
  tabPanels: Array.from(document.querySelectorAll(".concept-market__tab-panel")),
  reasoningOutput: document.getElementById("industry-market-reasoning"),
  reasoningUpdated: document.getElementById("industry-market-reasoning-updated"),
  newsList: document.getElementById("industry-market-news"),
  volumeOutput: document.getElementById("industry-volume-output"),
  volumeRunButton: document.getElementById("industry-volume-run"),
  volumeCancelButton: document.getElementById("industry-volume-cancel"),
  volumeMeta: document.getElementById("industry-volume-meta"),
  volumeHistoryToggle: document.getElementById("industry-volume-history-toggle"),
  volumeHistoryClose: document.getElementById("industry-volume-history-close"),
  volumeHistorySection: document.getElementById("industry-volume-history"),
  volumeHistoryList: document.getElementById("industry-volume-history-list"),
};

const state = {
  lang: getInitialLanguage(),
  watchlist: [],
  searchResults: [],
  searchMessage: null,
  searchTimer: null,
  statusTimer: null,
  currentIndustry: null,
  currentIndustryCode: null,
  industryStatus: null,
  chartInstance: null,
  chartRows: [],
  chartRequestToken: null,
  loadingChart: false,
  syncingIndustry: null,
  activeTab: "chart",
  insightSummary: null,
  insightSnapshot: null,
  insightLoading: false,
  insightError: null,
  news: {
    items: [],
    loading: false,
    error: null,
    industry: null,
  },
  newsCache: new Map(),
  volumeAnalysis: {
    running: false,
    controller: null,
    error: null,
    content: "",
    meta: null,
    lastIndustry: null,
  },
  volumeAnalysisCache: new Map(),
  volumeHistory: {
    industry: null,
    visible: false,
    loading: false,
    error: null,
    items: [],
  },
};

let echartsLoader = null;

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch {
    /* ignore */
  }
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
  }
  return "zh";
}

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch {
    /* ignore */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

function getDict() {
  return translations[state.lang] || translations.zh || translations.en;
}

function setStatusMessage(message, tone) {
  if (!elements.statusMessage) return;
  if (state.statusTimer) {
    clearTimeout(state.statusTimer);
    state.statusTimer = null;
  }
  if (!message) {
    elements.statusMessage.textContent = "";
    elements.statusMessage.removeAttribute("data-tone");
    elements.statusMessage.removeAttribute("data-visible");
    return;
  }
  elements.statusMessage.textContent = message;
  elements.statusMessage.dataset.visible = "1";
  if (tone) {
    elements.statusMessage.dataset.tone = tone;
  } else {
    elements.statusMessage.removeAttribute("data-tone");
  }
  state.statusTimer = window.setTimeout(() => {
    if (elements.statusMessage) {
      elements.statusMessage.removeAttribute("data-visible");
      elements.statusMessage.removeAttribute("data-tone");
      elements.statusMessage.textContent = "";
    }
    state.statusTimer = null;
  }, 2600);
}

function formatDate(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return date.toLocaleString(locale, { hour12: false });
}

function formatDateOnly(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return date.toLocaleDateString(locale, { year: "numeric", month: "short", day: "numeric" });
}

function parseJSON(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function formatVolumeSummary(summary) {
  const payload = parseJSON(summary) || {};
  if (!payload || typeof payload !== "object") {
    return null;
  }
  const dict = getDict();
  const sections = [];
  const parts = [];
  if (payload.wyckoffPhase) {
    const label = dict.volumeLabelPhase || "阶段";
    parts.push(`${label}：${payload.wyckoffPhase}`);
  }
  if (payload.compositeIntent) {
    const label = dict.volumeLabelIntent || "主力意图";
    parts.push(`${label}：${payload.compositeIntent}`);
  }
  if (payload.confidence != null && Number.isFinite(Number(payload.confidence))) {
    const confidenceLabel = dict.volumeLabelConfidence || "置信度";
    parts.push(`${confidenceLabel}：${(Number(payload.confidence) * 100).toFixed(0)}%`);
  }
  if (parts.length) {
    sections.push(parts.join(" · "));
  }
  if (payload.stageSummary) {
    sections.push(`【${dict.volumeLabelSummary || "量价结论"}】`);
    sections.push(String(payload.stageSummary));
  }
  const listSections = [
    { key: "volumeSignals", label: dict.volumeLabelVolumeSignals || "量能信号" },
    { key: "priceSignals", label: dict.volumeLabelPriceSignals || "价格/结构信号" },
    { key: "strategy", label: dict.volumeLabelStrategy || "策略建议" },
    { key: "risks", label: dict.volumeLabelRisks || "风险提示" },
    { key: "checklist", label: dict.volumeLabelChecklist || "后续观察" },
  ];
  listSections.forEach((section) => {
    const items = formatListValues(payload[section.key]);
    if (!items.length) return;
    sections.push(`【${section.label}】`);
    items.forEach((item, index) => {
      sections.push(`${index + 1}. ${item}`);
    });
  });
  return sections.join("\n");
}

function setActiveTab(tab, { force = false } = {}) {
  if (!tab || (!force && tab === state.activeTab)) {
    return;
  }
  state.activeTab = tab;
  if (elements.tabButtons && elements.tabButtons.length) {
    elements.tabButtons.forEach((button) => {
      const isActive = button.dataset.tab === tab;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });
  }
  if (elements.tabPanels && elements.tabPanels.length) {
    elements.tabPanels.forEach((panel) => {
      const isActive = panel.dataset.tabPanel === tab;
      panel.classList.toggle("is-active", isActive);
      if (isActive) {
        panel.removeAttribute("hidden");
      } else {
        panel.setAttribute("hidden", "");
      }
    });
  }
  if (tab === "chart" && state.chartInstance) {
    window.requestAnimationFrame(() => {
      if (state.chartInstance) {
        state.chartInstance.resize();
      }
    });
  }
  if (tab === "insight") {
    if (!state.insightSnapshot && !state.insightLoading) {
      ensureIndustryInsight().catch(() => {});
    } else {
      renderReasoning();
    }
  } else if (tab === "volume") {
    ensureVolumeAnalysisLoaded();
    updateVolumeAnalysisOutput();
  } else if (tab === "news") {
    ensureIndustryNewsLoaded();
  }
}

function initTabs() {
  if (!elements.tabButtons || !elements.tabButtons.length) {
    return;
  }
  elements.tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.dataset.tab || "chart";
      setActiveTab(tab);
    });
  });
  setActiveTab(state.activeTab, { force: true });
}

function handleLanguageSwitch(event) {
  const lang = event.currentTarget.dataset.lang;
  if (!lang || lang === state.lang || !translations[lang]) return;
  state.lang = lang;
  persistLanguage(lang);
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("lang-btn--active", btn.dataset.lang === lang);
  });
  renderWatchlist();
  renderChart(state.chartRows);
  renderReasoning();
  renderIndustryNews();
  updateVolumeAnalysisOutput();
  renderVolumeHistoryList();
  setStatusMessage("", null);
}

function initLanguage() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", handleLanguageSwitch);
    if (btn.dataset.lang === state.lang) {
      btn.classList.add("lang-btn--active");
    }
  });
  persistLanguage(state.lang);
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function debounceSearch(value) {
  clearTimeout(state.searchTimer);
  state.searchResults = [];
  state.searchMessage = null;
  if (!value || !value.trim()) {
    renderSearchResults();
    return;
  }
  state.searchMessage = getDict().searching || "Searching…";
  renderSearchResults();
  state.searchTimer = setTimeout(() => performSearch(value.trim()), SEARCH_DEBOUNCE_MS);
}

async function performSearch(keyword) {
  try {
    const data = await fetchJSON(`${API_BASE}/industries/search?q=${encodeURIComponent(keyword)}&limit=20`);
    state.searchResults = Array.isArray(data.items) ? data.items : [];
    state.searchMessage = null;
    renderSearchResults();
  } catch (error) {
    console.error("Industry search failed", error);
    state.searchResults = [];
    state.searchMessage = getDict().searchFailed || "Failed to load industries.";
    renderSearchResults();
  }
}

function renderSearchResults() {
  const container = elements.searchDropdown;
  if (!container) return;
  container.innerHTML = "";
  const keyword = elements.searchInput?.value?.trim();
  if (!keyword) {
    container.hidden = true;
    return;
  }
  if (state.searchMessage || !state.searchResults.length) {
    const empty = document.createElement("div");
    empty.className = "concept-search__empty";
    empty.textContent =
      state.searchMessage ||
      (state.searchResults.length === 0 ? getDict().searchNoResult || "No matching industries." : "");
    container.appendChild(empty);
    container.hidden = false;
    return;
  }
  state.searchResults.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.innerHTML = `<strong>${item.name}</strong><span>${item.code}</span>`;
    button.addEventListener("click", async () => {
      try {
        await addIndustryToWatchlist(item.name);
      } catch {
        /* ignore */
      }
      selectIndustry(item.name, item.code);
      container.hidden = true;
      elements.searchInput.value = item.name;
    });
    container.appendChild(button);
  });
  container.hidden = false;
}

async function addIndustryToWatchlist(industry) {
  if (!industry) return;
  try {
    await fetchJSON(`${API_BASE}/industries/watchlist`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ industry }),
    });
    await fetchWatchlist();
  } catch (error) {
    console.error("Failed to add industry to watchlist", error);
    setStatusMessage(getDict().watchError || "Failed to update watchlist.", "error");
  }
}

async function fetchWatchlist() {
  try {
    const data = await fetchJSON(`${API_BASE}/industries/watchlist`);
    state.watchlist = Array.isArray(data.items) ? data.items : [];
    renderWatchlist();
  } catch (error) {
    console.error("Industry watchlist fetch failed", error);
  }
  return state.watchlist;
}

function renderWatchlist() {
  const container = elements.watchlist;
  if (!container) return;
  container.innerHTML = "";
  const dict = getDict();
  if (!state.watchlist.length) {
    container.dataset.emptyI18n = dict.watchlistEmpty || "暂无监控行业";
    container.setAttribute("data-empty", "1");
    state.currentIndustry = null;
    state.currentIndustryCode = null;
    state.industryStatus = null;
    updateIndustryMeta();
    showChartPlaceholder(dict.noChartData || "No chart data.");
    renderReasoning();
    state.news.items = [];
    state.news.error = null;
    state.news.industry = null;
    renderIndustryNews();
    state.volumeHistory.visible = false;
    if (elements.volumeHistorySection) {
      elements.volumeHistorySection.hidden = true;
    }
    applyVolumeAnalysisCache(null);
    state.volumeHistory.industry = null;
    state.volumeHistory.items = [];
    state.volumeHistory.error = null;
    renderVolumeHistoryList();
    return;
  }
  delete container.dataset.emptyI18n;
  container.removeAttribute("data-empty");
  const fragment = document.createDocumentFragment();
  const entries = [...state.watchlist].sort((a, b) => {
    const watchDiff = Number(b.isWatched ? 1 : 0) - Number(a.isWatched ? 1 : 0);
    if (watchDiff !== 0) return watchDiff;
    const dateA = a.updatedAt ? new Date(a.updatedAt).getTime() : 0;
    const dateB = b.updatedAt ? new Date(b.updatedAt).getTime() : 0;
    return dateB - dateA;
  });
  let hasActive = false;
  entries.forEach((entry) => {
    const card = document.createElement("div");
    card.className = "concept-market__watch-item";
    card.dataset.industry = entry.industry;
    card.dataset.code = entry.industryCode || "";
    card.setAttribute("role", "button");
    card.tabIndex = 0;
    if (state.currentIndustry === entry.industry) {
      card.classList.add("is-active");
      hasActive = true;
    }
    card.innerHTML = `
      <div class="concept-market__watch-item-title">
        <span class="concept-market__watch-name">${entry.industry || "--"}</span>
      </div>
      <div class="concept-market__watch-item-meta">
        <span>${dict.latestTradeShort || "最新"}：${entry.latestTradeDate || "--"}</span>
      </div>
      <div class="concept-market__watch-actions">
        <button
          type="button"
          class="concept-market__watch-action concept-market__watch-action--sync"
          data-action="sync"
          aria-label="${dict.actionSync || dict.refreshButton || "Sync"}"
          title="${dict.actionSync || dict.refreshButton || "Sync"}"
        ></button>
        <button
          type="button"
          class="concept-market__watch-action concept-market__watch-action--favorite"
          data-action="favorite"
          aria-label=""
          title=""
        ></button>
        <button
          type="button"
          class="concept-market__watch-action concept-market__watch-action--delete"
          data-action="delete"
          aria-label="${dict.watchRemoved || "Remove"}"
          title="${dict.watchRemoved || "Remove"}"
        ></button>
      </div>
    `;
    const selectFromCard = () => selectIndustry(entry.industry, entry.industryCode, "watchlist");
    card.addEventListener("click", selectFromCard);
    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectFromCard();
      }
    });
    const syncButton = card.querySelector('[data-action="sync"]');
    if (syncButton) {
      syncButton.addEventListener("click", async (event) => {
        event.stopPropagation();
        if (syncButton.dataset.syncing === "1") {
          return;
        }
        syncButton.dataset.syncing = "1";
        syncButton.disabled = true;
        syncButton.classList.add("is-syncing");
        syncButton.setAttribute("aria-busy", "true");
        selectIndustry(entry.industry, entry.industryCode, "watchlist");
        try {
          await refreshIndustry();
        } finally {
          syncButton.disabled = false;
          syncButton.classList.remove("is-syncing");
          syncButton.removeAttribute("aria-busy");
          delete syncButton.dataset.syncing;
        }
      });
    }
    const favoriteButton = card.querySelector('[data-action="favorite"]');
    if (favoriteButton) {
      const isWatched = entry.isWatched !== false;
      favoriteButton.dataset.active = isWatched ? "1" : "0";
      const favLabel = isWatched
        ? dict.actionFavoriteRemove || dict.watchButtonActive || "Remove"
        : dict.actionFavoriteAdd || dict.watchButton || "Add";
      favoriteButton.setAttribute("aria-label", favLabel);
      favoriteButton.title = favLabel;
      favoriteButton.addEventListener("click", async (event) => {
        event.stopPropagation();
        favoriteButton.disabled = true;
        try {
          const nextState = favoriteButton.dataset.active !== "1";
          await updateWatchState(entry.industry, nextState);
        } catch {
          /* status message already set */
        } finally {
          favoriteButton.disabled = false;
        }
      });
    }
    const deleteButton = card.querySelector('[data-action="delete"]');
    if (deleteButton) {
      deleteButton.addEventListener("click", async (event) => {
        event.stopPropagation();
        deleteButton.disabled = true;
        try {
          await deleteWatchEntry(entry.industry);
        } finally {
          deleteButton.disabled = false;
        }
      });
    }
    fragment.appendChild(card);
  });
  container.appendChild(fragment);
  if (!state.currentIndustry || !hasActive) {
    const fallback = entries[0];
    if (fallback) {
      selectIndustry(fallback.industry, fallback.industryCode, "watchlist");
    }
  } else {
    updateWatchlistHighlight();
  }
  renderReasoning();
  if (state.activeTab === "volume") {
    ensureVolumeAnalysisLoaded({ force: false });
  }
  if (state.activeTab === "news") {
    ensureIndustryNewsLoaded({ force: false });
  } else {
    renderIndustryNews();
  }
}

async function updateWatchState(industry, watch) {
  if (!industry) return null;
  const dict = getDict();
  const endpoint = watch
    ? `${API_BASE}/industries/watchlist`
    : `${API_BASE}/industries/watchlist/${encodeURIComponent(industry)}`;
  const options = watch
    ? {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ industry }),
      }
    : { method: "DELETE" };
  try {
    const response = await fetchJSON(endpoint, options);
    await fetchWatchlist();
    setStatusMessage(
      watch ? dict.watchAdded || "Added to watchlist." : dict.watchRemoved || "Removed from watchlist.",
      "success"
    );
    return response;
  } catch (error) {
    console.error("Failed to update industry watchlist", error);
    setStatusMessage(dict.watchError || "Failed to update watchlist.", "error");
    throw error;
  }
}

async function deleteWatchEntry(industry) {
  if (!industry) return null;
  const dict = getDict();
  const endpoint = `${API_BASE}/industries/watchlist/${encodeURIComponent(industry)}?permanent=true`;
  try {
    const response = await fetchJSON(endpoint, { method: "DELETE" });
    await fetchWatchlist();
    setStatusMessage(dict.watchRemoved || "Removed from watchlist.", "success");
    return response;
  } catch (error) {
    console.error("Failed to delete industry watch entry", error);
    setStatusMessage(dict.watchError || "Failed to update watchlist.", "error");
    throw error;
  }
}

function updateWatchlistHighlight() {
  if (!elements.watchlist) return;
  const cards = elements.watchlist.querySelectorAll(".concept-market__watch-item");
  cards.forEach((card) => {
    if (card.dataset.industry === state.currentIndustry) {
      card.classList.add("is-active");
    } else {
      card.classList.remove("is-active");
    }
  });
}

function selectIndustry(name, code, source = "search") {
  if (!name) return;
  state.currentIndustry = name;
  state.currentIndustryCode = code || null;
  if (source === "search") {
    addIndustryToWatchlist(name).catch(() => {});
  }
  if (elements.codeLabel) {
    elements.codeLabel.textContent = state.currentIndustryCode || "--";
  }
  const chartTitle = document.getElementById("industry-market-chart-title");
  if (chartTitle) {
    chartTitle.textContent = name || getDict().chartTitle;
  }
  updateWatchlistHighlight();
  state.news.items = [];
  state.news.error = null;
  state.news.industry = null;
  renderIndustryNews();
  state.volumeHistory.industry = null;
  state.volumeHistory.items = [];
  state.volumeHistory.error = null;
  renderVolumeHistoryList();
  renderReasoning();
  if (state.activeTab === "volume") {
    ensureVolumeAnalysisLoaded({ force: false });
  } else {
    applyVolumeAnalysisCache(state.currentIndustry);
  }
  if (state.activeTab === "news") {
    ensureIndustryNewsLoaded({ force: true });
  }
  setStatusMessage(getDict().loading || "Loading…", "info");
  loadIndustryStatus(name).finally(() => {
    setStatusMessage("", null);
  });
  loadIndustryHistory(name);
}

async function loadIndustryStatus(industry) {
  try {
    const data = await fetchJSON(`${API_BASE}/industries/status?industry=${encodeURIComponent(industry)}`);
    state.industryStatus = data;
    if (!state.currentIndustryCode) {
      state.currentIndustryCode = data.industryCode || null;
      if (elements.codeLabel) {
        elements.codeLabel.textContent = state.currentIndustryCode || "--";
      }
    }
    updateIndustryMeta();
  } catch (error) {
    console.error("Industry status fetch failed", error);
    setStatusMessage(getDict().statusError || "Failed to load status.", "error");
  }
}

function updateIndustryMeta() {
  const dict = getDict();
  if (elements.latestDate) {
    elements.latestDate.textContent = formatDateOnly(state.industryStatus?.latestTradeDate);
  }
  if (elements.lastSync) {
    elements.lastSync.textContent = formatDate(state.industryStatus?.lastSyncedAt) || dict.lastSyncedShort || "--";
  }
}

async function refreshIndustry() {
  if (!state.currentIndustry) {
    setStatusMessage(getDict().noIndustrySelected || "Select an industry first.", "error");
    return;
  }
  if (state.syncingIndustry === state.currentIndustry) {
    return;
  }
  const dict = getDict();
  setStatusMessage(dict.refreshing || "Refreshing…", "info");
  state.syncingIndustry = state.currentIndustry;
  try {
    const payload = {
      industry: state.currentIndustry,
      lookbackDays: DEFAULT_LOOKBACK,
    };
    const result = await fetchJSON(`${API_BASE}/industries/refresh-history`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.industryStatus = {
      industry: result.industry,
      industryCode: result.industryCode,
      isWatched: result.isWatched ?? state.industryStatus?.isWatched ?? false,
      lastSyncedAt: result.lastSyncedAt,
      latestTradeDate: result.latestTradeDate,
    };
    updateIndustryMeta();
    await fetchWatchlist();
    loadIndustryHistory(state.currentIndustry);
    setStatusMessage(dict.refreshDone || "Industry updated.", "success");
  } catch (error) {
    console.error("Refresh industry failed", error);
    setStatusMessage(dict.refreshFailed || "Failed to refresh industry.", "error");
  } finally {
    state.syncingIndustry = null;
  }
}

async function loadIndustryHistory(industry) {
  if (!industry || !elements.chartContainer) return;
  const dict = getDict();
  elements.chartContainer.dataset.loading = "1";
  state.loadingChart = true;
  const token = Symbol("industryChart");
  state.chartRequestToken = token;
  try {
    const response = await fetchJSON(
      `${API_BASE}/market/industry-index-history?industry=${encodeURIComponent(industry)}&limit=240`
    );
    if (state.chartRequestToken !== token) {
      return;
    }
    const rows = Array.isArray(response.rows) ? response.rows : [];
    state.chartRows = rows;
    if (rows.length) {
      await renderChart(rows);
    } else {
      showChartPlaceholder(dict.noChartData || "No chart data available.");
    }
  } catch (error) {
    console.error("Failed to load industry index history", error);
    if (state.chartRequestToken === token) {
      showChartPlaceholder(dict.fetchFailed || "Failed to load data.");
    }
  } finally {
    if (state.chartRequestToken === token) {
      delete elements.chartContainer.dataset.loading;
      state.loadingChart = false;
    }
  }
}

async function ensureIndustryInsight(force = false) {
  if (state.insightLoading) {
    return;
  }
  if (state.insightSnapshot && !force) {
    return;
  }
  state.insightLoading = true;
  state.insightError = null;
  renderReasoning();
  try {
    const data = await fetchJSON(`${API_BASE}/market/industry-insight`);
    state.insightSummary = data?.insight || null;
    state.insightSnapshot = data?.snapshot || null;
  } catch (error) {
    console.error("Industry insight fetch failed", error);
    state.insightError = error;
  } finally {
    state.insightLoading = false;
    renderReasoning();
  }
}

function getSummaryIndustry(name) {
  if (!name) return null;
  const summaryJson = state.insightSummary?.summaryJson;
  if (!summaryJson || !Array.isArray(summaryJson.top_industries)) {
    return null;
  }
  const target = normalizeName(name);
  return (
    summaryJson.top_industries.find((item) => normalizeName(item?.name) === target) || null
  );
}

function getSnapshotIndustry(name) {
  if (!name) return null;
  const industries = state.insightSnapshot?.industries;
  if (!Array.isArray(industries)) {
    return null;
  }
  const target = normalizeName(name);
  return industries.find((item) => normalizeName(item?.name) === target) || null;
}

function renderReasoning() {
  const container = elements.reasoningOutput;
  if (!container) return;
  const dict = getDict();

  const showMessage = (message, stateKey = "empty") => {
    container.innerHTML = "";
    container.dataset.state = stateKey;
    const fallback = document.createElement("div");
    fallback.className = "concept-summary__fallback";
    fallback.textContent = message;
    container.appendChild(fallback);
  };

  if (state.insightLoading) {
    updateReasoningMeta("--");
    showMessage(dict.reasoningLoading || "Loading reasoning…", "loading");
    return;
  }

  if (state.insightError) {
    updateReasoningMeta("--");
    showMessage(dict.reasoningError || "Failed to load industry reasoning.", "error");
    return;
  }

  if (!state.currentIndustry) {
    updateReasoningMeta("--");
    showMessage(dict.reasoningEmpty || "Select an industry to view reasoning.", "empty");
    return;
  }

  const summaryEntry = getSummaryIndustry(state.currentIndustry);
  const snapshotEntry = getSnapshotIndustry(state.currentIndustry);
  const insightTime = state.insightSummary?.generatedAt || state.insightSnapshot?.generatedAt;
  updateReasoningMeta(formatDateTime(insightTime));

  if (!summaryEntry && !snapshotEntry) {
    showMessage(dict.reasoningUnavailable || "This industry is not covered in the latest insight.", "empty");
    return;
  }

  container.innerHTML = "";
  container.removeAttribute("data-state");

  if (summaryEntry) {
    const summaryCard = buildSummaryCard(summaryEntry, dict);
    if (summaryCard) {
      container.appendChild(summaryCard);
    }
  }

  if (snapshotEntry) {
    const fundBlock = buildFundBlock(snapshotEntry, dict);
    if (fundBlock) {
      container.appendChild(fundBlock);
    }
    const metricsBlock = buildMetricsBlock(snapshotEntry, dict);
    if (metricsBlock) {
      container.appendChild(metricsBlock);
    }
    const newsBlock = buildNewsBlock(snapshotEntry, dict);
    if (newsBlock) {
      container.appendChild(newsBlock);
    }
  }
}

function ensureIndustryNewsLoaded({ force = false } = {}) {
  if (!state.currentIndustry) {
    state.news.items = [];
    state.news.error = null;
    state.news.industry = null;
    renderIndustryNews();
    return;
  }
  if (!force) {
    const cached = state.newsCache.get(state.currentIndustry);
    if (cached) {
      state.news.items = cached.items;
      state.news.error = null;
      state.news.industry = state.currentIndustry;
      renderIndustryNews();
      return;
    }
  }
  fetchIndustryNews(state.currentIndustry);
}

async function fetchIndustryNews(industry) {
  if (!industry) return;
  state.news.loading = true;
  state.news.error = null;
  state.news.industry = industry;
  renderIndustryNews();
  try {
    const data = await fetchJSON(
      `${API_BASE}/industries/news?industry=${encodeURIComponent(industry)}&lookbackHours=48&limit=40`
    );
    const items = Array.isArray(data.items) ? data.items : [];
    state.news.items = items;
    state.newsCache.set(industry, { items });
    state.news.error = null;
  } catch (error) {
    console.error("Industry news fetch failed", error);
    state.news.items = [];
    state.news.error = getDict().newsError || "Failed to load industry news.";
  } finally {
    state.news.loading = false;
    renderIndustryNews();
  }
}

function renderIndustryNews() {
  const container = elements.newsList;
  if (!container) return;
  const dict = getDict();
  container.innerHTML = "";
  if (state.news.loading) {
    container.dataset.empty = "0";
    const loading = document.createElement("p");
    loading.className = "concept-summary__text";
    loading.textContent = dict.loading || "Loading…";
    container.appendChild(loading);
    return;
  }
  if (state.news.error) {
    container.dataset.empty = "0";
    const error = document.createElement("p");
    error.className = "concept-summary__text";
    error.textContent = state.news.error;
    container.appendChild(error);
    return;
  }
  if (!state.news.items.length) {
    container.dataset.empty = "1";
    return;
  }
  container.dataset.empty = "0";
  state.news.items.forEach((article) => {
    const card = document.createElement("article");
    card.className = "concept-market__news-card";

    if (article.title || article.impact_summary || article.url) {
      const titleNode = document.createElement(article.url ? "a" : "h4");
      titleNode.textContent = article.title || article.impact_summary || dict.newsTitle || "Article";
      if (article.url) {
        titleNode.href = article.url;
        titleNode.target = "_blank";
        titleNode.rel = "noopener noreferrer";
        titleNode.className = "concept-market__news-link";
      }
      card.appendChild(titleNode);
    }

    if (article.summary || article.impact_summary) {
      const summary = document.createElement("p");
      summary.className = "concept-market__news-summary";
      summary.textContent = article.summary || article.impact_summary;
      card.appendChild(summary);
    }

    const meta = document.createElement("div");
    meta.className = "concept-market__news-meta";
    if (article.source) {
      meta.appendChild(createMetaChip(dict.newsSourceLabel || "Source", article.source));
    }
    if (article.published_at) {
      meta.appendChild(createMetaChip(dict.newsTimeLabel || "Published", formatDateTime(article.published_at)));
    }
    if (article.impact_summary && !article.summary) {
      meta.appendChild(createMetaChip(dict.newsImpactLabel || "Impact", article.impact_summary));
    }
    if (meta.children.length) {
      card.appendChild(meta);
    }

    container.appendChild(card);
  });
}

function createMetaChip(label, value) {
  const span = document.createElement("span");
  span.textContent = `${label}: ${value}`;
  return span;
}

function normalizeVolumeRecord(record) {
  if (!record) return null;
  const parsedSummary = parseJSON(record.summary) || record.summary;
  return {
    id: record.id,
    industry: record.industry,
    industryCode: record.industryCode,
    lookbackDays: record.lookbackDays,
    summary: parsedSummary || {},
    rawText: record.rawText || "",
    model: record.model || null,
    generatedAt: record.generatedAt,
  };
}

function applyVolumeAnalysisCache(industry) {
  cancelVolumeAnalysis({ silent: true });
  if (!industry) {
    state.volumeAnalysis.content = "";
    state.volumeAnalysis.error = null;
    state.volumeAnalysis.meta = null;
    state.volumeAnalysis.lastIndustry = null;
    updateVolumeAnalysisOutput();
    return;
  }
  const cached = state.volumeAnalysisCache.get(industry);
  if (cached) {
    state.volumeAnalysis.content = cached.content || "";
    state.volumeAnalysis.meta = cached.meta || null;
  } else {
    state.volumeAnalysis.content = "";
    state.volumeAnalysis.meta = null;
  }
  state.volumeAnalysis.error = null;
  state.volumeAnalysis.lastIndustry = industry;
  updateVolumeAnalysisOutput();
}

function updateVolumeAnalysisOutput() {
  const container = elements.volumeOutput;
  if (!container) return;
  const dict = getDict();
  container.removeAttribute("data-loading");
  container.removeAttribute("data-error");
  container.removeAttribute("data-empty");
  if (state.volumeAnalysis.running) {
    container.textContent = dict.volumeStreaming || "模型推理中…";
    container.dataset.loading = "1";
  } else if (state.volumeAnalysis.error) {
    container.textContent = state.volumeAnalysis.error;
    container.dataset.error = "1";
  } else if (state.volumeAnalysis.content) {
    container.textContent = state.volumeAnalysis.content;
  } else {
    container.textContent = dict.volumeEmpty || "请选择行业后点击推理。";
    container.dataset.empty = "1";
  }
  if (elements.volumeRunButton) {
    elements.volumeRunButton.disabled = state.volumeAnalysis.running || !state.currentIndustry;
    elements.volumeRunButton.textContent = state.volumeAnalysis.running
      ? dict.volumeRunning || "生成中…"
      : dict.volumeButton || "生成推理";
  }
  if (elements.volumeCancelButton) {
    elements.volumeCancelButton.hidden = !state.volumeAnalysis.running;
    elements.volumeCancelButton.disabled = !state.volumeAnalysis.running;
  }
  if (elements.volumeMeta) {
    const meta = state.volumeAnalysis.meta;
    if (meta && meta.generatedAt) {
      const label = dict.volumeMetaLabel || "最新推理";
      const segments = [label, formatDateTime(meta.generatedAt)];
      if (meta.model) {
        segments.push(meta.model);
      }
      elements.volumeMeta.textContent = segments.filter(Boolean).join(" · ");
      elements.volumeMeta.dataset.empty = "0";
    } else {
      elements.volumeMeta.textContent = dict.volumeMetaEmpty || "暂无历史记录";
      elements.volumeMeta.dataset.empty = "1";
    }
  }
  renderVolumeHistoryList();
}

function ensureVolumeAnalysisLoaded({ force = false } = {}) {
  if (!state.currentIndustry) {
    applyVolumeAnalysisCache(null);
    return;
  }
  fetchLatestIndustryVolumeAnalysis(state.currentIndustry, { force });
}

async function fetchLatestIndustryVolumeAnalysis(industry, { force = false } = {}) {
  if (!industry) {
    applyVolumeAnalysisCache(null);
    return;
  }
  if (!force) {
    const cached = state.volumeAnalysisCache.get(industry);
    if (cached && !state.volumeAnalysis.running) {
      state.volumeAnalysis.content = cached.content || "";
      state.volumeAnalysis.meta = cached.meta || null;
      state.volumeAnalysis.error = null;
      state.volumeAnalysis.lastIndustry = industry;
      updateVolumeAnalysisOutput();
      return;
    }
  }
  try {
    const data = await fetchJSON(
      `${API_BASE}/industries/volume-price-analysis/latest?industry=${encodeURIComponent(industry)}`
    );
    const record = normalizeVolumeRecord(data);
    if (record) {
      const formatted = formatVolumeSummary(record.summary) || record.rawText || "";
      state.volumeAnalysisCache.set(industry, { content: formatted, meta: record });
      if (state.currentIndustry === industry && !state.volumeAnalysis.running) {
        state.volumeAnalysis.content = formatted;
        state.volumeAnalysis.meta = record;
        state.volumeAnalysis.error = null;
        state.volumeAnalysis.lastIndustry = industry;
        updateVolumeAnalysisOutput();
      }
    } else if (state.currentIndustry === industry && !state.volumeAnalysis.running) {
      state.volumeAnalysis.content = "";
      state.volumeAnalysis.meta = null;
      state.volumeAnalysis.error = null;
      state.volumeAnalysis.lastIndustry = industry;
      updateVolumeAnalysisOutput();
    }
  } catch (error) {
    if (error && typeof error.message === "string" && error.message.includes("404")) {
      state.volumeAnalysisCache.set(industry, null);
      if (state.currentIndustry === industry && !state.volumeAnalysis.running) {
        state.volumeAnalysis.content = "";
        state.volumeAnalysis.meta = null;
        state.volumeAnalysis.error = null;
        state.volumeAnalysis.lastIndustry = industry;
        updateVolumeAnalysisOutput();
      }
    } else {
      console.error("Failed to load latest volume analysis", error);
      if (state.currentIndustry === industry && !state.volumeAnalysis.running) {
        state.volumeAnalysis.error = getDict().volumeError || "量价推理失败。";
        updateVolumeAnalysisOutput();
      }
    }
  }
}

async function fetchIndustryVolumeHistory(industry, { force = false } = {}) {
  if (!industry) {
    state.volumeHistory.items = [];
    state.volumeHistory.error = null;
    state.volumeHistory.industry = null;
    renderVolumeHistoryList();
    return;
  }
  if (!force && state.volumeHistory.industry === industry && state.volumeHistory.items.length) {
    return;
  }
  state.volumeHistory.loading = true;
  state.volumeHistory.error = null;
  state.volumeHistory.industry = industry;
  renderVolumeHistoryList();
  try {
    const data = await fetchJSON(
      `${API_BASE}/industries/volume-price-analysis/history?industry=${encodeURIComponent(industry)}&limit=10`
    );
    state.volumeHistory.items = Array.isArray(data.items) ? data.items : [];
    state.volumeHistory.error = null;
  } catch (error) {
    console.error("Failed to load industry volume analysis history", error);
    state.volumeHistory.items = [];
    state.volumeHistory.error = getDict().volumeHistoryError || "历史记录获取失败。";
  } finally {
    state.volumeHistory.loading = false;
    renderVolumeHistoryList();
  }
}

function renderVolumeHistoryList() {
  const container = elements.volumeHistoryList;
  if (!container) return;
  const dict = getDict();
  container.innerHTML = "";
  if (state.volumeHistory.loading) {
    container.textContent = dict.volumeHistoryLoading || "加载历史记录…";
    return;
  }
  if (state.volumeHistory.error) {
    container.textContent = state.volumeHistory.error;
    return;
  }
  if (!state.volumeHistory.items.length) {
    container.dataset.empty = "1";
    container.textContent = dict.volumeHistoryEmpty || "暂无历史记录。";
    return;
  }
  container.dataset.empty = "0";
  state.volumeHistory.items.forEach((entry) => {
    const normalized = normalizeVolumeRecord(entry);
    if (!normalized) return;
    const displayText = formatVolumeSummary(normalized.summary) || normalized.rawText || "";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "concept-market__volume-history-item";
    button.innerHTML = `
      <div class="concept-market__volume-history-item-meta">
        <strong>${formatDateTime(normalized.generatedAt)}</strong>
        <span>${normalized.model || "DeepSeek"}</span>
      </div>
      <span>${normalized.lookbackDays ? `${normalized.lookbackDays}d` : ""}</span>
    `;
    button.addEventListener("click", () =>
      applyVolumeHistoryEntry({
        ...normalized,
        rawText: displayText,
      })
    );
    container.appendChild(button);
  });
}

function toggleVolumeHistory(visible) {
  state.volumeHistory.visible = visible;
  if (elements.volumeHistorySection) {
    elements.volumeHistorySection.hidden = !visible;
  }
  if (visible && state.currentIndustry) {
    fetchIndustryVolumeHistory(state.currentIndustry, { force: true });
  }
}

function applyVolumeHistoryEntry(entry) {
  if (!entry) return;
  cancelVolumeAnalysis({ silent: true });
  state.volumeAnalysis.content = formatVolumeSummary(entry.summary) || entry.rawText || "";
  state.volumeAnalysis.meta = entry;
  state.volumeAnalysis.error = null;
  state.volumeAnalysis.lastIndustry = state.currentIndustry;
  if (entry.industry) {
    state.volumeAnalysisCache.set(entry.industry, { content: state.volumeAnalysis.content, meta: entry });
  }
  updateVolumeAnalysisOutput();
}

function cancelVolumeAnalysis({ silent = false } = {}) {
  if (state.volumeAnalysis.controller) {
    state.volumeAnalysis.controller.abort();
    state.volumeAnalysis.controller = null;
  }
  const wasRunning = state.volumeAnalysis.running;
  state.volumeAnalysis.running = false;
  if (silent) {
    state.volumeAnalysis.error = null;
  } else if (wasRunning) {
    state.volumeAnalysis.error = getDict().volumeCancelled || "推理已取消。";
  }
  updateVolumeAnalysisOutput();
}

async function runVolumeAnalysis() {
  if (!state.currentIndustry || state.volumeAnalysis.running) {
    return;
  }
  const targetIndustry = state.currentIndustry;
  cancelVolumeAnalysis({ silent: true });
  const controller = new AbortController();
  state.volumeAnalysis.running = true;
  state.volumeAnalysis.controller = controller;
  state.volumeAnalysis.error = null;
  state.volumeAnalysis.content = "";
  state.volumeAnalysis.meta = null;
  state.volumeAnalysis.lastIndustry = state.currentIndustry;
  updateVolumeAnalysisOutput();
  try {
    const response = await fetch(`${API_BASE}/industries/volume-price-analysis`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        industry: state.currentIndustry,
        lookbackDays: VOLUME_ANALYSIS_LOOKBACK,
        runLlm: true,
      }),
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    if (!response.body || !response.body.getReader) {
      const text = await response.text();
      state.volumeAnalysis.content = text;
    } else {
      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let done = false;
      while (!done) {
        const { value, done: readerDone } = await reader.read();
        if (value) {
          state.volumeAnalysis.content += decoder.decode(value, { stream: !readerDone });
          updateVolumeAnalysisOutput();
        }
        done = readerDone;
      }
      state.volumeAnalysis.content += decoder.decode();
    }
    if (targetIndustry) {
      await fetchLatestIndustryVolumeAnalysis(targetIndustry, { force: true });
      if (state.volumeHistory.visible && targetIndustry === state.currentIndustry) {
        fetchIndustryVolumeHistory(targetIndustry, { force: true });
      }
    }
  } catch (error) {
    const aborted = controller.signal.aborted || (error && error.name === "AbortError");
    if (aborted) {
      state.volumeAnalysis.error = getDict().volumeCancelled || "推理已取消。";
    } else {
      console.error("Industry volume-price reasoning failed", error);
      state.volumeAnalysis.error = getDict().volumeError || "量价推理失败。";
    }
  } finally {
    state.volumeAnalysis.running = false;
    state.volumeAnalysis.controller = null;
    updateVolumeAnalysisOutput();
  }
}

function initVolumeAnalysis() {
  if (elements.volumeRunButton) {
    elements.volumeRunButton.addEventListener("click", runVolumeAnalysis);
  }
  if (elements.volumeCancelButton) {
    elements.volumeCancelButton.addEventListener("click", () => cancelVolumeAnalysis());
  }
  if (elements.volumeHistoryToggle) {
    elements.volumeHistoryToggle.addEventListener("click", () => toggleVolumeHistory(!state.volumeHistory.visible));
  }
  if (elements.volumeHistoryClose) {
    elements.volumeHistoryClose.addEventListener("click", () => toggleVolumeHistory(false));
  }
  updateVolumeAnalysisOutput();
  renderVolumeHistoryList();
}

function buildSummaryCard(industry, dict) {
  if (!industry) return null;
  const card = document.createElement("article");
  card.className = "concept-summary__top-card";

  const header = document.createElement("header");
  header.className = "concept-summary__top-header";
  const title = document.createElement("h4");
  title.textContent = industry.name || "--";
  header.appendChild(title);

  if (industry.stance) {
    const stance = document.createElement("span");
    const stanceMap = dict.stanceMap || { bullish: "Bullish", watch: "Watch", bearish: "Bearish" };
    stance.className = `concept-summary__stance concept-summary__stance--${industry.stance || "watch"}`;
    stance.textContent = stanceMap[industry.stance] || industry.stance;
    header.appendChild(stance);
  }

  if (industry.confidence !== undefined && industry.confidence !== null) {
    const confidence = document.createElement("span");
    confidence.className = "concept-summary__confidence";
    const value = formatPercentValue(industry.confidence * 100, 1);
    confidence.textContent = `${dict.confidenceLabel || "Confidence"} ${value}`;
    header.appendChild(confidence);
  }

  card.appendChild(header);

  if (industry.drivers) {
    const drivers = document.createElement("p");
    drivers.className = "concept-summary__drivers";
    drivers.textContent = industry.drivers;
    card.appendChild(drivers);
  }

  const metrics = formatListValues(industry.key_metrics);
  if (metrics.length) {
    const metricsList = document.createElement("ul");
    metricsList.className = "concept-summary__chips";
    metrics.slice(0, 4).forEach((metric) => {
      const li = document.createElement("li");
      li.textContent = metric;
      metricsList.appendChild(li);
    });
    card.appendChild(metricsList);
  }

  const stocks = formatListValues(industry.leading_stocks);
  if (stocks.length) {
    const stocksLine = document.createElement("p");
    stocksLine.className = "concept-summary__meta";
    stocksLine.textContent = `${dict.leadingStocks || "Leading Stocks"}: ${stocks.join(" · ")}`;
    card.appendChild(stocksLine);
  }

  const risks = formatListValues(industry.risk_flags);
  if (risks.length) {
    const riskTitle = document.createElement("h5");
    riskTitle.className = "concept-summary__subheading";
    riskTitle.textContent = dict.riskFlags || "Risk Flags";
    card.appendChild(riskTitle);
    const riskList = document.createElement("ul");
    riskList.className = "concept-summary__list";
    risks.forEach((risk) => {
      const li = document.createElement("li");
      li.textContent = risk;
      riskList.appendChild(li);
    });
    card.appendChild(riskList);
  }

  const actions = formatListValues(industry.suggested_actions);
  if (actions.length) {
    const actionTitle = document.createElement("h5");
    actionTitle.className = "concept-summary__subheading";
    actionTitle.textContent = dict.suggestedActions || "Actions";
    card.appendChild(actionTitle);
    const actionList = document.createElement("ul");
    actionList.className = "concept-summary__list";
    actions.forEach((action) => {
      const li = document.createElement("li");
      li.textContent = action;
      actionList.appendChild(li);
    });
    card.appendChild(actionList);
  }

  return card;
}

function buildFundBlock(entry, dict) {
  const fund = entry?.fundFlow;
  if (!fund) return null;
  const block = document.createElement("section");
  block.className = "concept-summary__block";
  const title = document.createElement("h3");
  title.className = "concept-summary__block-title";
  title.textContent = dict.reasoningFundsTitle || "Fund Flow Snapshot";
  block.appendChild(title);

  const chips = document.createElement("ul");
  chips.className = "concept-summary__chips";

  const items = [
    { label: dict.reasoningFundNet || "Net Inflow", value: formatMoneyValue(fund.totalNetAmount) },
    { label: dict.reasoningFundInflow || "Inflow", value: formatMoneyValue(fund.totalInflow) },
    { label: dict.reasoningFundOutflow || "Outflow", value: formatMoneyValue(fund.totalOutflow) },
    { label: dict.reasoningFundRank || "Best Rank", value: fund.bestRank ? `#${fund.bestRank}` : "--" },
  ];

  items.forEach((item) => {
    if (!item.value || item.value === "--") return;
    const li = document.createElement("li");
    li.textContent = `${item.label} ${item.value}`;
    chips.appendChild(li);
  });

  if (!chips.children.length) {
    return null;
  }
  block.appendChild(chips);

  return block;
}

function buildMetricsBlock(entry, dict) {
  const metrics = entry?.stageMetrics || {};
  const keys = [
    { key: "latestIndex", label: dict.metricLatestIndex || "Index", formatter: (v) => formatNumberValue(v, 2) },
    { key: "change1d", label: dict.metricChange1d || "1D", formatter: (v) => formatPercentValue(v) },
    { key: "change3d", label: dict.metricChange3d || "3D", formatter: (v) => formatPercentValue(v) },
    { key: "change5d", label: dict.metricChange5d || "5D", formatter: (v) => formatPercentValue(v) },
    { key: "change10d", label: dict.metricChange10d || "10D", formatter: (v) => formatPercentValue(v) },
    { key: "change20d", label: dict.metricChange20d || "20D", formatter: (v) => formatPercentValue(v) },
  ];

  const block = document.createElement("section");
  block.className = "concept-summary__block";
  const title = document.createElement("h3");
  title.className = "concept-summary__block-title";
  title.textContent = dict.reasoningMetricsTitle || "Stage Performance";
  block.appendChild(title);

  const chips = document.createElement("ul");
  chips.className = "concept-summary__chips";
  keys.forEach((item) => {
    const value = item.formatter(metrics[item.key]);
    if (!value || value === "--") return;
    const li = document.createElement("li");
    li.textContent = `${item.label} ${value}`;
    chips.appendChild(li);
  });

  if (!chips.children.length) {
    return null;
  }
  block.appendChild(chips);
  return block;
}

function buildNewsBlock(entry, dict) {
  const news = Array.isArray(entry?.news) ? entry.news.slice(0, 3) : [];
  const block = document.createElement("section");
  block.className = "concept-summary__block";
  const title = document.createElement("h3");
  title.className = "concept-summary__block-title";
  title.textContent = dict.reasoningNewsTitle || "Related News";
  block.appendChild(title);

  if (!news.length) {
    const empty = document.createElement("p");
    empty.className = "concept-summary__text";
    empty.textContent = dict.reasoningNoNews || "No related news.";
    block.appendChild(empty);
    return block;
  }

  const list = document.createElement("ul");
  list.className = "concept-summary__list";
  news.forEach((article) => {
    const li = document.createElement("li");
    const titleNode = document.createElement(article.url ? "a" : "span");
    titleNode.textContent = article.title || article.impact_summary || dict.reasoningNewsTitle || "Article";
    titleNode.className = "concept-summary__text";
    if (article.url) {
      titleNode.href = article.url;
      titleNode.target = "_blank";
      titleNode.rel = "noopener noreferrer";
    }
    li.appendChild(titleNode);

    if (article.impact_summary || article.summary) {
      const summary = document.createElement("div");
      summary.className = "concept-summary__text";
      summary.textContent = article.impact_summary || article.summary;
      li.appendChild(summary);
    }

    if (article.published_at) {
      const meta = document.createElement("div");
      meta.className = "concept-summary__meta";
      meta.textContent = formatDateTime(article.published_at);
      li.appendChild(meta);
    }

    list.appendChild(li);
  });

  block.appendChild(list);
  return block;
}

function showChartPlaceholder(message) {
  if (!elements.chartContainer) return;
  if (state.chartInstance) {
    state.chartInstance.dispose();
    state.chartInstance = null;
  }
  elements.chartContainer.innerHTML = `<div class="empty-placeholder">${message}</div>`;
}

async function renderChart(rows) {
  if (!elements.chartContainer) return;
  if (!rows || !rows.length) {
    showChartPlaceholder(getDict().noChartData || "No chart data available.");
    return;
  }
  if (state.chartInstance) {
    state.chartInstance.dispose();
    state.chartInstance = null;
  }
  elements.chartContainer.innerHTML = "";
  const echarts = await loadEcharts();
  state.chartInstance = echarts.init(elements.chartContainer);
  window.addEventListener("resize", () => {
    if (state.chartInstance) {
      state.chartInstance.resize();
    }
  });

  const sorted = [...rows].sort((a, b) => {
    const da = new Date(a.tradeDate || a.trade_date);
    const db = new Date(b.tradeDate || b.trade_date);
    return da - db;
  });
  const categories = sorted.map((item) => formatAxisDate(item.tradeDate || item.trade_date));
  const candles = sorted.map((item) => {
    const open = toNumber(item.open);
    const close = toNumber(item.close);
    const low = toNumber(item.low);
    const high = toNumber(item.high);
    const reference = close ?? open ?? toNumber(item.preClose || item.pre_close) ?? 0;
    return [
      open ?? reference,
      close ?? reference,
      low ?? Math.min(open ?? reference, close ?? reference, reference),
      high ?? Math.max(open ?? reference, close ?? reference, reference),
    ];
  });
  const volumes = sorted.map((item) => toNumber(item.vol) ?? 0);

  const option = {
    animation: false,
    tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
    axisPointer: { link: [{ xAxisIndex: [0, 1] }] },
    dataZoom: [
      { type: "inside", xAxisIndex: [0, 1], start: 60, end: 100 },
      { type: "slider", xAxisIndex: [0, 1], top: 408, start: 60, end: 100 },
    ],
    grid: [
      { left: 50, right: 24, top: 16, height: 230 },
      { left: 50, right: 24, top: 290, height: 110 },
    ],
    xAxis: [
      { type: "category", data: categories, boundaryGap: false, axisLine: { onZero: false } },
      {
        type: "category",
        gridIndex: 1,
        data: categories,
        boundaryGap: false,
        axisLine: { onZero: false },
        axisTick: { show: false },
      },
    ],
    yAxis: [
      { scale: true, splitArea: { show: true } },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: {
          formatter: (value) => (Math.abs(value) >= 1e8 ? `${(value / 1e8).toFixed(1)}B` : value),
        },
      },
    ],
    series: [
      {
        name: state.currentIndustry || "Industry",
        type: "candlestick",
        data: candles,
        itemStyle: {
          color: "#ef5350",
          color0: "#26a69a",
          borderColor: "#ef5350",
          borderColor0: "#26a69a",
        },
      },
      {
        name: "Volume",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
        itemStyle: { color: "#90caf9" },
      },
    ],
  };

  state.chartInstance.setOption(option, true);
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatAxisDate(value) {
  if (!value) return "--";
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{8}$/.test(trimmed)) {
      return `${trimmed.slice(0, 4)}-${trimmed.slice(4, 6)}-${trimmed.slice(6, 8)}`;
    }
    return trimmed.slice(0, 10);
  }
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  return String(value).slice(0, 10);
}

function formatDateTime(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return date.toLocaleString(locale, { hour12: false });
}

function formatPercentValue(value, digits = 2) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  const sign = numeric > 0 ? "+" : "";
  return `${sign}${numeric.toFixed(digits)}%`;
}

function formatNumberValue(value, digits = 2) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return numeric.toFixed(digits);
}

function formatMoneyValue(value) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  const absValue = Math.abs(numeric);
  if (state.lang === "zh") {
    if (absValue >= 1e8) {
      return `${(numeric / 1e8).toFixed(2)}亿`;
    }
    if (absValue >= 1e4) {
      return `${(numeric / 1e4).toFixed(1)}万`;
    }
  } else {
    if (absValue >= 1e9) {
      return `${(numeric / 1e9).toFixed(2)}B`;
    }
    if (absValue >= 1e6) {
      return `${(numeric / 1e6).toFixed(1)}M`;
    }
    if (absValue >= 1e3) {
      return `${(numeric / 1e3).toFixed(1)}K`;
    }
  }
  return numeric.toFixed(2);
}

function formatListValues(values) {
  if (!Array.isArray(values) || !values.length) return [];
  return values
    .map((item) => {
      if (item === null || item === undefined) return null;
      if (typeof item === "string") return item.trim();
      if (typeof item === "number") return item.toString();
      if (typeof item === "object") {
        if (item.label) return item.label;
        if (item.name) return item.name;
        if (item.title) return item.title;
      }
      return String(item);
    })
    .filter(Boolean);
}

function updateReasoningMeta(value) {
  if (!elements.reasoningUpdated) return;
  elements.reasoningUpdated.textContent = value || "--";
}

function normalizeName(value) {
  if (!value || typeof value !== "string") return "";
  return value.trim().toLowerCase();
}

function loadEcharts() {
  if (window.echarts) {
    return Promise.resolve(window.echarts);
  }
  if (echartsLoader) {
    return echartsLoader;
  }
  echartsLoader = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = ECHARTS_CDN;
    script.async = true;
    script.onload = () => resolve(window.echarts);
    script.onerror = (error) => reject(error);
    document.head.appendChild(script);
  }).finally(() => {
    echartsLoader = null;
  });
  return echartsLoader;
}

function attachEventListeners() {
  if (elements.searchInput) {
    elements.searchInput.addEventListener("input", (event) => debounceSearch(event.currentTarget.value));
  }
  document.addEventListener("click", (event) => {
    if (
      elements.searchDropdown &&
      !elements.searchDropdown.contains(event.target) &&
      event.target !== elements.searchInput
    ) {
      elements.searchDropdown.hidden = true;
    }
  });
}

function init() {
  initLanguage();
  initTabs();
  attachEventListeners();
  initVolumeAnalysis();
  renderReasoning();
  renderIndustryNews();
  fetchWatchlist();
  ensureIndustryInsight().catch(() => {});
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}

window.__industryMarketDebugState = state;
