console.info("Concept market module v20270444");

const translations = getTranslations("conceptMarket");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";
const DEFAULT_LOOKBACK = 180;
const SEARCH_DEBOUNCE_MS = 250;
const CONSTITUENT_MAX_PAGES = null;
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";
const VOLUME_ANALYSIS_LOOKBACK = 90;

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  chartContainer: document.getElementById("concept-market-chart"),
  searchInput: document.getElementById("concept-market-search"),
  searchDropdown: document.getElementById("concept-market-search-results"),
  statusMessage: document.getElementById("concept-market-status"),
  codeLabel: document.getElementById("concept-market-code"),
  latestDate: document.getElementById("concept-market-latest-date"),
  lastSync: document.getElementById("concept-market-last-sync"),
  watchlist: document.getElementById("concept-market-watchlist"),
  constituentList: document.getElementById("concept-market-constituents"),
  tabButtons: Array.from(document.querySelectorAll(".concept-market__tab-btn")),
  tabPanels: Array.from(document.querySelectorAll(".concept-market__tab-panel")),
  volumeOutput: document.getElementById("concept-volume-output"),
  volumeRunButton: document.getElementById("concept-volume-run"),
  volumeCancelButton: document.getElementById("concept-volume-cancel"),
  volumeMeta: document.getElementById("concept-volume-meta"),
  volumeHistoryToggle: document.getElementById("concept-volume-history-toggle"),
  volumeHistoryClose: document.getElementById("concept-volume-history-close"),
  volumeHistorySection: document.getElementById("concept-volume-history"),
  volumeHistoryList: document.getElementById("concept-volume-history-list"),
};

const state = {
  lang: getInitialLanguage(),
  watchlist: [],
  searchResults: [],
  searchMessage: null,
  searchTimer: null,
  statusTimer: null,
  currentConcept: null,
  currentConceptCode: null,
  conceptStatus: null,
  chartInstance: null,
  chartRows: [],
  chartRequestToken: null,
  loadingChart: false,
  syncingConcept: null,
  constituents: [],
  constituentMeta: null,
  constituentCache: new Map(),
  constituentsLoading: false,
  constituentError: null,
  constituentsRequestToken: null,
  skipNextConstituentLoad: false,
  constituentSortKey: null,
  constituentSortOrder: "desc",
  volumeAnalysis: {
    running: false,
    content: "",
    error: null,
    controller: null,
    lastConcept: null,
    meta: null,
  },
  volumeAnalysisCache: new Map(),
  volumeHistory: {
    concept: null,
    visible: false,
    loading: false,
    error: null,
    items: [],
  },
  activeTab: "chart",
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

function formatDateTime(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  const options =
    state.lang === "zh"
      ? { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }
      : { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false };
  return date.toLocaleString(locale, options);
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
  listSections.forEach(({ key, label }) => {
    const items = payload[key];
    if (Array.isArray(items) && items.length) {
      sections.push(`【${label}】`);
      items.forEach((item, index) => {
        sections.push(`${index + 1}. ${typeof item === "object" ? JSON.stringify(item) : String(item)}`);
      });
    }
  });
  return sections.length ? sections.join("\n") : null;
}

function handleLanguageSwitch(event) {
  const lang = event.currentTarget.dataset.lang;
  if (!lang || lang === state.lang || !translations[lang]) return;
  state.lang = lang;
  persistLanguage(lang);
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("lang-btn--active", btn.dataset.lang === lang);
  });
  renderHotlist();
  renderWatchlist();
  renderChart(state.chartRows);
  renderConstituents();
  updateConceptMeta();
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

function findWatchlistEntry(concept) {
  if (!concept) return null;
  return state.watchlist.find((entry) => entry.concept === concept) || null;
}

function isConceptWatched(concept) {
  const entry = findWatchlistEntry(concept);
  return Boolean(entry && entry.isWatched !== false);
}

async function updateWatchState(concept, watch, { silent = false } = {}) {
  if (!concept) return null;
  const dict = getDict();
  const endpoint = watch
    ? `${API_BASE}/concepts/watchlist`
    : `${API_BASE}/concepts/watchlist/${encodeURIComponent(concept)}`;
  const options = watch
    ? {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ concept }),
      }
    : { method: "DELETE" };
  try {
    const response = await fetchJSON(endpoint, options);
    if (!silent) {
      setStatusMessage(
        watch ? dict.watchAdded || "Concept added to watchlist." : dict.watchRemoved || "Concept removed.",
        "success"
      );
    }
    await fetchWatchlist();
    return response;
  } catch (error) {
    console.error("Failed to update watchlist", error);
    setStatusMessage(dict.watchError || "Failed to update watchlist.", "error");
    throw error;
  }
}

async function ensureConceptInWatchlist(concept) {
  if (!concept) return;
  if (isConceptWatched(concept)) {
    return;
  }
  try {
    await updateWatchState(concept, true, { silent: true });
  } catch (error) {
    console.error("Failed to auto-add watch entry", error);
  }
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
    const data = await fetchJSON(`${API_BASE}/concepts/search?q=${encodeURIComponent(keyword)}&limit=20`);
    state.searchResults = Array.isArray(data.items) ? data.items : [];
    state.searchMessage = null;
    renderSearchResults();
  } catch (error) {
    console.error("Concept search failed", error);
    state.searchResults = [];
    state.searchMessage = getDict().searchFailed || "Failed to load concepts.";
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
      (state.searchResults.length === 0 ? getDict().searchNoResult || "No matching concepts." : "");
    container.appendChild(empty);
    container.hidden = false;
    return;
  }
  state.searchResults.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.innerHTML = `<strong>${item.name}</strong><span>${item.code}</span>`;
    button.addEventListener("click", () => {
      selectConcept(item.name, item.code);
      container.hidden = true;
      elements.searchInput.value = item.name;
    });
    container.appendChild(button);
  });
  container.hidden = false;
}

function selectConcept(name, code, source = "search") {
  if (!name) return;
  state.currentConcept = name;
  state.currentConceptCode = code || null;
  if (source === "search") {
    ensureConceptInWatchlist(name);
  }
  // concept title shows the selected name directly
  if (elements.codeLabel) {
    elements.codeLabel.textContent = state.currentConceptCode || "--";
  }
  const chartTitle = document.getElementById("concept-market-chart-title");
  if (chartTitle) {
    chartTitle.textContent = name || getDict().chartTitle;
  }
  updateWatchlistHighlight();
  setStatusMessage(getDict().loading || "Loading…", "info");
  loadConceptStatus(name).finally(() => {
    setStatusMessage("", null);
  });
  if (source !== "chart") {
    loadConceptHistory(name);
  }
  if (state.skipNextConstituentLoad) {
    state.skipNextConstituentLoad = false;
  } else {
    loadConceptConstituents(name);
  }
  applyVolumeAnalysisCache(name);
  state.volumeHistory.items = [];
  state.volumeHistory.error = null;
  state.volumeHistory.loading = false;
  state.volumeHistory.concept = null;
  toggleVolumeHistory(false);
  renderVolumeHistoryList();
  fetchLatestVolumeAnalysis(name, { force: true });
}

async function loadConceptStatus(concept) {
  try {
    const data = await fetchJSON(`${API_BASE}/concepts/status?concept=${encodeURIComponent(concept)}`);
    state.conceptStatus = data;
    if (!state.currentConceptCode) {
      state.currentConceptCode = data.conceptCode || null;
    }
    if (elements.codeLabel) {
      elements.codeLabel.textContent = state.currentConceptCode || "--";
    }
    updateConceptMeta();
  } catch (error) {
    console.error("Concept status fetch failed", error);
    setStatusMessage(getDict().statusError || "Failed to load concept status.", "error");
  }
}

function updateConceptMeta() {
  const dict = getDict();
  if (elements.latestDate) {
    elements.latestDate.textContent = formatDateOnly(state.conceptStatus?.latestTradeDate);
  }
  if (elements.lastSync) {
    elements.lastSync.textContent = formatDate(state.conceptStatus?.lastSyncedAt);
  }
}

async function refreshConcept() {
  if (!state.currentConcept) {
    setStatusMessage(getDict().noConceptSelected || "Select a concept first.", "error");
    return;
  }
  if (state.syncingConcept === state.currentConcept) {
    return;
  }
  const dict = getDict();
  setStatusMessage(dict.refreshing || "Refreshing…", "info");
  state.syncingConcept = state.currentConcept;
  try {
    const payload = {
      concept: state.currentConcept,
      lookbackDays: DEFAULT_LOOKBACK,
    };
    const result = await fetchJSON(`${API_BASE}/concepts/refresh-history`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.conceptStatus = {
      concept: result.concept,
      conceptCode: result.conceptCode,
      isWatched: result.isWatched ?? state.conceptStatus?.isWatched ?? false,
      lastSyncedAt: result.lastSyncedAt,
      latestTradeDate: result.latestTradeDate,
    };
    updateConceptMeta();
    await fetchWatchlist();
    loadConceptHistory(state.currentConcept);
    await loadConceptConstituents(state.currentConcept, { force: true, refresh: true });
    setStatusMessage(dict.refreshDone || "Concept updated.", "success");
  } catch (error) {
    console.error("Refresh concept failed", error);
    setStatusMessage(dict.refreshFailed || "Failed to refresh concept.", "error");
  } finally {
    state.syncingConcept = null;
  }
}

async function fetchWatchlist() {
  try {
    const data = await fetchJSON(`${API_BASE}/concepts/watchlist`);
    state.watchlist = Array.isArray(data.items) ? data.items : [];
    renderWatchlist();
  } catch (error) {
    console.error("Watchlist fetch failed", error);
  }
  return state.watchlist;
}

function renderWatchlist() {
  const container = elements.watchlist;
  if (!container) return;
  container.innerHTML = "";
  const dict = getDict();
  if (!state.watchlist.length) {
    container.dataset.emptyI18n = dict.watchlistEmpty || "暂无监控概念";
    container.setAttribute("data-empty", "1");
    state.currentConcept = null;
    state.currentConceptCode = null;
    state.conceptStatus = null;
    updateConceptMeta();
    showChartPlaceholder(dict.noChartData || "No candlestick data available.");
    state.constituents = [];
    state.constituentMeta = null;
    state.constituentError = null;
    renderConstituents();
    applyVolumeAnalysisCache(null);
    resetVolumeHistoryState();
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
    card.dataset.concept = entry.concept;
    card.dataset.code = entry.conceptCode || "";
    card.setAttribute("role", "button");
    card.tabIndex = 0;
    if (state.currentConcept === entry.concept) {
      card.classList.add("is-active");
      hasActive = true;
    }
    card.innerHTML = `
      <div class="concept-market__watch-item-title">
        <span class="concept-market__watch-name">${entry.concept || "--"}</span>
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
      </div>
    `;
    const selectFromCard = () => selectConcept(entry.concept, entry.conceptCode, "watchlist");
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
        syncButton.disabled = true;
        syncButton.classList.add("is-syncing");
        syncButton.setAttribute("aria-busy", "true");
        state.skipNextConstituentLoad = true;
        selectConcept(entry.concept, entry.conceptCode, "watchlist");
        try {
          await refreshConcept();
        } finally {
          syncButton.disabled = false;
          syncButton.classList.remove("is-syncing");
          syncButton.removeAttribute("aria-busy");
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
          await updateWatchState(entry.concept, nextState);
        } catch {
          /* status message already set */
        } finally {
          favoriteButton.disabled = false;
        }
      });
    }
    fragment.appendChild(card);
  });
  container.appendChild(fragment);
  if (!state.currentConcept || !hasActive) {
    const fallback = entries[0];
    if (fallback) {
      selectConcept(fallback.concept, fallback.conceptCode, "watchlist");
    }
  } else {
    updateWatchlistHighlight();
  }
}

function setActiveTab(tab, { force = false } = {}) {
  if (!tab || (!force && tab === state.activeTab)) {
    return;
  }
  state.activeTab = tab;
  if (elements.tabButtons) {
    elements.tabButtons.forEach((button) => {
      const isActive = button.dataset.tab === tab;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
      button.tabIndex = isActive ? 0 : -1;
    });
  }
  if (elements.tabPanels) {
    elements.tabPanels.forEach((panel) => {
      const isActive = panel.dataset.tabPanel === tab;
      panel.classList.toggle("is-active", isActive);
      panel.hidden = !isActive;
    });
  }
  if (tab === "chart" && state.chartInstance) {
    window.requestAnimationFrame(() => {
      if (state.chartInstance) {
        state.chartInstance.resize();
      }
    });
  }
  if (tab === "volume") {
    ensureVolumeAnalysisLoaded();
    updateVolumeAnalysisOutput();
  }
}

function initTabs() {
  if (!elements.tabButtons || !elements.tabButtons.length) return;
  elements.tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const tab = button.dataset.tab;
      if (tab) {
        setActiveTab(tab);
      }
    });
  });
  setActiveTab(state.activeTab, { force: true });
}

function normalizeVolumeRecord(record) {
  if (!record) return null;
  const parsedSummary = parseJSON(record.summary) || record.summary;
  return {
    id: record.id,
    concept: record.concept,
    conceptCode: record.conceptCode,
    lookbackDays: record.lookbackDays,
    summary: parsedSummary || {},
    rawText: record.rawText || "",
    model: record.model || null,
    generatedAt: record.generatedAt,
  };
}

async function fetchLatestVolumeAnalysis(concept, { force = false } = {}) {
  if (!concept) {
    applyVolumeAnalysisCache(null);
    return;
  }
  if (!force) {
    const cached = state.volumeAnalysisCache.get(concept);
    if (cached && !state.volumeAnalysis.running) {
      state.volumeAnalysis.content = cached.content || "";
      state.volumeAnalysis.meta = cached.meta || null;
      state.volumeAnalysis.error = null;
      state.volumeAnalysis.lastConcept = concept;
      updateVolumeAnalysisOutput();
      return;
    }
  }
  try {
    const data = await fetchJSON(
      `${API_BASE}/concepts/volume-price-analysis/latest?concept=${encodeURIComponent(concept)}`
    );
    const record = normalizeVolumeRecord(data);
    if (record) {
      const formatted = formatVolumeSummary(record.summary) || record.rawText || "";
      state.volumeAnalysisCache.set(concept, { content: formatted, meta: record });
      if (state.currentConcept === concept && !state.volumeAnalysis.running) {
        state.volumeAnalysis.content = formatted;
        state.volumeAnalysis.meta = record;
        state.volumeAnalysis.error = null;
        state.volumeAnalysis.lastConcept = concept;
        updateVolumeAnalysisOutput();
      }
    } else if (state.currentConcept === concept && !state.volumeAnalysis.running) {
      state.volumeAnalysis.content = "";
      state.volumeAnalysis.meta = null;
      state.volumeAnalysis.error = null;
      state.volumeAnalysis.lastConcept = concept;
      updateVolumeAnalysisOutput();
    }
  } catch (error) {
    if (error && typeof error.message === "string" && error.message.includes("404")) {
      state.volumeAnalysisCache.set(concept, null);
      if (state.currentConcept === concept && !state.volumeAnalysis.running) {
        state.volumeAnalysis.content = "";
        state.volumeAnalysis.meta = null;
        state.volumeAnalysis.error = null;
        state.volumeAnalysis.lastConcept = concept;
        updateVolumeAnalysisOutput();
      }
      return;
    }
    console.error("Failed to load latest volume analysis", error);
  }
}

function ensureVolumeAnalysisLoaded() {
  if (!state.currentConcept) {
    applyVolumeAnalysisCache(null);
    return;
  }
  const cached = state.volumeAnalysisCache.get(state.currentConcept);
  if (cached && !state.volumeAnalysis.running) {
    state.volumeAnalysis.content = cached.content || "";
    state.volumeAnalysis.meta = cached.meta || null;
    state.volumeAnalysis.error = null;
    state.volumeAnalysis.lastConcept = state.currentConcept;
    updateVolumeAnalysisOutput();
    return;
  }
  fetchLatestVolumeAnalysis(state.currentConcept, { force: true });
}

async function fetchVolumeHistory(concept, { force = false } = {}) {
  if (!concept) {
    state.volumeHistory.items = [];
    state.volumeHistory.error = null;
    state.volumeHistory.concept = null;
    renderVolumeHistoryList();
    return;
  }
  if (!force && state.volumeHistory.concept === concept && state.volumeHistory.items.length) {
    renderVolumeHistoryList();
    return;
  }
  state.volumeHistory.loading = true;
  state.volumeHistory.error = null;
  state.volumeHistory.concept = concept;
  renderVolumeHistoryList();
  try {
    const data = await fetchJSON(
      `${API_BASE}/concepts/volume-price-analysis/history?concept=${encodeURIComponent(concept)}&limit=10`
    );
    state.volumeHistory.items = Array.isArray(data.items) ? data.items : [];
    state.volumeHistory.error = null;
  } catch (error) {
    console.error("Failed to load volume analysis history", error);
    state.volumeHistory.items = [];
    state.volumeHistory.error = getDict().volumeHistoryError || "历史记录获取失败。";
  } finally {
    state.volumeHistory.loading = false;
    renderVolumeHistoryList();
  }
}

function toggleVolumeHistory(show) {
  const visible = Boolean(show);
  state.volumeHistory.visible = visible;
  if (elements.volumeHistorySection) {
    elements.volumeHistorySection.hidden = !visible;
  }
  if (visible && state.currentConcept) {
    fetchVolumeHistory(state.currentConcept, { force: true });
  }
}

function resetVolumeHistoryState() {
  state.volumeHistory.concept = null;
  state.volumeHistory.items = [];
  state.volumeHistory.error = null;
  state.volumeHistory.loading = false;
  state.volumeHistory.visible = false;
  if (elements.volumeHistorySection) {
    elements.volumeHistorySection.hidden = true;
  }
  renderVolumeHistoryList();
}

function renderVolumeHistoryList() {
  const container = elements.volumeHistoryList;
  if (!container) return;
  const dict = getDict();
  container.innerHTML = "";
  container.removeAttribute("data-empty");
  if (state.volumeHistory.loading) {
    container.textContent = dict.volumeHistoryLoading || "加载历史记录…";
    return;
  }
  if (state.volumeHistory.error) {
    container.textContent = state.volumeHistory.error;
    container.dataset.empty = "1";
    return;
  }
  if (!state.volumeHistory.items.length) {
    container.dataset.empty = "1";
    return;
  }
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

function applyVolumeHistoryEntry(entry) {
  if (!entry) return;
  cancelVolumeAnalysis({ silent: true });
  state.volumeAnalysis.content = formatVolumeSummary(entry.summary) || entry.rawText || "";
  state.volumeAnalysis.meta = entry;
  state.volumeAnalysis.error = null;
  state.volumeAnalysis.lastConcept = state.currentConcept;
  if (entry.concept) {
    state.volumeAnalysisCache.set(entry.concept, { content: state.volumeAnalysis.content, meta: entry });
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
    state.volumeAnalysis.error = getDict().volumeCancelled || "分析已取消。";
  }
  updateVolumeAnalysisOutput();
}

function applyVolumeAnalysisCache(concept) {
  cancelVolumeAnalysis({ silent: true });
  if (!concept) {
    state.volumeAnalysis.content = "";
    state.volumeAnalysis.error = null;
    state.volumeAnalysis.meta = null;
    state.volumeAnalysis.lastConcept = null;
    updateVolumeAnalysisOutput();
    return;
  }
  const cached = state.volumeAnalysisCache.get(concept);
  if (cached) {
    state.volumeAnalysis.content = cached.content || "";
    state.volumeAnalysis.meta = cached.meta || null;
  } else {
    state.volumeAnalysis.content = "";
    state.volumeAnalysis.meta = null;
  }
  state.volumeAnalysis.error = null;
  state.volumeAnalysis.lastConcept = concept;
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
    container.textContent = dict.volumeEmpty || "请选择概念后点击推理。";
    container.dataset.empty = "1";
  }
  if (elements.volumeRunButton) {
    elements.volumeRunButton.disabled = state.volumeAnalysis.running || !state.currentConcept;
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
      const timestamp = formatDateTime(meta.generatedAt);
      const daysLabel = meta.lookbackDays ? `${meta.lookbackDays}d` : "";
      const modelLabel = meta.model || "DeepSeek";
      const segments = [label, timestamp, daysLabel, modelLabel].filter(Boolean);
      elements.volumeMeta.textContent = segments.join(" · ");
      elements.volumeMeta.dataset.empty = "0";
    } else {
      elements.volumeMeta.textContent = dict.volumeMetaEmpty || "暂无历史记录";
      elements.volumeMeta.dataset.empty = "1";
    }
  }
}

async function runVolumeAnalysis() {
  if (!state.currentConcept) {
    setStatusMessage(getDict().noConceptSelected || "Select a concept first.", "error");
    return;
  }
  const targetConcept = state.currentConcept;
  cancelVolumeAnalysis({ silent: true });
  const controller = new AbortController();
  state.volumeAnalysis.running = true;
  state.volumeAnalysis.controller = controller;
  state.volumeAnalysis.error = null;
  state.volumeAnalysis.content = "";
  state.volumeAnalysis.meta = null;
  state.volumeAnalysis.lastConcept = state.currentConcept;
  updateVolumeAnalysisOutput();
  try {
    const response = await fetch(`${API_BASE}/concepts/volume-price-analysis`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        concept: state.currentConcept,
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
    if (targetConcept) {
      await fetchLatestVolumeAnalysis(targetConcept, { force: true });
      if (state.volumeHistory.visible && targetConcept === state.currentConcept) {
        fetchVolumeHistory(targetConcept, { force: true });
      }
    }
  } catch (error) {
    const aborted = controller.signal.aborted || (error && error.name === "AbortError");
    if (aborted) {
      state.volumeAnalysis.error = getDict().volumeCancelled || "分析已取消。";
    } else {
      console.error("Volume-price reasoning failed", error);
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
  renderVolumeHistoryList();
  updateVolumeAnalysisOutput();
}

function updateWatchlistHighlight() {
  if (!elements.watchlist) return;
  const cards = elements.watchlist.querySelectorAll(".concept-market__watch-item");
  cards.forEach((card) => {
    if (card.dataset.concept === state.currentConcept) {
      card.classList.add("is-active");
    } else {
      card.classList.remove("is-active");
    }
  });
}

function renderConstituents() {
  const container = elements.constituentList;
  if (!container) return;
  const dict = getDict();
  container.innerHTML = "";
  if (state.constituentsLoading) {
    container.innerHTML = `<div class="concept-market__constituent-status">${dict.constituentLoading || "Loading constituents…"}</div>`;
    return;
  }
  if (state.constituentError) {
    container.innerHTML = `<div class="concept-market__constituent-status is-error">${state.constituentError}</div>`;
    return;
  }
  if (!state.constituents.length) {
    const message = dict.constituentEmpty || "No constituent snapshot.";
    container.innerHTML = `<div class="concept-market__constituent-status">${message}</div>`;
    return;
  }
  const table = document.createElement("table");
  table.className = "concept-market__constituent-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>${dict.constituentRank || "#"}</th>
        <th>${dict.constituentName || "Name"}</th>
        <th>${dict.constituentCode || "Code"}</th>
        <th>${dict.constituentPrice || "Price"}</th>
        <th>
          <button type="button" class="concept-market__sort-btn" data-sort-key="changePercent">
            <span>${dict.constituentChange || "Change"}</span>
            <span class="concept-market__sort-icon" aria-hidden="true"></span>
          </button>
        </th>
        <th>
          <button type="button" class="concept-market__sort-btn" data-sort-key="turnoverAmount">
            <span>${dict.constituentTurnover || "Turnover"}</span>
            <span class="concept-market__sort-icon" aria-hidden="true"></span>
          </button>
        </th>
      </tr>
    </thead>
  `;
  const tbody = document.createElement("tbody");
  const rows = getSortedConstituents();
  rows.forEach((item) => {
    const tr = document.createElement("tr");
    const changeValue = item.changePercent;
    const turnoverValue = item.turnoverAmount;
    tr.innerHTML = `
      <td>${item.rank ?? "--"}</td>
      <td>${item.name || "--"}</td>
      <td>${item.symbol || "--"}</td>
      <td>${item.lastPrice != null ? Number(item.lastPrice).toFixed(2) : "--"}</td>
      <td>${changeValue != null ? `${Number(changeValue).toFixed(2)}%` : "--"}</td>
      <td>${turnoverValue != null ? formatMoney(turnoverValue) : "--"}</td>
    `;
    const changeCell = tr.children[4];
    if (changeValue != null) {
      const numeric = Number(changeValue);
      if (Number.isFinite(numeric)) {
        changeCell.dataset.trend = numeric > 0 ? "up" : numeric < 0 ? "down" : "flat";
      }
    }
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  container.appendChild(table);
  attachConstituentSortHandlers(table);
}

function getSortedConstituents() {
  if (!Array.isArray(state.constituents) || !state.constituents.length) {
    return [];
  }
  const key = state.constituentSortKey;
  if (!key) {
    return [...state.constituents];
  }
  const direction = state.constituentSortOrder === "asc" ? 1 : -1;
  return [...state.constituents].sort((a, b) => {
    const aValue = toNumber(a?.[key]);
    const bValue = toNumber(b?.[key]);
    if (aValue === null && bValue === null) return 0;
    if (aValue === null) return 1;
    if (bValue === null) return -1;
    if (aValue === bValue) return 0;
    return aValue > bValue ? direction : -direction;
  });
}

function attachConstituentSortHandlers(table) {
  const buttons = table.querySelectorAll(".concept-market__sort-btn[data-sort-key]");
  buttons.forEach((button) => {
    const key = button.dataset.sortKey;
    button.dataset.sortActive = state.constituentSortKey === key ? "1" : "0";
    button.dataset.sortOrder =
      state.constituentSortKey === key ? state.constituentSortOrder || "desc" : "desc";
    button.addEventListener("click", () => handleConstituentSort(key));
  });
}

function handleConstituentSort(key) {
  if (!key) return;
  if (state.constituentSortKey === key) {
    state.constituentSortOrder = state.constituentSortOrder === "asc" ? "desc" : "asc";
  } else {
    state.constituentSortKey = key;
    state.constituentSortOrder = "desc";
  }
  renderConstituents();
}

async function loadConceptConstituents(concept, { force = false, refresh = false } = {}) {
  if (!concept || !elements.constituentList) return;
  if (!force && state.constituentCache.has(concept)) {
    const cached = state.constituentCache.get(concept);
    state.constituents = cached.items;
    state.constituentMeta = cached.meta;
    state.constituentError = null;
    state.constituentsLoading = false;
    renderConstituents();
    return;
  }
  const token = Symbol("constituents");
  state.constituentsRequestToken = token;
  state.constituentsLoading = true;
  state.constituentError = null;
  renderConstituents();
  try {
    const params = new URLSearchParams({ concept, refresh: refresh ? "1" : "0" });
    if (Number.isFinite(CONSTITUENT_MAX_PAGES) && CONSTITUENT_MAX_PAGES > 0) {
      params.set("maxPages", String(CONSTITUENT_MAX_PAGES));
    }
    const response = await fetchJSON(`${API_BASE}/concepts/constituents?${params.toString()}`);
    if (state.constituentsRequestToken !== token) {
      return;
    }
    const items = Array.isArray(response.items) ? response.items : [];
    state.constituents = items;
    state.constituentMeta = response;
    state.constituentCache.set(concept, { items, meta: response });
    state.constituentError = null;
  } catch (error) {
    console.error("Failed to load concept constituents", error);
    if (state.constituentsRequestToken !== token) {
      return;
    }
    state.constituents = [];
    state.constituentMeta = null;
    state.constituentError = getDict().constituentFetchError || "Failed to load constituents.";
  } finally {
    if (state.constituentsRequestToken === token) {
      state.constituentsLoading = false;
      state.constituentsRequestToken = null;
      renderConstituents();
    }
  }
}

function formatMoney(value) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  const abs = Math.abs(numeric);
  if (abs >= 1e8) {
    return `${(numeric / 1e8).toFixed(2)}${state.lang === "zh" ? "亿" : "B"}`;
  }
  if (abs >= 1e4) {
    return `${(numeric / 1e4).toFixed(1)}${state.lang === "zh" ? "万" : "K"}`;
  }
  return numeric.toFixed(2);
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return `${numeric.toFixed(digits)}%`;
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

async function loadConceptHistory(concept) {
  if (!concept || !elements.chartContainer) return;
  const dict = getDict();
  elements.chartContainer.dataset.loading = "1";
  state.loadingChart = true;
  const token = Symbol("conceptChart");
  state.chartRequestToken = token;
  try {
    const response = await fetchJSON(
      `${API_BASE}/market/concept-index-history?concept=${encodeURIComponent(concept)}&limit=240`
    );
    if (state.chartRequestToken !== token) {
      return;
    }
    const rows = Array.isArray(response.rows) ? response.rows : [];
    state.chartRows = rows;
    if (rows.length) {
      await renderChart(rows);
    } else {
      showChartPlaceholder(dict.noChartData || "No candlestick data available.");
    }
  } catch (error) {
    console.error("Failed to load concept index history", error);
    if (state.chartRequestToken === token) {
      showChartPlaceholder(dict.fetchFailed || "Failed to load concept data.");
    }
  } finally {
    if (state.chartRequestToken === token) {
      delete elements.chartContainer.dataset.loading;
      state.loadingChart = false;
    }
  }
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
    showChartPlaceholder(getDict().noChartData || "No candlestick data available.");
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
        name: state.currentConcept || "Concept",
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
  attachEventListeners();
  initTabs();
  initVolumeAnalysis();
  renderConstituents();
  fetchWatchlist();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}

window.__conceptMarketDebugState = state;
