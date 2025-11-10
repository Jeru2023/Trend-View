const translations = getTranslations("controlPanel");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

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
  return translations[currentLang] || translations.zh || translations.en;
}

let currentLang = getInitialLanguage();
let toastTimer = null;

const CONCEPT_SEARCH_DEBOUNCE_MS = 220;

const configState = {
  includeST: false,
  includeDelisted: false,
  dailyTradeWindowDays: 365,
  peripheralAggregateTime: "06:00",
  globalFlashFrequencyMinutes: 180,
  conceptAliasMap: {},
  volumeSurge: {
    minVolumeRatio: 3,
    breakoutPercent: 3,
    dailyChangePercent: 7,
    maxRangePercent: 25,
  },
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  includeSt: document.getElementById("config-include-st"),
  includeDelisted: document.getElementById("config-include-delisted"),
  windowDays: document.getElementById("config-window"),
  windowSkeleton: document.querySelector("[data-skeleton=window]"),
  peripheralAggregateTime: document.getElementById("config-peripheral-aggregate-time"),
  peripheralSkeleton: document.querySelector("[data-skeleton=peripheral]"),
  globalFlashFrequency: document.getElementById("config-global-flash-frequency"),
  globalFlashSkeleton: document.querySelector("[data-skeleton=global-flash]"),
  volumeSurgeRatio: document.getElementById("config-volume-surge-ratio"),
  volumeSurgeBreakout: document.getElementById("config-volume-surge-breakout"),
  volumeSurgeDailyChange: document.getElementById("config-volume-surge-daily-change"),
  volumeSurgeRange: document.getElementById("config-volume-surge-range"),
  toastContainer: document.getElementById("config-toast-container"),
  toast: document.getElementById("config-toast"),
  toastMessage: document.getElementById("config-toast-message"),
  saveButton: document.getElementById("save-config"),
  conceptAliasRows: document.getElementById("concept-alias-rows"),
  conceptAliasAdd: document.getElementById("concept-alias-add-row"),
  conceptAliasDirectory: document.getElementById("concept-alias-directory"),
};

const aliasState = {
  directory: [],
  loadingDirectory: false,
};

const conceptSearchState = {
  timer: null,
  lastQuery: "",
};

function sanitizeNumber(value, fallback, { min = -Infinity, max = Infinity } = {}) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(Math.max(numeric, min), max);
}

function showSkeleton(skeleton, input, isLoading) {
  if (skeleton) {
    skeleton.classList.toggle("hidden", !isLoading);
  }
  if (input) {
    input.classList.toggle("hidden", isLoading);
  }
}

async function fetchConceptSuggestions(keyword) {
  const query = keyword.trim();
  if (!query) return [];
  const response = await fetch(`${API_BASE}/concepts/search?q=${encodeURIComponent(query)}&limit=20`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const data = await response.json();
  return Array.isArray(data.items) ? data.items : [];
}

function normalizeAliasMapPayload(map) {
  if (!map || typeof map !== "object") {
    return {};
  }
  const normalized = {};
  Object.keys(map).forEach((concept) => {
    if (!concept) return;
    const value = map[concept];
    let tokens = [];
    if (Array.isArray(value)) {
      tokens = value;
    } else if (typeof value === "string") {
      tokens = value.split(/\s+/g);
    }
    const cleaned = tokens
      .map((token) => String(token || "").trim())
      .filter((token, index, arr) => token && arr.indexOf(token) === index);
    if (cleaned.length) {
      normalized[concept] = cleaned;
    }
  });
  return normalized;
}

function refreshConceptDirectoryOptions() {
  if (!elements.conceptAliasDirectory) return;
  elements.conceptAliasDirectory.innerHTML = "";
  aliasState.directory.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.name;
    option.label = item.code ? `${item.name} (${item.code})` : item.name;
    elements.conceptAliasDirectory.appendChild(option);
  });
}

function updateAliasInputPlaceholders() {
  document.querySelectorAll("[data-i18n-placeholder]").forEach((node) => {
    const key = node.dataset.i18nPlaceholder;
    if (key && getDict()[key]) {
      node.setAttribute("placeholder", getDict()[key]);
    }
  });
}

function updateAliasEmptyState() {
  if (!elements.conceptAliasRows) return;
  const hasRow = elements.conceptAliasRows.querySelector(".concept-alias-row");
  let emptyNode = elements.conceptAliasRows.querySelector(".concept-alias-empty");
  if (hasRow) {
    if (emptyNode) emptyNode.remove();
    return;
  }
  if (!emptyNode) {
    emptyNode = document.createElement("p");
    emptyNode.className = "concept-alias-empty";
    emptyNode.dataset.i18n = "conceptAliasEmpty";
    elements.conceptAliasRows.appendChild(emptyNode);
  }
  emptyNode.textContent = getDict().conceptAliasEmpty || "No alias mappings yet.";
}

function createAliasRow({ concept = "", aliases = [] } = {}) {
  if (!elements.conceptAliasRows) return;
  const row = document.createElement("div");
  row.className = "concept-alias-row";

  const conceptField = document.createElement("div");
  conceptField.className = "concept-alias-field";
  const conceptLabel = document.createElement("label");
  conceptLabel.dataset.i18n = "conceptAliasSelectLabel";
  conceptLabel.textContent = getDict().conceptAliasSelectLabel || "Concept";
  const conceptInput = document.createElement("input");
  conceptInput.type = "text";
  conceptInput.dataset.role = "concept-input";
  conceptInput.dataset.i18nPlaceholder = "conceptAliasSelectPlaceholder";
  conceptInput.setAttribute("list", "concept-alias-directory");
  conceptInput.value = concept || "";
  conceptField.appendChild(conceptLabel);
  conceptField.appendChild(conceptInput);
  const searchContainer = document.createElement("div");
  searchContainer.className = "concept-alias-search is-hidden";
  searchContainer.dataset.role = "concept-search";
  conceptField.appendChild(searchContainer);
  attachConceptInputHandlers(conceptInput);

  const aliasField = document.createElement("div");
  aliasField.className = "concept-alias-field";
  const aliasLabel = document.createElement("label");
  aliasLabel.dataset.i18n = "conceptAliasInputLabel";
  aliasLabel.textContent = getDict().conceptAliasInputLabel || "Aliases";
  const aliasInput = document.createElement("input");
  aliasInput.type = "text";
  aliasInput.dataset.role = "concept-alias-input";
  aliasInput.dataset.i18nPlaceholder = "conceptAliasInputPlaceholder";
  aliasInput.value = Array.isArray(aliases) ? aliases.join(" ") : "";
  aliasField.appendChild(aliasLabel);
  aliasField.appendChild(aliasInput);

  const removeButton = document.createElement("button");
  removeButton.type = "button";
  removeButton.className = "concept-alias-remove";
  removeButton.dataset.i18n = "conceptAliasRemoveRow";
  removeButton.textContent = getDict().conceptAliasRemoveRow || "Remove";
  removeButton.addEventListener("click", () => {
    row.remove();
    updateAliasEmptyState();
  });

  row.appendChild(conceptField);
  row.appendChild(aliasField);
  row.appendChild(removeButton);

  elements.conceptAliasRows.appendChild(row);
  updateAliasInputPlaceholders();
  updateAliasEmptyState();
}

function attachConceptInputHandlers(inputEl) {
  inputEl.addEventListener("input", () => scheduleConceptSearch(inputEl));
  inputEl.addEventListener("focus", () => scheduleConceptSearch(inputEl, { immediate: true }));
  inputEl.addEventListener("blur", () => {
    window.setTimeout(() => hideConceptSearch(inputEl), 150);
  });
}

function hideConceptSearch(inputEl) {
  const row = inputEl.closest(".concept-alias-row");
  const container = row?.querySelector('[data-role="concept-search"]');
  if (container) {
    container.innerHTML = "";
    container.classList.add("is-hidden");
  }
}

function renderConceptSearchResults(inputEl, results) {
  const row = inputEl.closest(".concept-alias-row");
  const container = row?.querySelector('[data-role="concept-search"]');
  if (!container) return;
  container.innerHTML = "";
  if (!results || !results.length) {
    container.classList.remove("is-hidden");
    const empty = document.createElement("div");
    empty.className = "concept-alias-search__dropdown";
    const message = document.createElement("p");
    message.className = "concept-summary__text";
    message.style.margin = "0";
    message.style.padding = "6px 12px";
    message.textContent = getDict().conceptAliasNoMatch || "No related concept found.";
    empty.appendChild(message);
    container.appendChild(empty);
    return;
  }
  container.classList.remove("is-hidden");
  const list = document.createElement("div");
  list.className = "concept-alias-search__dropdown";
  results.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "concept-alias-search__item";
    const nameSpan = document.createElement("span");
    nameSpan.textContent = item.name || "--";
    const codeSpan = document.createElement("span");
    codeSpan.className = "concept-alias-search__item-code";
    codeSpan.textContent = item.code || "";
    button.appendChild(nameSpan);
    if (item.code) {
      button.appendChild(codeSpan);
    }
    button.addEventListener("click", () => {
      inputEl.value = item.name || "";
      hideConceptSearch(inputEl);
    });
    list.appendChild(button);
  });
  container.appendChild(list);
}

function scheduleConceptSearch(inputEl, { immediate = false } = {}) {
  if (conceptSearchState.timer) {
    clearTimeout(conceptSearchState.timer);
    conceptSearchState.timer = null;
  }
  const query = inputEl.value.trim();
  if (!query) {
    hideConceptSearch(inputEl);
    return;
  }
  const execute = () => performConceptSearch(query, inputEl);
  if (immediate) {
    execute();
  } else {
    conceptSearchState.timer = window.setTimeout(execute, CONCEPT_SEARCH_DEBOUNCE_MS);
  }
}

async function performConceptSearch(query, inputEl) {
  try {
    const results = await fetchConceptSuggestions(query);
    renderConceptSearchResults(inputEl, results);
  } catch (error) {
    console.error("Concept search failed", error);
    renderConceptSearchResults(inputEl, []);
  }
}

function resetAliasRows(aliasMap) {
  if (!elements.conceptAliasRows) return;
  elements.conceptAliasRows.innerHTML = "";
  const entries = aliasMap ? Object.entries(aliasMap) : [];
  if (entries.length) {
    entries.forEach(([concept, aliases]) => {
      createAliasRow({ concept, aliases });
    });
  } else {
    updateAliasEmptyState();
  }
}

function collectAliasMap() {
  if (!elements.conceptAliasRows) return {};
  const rows = elements.conceptAliasRows.querySelectorAll(".concept-alias-row");
  const map = {};
  rows.forEach((row) => {
    const selectEl = row.querySelector('input[data-role="concept-input"]');
    const inputEl = row.querySelector('input[data-role="concept-alias-input"]');
    if (!selectEl || !inputEl) return;
    const concept = selectEl.value.trim();
    if (!concept) return;
    const aliases = inputEl.value
      .split(/\s+/g)
      .map((token) => token.trim())
      .filter((token, index, arr) => token && arr.indexOf(token) === index);
    if (aliases.length) {
      map[concept] = aliases;
    }
  });
  return map;
}

function handleAddAliasRow() {
  createAliasRow();
}

async function loadConceptDirectory() {
  if (aliasState.loadingDirectory) return;
  aliasState.loadingDirectory = true;
  try {
    const response = await fetch(`${API_BASE}/concepts/search?limit=500`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    aliasState.directory = Array.isArray(data.items) ? data.items : [];
    aliasState.directory.sort((a, b) => a.name.localeCompare(b.name, "zh-CN"));
  } catch (error) {
    console.error("Failed to load concept directory", error);
    aliasState.directory = [];
  } finally {
    aliasState.loadingDirectory = false;
    refreshConceptDirectoryOptions();
  }
}

function renderToast(message, tone = "") {
  if (!elements.toastContainer || !elements.toast || !elements.toastMessage) {
    return;
  }
  if (toastTimer) {
    clearTimeout(toastTimer);
    toastTimer = null;
  }
  if (!message) {
    elements.toastContainer.classList.add("hidden");
    elements.toast.classList.remove("toast--success", "toast--error");
    elements.toastMessage.textContent = "";
    return;
  }
  elements.toastContainer.classList.remove("hidden");
  elements.toast.classList.remove("toast--success", "toast--error");
  if (tone === "success") {
    elements.toast.classList.add("toast--success");
  } else if (tone === "error") {
    elements.toast.classList.add("toast--error");
  }
  elements.toastMessage.textContent = message;
  toastTimer = window.setTimeout(() => renderToast(null), 3200);
}

function applyTranslations() {
  renderToast(null);
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = translations[currentLang].configTitle;

  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (key && translations[currentLang][key]) {
      node.textContent = translations[currentLang][key];
    }
  });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((node) => {
    const key = node.dataset.i18nPlaceholder;
    if (key && translations[currentLang][key]) {
      node.setAttribute("placeholder", translations[currentLang][key]);
    }
  });
  refreshConceptDirectoryOptions();
  updateAliasEmptyState();
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

function setLang(lang) {
  if (!lang || !translations[lang]) {
    return;
  }
  persistLanguage(lang);
  currentLang = lang;
  applyTranslations();
}

async function loadConfig() {
  showSkeleton(elements.windowSkeleton, elements.windowDays, true);
  showSkeleton(elements.peripheralSkeleton, elements.peripheralAggregateTime, true);
  showSkeleton(elements.globalFlashSkeleton, elements.globalFlashFrequency, true);
  try {
    const response = await fetch(`${API_BASE}/control/status`);
    const data = await response.json();
    const config = data.config || {};
    configState.includeST = !!config.includeST;
    configState.includeDelisted = !!config.includeDelisted;
    elements.includeSt.checked = configState.includeST;
    elements.includeDelisted.checked = configState.includeDelisted;
    const windowDays = Number(config.dailyTradeWindowDays);
    if (Number.isFinite(windowDays) && windowDays > 0) {
      configState.dailyTradeWindowDays = windowDays;
      elements.windowDays.value = windowDays;
    } else {
      elements.windowDays.value = "";
    }
    if (elements.peripheralAggregateTime) {
      const scheduleValue =
        typeof config.peripheralAggregateTime === "string" && config.peripheralAggregateTime
          ? config.peripheralAggregateTime
          : configState.peripheralAggregateTime;
      configState.peripheralAggregateTime = scheduleValue || "06:00";
      elements.peripheralAggregateTime.value = scheduleValue || "";
    }
    if (elements.globalFlashFrequency) {
      const frequencyValue = Number(config.globalFlashFrequencyMinutes);
      if (Number.isFinite(frequencyValue) && frequencyValue >= 10) {
        configState.globalFlashFrequencyMinutes = Math.min(Math.max(frequencyValue, 10), 1440);
      }
      elements.globalFlashFrequency.value = configState.globalFlashFrequencyMinutes;
    }
    configState.conceptAliasMap = normalizeAliasMapPayload(config.conceptAliasMap);
    const surge = config.volumeSurgeConfig || {};
    const volumeDefaults = configState.volumeSurge;
    const minVolumeRatio = sanitizeNumber(
      surge.minVolumeRatio,
      volumeDefaults.minVolumeRatio,
      { min: 0.5, max: 1000 }
    );
    const breakoutPercent = sanitizeNumber(
      surge.breakoutPercent ?? surge.breakout_threshold_percent,
      volumeDefaults.breakoutPercent,
      { min: 0, max: 100 }
    );
    const dailyChangePercent = sanitizeNumber(
      surge.dailyChangePercent ?? surge.daily_change_threshold_percent,
      volumeDefaults.dailyChangePercent,
      { min: 0, max: 200 }
    );
    const maxRangePercent = sanitizeNumber(
      surge.maxRangePercent ?? surge.max_range_percent,
      volumeDefaults.maxRangePercent,
      { min: 1, max: 200 }
    );
    configState.volumeSurge = {
      minVolumeRatio,
      breakoutPercent,
      dailyChangePercent,
      maxRangePercent,
    };
    if (elements.volumeSurgeRatio) {
      elements.volumeSurgeRatio.value = minVolumeRatio;
    }
    if (elements.volumeSurgeBreakout) {
      elements.volumeSurgeBreakout.value = breakoutPercent;
    }
    if (elements.volumeSurgeDailyChange) {
      elements.volumeSurgeDailyChange.value = dailyChangePercent;
    }
    if (elements.volumeSurgeRange) {
      elements.volumeSurgeRange.value = maxRangePercent;
    }
    resetAliasRows(configState.conceptAliasMap);
  } catch (error) {
    console.error("Failed to load configuration", error);
    renderToast(translations[currentLang].toastConfigFailed || "Failed to save configuration", "error");
  } finally {
    showSkeleton(elements.windowSkeleton, elements.windowDays, false);
    showSkeleton(elements.peripheralSkeleton, elements.peripheralAggregateTime, false);
    showSkeleton(elements.globalFlashSkeleton, elements.globalFlashFrequency, false);
  }
}

async function saveConfig() {
  const windowDaysValue = Number(elements.windowDays.value);
  const dailyWindow =
    Number.isFinite(windowDaysValue) && windowDaysValue > 0
      ? windowDaysValue
      : configState.dailyTradeWindowDays;
  const scheduleValue =
    elements.peripheralAggregateTime?.value?.trim() ||
    configState.peripheralAggregateTime ||
    "06:00";
  const frequencyInput = Number(elements.globalFlashFrequency?.value);
  const globalFlashFrequency = Number.isFinite(frequencyInput) && frequencyInput >= 10
    ? Math.min(Math.max(frequencyInput, 10), 1440)
    : configState.globalFlashFrequencyMinutes;
  const minVolumeRatio = sanitizeNumber(
    elements.volumeSurgeRatio?.value,
    configState.volumeSurge.minVolumeRatio,
    { min: 0.5, max: 1000 }
  );
  const breakoutPercent = sanitizeNumber(
    elements.volumeSurgeBreakout?.value,
    configState.volumeSurge.breakoutPercent,
    { min: 0, max: 100 }
  );
  const dailyChangePercent = sanitizeNumber(
    elements.volumeSurgeDailyChange?.value,
    configState.volumeSurge.dailyChangePercent,
    { min: 0, max: 200 }
  );
  const maxRangePercent = sanitizeNumber(
    elements.volumeSurgeRange?.value,
    configState.volumeSurge.maxRangePercent,
    { min: 1, max: 200 }
  );
  const aliasMap = collectAliasMap();
  configState.includeST = elements.includeSt.checked;
  configState.includeDelisted = elements.includeDelisted.checked;
  configState.dailyTradeWindowDays = dailyWindow;
  configState.peripheralAggregateTime = scheduleValue;
  configState.globalFlashFrequencyMinutes = globalFlashFrequency;
  configState.conceptAliasMap = aliasMap;
  configState.volumeSurge = {
    minVolumeRatio,
    breakoutPercent,
    dailyChangePercent,
    maxRangePercent,
  };
  const payload = {
    includeST: elements.includeSt.checked,
    includeDelisted: elements.includeDelisted.checked,
    dailyTradeWindowDays: dailyWindow,
    peripheralAggregateTime: scheduleValue,
    globalFlashFrequencyMinutes: globalFlashFrequency,
    conceptAliasMap: aliasMap,
    volumeSurgeConfig: {
      minVolumeRatio,
      breakoutPercent,
      dailyChangePercent,
      maxRangePercent,
    },
  };
  elements.saveButton.disabled = true;
  try {
    const response = await fetch(`${API_BASE}/control/config`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      keepalive: true,
    });
    if (!response.ok) {
      const message = await response.text();
      throw new Error(message || `Request failed with status ${response.status}`);
    }
    renderToast(translations[currentLang].toastConfigSaved, "success");
    await loadConfig();
  } catch (error) {
    console.error("Failed to save configuration", error);
    const message =
      (error && error.message) || translations[currentLang].toastConfigFailed || "Failed to save configuration";
    renderToast(message, "error");
  } finally {
    elements.saveButton.disabled = false;
  }
}

function initLanguageSwitch() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => setLang(btn.dataset.lang))
  );
}

function initActions() {
  elements.saveButton.addEventListener("click", saveConfig);
  if (elements.conceptAliasAdd) {
    elements.conceptAliasAdd.addEventListener("click", handleAddAliasRow);
  }
}

initLanguageSwitch();
initActions();
setLang(currentLang);
loadConfig();
loadConceptDirectory();
