const translations = getTranslations("realtimeIndices");

const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

let currentLang = getInitialLanguage();
let latestItems = [];

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  refreshButton: document.getElementById("realtime-indices-refresh"),
  lastSynced: document.getElementById("realtime-indices-last-synced"),
  tableBody: document.getElementById("realtime-indices-tbody"),
  filterForm: document.getElementById("realtime-indices-filter-form"),
  turnoverInput: document.getElementById("realtime-indices-turnover-input"),
  filterApply: document.getElementById("realtime-indices-filter-apply"),
};

const state = {
  turnoverMin: null,
};

const TURNOVER_INPUT_UNIT = 1e8; // Input interpreted as 100M CNY increments.

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

function hasActiveFilter() {
  return state.turnoverMin !== null;
}

function parseTurnoverInputValue() {
  if (!elements.turnoverInput) {
    return null;
  }
  const raw = Number(elements.turnoverInput.value);
  if (!Number.isFinite(raw) || raw <= 0) {
    return null;
  }
  return raw * TURNOVER_INPUT_UNIT;
}

function applyFilters(items = []) {
  if (!Array.isArray(items) || !items.length) {
    return [];
  }
  if (!hasActiveFilter()) {
    return items;
  }
  return items.filter((item) => {
    const turnoverValue = Number(getField(item, "turnover"));
    return Number.isFinite(turnoverValue) && turnoverValue >= state.turnoverMin;
  });
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

function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(numeric) + "%";
}

function formatVolume(value) {
  return formatNumber(value, { notation: "compact", maximumFractionDigits: 2 });
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

function setButtonLoading(isLoading) {
  if (!elements.refreshButton) {
    return;
  }
  const dict = getDict();
  elements.refreshButton.disabled = isLoading;
  elements.refreshButton.dataset.loading = isLoading ? "1" : "0";
  elements.refreshButton.textContent = isLoading ? dict.refreshing || "Refreshing..." : dict.refreshButton;
}

function applyTurnoverFilter(event) {
  if (event) {
    event.preventDefault();
  }
  state.turnoverMin = parseTurnoverInputValue();
  renderTable(latestItems);
}

function handleTurnoverInputChange() {
  if (!elements.turnoverInput) {
    return;
  }
  const rawValue = elements.turnoverInput.value;
  if (rawValue === "") {
    if (state.turnoverMin !== null) {
      state.turnoverMin = null;
      renderTable(latestItems);
    }
    return;
  }

  const numeric = Number(rawValue);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    if (state.turnoverMin !== null) {
      state.turnoverMin = null;
      renderTable(latestItems);
    }
  }
}

function renderEmptyRow(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 11;
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
  const dict = getDict();
  const baseItems = Array.isArray(items) ? items : [];
  const filteredItems = applyFilters(baseItems);
  const hasFilter = hasActiveFilter();

  if (!filteredItems.length) {
    const key = `empty${currentLang.toUpperCase()}`;
    const defaultEmpty = currentLang === "zh" ? "暂无实时指数数据。" : "No realtime index data.";
    const fallback = (elements.tableBody.dataset && elements.tableBody.dataset[key]) || dict.empty || defaultEmpty;
    const message = hasFilter && baseItems.length ? dict.emptyFiltered || fallback : fallback;
    renderEmptyRow(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  filteredItems.forEach((item) => {
    const row = document.createElement("tr");

    const latestPrice = getField(item, "latestPrice");
    const changeValue = getField(item, "changePercent");
    const amountValue = getField(item, "changeAmount");
    const prevClose = getField(item, "prevClose");
    const openPrice = getField(item, "openPrice");
    const highPrice = getField(item, "highPrice");
    const lowPrice = getField(item, "lowPrice");
    const volume = getField(item, "volume");
    const turnover = getField(item, "turnover");

    const change = Number(changeValue);
    const amount = Number(amountValue);
    const changeClass = Number.isFinite(change) ? (change > 0 ? "index-rise" : change < 0 ? "index-fall" : "") : "";
    const amountClass = Number.isFinite(amount) ? (amount > 0 ? "index-rise" : amount < 0 ? "index-fall" : "") : "";

    const cells = [
      { text: item.code || "--" },
      { text: item.name || "--" },
      { text: formatNumber(latestPrice, { maximumFractionDigits: 2 }) },
      { text: formatNumber(amount, { maximumFractionDigits: 2 }), className: amountClass },
      { text: formatPercent(change), className: changeClass },
      { text: formatNumber(prevClose, { maximumFractionDigits: 2 }) },
      { text: formatNumber(openPrice, { maximumFractionDigits: 2 }) },
      { text: formatNumber(highPrice, { maximumFractionDigits: 2 }) },
      { text: formatNumber(lowPrice, { maximumFractionDigits: 2 }) },
      { text: formatVolume(volume) },
      { text: formatVolume(turnover) },
    ];

    cells.forEach((cellData) => {
      const cell = document.createElement("td");
      cell.textContent = cellData.text;
      if (cellData.className) {
        cell.classList.add(cellData.className);
      }
      row.appendChild(cell);
    });

    fragment.appendChild(row);
  });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
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

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });

  if (elements.turnoverInput) {
    elements.turnoverInput.placeholder = dict.filterPlaceholder || "";
  }

  renderTable(latestItems);
}

async function loadRealtimeIndices(force = false) {
  if (!force && elements.refreshButton?.dataset.loading === "1") {
    return;
  }

  setButtonLoading(true);
  const dict = getDict();

  try {
    const response = await fetch(`${API_BASE}/markets/realtime-indices?limit=600`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    latestItems = Array.isArray(payload.items) ? payload.items : [];
    if (elements.lastSynced) {
      elements.lastSynced.textContent = formatDateTime(payload.lastSyncedAt || payload.last_synced_at);
    }
    renderTable(latestItems);
  } catch (error) {
    console.error("Failed to load realtime indices:", error);
    latestItems = [];
    if (elements.lastSynced) {
      elements.lastSynced.textContent = "--";
    }
    renderTable([]);
  } finally {
    setButtonLoading(false);
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

  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => loadRealtimeIndices(true));
  }

  if (elements.filterForm) {
    elements.filterForm.addEventListener("submit", applyTurnoverFilter);
  }

  if (elements.turnoverInput) {
    elements.turnoverInput.addEventListener("input", handleTurnoverInputChange);
  }
}

function initialize() {
  applyTranslations();
  bindEvents();
  loadRealtimeIndices(true);
}

initialize();
