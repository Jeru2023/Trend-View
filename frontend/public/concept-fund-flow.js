const translations = getTranslations("conceptFundFlow");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const TABS = [
  { value: "即时", key: "symbolInstant" },
  { value: "3日排行", key: "symbol3Day" },
  { value: "5日排行", key: "symbol5Day" },
  { value: "10日排行", key: "symbol10Day" },
  { value: "20日排行", key: "symbol20Day" },
];

const LANG_STORAGE_KEY = "trend-view-lang";

const STATE = {
  activeSymbol: TABS[0].value,
  limit: 30,
  cache: new Map(),
  totals: new Map(),
  pages: new Map(),
  loading: false,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tabButtons: document.querySelectorAll(".tab-btn[data-symbol]"),
  tableBody: document.getElementById("fund-flow-tbody"),
  prevButton: document.getElementById("fund-flow-prev"),
  nextButton: document.getElementById("fund-flow-next"),
  pageInfo: document.getElementById("fund-flow-page-info"),
};

let currentLang = getInitialLanguage();

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

function escapeHTML(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatNumber(value, options = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(numeric);
}

function formatAmount(value, { withSign = false } = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const formatted = formatNumber(numeric, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (!withSign || numeric === 0) {
    return formatted;
  }
  return `${numeric > 0 ? "+" : ""}${formatted}`;
}

function formatPercent(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const digits = Math.abs(numeric) >= 100 ? 0 : 2;
  return `${numeric > 0 ? "+" : ""}${numeric.toFixed(digits)}%`;
}

function renderDelta(value, formatter) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return formatter(numeric);
  }
  const cls = numeric > 0 ? "text-up" : "text-down";
  return `<span class="${cls}">${formatter(numeric)}</span>`;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  try {
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    const date = new Date(value);
    if (!Number.isFinite(date.getTime())) {
      return String(value);
    }
    return date.toLocaleString(locale, { hour12: false });
  } catch (error) {
    return String(value);
  }
}

async function fetchSymbol(symbol) {
  const params = new URLSearchParams();
  params.set("limit", "500");
  params.set("offset", "0");
  params.set("symbol", symbol);

  const response = await fetch(`${API_BASE}/fund-flow/concept?${params.toString()}`);
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return response.json();
}

async function loadAllData() {
  STATE.loading = true;
  renderLoading();

  try {
    const results = await Promise.all(
      TABS.map(async (tab) => {
        try {
          const data = await fetchSymbol(tab.value);
          return { symbol: tab.value, data };
        } catch (error) {
          console.error(`Failed to load concept fund flow for ${tab.value}`, error);
          return { symbol: tab.value, data: { total: 0, items: [] } };
        }
      })
    );

    results.forEach(({ symbol, data }) => {
      STATE.cache.set(symbol, Array.isArray(data.items) ? data.items : []);
      STATE.totals.set(symbol, Number(data.total) || 0);
      if (!STATE.pages.has(symbol)) {
        STATE.pages.set(symbol, 1);
      }
    });
  } finally {
    STATE.loading = false;
    renderActiveTab();
  }
}

function getActivePage() {
  return STATE.pages.get(STATE.activeSymbol) || 1;
}

function setActivePage(page) {
  STATE.pages.set(STATE.activeSymbol, page);
}

function renderLoading() {
  const tbody = elements.tableBody;
  if (!tbody) {
    return;
  }
  tbody.innerHTML = "";
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 4;
  cell.textContent = "...";
  row.appendChild(cell);
  tbody.appendChild(row);
  if (elements.pageInfo) {
    elements.pageInfo.textContent = "--";
  }
  if (elements.prevButton) elements.prevButton.disabled = true;
  if (elements.nextButton) elements.nextButton.disabled = true;
}

function renderActiveTab() {
  const items = STATE.cache.get(STATE.activeSymbol) || [];
  const total = STATE.totals.get(STATE.activeSymbol) || items.length;
  const totalPages = Math.max(1, Math.ceil(total / STATE.limit));
  const page = Math.min(getActivePage(), totalPages);
  setActivePage(page);

  const start = (page - 1) * STATE.limit;
  const pagedItems = items.slice(start, start + STATE.limit);

  renderTable(pagedItems);
  updatePagination(total, page);
}

function renderTable(items) {
  const tbody = elements.tableBody;
  if (!tbody) {
    return;
  }
  tbody.innerHTML = "";

  if (STATE.loading) {
    renderLoading();
    return;
  }

  if (!items.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 4;
    const emptyText =
      tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] ||
      "--";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  const dict = translations[currentLang];

  items.forEach((item) => {
    const rank = item.rank ?? "--";
    const concept = escapeHTML(item.concept || "--");
    const conceptIndex = formatNumber(item.conceptIndex, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const priceChange = renderDelta(item.priceChangePercent, formatPercent);
    const stageChange = item.stageChangePercent !== null && item.stageChangePercent !== undefined
      ? renderDelta(item.stageChangePercent, formatPercent)
      : "--";
    const updatedAt = formatDateTime(item.updatedAt);

    const inflow = formatAmount(item.inflow);
    const outflow = formatAmount(item.outflow);
    const netAmount = renderDelta(item.netAmount, (value) =>
      formatAmount(value, { withSign: true })
    );

    const companyCount = formatNumber(item.companyCount, { maximumFractionDigits: 0 });
    const leadingStock = escapeHTML(item.leadingStock || "--");
    const leadingChange = item.leadingStockChangePercent !== null && item.leadingStockChangePercent !== undefined
      ? renderDelta(item.leadingStockChangePercent, formatPercent)
      : "--";
    const currentPrice = formatNumber(item.currentPrice, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelRank}</span>
            <span class="metric-row__value">${rank}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.colConcept}</span>
            <span class="metric-row__value metric-row__value--accent">${concept}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelUpdatedAt}</span>
            <span class="metric-row__value">${updatedAt}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelIndex}</span>
            <span class="metric-row__value">${conceptIndex}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelPriceChange}</span>
            <span class="metric-row__value">${priceChange}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelStageChange}</span>
            <span class="metric-row__value">${stageChange}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelInflow}</span>
            <span class="metric-row__value">${inflow}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelOutflow}</span>
            <span class="metric-row__value">${outflow}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelNet}</span>
            <span class="metric-row__value">${netAmount}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelCompanyCount}</span>
            <span class="metric-row__value">${companyCount}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelLeadingStock}</span>
            <span class="metric-row__value metric-row__value--wrap">${leadingStock}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelLeadingChange}</span>
            <span class="metric-row__value">${leadingChange}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelCurrentPrice}</span>
            <span class="metric-row__value">${currentPrice}</span>
          </div>
        </div>
      </td>
    `;
    tbody.appendChild(row);
  });
}

function updatePagination(total, page) {
  const totalPages = Math.max(1, Math.ceil(total / STATE.limit));
  const dict = translations[currentLang];
  if (elements.pageInfo) {
    elements.pageInfo.textContent = dict.paginationInfo
      .replace("{current}", String(page))
      .replace("{totalPages}", String(totalPages))
      .replace("{total}", String(total));
  }
  if (elements.prevButton) elements.prevButton.disabled = page <= 1;
  if (elements.nextButton) elements.nextButton.disabled = page >= totalPages;
}

function setLanguage(lang) {
  if (!translations[lang]) {
    return;
  }
  currentLang = lang;
  persistLanguage(lang);
  elements.langButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.lang === lang);
  });
  applyTranslations();
  renderActiveTab();
}

function setActiveSymbol(symbol) {
  if (STATE.activeSymbol === symbol) {
    return;
  }
  STATE.activeSymbol = symbol;
  elements.tabButtons.forEach((button) => {
    const active = button.dataset.symbol === symbol;
    button.classList.toggle("tab-btn--active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  });
  if (!STATE.pages.has(symbol)) {
    STATE.pages.set(symbol, 1);
  }
  renderActiveTab();
}

function goToPreviousPage() {
  const currentPage = getActivePage();
  if (currentPage > 1) {
    setActivePage(currentPage - 1);
    renderActiveTab();
  }
}

function goToNextPage() {
  const total = STATE.totals.get(STATE.activeSymbol) || 0;
  const totalPages = Math.max(1, Math.ceil(total / STATE.limit));
  const currentPage = getActivePage();
  if (currentPage < totalPages) {
    setActivePage(currentPage + 1);
    renderActiveTab();
  }
}

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.title = dict.title;

  document
    .querySelectorAll("[data-i18n]")
    .forEach((el) => {
      const key = el.dataset.i18n;
      const value = dict[key];
      if (typeof value === "string") {
        el.textContent = value;
      }
    });

  elements.tabButtons.forEach((button) => {
    const entry = TABS.find((tab) => tab.value === button.dataset.symbol);
    if (entry) {
      const label = dict[entry.key];
      if (label) {
        button.textContent = label;
      }
    }
  });
}

function initLanguageSwitch() {
  elements.langButtons.forEach((button) =>
    button.addEventListener("click", () => setLanguage(button.dataset.lang))
  );
}

function initTabSwitch() {
  elements.tabButtons.forEach((button) =>
    button.addEventListener("click", () => setActiveSymbol(button.dataset.symbol))
  );
}

function initPaginationControls() {
  if (elements.prevButton) {
    elements.prevButton.addEventListener("click", goToPreviousPage);
  }
  if (elements.nextButton) {
    elements.nextButton.addEventListener("click", goToNextPage);
  }
}

function init() {
  initLanguageSwitch();
  initTabSwitch();
  initPaginationControls();
  STATE.loading = true;
  renderLoading();
  setLanguage(currentLang);
  setActiveSymbol(STATE.activeSymbol);
  loadAllData();
}

document.addEventListener("DOMContentLoaded", init);
