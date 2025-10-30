const translations = getTranslations("individualFundFlow");
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

function normalizeStockCode(value) {
  if (value === null || value === undefined) {
    return { detailCode: "", displayCode: "" };
  }
  const text = String(value).trim().toUpperCase();
  if (!text) {
    return { detailCode: "", displayCode: "" };
  }

  if (text.includes(".")) {
    const [symbolPart, suffixPart = ""] = text.split(".", 2);
    const symbol = symbolPart && /^\d+$/.test(symbolPart) ? symbolPart.padStart(6, "0") : symbolPart;
    const suffix = suffixPart.trim();
    const detail = suffix ? `${symbol}.${suffix}` : symbol;
    return { detailCode: detail, displayCode: symbol };
  }

  const digitsMatch = text.match(/^(\d{1,6})$/);
  if (digitsMatch) {
    const symbol = digitsMatch[1].padStart(6, "0");
    const first = symbol[0];
    let suffix = "";
    if (symbol.startsWith("43") || symbol.startsWith("83") || symbol.startsWith("87") || first === "4" || first === "8") {
      suffix = "BJ";
    } else if (first === "6" || first === "9" || first === "5") {
      suffix = "SH";
    } else if (first === "0" || first === "2" || first === "3") {
      suffix = "SZ";
    }
    const detail = suffix ? `${symbol}.${suffix}` : symbol;
    return { detailCode: detail, displayCode: symbol };
  }

  return { detailCode: text, displayCode: text.replace(/\..*$/, "") || text };
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

function formatCurrency(value, { withSign = false } = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  const formatted = new Intl.NumberFormat(locale, {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(numeric);
  if (!withSign || numeric === 0) {
    return formatted;
  }
  if (numeric > 0 && !formatted.startsWith("+")) {
    return `+${formatted}`;
  }
  return formatted;
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

  const response = await fetch(`${API_BASE}/fund-flow/individual?${params.toString()}`);
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
          console.error(`Failed to load individual fund flow for ${tab.value}`, error);
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
      tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] || "--";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  const dict = translations[currentLang];

  items.forEach((item) => {
    const rank = item.rank ?? "--";
    const { detailCode, displayCode } = normalizeStockCode(item.stockCode);
    const code = escapeHTML(displayCode || detailCode || "--");
    const name = escapeHTML(item.stockName || "--");
    const updatedAt = formatDateTime(item.updatedAt);
    const price = formatNumber(item.latestPrice, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const priceChange = renderDelta(item.priceChangePercent, formatPercent);
    const stageChange =
      item.stageChangePercent !== null && item.stageChangePercent !== undefined
        ? renderDelta(item.stageChangePercent, formatPercent)
        : "--";

    const inflow = formatCurrency(item.inflow);
    const outflow = formatCurrency(item.outflow);
    const netAmount = renderDelta(item.netAmount, (value) =>
      formatCurrency(value, { withSign: true })
    );
    const netInflow = renderDelta(item.netInflow, (value) =>
      formatCurrency(value, { withSign: true })
    );

    const turnoverRate =
      item.turnoverRate !== null && item.turnoverRate !== undefined
        ? formatPercent(item.turnoverRate)
        : "--";
    const continuousTurnover =
      item.continuousTurnoverRate !== null && item.continuousTurnoverRate !== undefined
        ? formatPercent(item.continuousTurnoverRate)
        : "--";
    const turnoverAmount = formatCurrency(item.turnoverAmount);

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelRank}</span>
            <span class="metric-row__value">${rank}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelCode}</span>
            ${
              detailCode
                ? `<a class="metric-row__value link" href="stock-detail.html?code=${encodeURIComponent(detailCode)}" target="_blank" rel="noopener noreferrer">${code}</a>`
                : `<span class="metric-row__value">${code}</span>`
            }
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelName}</span>
            ${
              detailCode
                ? `<a class="metric-row__value metric-row__value--accent link" href="stock-detail.html?code=${encodeURIComponent(detailCode)}" target="_blank" rel="noopener noreferrer">${name}</a>`
                : `<span class="metric-row__value metric-row__value--accent">${name}</span>`
            }
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
            <span class="metric-row__label">${dict.labelPrice}</span>
            <span class="metric-row__value">${price}</span>
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
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelNetInflow}</span>
            <span class="metric-row__value">${netInflow}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelTurnoverRate}</span>
            <span class="metric-row__value">${turnoverRate}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelContinuousTurnover}</span>
            <span class="metric-row__value">${continuousTurnover}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelTurnoverAmount}</span>
            <span class="metric-row__value">${turnoverAmount}</span>
          </div>
        </div>
      </td>
    `;
    tbody.appendChild(row);
  });
}

function updatePagination(total, page) {
  if (!elements.pageInfo || !elements.prevButton || !elements.nextButton) {
    return;
  }
  const totalPages = Math.max(1, Math.ceil(total / STATE.limit));
  const dict = translations[currentLang];
  elements.pageInfo.textContent = dict.paginationInfo
    .replace("{current}", String(page))
    .replace("{totalPages}", String(totalPages))
    .replace("{total}", String(total));
  elements.prevButton.disabled = page <= 1;
  elements.nextButton.disabled = page >= totalPages;
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

  document.querySelectorAll("[data-i18n]").forEach((el) => {
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

function initTabs() {
  elements.tabButtons.forEach((button) =>
    button.addEventListener("click", () => setActiveSymbol(button.dataset.symbol))
  );
}

function initPagination() {
  if (elements.prevButton) {
    elements.prevButton.addEventListener("click", goToPreviousPage);
  }
  if (elements.nextButton) {
    elements.nextButton.addEventListener("click", goToNextPage);
  }
}

// Boot
initLanguageSwitch();
initTabs();
initPagination();
setLanguage(currentLang);
loadAllData();
