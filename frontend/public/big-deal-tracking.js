const translations = getTranslations("bigDealFundFlow");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";
const SIDES = [
  { value: "all", key: "tabAll" },
  { value: "买盘", key: "tabBuy" },
  { value: "卖盘", key: "tabSell" },
];

const STATE = {
  activeSide: "all",
  limit: 40,
  page: 1,
  items: [],
  loading: false,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tabButtons: document.querySelectorAll(".tab-btn[data-side]"),
  tableBody: document.getElementById("big-deal-tbody"),
  prevButton: document.getElementById("big-deal-prev"),
  nextButton: document.getElementById("big-deal-next"),
  pageInfo: document.getElementById("big-deal-page-info"),
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

  const withSuffix = text.includes(".");
  if (withSuffix) {
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

function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(numeric);
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
  const text = `${numeric > 0 ? "+" : ""}${numeric.toFixed(digits)}%`;
  const cls = numeric > 0 ? "text-up" : numeric < 0 ? "text-down" : "";
  return cls ? `<span class="${cls}">${text}</span>` : text;
}

function formatChangeAmount(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const formatted = formatNumber(numeric, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  const text = numeric > 0 ? `+${formatted}` : formatted;
  const cls = numeric > 0 ? "text-up" : numeric < 0 ? "text-down" : "";
  return cls ? `<span class="${cls}">${text}</span>` : text;
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

async function fetchBigDeals() {
  const params = new URLSearchParams();
  params.set("limit", "500");
  params.set("offset", "0");

  const response = await fetch(`${API_BASE}/fund-flow/big-deal?${params.toString()}`);
  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }
  return response.json();
}

async function loadData() {
  STATE.loading = true;
  renderLoading();
  try {
    const data = await fetchBigDeals();
    STATE.items = Array.isArray(data.items)
      ? data.items.map((item) => ({
          tradeTime: item.tradeTime ?? item.trade_time ?? null,
          stockCode: item.stockCode ?? item.stock_code ?? null,
          stockName: item.stockName ?? item.stock_name ?? null,
          tradePrice: item.tradePrice ?? item.trade_price ?? null,
          tradeVolume: item.tradeVolume ?? item.trade_volume ?? null,
          tradeAmount: item.tradeAmount ?? item.trade_amount ?? null,
          tradeSide: item.tradeSide ?? item.trade_side ?? null,
          priceChangePercent: item.priceChangePercent ?? item.price_change_percent ?? null,
          priceChange: item.priceChange ?? item.price_change ?? null,
          updatedAt: item.updatedAt ?? item.updated_at ?? null,
        }))
      : [];
    STATE.page = 1;
  } catch (error) {
    console.error("Failed to load big deal data", error);
    STATE.items = [];
  } finally {
    STATE.loading = false;
    renderActiveSide();
  }
}

function getFilteredItems() {
  if (STATE.activeSide === "all") {
    return STATE.items;
  }
  return STATE.items.filter((item) => item.tradeSide === STATE.activeSide);
}

function renderLoading() {
  if (!elements.tableBody) {
    return;
  }
  elements.tableBody.innerHTML = "";
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 4;
  cell.textContent = "...";
  row.appendChild(cell);
  elements.tableBody.appendChild(row);
  if (elements.pageInfo) elements.pageInfo.textContent = "--";
  if (elements.prevButton) elements.prevButton.disabled = true;
  if (elements.nextButton) elements.nextButton.disabled = true;
}

function renderActiveSide() {
  const items = getFilteredItems();
  const totalPages = Math.max(1, Math.ceil(items.length / STATE.limit));
  STATE.page = Math.min(Math.max(STATE.page, 1), totalPages);
  const start = (STATE.page - 1) * STATE.limit;
  const paged = items.slice(start, start + STATE.limit);
  renderTable(paged);
  updatePagination(items.length, STATE.page, totalPages);
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
    const key = currentLang === "zh" ? "Zh" : "En";
    cell.textContent = tbody.dataset[`empty${key}`] || "--";
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  const dict = translations[currentLang];

  items.forEach((item) => {
    const tradeTime = formatDateTime(item.tradeTime);
    const { detailCode, displayCode } = normalizeStockCode(item.stockCode);
    const codeDisplay = detailCode ? escapeHTML(displayCode || detailCode) : "--";
    const name = escapeHTML(item.stockName || "--");
    const sideRaw = item.tradeSide;
    const side = escapeHTML(sideRaw || "--");
    const price = formatNumber(item.tradePrice, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const pctChange = formatPercent(item.priceChangePercent);
    const changeAmount = formatChangeAmount(item.priceChange);
    const volume = formatNumber(item.tradeVolume);
    const amount = formatCurrency(item.tradeAmount);
    const updatedAt = formatDateTime(item.updatedAt);

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelTime}</span>
            <span class="metric-row__value">${tradeTime}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelCode}</span>
            ${
              detailCode
                ? `<a class="metric-row__value metric-row__value--accent link" href="stock-detail.html?code=${encodeURIComponent(detailCode)}" target="_blank" rel="noopener noreferrer">${codeDisplay}</a>`
                : `<span class="metric-row__value metric-row__value--accent">${codeDisplay}</span>`
            }
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelName}</span>
            ${
              detailCode
                ? `<a class="metric-row__value metric-row__value--wrap link" href="stock-detail.html?code=${encodeURIComponent(detailCode)}" target="_blank" rel="noopener noreferrer">${name}</a>`
                : `<span class="metric-row__value metric-row__value--wrap">${name}</span>`
            }
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelSide}</span>
            <span class="metric-row__value">${side || "--"}</span>
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
            <span class="metric-row__label">${dict.labelChangePercent}</span>
            <span class="metric-row__value">${pctChange}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelChangeAmount}</span>
            <span class="metric-row__value">${changeAmount}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelVolume}</span>
            <span class="metric-row__value">${volume}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelAmount}</span>
            <span class="metric-row__value">${amount}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelUpdatedAt}</span>
            <span class="metric-row__value">${updatedAt}</span>
          </div>
        </div>
      </td>
    `;
    tbody.appendChild(row);
  });
}

function updatePagination(total, page, totalPages) {
  if (elements.pageInfo) {
    const dict = translations[currentLang];
    elements.pageInfo.textContent = dict.paginationInfo
      .replace("{current}", String(page))
      .replace("{totalPages}", String(totalPages))
      .replace("{total}", String(total));
  }
  if (elements.prevButton) {
    elements.prevButton.disabled = page <= 1;
  }
  if (elements.nextButton) {
    elements.nextButton.disabled = page >= totalPages;
  }
}

function setLanguage(lang) {
  if (!translations[lang]) {
    return;
  }
  currentLang = lang;
  persistLanguage(lang);
  elements.langButtons.forEach((button) =>
    button.classList.toggle("active", button.dataset.lang === lang)
  );
  applyTranslations();
  renderActiveSide();
}

function setActiveSide(side) {
  if (STATE.activeSide === side) {
    return;
  }
  STATE.activeSide = side;
  STATE.page = 1;
  elements.tabButtons.forEach((button) => {
    const isActive = button.dataset.side === side;
    button.classList.toggle("tab-btn--active", isActive);
    button.setAttribute("aria-selected", isActive ? "true" : "false");
  });
  renderActiveSide();
}

function goToPreviousPage() {
  if (STATE.page > 1) {
    STATE.page -= 1;
    renderActiveSide();
  }
}

function goToNextPage() {
  const total = getFilteredItems().length;
  const totalPages = Math.max(1, Math.ceil(total / STATE.limit));
  if (STATE.page < totalPages) {
    STATE.page += 1;
    renderActiveSide();
  }
}

function applyTranslations() {
  const dict = translations[currentLang];
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = dict.title;

  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    const value = dict[key];
    if (typeof value === "string") {
      el.textContent = value;
    }
  });

  elements.tabButtons.forEach((button) => {
    const entry = SIDES.find((side) => side.value === button.dataset.side);
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
    button.addEventListener("click", () => setActiveSide(button.dataset.side))
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


window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

// Boot
initLanguageSwitch();
initTabs();
initPagination();
setLanguage(currentLang);
loadData();
