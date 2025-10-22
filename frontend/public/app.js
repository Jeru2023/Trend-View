const translations = getTranslations("basicInfo");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const PAGE_SIZE = 20;

const exchangeLabels = {
  en: { SSE: "SSE", SZSE: "SZSE", BSE: "BSE" },
  zh: { SSE: "上交所", SZSE: "深交所", BSE: "北交所" },
};

const marketLabels = {
  en: {
    "主板": "Main Board",
    "创业板": "ChiNext",
    "科创板": "STAR Market",
    "Main Board": "Main Board",
    "ChiNext": "ChiNext",
    "STAR Market": "STAR Market",
  },
  zh: {
    "主板": "主板",
    "创业板": "创业板",
    "科创板": "科创板",
    "Main Board": "主板",
    "ChiNext": "创业板",
    "STAR Market": "科创板",
  },
};

const LANG_STORAGE_KEY = "trend-view-lang";

function getInitialLanguage() {
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
  }
  const htmlLang = document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) {
    return htmlLang;
  }
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch (error) {
    /* no-op */
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

let currentLang = getInitialLanguage();
const state = {
  page: 1,
  total: 0,
  items: [],
  filters: {
    keyword: "",
    market: "all",
    exchange: "all",
  },
};

const elements = {
  fundamentalsBody: document.getElementById("fundamentals-body"),
  tabs: document.querySelectorAll(".tab"),
  tables: {
    fundamentals: document.getElementById("fundamentals-table"),
    statistics: document.getElementById("statistics-placeholder"),
  },
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

function formatNumber(value) {
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 2 }).format(
    value ?? 0
  );
}

function formatOptionalNumber(value, options = {}) {
  if (value === null || value === undefined) {
    return "—";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(value);
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

  document
    .querySelectorAll("[data-placeholder-en]")
    .forEach((el) => {
      const placeholder = el.dataset[`placeholder${currentLang.toUpperCase()}`];
      if (typeof placeholder === "string") {
        el.placeholder = placeholder;
      }
    });
}

function formatChange(value) {
  if (value === null || value === undefined) {
    return "—";
  }
  const formatted = value.toFixed(2);
  return `${value >= 0 ? "+" : ""}${formatted}%`;
}

function renderTable(data = state.items) {
  elements.fundamentalsBody.innerHTML = "";
  if (!data.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 11;
    cell.textContent = translations[currentLang].noData;
    cell.style.textAlign = "center";
    cell.style.color = "#6b7280";
    row.appendChild(cell);
    elements.fundamentalsBody.appendChild(row);
    return;
  }

  data.forEach((item) => {
    const marketMap = marketLabels[currentLang] || {};
    const exchangeMap = exchangeLabels[currentLang] || {};
    const marketLabel = item.market ? marketMap[item.market] ?? item.market : "—";
    const exchangeLabel = item.exchange ? exchangeMap[item.exchange] ?? item.exchange : "—";
    const changeClass =
      item.pct_change == null
        ? ""
        : item.pct_change >= 0
        ? "text-up"
        : "text-down";
    const lastPrice = formatOptionalNumber(item.last_price, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const changeDisplay = formatChange(item.pct_change);
    const volumeDisplay =
      item.volume == null
        ? "—"
        : formatOptionalNumber(item.volume, { maximumFractionDigits: 0 });
    const marketCapDisplay =
      item.market_cap == null
        ? "—"
        : formatOptionalNumber(item.market_cap, { maximumFractionDigits: 0 });
    const peDisplay = formatOptionalNumber(item.pe_ratio, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const turnoverDisplay =
      item.turnover_rate == null
        ? "—"
        : `${formatOptionalNumber(item.turnover_rate, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}%`;

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.code}</td>
      <td>${item.name ?? "—"}</td>
      <td>${item.industry ?? "—"}</td>
      <td>${marketLabel}</td>
      <td>${exchangeLabel}</td>
      <td>${lastPrice}</td>
      <td class="${changeClass}">
        ${changeDisplay}
      </td>
      <td>${volumeDisplay}</td>
      <td>${marketCapDisplay}</td>
      <td>${peDisplay}</td>
      <td>${turnoverDisplay}</td>
    `;
    elements.fundamentalsBody.appendChild(row);
  });
}

function collectFilters() {
  return {
    keyword: elements.keywordInput.value.trim(),
    market: elements.marketSelect.value,
    exchange: elements.exchangeSelect.value,
  };
}

function setActiveTab(tabName) {
  elements.tabs.forEach((tab) => {
    const isActive = tab.dataset.tab === tabName;
    tab.classList.toggle("tab--active", isActive);
  });

  elements.tables.fundamentals.classList.toggle(
    "hidden",
    tabName !== "fundamentals"
  );
  elements.tables.statistics.classList.toggle(
    "hidden",
    tabName !== "statistics"
  );
}

function updateLanguage(lang) {
  persistLanguage(lang);
  currentLang = lang;
  elements.langButtons.forEach((btn) =>
    btn.classList.toggle("active", btn.dataset.lang === lang)
  );
  applyTranslations();
  renderTable();
  updatePaginationControls();
}

function updatePaginationControls() {
  const totalPages = Math.max(1, Math.ceil(state.total / PAGE_SIZE));
  const dict = translations[currentLang];
  const pageText = dict.paginationInfo
    .replace("{current}", state.page)
    .replace("{totalPages}", totalPages)
    .replace("{total}", formatNumber(state.total));
  elements.pageInfo.textContent = pageText;
  elements.prevPage.disabled = state.page <= 1;
  elements.nextPage.disabled = state.page >= totalPages;
}

async function loadStocks(page = 1) {
  state.page = page;
  const filters = collectFilters();
  state.filters = filters;

  const params = new URLSearchParams();
  params.set("limit", PAGE_SIZE.toString());
  params.set("offset", ((state.page - 1) * PAGE_SIZE).toString());

  if (filters.keyword) params.set("keyword", filters.keyword);
  if (filters.market && filters.market !== "all")
    params.set("market", filters.market);
  if (filters.exchange && filters.exchange !== "all")
    params.set("exchange", filters.exchange);

  try {
    const response = await fetch(`${API_BASE}/stocks?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    state.total = data.total;
    state.items = data.items.map((item) => ({
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
    }));
  } catch (error) {
    console.error("Failed to fetch stock data:", error);
    state.total = 0;
    state.items = [];
  }

  renderTable();
  updatePaginationControls();
}

elements.langButtons.forEach((btn) =>
  btn.addEventListener("click", () => updateLanguage(btn.dataset.lang))
);

elements.applyButton.addEventListener("click", () => {
  loadStocks(1);
});

elements.resetButton.addEventListener("click", () => {
  elements.keywordInput.value = "";
  elements.marketSelect.value = "all";
  elements.exchangeSelect.value = "all";
  elements.searchBox.value = "";
  loadStocks(1);
});

elements.tabs.forEach((tab) =>
  tab.addEventListener("click", () => setActiveTab(tab.dataset.tab))
);

elements.prevPage.addEventListener("click", () => {
  if (state.page > 1) {
    loadStocks(state.page - 1);
  }
});

elements.nextPage.addEventListener("click", () => {
  const totalPages = Math.max(1, Math.ceil(state.total / PAGE_SIZE));
  if (state.page < totalPages) {
    loadStocks(state.page + 1);
  }
});

elements.searchBox.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    elements.keywordInput.value = event.target.value.trim();
    loadStocks(1);
  }
});

// Initial render
setActiveTab("fundamentals");
updateLanguage(currentLang);
loadStocks(1);



























