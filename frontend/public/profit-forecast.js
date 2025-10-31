const translations = getTranslations("profitForecast");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";
const DEFAULT_PAGE_SIZE = 50;

const state = {
  page: 1,
  limit: DEFAULT_PAGE_SIZE,
  total: 0,
  keyword: "",
  industry: "all",
  year: null,
  items: [],
  industries: [],
  years: [],
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  searchInput: document.getElementById("profit-forecast-search"),
  industrySelect: document.getElementById("profit-forecast-industry"),
  yearChips: document.getElementById("profit-forecast-year-chips"),
  tableHead: document.getElementById("profit-forecast-thead"),
  tableBody: document.getElementById("profit-forecast-tbody"),
  pageInfo: document.getElementById("profit-forecast-page-info"),
  prevButton: document.getElementById("profit-forecast-prev"),
  nextButton: document.getElementById("profit-forecast-next"),
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

function updateLanguage(lang) {
  if (!translations[lang]) {
    return;
  }
  currentLang = lang;
  persistLanguage(lang);
  applyTranslations();
  renderTableHead();
  renderTableBody();
  renderPagination();
}

function getDict() {
  return translations[currentLang] || translations.en;
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

function formatEps(value) {
  return formatNumber(value, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 3,
  });
}

function applyTranslations() {
  const dict = getDict();
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

  if (elements.searchInput) {
    const placeholder = elements.searchInput.dataset[`placeholder${currentLang.toUpperCase()}`];
    if (typeof placeholder === "string") {
      elements.searchInput.placeholder = placeholder;
    }
  }

  if (elements.yearChips) {
    elements.yearChips.setAttribute("aria-label", dict.yearFilterLabel || "Forecast Year");
  }

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });
}

function buildIndustryOptions() {
  if (!elements.industrySelect) {
    return;
  }
  const previous = elements.industrySelect.value || "all";
  const select = elements.industrySelect;
  const fragment = document.createDocumentFragment();

  const options = new Set(["all", ...state.industries.filter(Boolean)]);
  select.innerHTML = "";
  options.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value === "all" ? getDict().filterAll || "All" : value;
    fragment.appendChild(option);
  });
  select.appendChild(fragment);

  if ([...options].includes(previous)) {
    select.value = previous;
  } else {
    select.value = "all";
    state.industry = "all";
  }
}

function buildYearChips() {
  if (!elements.yearChips) {
    return;
  }
  const dict = getDict();
  const container = elements.yearChips;
  container.innerHTML = "";

  const years = [...state.years].sort((a, b) => a - b);
  const chips = [];

  const allButton = document.createElement("button");
  allButton.type = "button";
  allButton.className = "chip-btn" + (state.year === null ? " chip-btn--active" : "");
  allButton.dataset.year = "all";
  allButton.textContent = dict.filterAll || "All";
  chips.push(allButton);

  years.forEach((year) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip-btn" + (state.year === year ? " chip-btn--active" : "");
    chip.dataset.year = String(year);
    chip.textContent = String(year);
    chips.push(chip);
  });

  chips.forEach((chip) => {
    chip.addEventListener("click", () => {
      const target = chip.dataset.year;
      state.year = target === "all" ? null : Number(target);
      state.page = 1;
      buildYearChips();
      fetchProfitForecast();
    });
    container.appendChild(chip);
  });
}

function renderTableHead() {
  if (!elements.tableHead) {
    return;
  }
  const dict = getDict();
  const head = elements.tableHead;
  head.innerHTML = "";

  const row = document.createElement("tr");
  const columns = [
    dict.colCode || "Code",
    dict.colName || "Name",
    dict.colIndustry || "Industry",
    dict.colReportCount || "Reports",
    dict.colRatings || "Ratings",
  ];

  columns.forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    row.appendChild(th);
  });

  const years = [...state.years].sort((a, b) => a - b);
  years.forEach((year) => {
    const th = document.createElement("th");
    const template = dict.yearColumnLabel || "EPS {year}";
    th.textContent = template.replace("{year}", String(year));
    row.appendChild(th);
  });

  head.appendChild(row);
}

function renderEmpty(message) {
  const tbody = elements.tableBody;
  if (!tbody) {
    return;
  }
  const totalColumns = 5 + state.years.length;
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = Math.max(totalColumns, 5);
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  tbody.innerHTML = "";
  tbody.appendChild(row);
}

function renderRatingsCell(ratings) {
  const dict = getDict();
  const container = document.createElement("div");
  container.className = "forecast-ratings";

  const ratingEntries = [
    ["buy", dict.ratingBuy || "Buy"],
    ["add", dict.ratingAdd || "Outperform"],
    ["neutral", dict.ratingNeutral || "Hold"],
    ["reduce", dict.ratingReduce || "Reduce"],
    ["sell", dict.ratingSell || "Sell"],
  ];

  let rendered = false;
  ratingEntries.forEach(([key, label]) => {
    const value = ratings ? ratings[key] : null;
    if (value === null || value === undefined) {
      return;
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return;
    }
    const item = document.createElement("span");
    item.className = "forecast-rating__item";
    const spanLabel = document.createElement("span");
    spanLabel.className = "forecast-rating__label";
    spanLabel.textContent = label;
    const spanValue = document.createElement("strong");
    spanValue.className = "forecast-rating__value";
    spanValue.textContent = formatNumber(numeric, { maximumFractionDigits: 1 });
    item.appendChild(spanLabel);
    item.appendChild(spanValue);
    container.appendChild(item);
    rendered = true;
  });

  if (!rendered) {
    const placeholder = document.createElement("span");
    placeholder.className = "forecast-rating__label";
    placeholder.textContent = "--";
    container.appendChild(placeholder);
  }

  return container;
}

function renderTableBody() {
  if (!elements.tableBody) {
    return;
  }
  const tbody = elements.tableBody;
  tbody.innerHTML = "";

  if (!state.items.length) {
    const dict = getDict();
    const emptyMessage = tbody.dataset[`empty${currentLang.toUpperCase()}`] || dict.empty || "No data.";
    renderEmpty(emptyMessage);
    return;
  }

  const years = [...state.years].sort((a, b) => a - b);

  state.items.forEach((item) => {
    const row = document.createElement("tr");

    const codeCell = document.createElement("td");
    const code = item.code || item.symbol;
    if (code) {
      const link = document.createElement("a");
      link.href = `stock-detail.html?code=${encodeURIComponent(code)}`;
      link.className = "table-link";
      link.textContent = code;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      codeCell.appendChild(link);
    } else {
      codeCell.textContent = "--";
    }
    row.appendChild(codeCell);

    const nameCell = document.createElement("td");
    nameCell.textContent = item.name || "--";
    row.appendChild(nameCell);

    const industryCell = document.createElement("td");
    industryCell.textContent = item.industry || "--";
    row.appendChild(industryCell);

    const reportCell = document.createElement("td");
    reportCell.textContent = formatNumber(item.reportCount, { maximumFractionDigits: 0 });
    row.appendChild(reportCell);

    const ratingsCell = document.createElement("td");
    ratingsCell.appendChild(renderRatingsCell(item.ratings));
    row.appendChild(ratingsCell);

    const forecastLookup = new Map((item.forecasts || []).map((entry) => [Number(entry.year), entry]));

    years.forEach((year) => {
      const cell = document.createElement("td");
      cell.className = "forecast-eps";
      const info = forecastLookup.get(year);
      cell.textContent = formatEps(info ? info.eps : null);
      row.appendChild(cell);
    });

    tbody.appendChild(row);
  });
}

function renderPagination() {
  const dict = getDict();
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  const pageText = (dict.paginationInfo || "Page {current} of {totalPages} · {total} results")
    .replace("{current}", String(state.page))
    .replace("{totalPages}", String(totalPages))
    .replace("{total}", formatNumber(state.total, { maximumFractionDigits: 0 }));

  if (elements.pageInfo) {
    elements.pageInfo.textContent = pageText;
  }
  if (elements.prevButton) {
    elements.prevButton.disabled = state.page <= 1;
  }
  if (elements.nextButton) {
    elements.nextButton.disabled = state.page >= totalPages;
  }
}

async function fetchProfitForecast(page = state.page) {
  const dict = getDict();
  if (!elements.tableBody) {
    return;
  }
  renderEmpty(dict.loading || "Loading...");

  try {
    state.page = page;
    const params = new URLSearchParams();
    params.set("limit", String(state.limit));
    params.set("offset", String((state.page - 1) * state.limit));
    if (state.keyword) {
      params.set("keyword", state.keyword);
    }
    if (state.industry && state.industry !== "all") {
      params.set("industry", state.industry);
    }
    if (typeof state.year === "number") {
      params.set("year", String(state.year));
    }

    const response = await fetch(`${API_BASE}/profit-forecast?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    state.total = Number(data.total) || 0;
    state.items = Array.isArray(data.items) ? data.items : [];
    state.industries = Array.isArray(data.industries) ? data.industries : [];
    state.years = Array.isArray(data.years) ? data.years.map((year) => Number(year)) : [];

    buildIndustryOptions();
    buildYearChips();
    renderTableHead();
    renderTableBody();
    renderPagination();
  } catch (error) {
    console.error("Failed to fetch profit forecast:", error);
    renderEmpty(typeof error?.message === "string" ? error.message : "Failed to load data");
  }
}

function bindEvents() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => updateLanguage(btn.dataset.lang));
  });

  if (elements.searchInput) {
    elements.searchInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        state.keyword = event.target.value.trim();
        state.page = 1;
        fetchProfitForecast();
      }
    });
    elements.searchInput.addEventListener("search", (event) => {
      state.keyword = event.target.value.trim();
      state.page = 1;
      fetchProfitForecast();
    });
  }

  if (elements.industrySelect) {
    elements.industrySelect.addEventListener("change", (event) => {
      state.industry = event.target.value || "all";
      state.page = 1;
      fetchProfitForecast();
    });
  }

  if (elements.prevButton) {
    elements.prevButton.addEventListener("click", () => {
      if (state.page > 1) {
        fetchProfitForecast(state.page - 1);
      }
    });
  }

  if (elements.nextButton) {
    elements.nextButton.addEventListener("click", () => {
      const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
      if (state.page < totalPages) {
        fetchProfitForecast(state.page + 1);
      }
    });
  }
}

async function initialize() {
  applyTranslations();
  bindEvents();
  await fetchProfitForecast(1);
}


window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

document.addEventListener("DOMContentLoaded", initialize);
