const translations = getTranslations("performanceForecast");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const STATE = {
  page: 1,
  limit: 20,
  total: 0,
  filters: {
    keyword: "",
    startDate: null,
    endDate: null,
  },
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  keywordInput: document.getElementById("forecast-keyword"),
  startInput: document.getElementById("forecast-start"),
  endInput: document.getElementById("forecast-end"),
  applyButton: document.getElementById("forecast-apply"),
  resetButton: document.getElementById("forecast-reset"),
  tableBody: document.getElementById("forecast-tbody"),
  pageInfo: document.getElementById("forecast-page-info"),
  prevButton: document.getElementById("forecast-prev"),
  nextButton: document.getElementById("forecast-next"),
};

const LANG_STORAGE_KEY = "trend-view-lang";
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

function formatNumber(value, fractionDigits = 2) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  }).format(numeric);
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  try {
    if (value instanceof Date) {
      return value.toISOString().slice(0, 10);
    }
    const text = String(value).trim();
    if (/^\d{8}$/.test(text)) {
      return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
      return text;
    }
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString().slice(0, 10);
    }
  } catch (error) {
    /* ignore */
  }
  return String(value);
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

  if (elements.keywordInput) {
    const placeholderKey = currentLang === "zh" ? "data-placeholder-zh" : "data-placeholder-en";
    const placeholder = elements.keywordInput.getAttribute(placeholderKey);
    if (placeholder) {
      elements.keywordInput.setAttribute("placeholder", placeholder);
    }
  }
}

function updateFiltersFromInputs() {
  STATE.filters.keyword = (elements.keywordInput.value || "").trim();
  STATE.filters.startDate = elements.startInput.value || null;
  STATE.filters.endDate = elements.endInput.value || null;
}

function setInputsFromFilters() {
  elements.keywordInput.value = STATE.filters.keyword;
  elements.startInput.value = STATE.filters.startDate || "";
  elements.endInput.value = STATE.filters.endDate || "";
}

function buildQueryParams() {
  const params = new URLSearchParams();
  params.set("limit", String(STATE.limit));
  params.set("offset", String((STATE.page - 1) * STATE.limit));
  if (STATE.filters.keyword) {
    params.set("keyword", STATE.filters.keyword);
  }
  if (STATE.filters.startDate) {
    params.set("startDate", STATE.filters.startDate.replace(/-/g, ""));
  }
  if (STATE.filters.endDate) {
    params.set("endDate", STATE.filters.endDate.replace(/-/g, ""));
  }
  return params;
}

function renderTable(items) {
  const tbody = elements.tableBody;
  tbody.innerHTML = "";
  if (!items || items.length === 0) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 11;
    const emptyText = tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] || "--";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  items.forEach((item) => {
    const pctMin = item.pctChangeMin ?? item.p_change_min;
    const pctMax = item.pctChangeMax ?? item.p_change_max;
    const netMin = item.netProfitMin ?? item.net_profit_min;
    const netMax = item.netProfitMax ?? item.net_profit_max;

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${item.tsCode || item.ts_code || "--"}</td>
      <td>${item.name || "--"}</td>
      <td>${item.industry || "--"}</td>
      <td>${formatDate(item.annDate || item.ann_date)}</td>
      <td>${formatDate(item.endDate || item.end_date)}</td>
      <td>${item.type || "--"}</td>
      <td class="text-right">${formatNumber(pctMin, 2)} ~ ${formatNumber(pctMax, 2)}</td>
      <td class="text-right">${formatNumber(netMin, 0)} ~ ${formatNumber(netMax, 0)}</td>
      <td class="text-right">${formatNumber(item.lastParentNet || item.last_parent_net, 0)}</td>
      <td>${formatDate(item.firstAnnDate || item.first_ann_date)}</td>
      <td>${item.summary || "--"}</td>
    `;
    tbody.appendChild(row);
  });
}

function updatePagination() {
  const totalPages = Math.max(1, Math.ceil(STATE.total / STATE.limit));
  const dict = translations[currentLang];
  elements.pageInfo.textContent = dict.paginationInfo
    .replace("{current}", String(STATE.page))
    .replace("{totalPages}", String(totalPages))
    .replace("{total}", String(STATE.total));
  elements.prevButton.disabled = STATE.page <= 1;
  elements.nextButton.disabled = STATE.page >= totalPages;
}

async function loadData() {
  updateFiltersFromInputs();
  const params = buildQueryParams();
  try {
    const response = await fetch(`${API_BASE}/performance/forecast?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    STATE.total = Number(data.total) || 0;
    renderTable(data.items || []);
    updatePagination();
  } catch (error) {
    console.error("Failed to load performance forecast data", error);
    renderTable([]);
    STATE.total = 0;
    updatePagination();
  }
}

function resetFilters() {
  STATE.filters.keyword = "";
  STATE.filters.startDate = null;
  STATE.filters.endDate = null;
  STATE.page = 1;
  setInputsFromFilters();
  loadData();
}

function initEvents() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => {
      const lang = btn.dataset.lang;
      persistLanguage(lang);
      currentLang = lang;
      applyTranslations();
      setInputsFromFilters();
      loadData();
    })
  );

  elements.applyButton.addEventListener("click", () => {
    STATE.page = 1;
    loadData();
  });
  elements.resetButton.addEventListener("click", resetFilters);
  elements.keywordInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      STATE.page = 1;
      loadData();
    }
  });

  elements.prevButton.addEventListener("click", () => {
    if (STATE.page > 1) {
      STATE.page -= 1;
      loadData();
    }
  });

  elements.nextButton.addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil(STATE.total / STATE.limit));
    if (STATE.page < totalPages) {
      STATE.page += 1;
      loadData();
    }
  });
}

function bootstrap() {
  applyTranslations();
  setInputsFromFilters();
  initEvents();
  loadData();
}

bootstrap();
