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
  items: [],
  filterType: "all",
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  filterButtons: document.querySelectorAll(".chip-btn[data-filter]"),
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

function formatCurrency(value) {
  return formatNumber(value, 0);
}

function formatPercent(value, fractionDigits = 1) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const formatted = numeric.toFixed(fractionDigits);
  return numeric > 0 ? `+${formatted}%` : `${formatted}%`;
}

function renderPercent(value) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const cls = numeric > 0 ? "text-up" : numeric < 0 ? "text-down" : "";
  return `<span class="${cls}">${formatPercent(numeric, Math.abs(numeric) >= 100 ? 0 : 1)}</span>`;
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
}

function buildQueryParams() {
  const params = new URLSearchParams();
  params.set("limit", String(STATE.limit));
  params.set("offset", String((STATE.page - 1) * STATE.limit));
  return params;
}

function truncatedTextWithTooltip(text, limit = 100) {
  if (!text) {
    return { summary: "--", tooltip: "" };
  }
  const normalized = String(text).trim();
  if (normalized.length <= limit) {
    const escaped = escapeHTML(normalized);
    return { summary: escaped, tooltip: escaped };
  }
  const summary = escapeHTML(normalized.slice(0, limit)) + "…";
  return { summary, tooltip: escapeHTML(normalized) };
}

function filterByType(items, filterType) {
  if (filterType === "positive") {
    return items.filter((item) => {
      const type = String(item.forecast_type || item.type || "").trim();
      return type.includes("增") || type.includes("盈");
    });
  }
  if (filterType === "negative") {
    return items.filter((item) => {
      const type = String(item.forecast_type || item.type || "").trim();
      return type.includes("减") || type.includes("亏");
    });
  }
  return items;
}

function renderTable(items) {
  const tbody = elements.tableBody;
  tbody.innerHTML = "";
  if (!items || items.length === 0) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 4;
    const emptyText = tbody.dataset[`empty${currentLang === "zh" ? "Zh" : "En"}`] || "--";
    cell.textContent = emptyText;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  const dict = translations[currentLang];

  items.forEach((item) => {
    const code = item.tsCode || item.ts_code || "";
    const name = item.name || "";
    const detailUrl = code ? `stock-detail.html?code=${encodeURIComponent(code)}` : "#";
    const industry = item.industry ? `<span class="badge">${escapeHTML(item.industry)}</span>` : "";
    const market = item.market ? `<span class="badge badge--muted">${escapeHTML(item.market)}</span>` : "";

    const forecastMetricRaw = item.forecast_metric || item.forecastMetric || "--";
    const forecastMetric = String(forecastMetricRaw).trim() || "--";
    const changeDescriptionRaw = item.change_description || item.changeDescription || "";
    const changeDescriptionText = String(changeDescriptionRaw).trim();
    const changeDescription = changeDescriptionText ? escapeHTML(changeDescriptionText) : "--";
    const forecastValue = formatCurrency(item.forecast_value ?? item.forecastValue);
    const changeRate = renderPercent(item.change_rate ?? item.changeRate);
    const changeReasonData = truncatedTextWithTooltip(item.change_reason || item.changeReason);
    const forecastTypeRaw = item.forecast_type || item.type || "--";
    const forecastType = String(forecastTypeRaw).trim() || "--";
    const lastYearValue = formatCurrency(item.last_year_value ?? item.lastYearValue);
    const announcement = formatDate(item.annDate || item.ann_date || item.announcement_date);
    const period = formatDate(item.endDate || item.end_date || item.reportPeriod || item.report_period);
    const announcementDisplay = escapeHTML(announcement);
    const periodDisplay = escapeHTML(period);

    const changeReasonValue = changeReasonData.tooltip
      ? `<span class="metric-row__value metric-row__value--wrap" title="${changeReasonData.tooltip}">${changeReasonData.summary}</span>`
      : `<span class="metric-row__value metric-row__value--wrap">${changeReasonData.summary}</span>`;

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>
        <div class="cell-primary">
          <span class="cell-code">${
            code && detailUrl !== "#"
              ? `<a class="table-link" href="${detailUrl}">${escapeHTML(code)}</a>`
              : "--"
          }</span>
          <span class="cell-name">${
            name && detailUrl !== "#"
              ? `<a class="table-link" href="${detailUrl}">${escapeHTML(name)}</a>`
              : "--"
          }</span>
        </div>
        <div class="cell-meta">
          ${industry}${market}
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelAnnouncement}</span>
            <span class="metric-row__value">${announcementDisplay}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelPeriod}</span>
            <span class="metric-row__value">${periodDisplay}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelForecastType}</span>
            <span class="metric-row__value">${escapeHTML(forecastType)}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelForecastMetric}</span>
            <span class="metric-row__value metric-row__value--accent">${escapeHTML(forecastMetric)}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelForecastValue}</span>
            <span class="metric-row__value">${forecastValue}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelLastYearValue}</span>
            <span class="metric-row__value">${lastYearValue}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelChangeRate}</span>
            <span>${changeRate}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelChangeDescription}</span>
            <span class="metric-row__value metric-row__value--wrap">${changeDescription}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelChangeReason}</span>
            ${changeReasonValue}
          </div>
        </div>
      </td>
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
  const params = buildQueryParams();
  try {
    const response = await fetch(`${API_BASE}/performance/forecast?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    const incoming = Array.isArray(data.items) ? data.items : [];
    const filtered = filterByType(incoming, STATE.filterType);
    STATE.items = filtered;
    const total = Number(data.total);
    STATE.total = Number.isFinite(total) && total >= 0 ? total : filtered.length;
    renderTable(filtered);
    updatePagination();
  } catch (error) {
    console.error("Failed to load performance forecast data", error);
    STATE.items = [];
    STATE.total = 0;
    renderTable([]);
    updatePagination();
  }
}

function setActiveFilterButton(value) {
  elements.filterButtons.forEach((button) => {
    if (button.dataset.filter === value) {
      button.classList.add("chip-btn--active");
    } else {
      button.classList.remove("chip-btn--active");
    }
  });
}

function initEvents() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => {
      const lang = btn.dataset.lang;
      persistLanguage(lang);
      currentLang = lang;
      applyTranslations();
      loadData();
    })
  );

  elements.filterButtons.forEach((button) =>
    button.addEventListener("click", () => {
      const value = button.dataset.filter || "all";
      if (STATE.filterType !== value) {
        STATE.filterType = value;
        setActiveFilterButton(value);
        STATE.page = 1;
        loadData();
      }
    })
  );

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

function init() {
  applyTranslations();
  setActiveFilterButton(STATE.filterType);
  initEvents();
  loadData();
}

document.addEventListener("DOMContentLoaded", init);
