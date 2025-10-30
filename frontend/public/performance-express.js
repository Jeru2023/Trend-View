const translations = getTranslations("performanceExpress");
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
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  tableBody: document.getElementById("express-tbody"),
  pageInfo: document.getElementById("express-page-info"),
  prevButton: document.getElementById("express-prev"),
  nextButton: document.getElementById("express-next"),
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

function formatCompactNumber(value) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  try {
    return new Intl.NumberFormat(locale, {
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(numeric);
  } catch (error) {
    return formatNumber(numeric, 1);
  }
}

function formatPercent(value, fractionDigits = 1, withSign = true) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const formatted = numeric.toFixed(fractionDigits);
  return withSign && numeric > 0 ? `+${formatted}%` : `${formatted}%`;
}

function renderTrendChip(value) {
  if (value === null || value === undefined) {
    return `<span class="chip chip--neutral">--</span>`;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return `<span class="chip chip--neutral">--</span>`;
  }
  const cls = numeric > 0 ? "chip chip--positive" : numeric < 0 ? "chip chip--negative" : "chip chip--neutral";
  const fractionDigits = Math.abs(numeric) >= 100 ? 0 : 1;
  const formatted = formatPercent(numeric, fractionDigits, true);
  return `<span class="${cls}">${formatted}</span>`;
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
    const code = item.tsCode || item.ts_code || "--";
    const name = item.name || "--";
    const industry = item.industry ? `<span class="badge">${item.industry}</span>` : "";
    const market = item.market ? `<span class="badge badge--muted">${item.market}</span>` : "";
    const annDate = formatDate(item.annDate || item.ann_date || item.announcement_date);
    const period = formatDate(item.endDate || item.end_date || item.reportPeriod || item.report_period);
    const revenue = Number(item.revenue ?? item.operateRevenue);
    const revenueYoY = Number(
      item.revenue_yoy ?? item.revenueYoy ?? item.revenueYearlyGrowth
    );
    const revenueQoQ = Number(
      item.revenue_qoq ?? item.revenueQoq ?? item.revenueQuarterlyGrowth
    );
    const netProfit = Number(item.net_profit ?? item.netProfit ?? item.n_income);
    const netProfitYoY = Number(
      item.net_profit_yoy ??
        item.yoy_net_profit ??
        item.yoyNetProfit ??
        item.netProfitYearlyGrowth
    );
    const netProfitQoQ = Number(
      item.net_profit_qoq ?? item.netProfitQoq ?? item.netProfitQuarterlyGrowth
    );
    const eps = Number(item.eps ?? item.diluted_eps ?? item.dilutedEps);
    const roe = Number(
      item.return_on_equity ?? item.returnOnEquity ?? item.diluted_roe ?? item.dilutedRoe
    );

    const detailUrl = code ? `stock-detail.html?code=${encodeURIComponent(code)}` : "#";

    const row = document.createElement("tr");
    row.innerHTML = `
      <td>
        <div class="cell-primary">
          <span class="cell-code">${
            code && detailUrl !== "#" ? `<a class="table-link" href="${detailUrl}">${escapeHTML(code)}</a>` : "--"
          }</span>
          <span class="cell-name">${
            name && detailUrl !== "#" ? `<a class="table-link" href="${detailUrl}">${escapeHTML(name)}</a>` : "--"
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
            <span class="metric-row__value">${annDate}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelPeriod}</span>
            <span class="metric-row__value">${period}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelRevenue}</span>
            <span class="metric-row__value metric-row__value--accent">${formatCompactNumber(revenue)}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelRevenueYoY}</span>
            <span>${renderTrendChip(revenueYoY)}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelRevenueQoQ}</span>
            <span>${renderTrendChip(revenueQoQ)}</span>
          </div>
        </div>
      </td>
      <td>
        <div class="metric-stack">
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelNetProfit}</span>
            <span class="metric-row__value metric-row__value--accent">${formatCompactNumber(netProfit)}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelNetProfitYoY}</span>
            <span>${renderTrendChip(netProfitYoY)}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelNetProfitQoQ}</span>
            <span>${renderTrendChip(netProfitQoQ)}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelEps}</span>
            <span class="metric-row__value">${formatNumber(eps, 2)}</span>
          </div>
          <div class="metric-row">
            <span class="metric-row__label">${dict.labelRoe}</span>
            <span>${renderTrendChip(roe)}</span>
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
    const response = await fetch(`${API_BASE}/performance/express?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const data = await response.json();
    STATE.total = Number(data.total) || 0;
    STATE.items = Array.isArray(data.items) ? data.items : [];
    renderTable(STATE.items);
    updatePagination();
  } catch (error) {
    console.error("Failed to load performance express data", error);
    STATE.items = [];
    STATE.total = 0;
    renderTable([]);
    updatePagination();
  }
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
  initEvents();
  loadData();
}

document.addEventListener("DOMContentLoaded", init);
