const translations = getTranslations("marginAccount");
const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js";

let currentLang = getInitialLanguage();
let currentItems = [];
let chartInstance = null;
let resizeListenerBound = false;
let echartsLoader = null;

const state = {
  startDate: "",
  endDate: "",
  total: 0,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  startDate: document.getElementById("margin-start-date"),
  endDate: document.getElementById("margin-end-date"),
  resetButton: document.getElementById("margin-reset"),
  tableBody: document.getElementById("margin-account-tbody"),
  chartContainer: document.getElementById("margin-account-chart"),
  chartEmpty: document.getElementById("margin-account-chart-empty"),
};

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

function ensureEchartsLoaded() {
  if (window.echarts) {
    return Promise.resolve();
  }
  if (echartsLoader) {
    return echartsLoader;
  }
  echartsLoader = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = ECHARTS_CDN;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => {
      echartsLoader = null;
      reject(new Error("Failed to load chart library"));
    };
    document.head.appendChild(script);
  });
  return echartsLoader;
}

function ensureChartInstance() {
  if (!window.echarts || !elements.chartContainer) {
    return null;
  }
  if (!chartInstance) {
    chartInstance = window.echarts.init(elements.chartContainer);
  }
  if (!resizeListenerBound) {
    window.addEventListener("resize", handleResize);
    resizeListenerBound = true;
  }
  return chartInstance;
}

function handleResize() {
  if (chartInstance) {
    chartInstance.resize();
  }
}

function clearChart() {
  if (chartInstance) {
    chartInstance.dispose();
    chartInstance = null;
  }
  if (resizeListenerBound) {
    window.removeEventListener("resize", handleResize);
    resizeListenerBound = false;
  }
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

function formatPercent(value, fractionDigits = 1) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${formatNumber(numeric, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  })}%`;
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  if (typeof value === "string") {
    return value;
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString().slice(0, 10);
    }
  } catch (error) {
    /* no-op */
  }
  return String(value);
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

  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });

  if (elements.chartEmpty && dict.chartEmpty) {
    elements.chartEmpty.textContent = dict.chartEmpty;
  }

  syncDateInputs();
  renderTable(currentItems);
  renderChart(currentItems);
}

function syncDateInputs() {
  if (elements.startDate) {
    elements.startDate.value = state.startDate || "";
  }
  if (elements.endDate) {
    elements.endDate.value = state.endDate || "";
  }
}

function renderEmptyRow(message) {
  if (!elements.tableBody) {
    return;
  }
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = 8;
  cell.className = "table-empty";
  cell.textContent = message;
  row.appendChild(cell);
  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(row);
}

function renderChart(items = currentItems) {
  if (!elements.chartContainer || !elements.chartEmpty) {
    return;
  }

  if (!items || !items.length) {
    clearChart();
    elements.chartContainer.classList.add("hidden");
    elements.chartEmpty.classList.remove("hidden");
    return;
  }

  ensureEchartsLoaded()
    .then(() => {
      const instance = ensureChartInstance();
      if (!instance) {
        return;
      }

      const dict = getDict();
      const sorted = [...items]
        .filter((item) => item.tradeDate || item.trade_date)
        .sort((a, b) => {
          const left = new Date(a.tradeDate || a.trade_date).getTime();
          const right = new Date(b.tradeDate || b.trade_date).getTime();
          return left - right;
        });

      if (!sorted.length) {
        clearChart();
        elements.chartContainer.classList.add("hidden");
        elements.chartEmpty.classList.remove("hidden");
        return;
      }

      const xAxis = sorted.map((item) => item.tradeDate || item.trade_date);
      const financingBalance = sorted.map((item) => {
        const raw = item.financingBalance ?? item.financing_balance;
        const numeric = Number(raw);
        return Number.isFinite(numeric) ? numeric : null;
      });
      const financingPurchase = sorted.map((item) => {
        const raw = item.financingPurchaseAmount ?? item.financing_purchase_amount;
        const numeric = Number(raw);
        return Number.isFinite(numeric) ? numeric : null;
      });

      const option = {
        tooltip: {
          trigger: "axis",
          valueFormatter: (value) => formatNumber(value, { maximumFractionDigits: 2 }),
        },
        legend: {
          data: [dict.legendFinancingBalance, dict.legendFinancingPurchase],
          top: 0,
        },
        grid: { left: 48, right: 24, top: 40, bottom: 40 },
        xAxis: {
          type: "category",
          boundaryGap: false,
          data: xAxis,
          axisLabel: {
            formatter: (value) => value,
          },
        },
        yAxis: {
          type: "value",
          axisLabel: {
            formatter: (value) => formatNumber(value, { maximumFractionDigits: 0 }),
          },
        },
        series: [
          {
            name: dict.legendFinancingBalance,
            type: "line",
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 2 },
            areaStyle: { opacity: 0.12 },
            emphasis: { focus: "series" },
            data: financingBalance,
          },
          {
            name: dict.legendFinancingPurchase,
            type: "line",
            smooth: true,
            showSymbol: false,
            lineStyle: { width: 2 },
            emphasis: { focus: "series" },
            data: financingPurchase,
          },
        ],
      };

      elements.chartEmpty.classList.add("hidden");
      elements.chartContainer.classList.remove("hidden");
      instance.clear();
      instance.setOption(option, { notMerge: true });
      requestAnimationFrame(() => {
        instance.resize();
      });
    })
    .catch(() => {
      clearChart();
      elements.chartContainer.classList.add("hidden");
      elements.chartEmpty.classList.remove("hidden");
    });
}

function renderTable(items = currentItems) {
  if (!elements.tableBody) {
    return;
  }
  const filtered = items;

  if (!filtered.length) {
    const dict = getDict();
    const key = currentLang === "zh" ? "data-empty-zh" : "data-empty-en";
    const fallback = currentLang === "zh" ? "暂无两融账户数据。" : "No margin account data.";
    const message =
      (elements.tableBody.dataset && elements.tableBody.dataset[key]) || dict.empty || fallback;
    renderEmptyRow(message);
    return;
  }

  const fragment = document.createDocumentFragment();
  const dict = getDict();

  filtered.forEach((item) => {
    const tradeDate = item.tradeDate || item.trade_date;
    const financingBalance = item.financingBalance ?? item.financing_balance;
    const financingPurchase = item.financingPurchaseAmount ?? item.financing_purchase_amount;
    const securitiesBalance = item.securitiesLendingBalance ?? item.securities_lending_balance;
    const securitiesSell = item.securitiesLendingSellAmount ?? item.securities_lending_sell_amount;
    const collateralValue = item.collateralValue ?? item.collateral_value;
    const collateralRatio = item.averageCollateralRatio ?? item.average_collateral_ratio;
    const participatingInvestors =
      item.participatingInvestorCount ?? item.participating_investor_count;

    const securitiesCompanies =
      item.securitiesCompanyCount ?? item.securities_company_count;
    const businessDepartments =
      item.businessDepartmentCount ?? item.business_department_count;
    const individualInvestors =
      item.individualInvestorCount ?? item.individual_investor_count;
    const institutionalInvestors =
      item.institutionalInvestorCount ?? item.institutional_investor_count;
    const liabilityInvestors =
      item.liabilityInvestorCount ?? item.liability_investor_count;

    const mainRow = document.createElement("tr");
    mainRow.className = "margin-account-row";

    const cells = [
      formatDate(tradeDate),
      formatNumber(financingBalance, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      formatNumber(financingPurchase, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      formatNumber(securitiesBalance, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      formatNumber(securitiesSell, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      formatNumber(collateralValue, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
      formatPercent(collateralRatio, 1),
      formatNumber(participatingInvestors, { maximumFractionDigits: 0 }),
    ];

    cells.forEach((text) => {
      const cell = document.createElement("td");
      cell.textContent = text;
      mainRow.appendChild(cell);
    });

    fragment.appendChild(mainRow);

    const detailRow = document.createElement("tr");
    detailRow.className = "margin-account-detail-row";
    const detailCell = document.createElement("td");
    detailCell.colSpan = 8;

    const detailContainer = document.createElement("div");
    detailContainer.className = "margin-account-detail";

    const detailItems = [
      {
        label: dict.colSecuritiesCompanies,
        value: formatNumber(securitiesCompanies, { maximumFractionDigits: 0 }),
      },
      {
        label: dict.colBusinessDepartments,
        value: formatNumber(businessDepartments, { maximumFractionDigits: 0 }),
      },
      {
        label: dict.colIndividualInvestors,
        value: formatNumber(individualInvestors, { maximumFractionDigits: 0 }),
      },
      {
        label: dict.colInstitutionalInvestors,
        value: formatNumber(institutionalInvestors, { maximumFractionDigits: 0 }),
      },
      {
        label: dict.colLiabilityInvestors,
        value: formatNumber(liabilityInvestors, { maximumFractionDigits: 0 }),
      },
    ];

    detailItems.forEach((detail) => {
      const dl = document.createElement("dl");
      const dt = document.createElement("dt");
      const dd = document.createElement("dd");
      dt.textContent = detail.label || "";
      dd.textContent = detail.value;
      dl.appendChild(dt);
      dl.appendChild(dd);
      detailContainer.appendChild(dl);
    });

    detailCell.appendChild(detailContainer);
    detailRow.appendChild(detailCell);
    fragment.appendChild(detailRow);
  });

  elements.tableBody.innerHTML = "";
  elements.tableBody.appendChild(fragment);
}

function onDateChange() {
  const start = elements.startDate?.value || "";
  const end = elements.endDate?.value || "";
  state.startDate = start;
  state.endDate = end;
  loadMarginAccountData();
}

function resetFilters() {
  state.startDate = "";
  state.endDate = "";
  syncDateInputs();
  loadMarginAccountData();
}

function validateDateOrder() {
  if (!state.startDate || !state.endDate) {
    return;
  }
  if (state.startDate > state.endDate) {
    const temp = state.startDate;
    state.startDate = state.endDate;
    state.endDate = temp;
  }
  syncDateInputs();
}

async function loadMarginAccountData() {
  try {
    validateDateOrder();
    syncDateInputs();
    const params = new URLSearchParams();
    params.set("limit", "100");
    params.set("offset", "0");
    if (state.startDate) {
      params.set("startDate", state.startDate);
    }
    if (state.endDate) {
      params.set("endDate", state.endDate);
    }

    const response = await fetch(`${API_BASE}/margin/account?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }

    const payload = await response.json();
    currentItems = Array.isArray(payload.items) ? payload.items : [];
    state.total = Number(payload.total) || currentItems.length;
    renderTable(currentItems);
    renderChart(currentItems);
  } catch (error) {
    console.error("Failed to load margin account data", error);
    currentItems = [];
    state.total = 0;
    syncDateInputs();
    renderTable([]);
    renderChart([]);
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

  if (elements.startDate) {
    elements.startDate.addEventListener("change", onDateChange);
  }
  if (elements.endDate) {
    elements.endDate.addEventListener("change", onDateChange);
  }

  if (elements.resetButton) {
    elements.resetButton.addEventListener("click", resetFilters);
  }
}

function initialize() {
  bindEvents();
  applyTranslations();
  loadMarginAccountData();
}

initialize();
