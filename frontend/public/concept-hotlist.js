console.info("Concept hotlist module v20270437");

const translations = getTranslations("conceptHotlist");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  refreshButton: document.getElementById("concept-hotlist-refresh"),
  status: document.getElementById("concept-hotlist-status"),
  generatedAt: document.getElementById("concept-hotlist-generated-at"),
  symbols: document.getElementById("concept-hotlist-symbols"),
  grid: document.getElementById("concept-hotlist-grid"),
  langButtons: document.querySelectorAll(".lang-btn"),
};

const state = {
  lang: null,
  snapshot: null,
  loading: false,
};

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) return stored;
  } catch (error) {
    /* ignore */
  }
  const htmlLang = document.documentElement.getAttribute("data-pref-lang") || document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) return htmlLang;
  const navigatorLang = (navigator.language || "").toLowerCase();
  return translations[navigatorLang] ? navigatorLang : "zh";
}

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    /* ignore */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

function getDict() {
  return translations[state.lang] || translations.zh || translations.en;
}

function setStatus(message, tone) {
  const node = elements.status;
  if (!node) return;
  if (!message) {
    node.textContent = "";
    node.removeAttribute("data-tone");
    return;
  }
  node.textContent = message;
  if (tone) {
    node.setAttribute("data-tone", tone);
  } else {
    node.removeAttribute("data-tone");
  }
}

function setLoading(isLoading) {
  state.loading = isLoading;
  const button = elements.refreshButton;
  const dict = getDict();
  if (!button) return;
  const label = button.querySelector(".btn__label");
  if (isLoading) {
    button.disabled = true;
    button.dataset.loading = "1";
    if (label) label.textContent = dict.refreshing || "Refreshing...";
  } else {
    button.disabled = false;
    delete button.dataset.loading;
    if (label) label.textContent = dict.refreshButton || "Refresh";
  }
}

function formatDateTime(value) {
  if (!value) return "--";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, {
    hour: "2-digit",
    minute: "2-digit",
  })}`;
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return numeric.toFixed(digits);
}

function formatPercent(value, digits = 1) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  return `${numeric.toFixed(digits)}%`;
}

function formatMoney(value) {
  if (value === null || value === undefined) return "--";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "--";
  const absValue = Math.abs(numeric);
  if (absValue >= 1e8) {
    const unit = state.lang === "zh" ? "亿" : "B";
    return `${(numeric / 1e8).toFixed(2)}${unit}`;
  }
  if (absValue >= 1e4) {
    const unit = state.lang === "zh" ? "万" : "K";
    return `${(numeric / 1e4).toFixed(1)}${unit}`;
  }
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  return numeric.toLocaleString(locale, { maximumFractionDigits: 2 });
}

function formatLeading(stock, change) {
  if (!stock) return "--";
  const changeText = formatPercent(change);
  return changeText === "--" ? stock : `${stock} (${changeText})`;
}

function clearContainer(node, message) {
  if (!node) return;
  node.innerHTML = "";
  if (message) {
    const placeholder = document.createElement("div");
    placeholder.className = "flow-card__empty";
    placeholder.textContent = message;
    node.appendChild(placeholder);
  }
}

function renderSymbols(snapshot) {
  const container = elements.symbols;
  if (!container) return;
  container.innerHTML = "";
  const dict = getDict();
  const symbols = snapshot?.symbols || [];
  if (!symbols.length) {
    const placeholder = document.createElement("span");
    placeholder.textContent = dict.emptyHotlist || "No data available.";
    container.appendChild(placeholder);
    return;
  }
  symbols.forEach((item) => {
    const chip = document.createElement("span");
    chip.textContent = `${item.symbol} · ${dict.weightLabel || "weight"} ${formatNumber(item.weight, 2)}`;
    container.appendChild(chip);
  });
}

function buildStageTable(stages) {
  const dict = getDict();
  const table = document.createElement("table");
  table.className = "hotlist-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  [
    dict.tableHeaderSymbol || "Period",
    dict.tableHeaderRank || "Rank",
    dict.tableHeaderNet || "Net inflow",
    dict.tableHeaderInflow || "Inflow",
    dict.tableHeaderOutflow || "Outflow",
    dict.tableHeaderPriceChange || "Price %",
    dict.tableHeaderStageChange || "Stage %",
    dict.tableHeaderLeading || "Leader",
    dict.tableHeaderUpdated || "Updated",
  ].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  stages.forEach((stage) => {
    const row = document.createElement("tr");
    const cells = [
      stage.symbol,
      stage.rank ?? "--",
      formatMoney(stage.netAmount),
      formatMoney(stage.inflow),
      formatMoney(stage.outflow),
      formatPercent(stage.priceChangePercent, 1),
      formatPercent(stage.stageChangePercent, 1),
      formatLeading(stage.leadingStock, stage.leadingStockChangePercent),
      stage.updatedAt ? formatDateTime(stage.updatedAt) : "--",
    ];
    cells.forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value === undefined ? "--" : value;
      row.appendChild(td);
    });
    tbody.appendChild(row);
  });
  table.appendChild(tbody);
  return table;
}

function renderConceptCards(items) {
  const container = elements.grid;
  if (!container) return;
  const dict = getDict();
  if (!Array.isArray(items) || !items.length) {
    const message = dict.emptyHotlist || container.dataset[`empty${state.lang === "zh" ? "Zh" : "En"}`];
    clearContainer(container, message || "No data available.");
    return;
  }

  container.innerHTML = "";
  items.forEach((entry) => {
    const card = document.createElement("article");
    card.className = "flow-card";

    const header = document.createElement("header");
    header.className = "flow-card__header";
    const title = document.createElement("h3");
    title.className = "flow-card__title";
    title.textContent = entry.name || "--";
    header.appendChild(title);
    card.appendChild(header);

    const summary = document.createElement("div");
    summary.className = "flow-card__summary";
    summary.innerHTML = `
      <div class="flow-card__summary-item">
        <span>${dict.scoreLabel || "Score"}</span>
        <strong>${formatNumber(entry.score, 3)}</strong>
      </div>
      <div class="flow-card__summary-item">
        <span>${dict.totalNetLabel || "Total net"}</span>
        <strong>${formatMoney(entry.totalNetAmount)}</strong>
      </div>
      <div class="flow-card__summary-item">
        <span>${dict.bestRankLabel || "Best rank"}</span>
        <strong>${entry.bestRank ?? "--"}</strong>
      </div>
    `;
    card.appendChild(summary);

    const stats = document.createElement("div");
    stats.className = "flow-card__stats";
    stats.innerHTML = `
      <div class="flow-card__stat">
        <span>${dict.totalInflowLabel || "Inflow"}</span>
        <strong>${formatMoney(entry.totalInflow)}</strong>
      </div>
      <div class="flow-card__stat">
        <span>${dict.totalOutflowLabel || "Outflow"}</span>
        <strong>${formatMoney(entry.totalOutflow)}</strong>
      </div>
      <div class="flow-card__stat">
        <span>${dict.bestStageLabel || "Best period"}</span>
        <strong>${entry.bestSymbol || "--"}</strong>
      </div>
    `;
    card.appendChild(stats);

    if (Array.isArray(entry.stages) && entry.stages.length) {
      const table = buildStageTable(entry.stages);
      if (table) {
        const tableWrapper = document.createElement("div");
        tableWrapper.className = "flow-card__table-wrapper";
        tableWrapper.appendChild(table);
        card.appendChild(tableWrapper);
      }
    }

    container.appendChild(card);
  });
}

function renderAll() {
  renderSymbols(state.snapshot);
  renderConceptCards(state.snapshot?.concepts || []);
}

async function fetchHotlist() {
  setLoading(true);
  setStatus(getDict().statusLoading || "Loading latest rankings…", "info");
  try {
    const response = await fetch(`${API_BASE}/fund-flow/sector-hotlist`);
    if (!response.ok) throw new Error(`Failed to load sector hotlist: ${response.status}`);
    const snapshot = await response.json();
    state.snapshot = snapshot;
    renderAll();
    setStatus(`${getDict().statusLoaded || "Ranking updated."} ${formatDateTime(snapshot.generatedAt)}`);
  } catch (error) {
    console.error(error);
    setStatus(getDict().statusError || "Failed to load ranking, please retry.", "error");
  } finally {
    setLoading(false);
  }
}

function handleLanguageSwitch(event) {
  const button = event.currentTarget;
  const lang = button?.dataset?.lang;
  if (!lang || lang === state.lang || !translations[lang]) return;
  state.lang = lang;
  persistLanguage(lang);
  elements.langButtons.forEach((node) => {
    if (node.dataset.lang === lang) {
      node.classList.add("lang-btn--active");
    } else {
      node.classList.remove("lang-btn--active");
    }
  });
  renderAll();
}

function initLanguage() {
  state.lang = getInitialLanguage();
  elements.langButtons.forEach((button) => {
    if (button.dataset.lang === state.lang) {
      button.classList.add("lang-btn--active");
    }
    button.addEventListener("click", handleLanguageSwitch);
  });
  persistLanguage(state.lang);
}

function init() {
  initLanguage();
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => {
      if (!state.loading) fetchHotlist();
    });
  }
  fetchHotlist();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
