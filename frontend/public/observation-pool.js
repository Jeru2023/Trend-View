console.info("Observation Pool bundle v20270444");

const translations = getTranslations("observationPool");
const LANG_STORAGE_KEY = "trend-view-lang";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const elements = {
  root: document.getElementById("observation-pool-root"),
  refresh: document.getElementById("observation-refresh"),
  generated: document.getElementById("observation-generated"),
  tradeDate: document.getElementById("observation-trade-date"),
  status: document.getElementById("observation-status"),
  summary: document.getElementById("observation-summary"),
  strategies: document.getElementById("observation-strategies"),
  langButtons: document.querySelectorAll(".lang-btn"),
};

const state = {
  lang: getInitialLanguage(),
  data: null,
  loading: false,
  statusTimer: null,
};

persistLanguage(state.lang);

function getInitialLanguage() {
  try {
    const stored = window.localStorage.getItem(LANG_STORAGE_KEY);
    if (stored && translations[stored]) {
      return stored;
    }
  } catch (error) {
    /* ignore */
  }
  const attr = document.documentElement.getAttribute("data-pref-lang");
  if (attr && translations[attr]) {
    return attr;
  }
  return "zh";
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

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  const locale = state.lang === "zh" ? "zh-CN" : "en-US";
  const opts = state.lang === "zh"
    ? { year: "numeric", month: "2-digit", day: "2-digit", hour12: false, hour: "2-digit", minute: "2-digit" }
    : { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false };
  return date.toLocaleString(locale, opts);
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return numeric.toFixed(digits);
}

function setStatus(message, tone = null, timeout = 2500) {
  const target = elements.status;
  if (!target) {
    return;
  }
  target.textContent = message || "";
  if (tone) {
    target.dataset.tone = tone;
  } else {
    target.removeAttribute("data-tone");
  }
  if (state.statusTimer) {
    clearTimeout(state.statusTimer);
  }
  if (message) {
    state.statusTimer = window.setTimeout(() => {
      target.textContent = "";
      target.removeAttribute("data-tone");
      state.statusTimer = null;
    }, timeout);
  }
}

async function fetchObservationPool() {
  if (state.loading) {
    return;
  }
  state.loading = true;
  setStatus(getDict().statusLoading || "刷新中…", "info", 4000);
  if (elements.refresh) {
    elements.refresh.disabled = true;
  }
  try {
    const response = await fetch(`${API_BASE}/observation-pool`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    state.data = payload;
    renderObservationPool();
    setStatus(getDict().statusUpdated || "已更新", "success");
  } catch (error) {
    console.error("Failed to load observation pool", error);
    setStatus(getDict().statusError || "刷新失败，请稍后重试。", "error", 4000);
  } finally {
    state.loading = false;
    if (elements.refresh) {
      elements.refresh.disabled = false;
    }
  }
}

function renderObservationPool() {
  const dict = getDict();
  const data = state.data;
  if (elements.generated) {
    elements.generated.textContent = data?.generatedAt ? formatDateTime(data.generatedAt) : "--";
  }
  if (elements.tradeDate) {
    elements.tradeDate.textContent = data?.latestTradeDate || "--";
  }
  renderSummary(dict, data);
  renderStrategies(dict, data);
}

function renderSummary(dict, data) {
  const container = elements.summary;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!data) {
    container.dataset.empty = "1";
    return;
  }
  container.dataset.empty = "0";
  const statsList = document.createElement("ul");
  statsList.className = "insight-summary__list";
  const listItems = [
    {
      label: dict.summaryUniverse || "覆盖标的",
      value: data.universeTotal ?? 0,
    },
    {
      label: dict.summaryTotal || "候选数量",
      value: data.totalCandidates ?? 0,
    },
  ];
  listItems.forEach((item) => {
    const li = document.createElement("li");
    li.innerHTML = `<span>${item.label}</span><strong>${item.value ?? "--"}</strong>`;
    statsList.appendChild(li);
  });
  container.appendChild(statsList);
  if (Array.isArray(data.summaryNotes) && data.summaryNotes.length) {
    const noteList = document.createElement("ul");
    noteList.className = "insight-summary__notes";
    data.summaryNotes.forEach((note) => {
      const li = document.createElement("li");
      li.textContent = note;
      noteList.appendChild(li);
    });
    container.appendChild(noteList);
  }
}

function renderStrategies(dict, data) {
  const container = elements.strategies;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!data || !Array.isArray(data.strategies) || !data.strategies.length) {
    container.dataset.empty = "1";
    return;
  }
  container.dataset.empty = "0";
  data.strategies.forEach((strategy) => {
    const card = document.createElement("section");
    card.className = "observation-strategy";
    const header = document.createElement("header");
    header.className = "observation-strategy__header";
    const title = document.createElement("div");
    title.innerHTML = `<h3>${strategy.name}</h3><p>${strategy.description || ""}</p>`;
    header.appendChild(title);
    const stats = document.createElement("div");
    stats.className = "observation-strategy__stats";
    stats.innerHTML = `
      <span>${dict.strategyCandidates || "候选"}: <strong>${strategy.candidateCount}</strong></span>
      <span>${dict.strategyVolumeRatio || "最小放量倍数"}: ${strategy.parameters?.volumeRatio ?? "--"}</span>
      <span>${dict.strategyRange || "区间振幅"}: ${strategy.parameters?.maxRangePercent ?? "--"}%</span>
    `;
    header.appendChild(stats);
    card.appendChild(header);
    const table = document.createElement("table");
    table.className = "data-table observation-strategy__table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>${dict.columnCode || "代码"}</th>
          <th>${dict.columnName || "名称"}</th>
          <th>${dict.columnClose || "收盘"}</th>
          <th>${dict.columnPctChange || "涨跌幅"}</th>
          <th>${dict.columnVolumeRatio || "量能倍数"}</th>
          <th>${dict.columnRange || "区间振幅"}</th>
          <th>${dict.columnBreakout || "突破价"}</th>
          <th>${dict.columnTradeDate || "交易日"}</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");
    strategy.candidates.forEach((item) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${item.symbol || item.tsCode}</td>
        <td>${item.name || "--"}</td>
        <td>${formatNumber(item.close)}</td>
        <td>${formatNumber(item.pctChange)}</td>
        <td>${formatNumber(item.volumeRatio)}</td>
        <td>${formatNumber(item.rangeAmplitude)}%</td>
        <td>${formatNumber(item.breakoutLevel)}</td>
        <td>${item.latestTradeDate || "--"}</td>
      `;
      tbody.appendChild(tr);
    });
    card.appendChild(table);
    container.appendChild(card);
  });
}

function setLanguage(lang) {
  if (lang === state.lang) {
    return;
  }
  state.lang = lang;
  persistLanguage(state.lang);
  window.applyTranslations();
  renderObservationPool();
}

function initEventListeners() {
  if (elements.refresh) {
    elements.refresh.addEventListener("click", () => fetchObservationPool());
  }
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => setLanguage(btn.dataset.lang))
  );
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

function applyTranslations() {
  const dict = getDict();
  elements.langButtons.forEach((btn) => {
    const lang = btn.dataset.lang;
    btn.classList.toggle("lang-btn--active", lang === state.lang);
  });
  if (elements.status && !state.loading && state.data) {
    setStatus(dict.statusIdle || "观察池就绪", "info", 2000);
  }
}

function init() {
  initEventListeners();
  fetchObservationPool();
}

document.addEventListener("DOMContentLoaded", init);
