console.info("Observation Pool bundle v20270452");

const translations = getTranslations("observationPool");
const LANG_STORAGE_KEY = "trend-view-lang";
const HISTORY_STORAGE_KEY = "observation-pool-history";
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const TAB_STRATEGY_MAP = {
  rangeBreakout: { id: "range_breakout", labelKey: "tabRangeBreakout" },
  bottoming: { id: "bottoming_stage1", labelKey: "tabBottoming" },
  mainRally: { id: "main_rally_stage2", labelKey: "tabMainRally" },
  vcp: { id: "volatility_contraction", labelKey: "tabVcp" },
};

const elements = {
  root: document.getElementById("observation-pool-root"),
  refresh: document.getElementById("observation-refresh"),
  history: document.getElementById("observation-history"),
  generated: document.getElementById("observation-generated"),
  tradeDate: document.getElementById("observation-trade-date"),
  status: document.getElementById("observation-status"),
  rangePanel: document.getElementById("observation-range-breakout"),
  bottomingPanel: document.getElementById("observation-bottoming"),
  mainRallyPanel: document.getElementById("observation-main-rally"),
  vcpPanel: document.getElementById("observation-vcp"),
  tabButtons: document.querySelectorAll(".detail-tabs__btn"),
  langButtons: document.querySelectorAll(".lang-btn"),
  historyModal: document.getElementById("observation-history-modal"),
  historyOverlay: document.getElementById("observation-history-overlay"),
  historyClose: document.getElementById("observation-history-close"),
  historyList: document.getElementById("observation-history-list"),
};

const state = {
  lang: getInitialLanguage(),
  data: null,
  loading: false,
  statusTimer: null,
  history: loadHistoryRecords(),
  activeTab: "rangeBreakout",
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

function loadHistoryRecords() {
  try {
    const stored = window.localStorage.getItem(HISTORY_STORAGE_KEY);
    if (!stored) {
      return [];
    }
    const parsed = JSON.parse(stored);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((item) => item && typeof item === "object");
  } catch (error) {
    return [];
  }
}

function saveHistoryRecords(records) {
  try {
    window.localStorage.setItem(HISTORY_STORAGE_KEY, JSON.stringify(records));
  } catch (error) {
    /* ignore */
  }
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

function storeHistorySnapshot(payload) {
  if (!payload || typeof payload !== "object") {
    return;
  }
  const entryId = payload.generatedAt || `${Date.now()}`;
  const entry = {
    id: entryId,
    generatedAt: payload.generatedAt,
    latestTradeDate: payload.latestTradeDate,
    totalCandidates: payload.totalCandidates,
    universeTotal: payload.universeTotal,
    data: payload,
  };
  const existing = loadHistoryRecords();
  const deduped = existing.filter((item) => item && item.id !== entryId);
  const records = [entry, ...deduped].slice(0, 12);
  saveHistoryRecords(records);
  state.history = records;
  renderHistoryList();
}

function applyHistoryEntry(entry, { silentStatus = false } = {}) {
  if (!entry) {
    return;
  }
  const payload = entry.data || entry;
  if (!payload || typeof payload !== "object") {
    return;
  }
  state.data = payload;
  renderObservationPool();
  if (!silentStatus) {
    setStatus(getDict().statusHistoryLoaded || "已载入历史记录", "info", 3000);
  }
}

function extractStrategyFromSnapshot(snapshot, strategyId) {
  if (!snapshot || !Array.isArray(snapshot.strategies)) {
    return null;
  }
  return snapshot.strategies.find((item) => item.id === strategyId) || null;
}

function renderHistoryList() {
  const container = elements.historyList;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const dict = getDict();
  const mapping = TAB_STRATEGY_MAP[state.activeTab] || TAB_STRATEGY_MAP.rangeBreakout;
  const strategyLabel = dict[mapping.labelKey] || dict.tabRangeBreakout || "Range Breakout";
  const records = state.history || [];
  const filtered = records
    .map((entry) => {
      const snapshot = entry.data || entry;
      if (!snapshot) {
        return null;
      }
      const strategy = extractStrategyFromSnapshot(snapshot, mapping.id);
      if (!strategy) {
        return null;
      }
      const strategyCount =
        strategy?.candidateCount ??
        strategy?.candidate_count ??
        snapshot?.totalCandidates ??
        entry.totalCandidates ??
        "--";
      return { entry, strategyCount };
    })
    .filter(Boolean);

  if (!filtered.length) {
    container.dataset.empty = "1";
    const empty = document.createElement("p");
    empty.className = "observation-history__empty";
    const zhText = container.dataset.emptyZh;
    const enText = container.dataset.emptyEn;
    empty.textContent = state.lang === "zh" ? zhText || dict.historyEmpty || "暂无历史记录。" : enText || dict.historyEmpty || "No history yet.";
    container.appendChild(empty);
    return;
  }

  container.dataset.empty = "0";
  filtered.forEach(({ entry, strategyCount }) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "observation-history__item";
    button.dataset.historyId = entry.id;
    button.innerHTML = `
      <div class="observation-history__title">
        <strong>${entry.latestTradeDate || "--"}</strong>
        <span>${formatDateTime(entry.generatedAt)}</span>
      </div>
      <div class="observation-history__meta">
        <span>${strategyLabel}: ${strategyCount}</span>
        <span>${dict.summaryUniverse || "覆盖标的"}: ${entry.universeTotal ?? "--"}</span>
      </div>
    `;
    container.appendChild(button);
  });
}

function openHistoryModal() {
  if (!elements.historyModal) return;
  elements.historyModal.classList.remove("hidden");
}

function closeHistoryModal() {
  if (!elements.historyModal) return;
  elements.historyModal.classList.add("hidden");
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
    storeHistorySnapshot(payload);
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
  renderStrategies(dict);
}

function renderStrategies(dict) {
  const panels = {
    rangeBreakout: elements.rangePanel,
    bottoming: elements.bottomingPanel,
    mainRally: elements.mainRallyPanel,
    vcp: elements.vcpPanel,
  };
  Object.entries(panels).forEach(([key, container]) => {
    if (!container) {
      return;
    }
    container.innerHTML = "";
    const mapping = TAB_STRATEGY_MAP[key];
    if (!mapping) {
      container.dataset.empty = "1";
      return;
    }
    const strategy = extractStrategyFromSnapshot(state.data, mapping.id);
    if (!strategy) {
      container.dataset.empty = "1";
      return;
    }
    container.dataset.empty = "0";
    const card = document.createElement("section");
    card.className = "observation-strategy";
    const paramsRow = buildStrategyParameterRow(strategy, dict);
    if (paramsRow) {
      card.appendChild(paramsRow);
    }
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
      const code = item.tsCode || item.ts_code || item.symbol || "";
      const detailUrl = code
        ? `stock-detail.html?code=${encodeURIComponent(code)}&back=${encodeURIComponent("observation-pool.html")}`
        : null;
      tr.innerHTML = `
        <td>${detailUrl ? `<a href="${detailUrl}">${code}</a>` : code || "--"}</td>
        <td>${detailUrl ? `<a href="${detailUrl}">${item.name || "--"}</a>` : item.name || "--"}</td>
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

function buildStrategyParameterRow(strategy, dict) {
  const parameters = strategy?.parameters || {};
  const chips = [];
  chips.push(`${dict.strategyCandidates || "候选"}: ${strategy?.candidateCount ?? "--"}`);
  if (parameters.volumeRatio !== undefined) {
    chips.push(`${dict.strategyVolumeRatio || "最小放量倍数"}: ${parameters.volumeRatio}`);
  }
  if (parameters.maxRangePercent !== undefined) {
    chips.push(`${dict.strategyRange || "区间振幅"}: ${parameters.maxRangePercent}%`);
  }
  if (!parameters) {
    return null;
  }
  if (parameters.lookbackDays) {
    chips.push(`${dict.paramLookback || "窗口"} ${parameters.lookbackDays}D`);
  }
  const minHistory = parameters.minHistoryDays ?? parameters.minHistory;
  if (minHistory) {
    chips.push(`${dict.paramMinHistory || "样本"} ${minHistory}D`);
  }
  if (parameters.breakoutBufferPercent !== undefined) {
    chips.push(`${dict.paramBreakout || "突破缓冲"} ${parameters.breakoutBufferPercent}%`);
  }
  if (parameters.volumeAverageWindow) {
    chips.push(`${dict.paramVolumeWindow || "均量窗口"} ${parameters.volumeAverageWindow}D`);
  }
  if (parameters.maxWeeklyGainPercent !== undefined) {
    chips.push(`${dict.paramWeeklyGain || "5日涨幅≤"} ${parameters.maxWeeklyGainPercent}%`);
  }
  if (parameters.requireBigDealInflow) {
    chips.push(dict.paramBigDeal || "需大单净流入");
  }
  if (!chips.length) {
    return null;
  }
  const wrapper = document.createElement("div");
  wrapper.className = "observation-strategy__params";
  wrapper.innerHTML = chips.map((chip) => `<span class="observation-param">${chip}</span>`).join("");
  return wrapper;
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

function setActiveTab(tabKey) {
  if (!tabKey) {
    return;
  }
  if (state.activeTab === tabKey) {
    renderHistoryList();
    return;
  }
  state.activeTab = tabKey;
  elements.tabButtons.forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.tab === state.activeTab);
  });
  document.querySelectorAll(".observation-tab-panel").forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.panel === state.activeTab);
  });
  renderHistoryList();
}

function initEventListeners() {
  if (elements.refresh) {
    elements.refresh.addEventListener("click", () => fetchObservationPool());
  }
  if (elements.history) {
    elements.history.addEventListener("click", () => openHistoryModal());
  }
  if (elements.historyOverlay) {
    elements.historyOverlay.addEventListener("click", closeHistoryModal);
  }
  if (elements.historyClose) {
    elements.historyClose.addEventListener("click", closeHistoryModal);
  }
  if (elements.historyList) {
    elements.historyList.addEventListener("click", (event) => {
      const target = event.target.closest("[data-history-id]");
      if (!target) return;
      const selected = state.history?.find((item) => item.id === target.dataset.historyId);
      if (selected) {
        applyHistoryEntry(selected);
        closeHistoryModal();
      }
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && elements.historyModal && !elements.historyModal.classList.contains("hidden")) {
      closeHistoryModal();
    }
  });
  elements.tabButtons.forEach((btn) => {
    btn.addEventListener("click", () => setActiveTab(btn.dataset.tab));
  });
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
  renderHistoryList();
}

function init() {
  initEventListeners();
  setActiveTab(state.activeTab);
  renderHistoryList();
  const mapping = TAB_STRATEGY_MAP[state.activeTab] || TAB_STRATEGY_MAP.rangeBreakout;
  const initialEntry = state.history?.find((entry) =>
    Boolean(extractStrategyFromSnapshot(entry.data || entry, mapping.id))
  );
  if (initialEntry) {
    applyHistoryEntry(initialEntry, { silentStatus: true });
  } else {
    setStatus(getDict().statusIdle || "观察池就绪", "info", 2000);
  }
}

document.addEventListener("DOMContentLoaded", init);
