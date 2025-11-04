const translations = getTranslations("marketOverview");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  reasonedAt: document.getElementById("market-overview-reasoned-at"),
  realtimeTable: document.querySelector("#market-overview-realtime tbody"),
  marketFundFlow: document.querySelector("#market-overview-market-fund-flow tbody"),
  hsgtFundFlow: document.querySelector("#market-overview-hsgt-fund-flow tbody"),
  marginAccount: document.querySelector("#market-overview-margin-account tbody"),
  activityTable: document.querySelector("#market-overview-activity tbody"),
  historyContainer: document.getElementById("market-overview-history"),
  streamContainer: document.getElementById("market-overview-stream"),
  modelLabel: document.getElementById("market-overview-model"),
  reasonButton: document.getElementById("market-overview-reason"),
  summaryContainer: document.getElementById("market-overview-summary"),
  sourceInsights: {
    market: {
      container: document.getElementById("market-overview-source-market"),
      body: document.getElementById("market-overview-source-market-body"),
      time: document.getElementById("market-overview-source-market-time"),
    },
    peripheral: {
      container: document.getElementById("market-overview-source-peripheral"),
      body: document.getElementById("market-overview-source-peripheral-body"),
      time: document.getElementById("market-overview-source-peripheral-time"),
    },
    macro: {
      container: document.getElementById("market-overview-source-macro"),
      body: document.getElementById("market-overview-source-macro-body"),
      time: document.getElementById("market-overview-source-macro-time"),
    },
  },
};

let currentLang = getInitialLanguage();
let isStreaming = false;

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

function formatNumber(value, options = {}) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return String(value);
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(numeric);
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value) * 100;
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric.toFixed(digits)}%`;
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  if (typeof value === "string") {
    return value.slice(0, 10);
  }
  try {
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString().slice(0, 10);
    }
  } catch (error) {
    /* no-op */
  }
  return String(value);
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
      return trimmed;
    }
  }
  let isoString;
  if (value instanceof Date) {
    isoString = value.toISOString();
  } else if (typeof value === "string") {
    isoString = value.trim().replace(/\s/, "T");
  } else {
    isoString = String(value);
  }

  if (!isoString) {
    return "--";
  }

  if (typeof value === "string" && !/[zZ]|[+-]\d{2}:?\d{2}$/.test(isoString)) {
    isoString = `${isoString}+08:00`;
  }

  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) {
    return formatDate(value);
  }

  const locale = currentLang === "zh" ? "zh-CN" : "en-GB";
  const formatter = new Intl.DateTimeFormat(locale, {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  const parts = formatter.formatToParts(date);
  const lookup = {};
  parts.forEach((part) => {
    if (part.type !== "literal") {
      lookup[part.type] = part.value;
    }
  });

  if (!lookup.year || !lookup.month || !lookup.day || !lookup.hour || !lookup.minute) {
    return formatter.format(date);
  }

  return `${lookup.year}-${lookup.month}-${lookup.day} ${lookup.hour}:${lookup.minute}`;
}

function applyTranslations() {
  const dict = getDict();
  document.title = dict.title || document.title;
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const key = el.dataset.i18n;
    if (dict[key]) {
      el.textContent = dict[key];
    }
  });
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });
}

function renderRealtime(rows) {
  const body = elements.realtimeTable;
  if (!body) {
    return;
  }
  body.innerHTML = "";
  if (!rows || !rows.length) {
    const key = currentLang === "zh" ? "emptyZh" : "emptyEn";
    const message = body.dataset && body.dataset[key] ? body.dataset[key] : "--";
    const emptyRow = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 6;
    cell.className = "table-empty";
    cell.textContent = message;
    emptyRow.appendChild(cell);
    body.appendChild(emptyRow);
    return;
  }

  rows.forEach((item) => {
    const row = document.createElement("tr");
    const cells = [
      item.code,
      item.name,
      formatNumber(item.latest_price, { maximumFractionDigits: 2 }),
      formatNumber(item.change_amount, { maximumFractionDigits: 2 }),
      formatPercent(item.change_percent, 2),
      formatNumber(item.turnover, { notation: "compact", maximumFractionDigits: 2 }),
    ];
    cells.forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.appendChild(cell);
    });
    body.appendChild(row);
  });
}

function renderFundFlow(table, rows, columns) {
  if (!table) {
    return;
  }
  table.innerHTML = "";
  if (!rows || !rows.length) {
    const key = currentLang === "zh" ? "emptyZh" : "emptyEn";
    const message = table.dataset && table.dataset[key] ? table.dataset[key] : "--";
    const emptyRow = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = columns.length;
    cell.className = "table-empty";
    cell.textContent = message;
    emptyRow.appendChild(cell);
    table.appendChild(emptyRow);
    return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    columns.forEach((column) => {
      const cell = document.createElement("td");
      const value = item[column.key];
      if (column.format === "percent") {
        cell.textContent = formatPercent(value, 2);
      } else if (column.format === "number") {
        cell.textContent = formatNumber(value, { notation: "compact", maximumFractionDigits: 2 });
      } else {
        cell.textContent = column.key === "trade_date" ? formatDate(value) : formatNumber(value, { maximumFractionDigits: 2 });
      }
      row.appendChild(cell);
    });
    table.appendChild(row);
  });
}

function renderHistory(history) {
  const container = elements.historyContainer;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!history) {
    return;
  }
  const dict = getDict();
  const labels = {
    close: dict.historyCloseLabel || "Close",
    open: dict.historyOpenLabel || "Open",
    high: dict.historyHighLabel || "High",
    low: dict.historyLowLabel || "Low",
    amplitude: dict.historyAmplitudeLabel || "Amplitude",
    turnover: dict.historyTurnoverLabel || "Turnover",
    volume: dict.historyVolumeLabel || "Volume",
    amount: dict.historyAmountLabel || "Amount",
  };
  Object.entries(history).forEach(([code, series]) => {
    const block = document.createElement("div");
    block.className = "market-overview-history-block";
    const title = document.createElement("h4");
    const indexName = series && series.length ? series[0].index_name || series[0].indexName : null;
    title.textContent = indexName ? `${indexName} (${code})` : code;
    block.appendChild(title);
    const list = document.createElement("ul");
    list.className = "market-overview-history-list";
    (series || []).forEach((entry) => {
      const li = document.createElement("li");
      li.className = "market-overview-history-row";
      const date = formatDate(entry.trade_date);
      const open = formatNumber(entry.open, { maximumFractionDigits: 2 });
      const close = formatNumber(entry.close, { maximumFractionDigits: 2 });
      const high = formatNumber(entry.high, { maximumFractionDigits: 2 });
      const low = formatNumber(entry.low, { maximumFractionDigits: 2 });
      const pct = formatPercent(entry.pct_change, 2);
      const amplitude = formatPercent(entry.amplitude, 2);
      const turnover = formatPercent(entry.turnover, 2);
      const volumeText =
        entry.volume === undefined || entry.volume === null
          ? "--"
          : formatNumber(entry.volume, { notation: "compact", maximumFractionDigits: 2 });
      const amountText =
        entry.amount === undefined || entry.amount === null
          ? "--"
          : formatNumber(entry.amount, { notation: "compact", maximumFractionDigits: 2 });

      li.innerHTML = `
        <div class="market-overview-history-row__primary">
          <span>${date}</span>
          <span>${labels.close} ${close} (${pct})</span>
        </div>
        <div class="market-overview-history-row__meta">
          <span>${labels.open} ${open}</span>
          <span>${labels.high} ${high}</span>
          <span>${labels.low} ${low}</span>
          <span>${labels.amplitude} ${amplitude}</span>
        </div>
        <div class="market-overview-history-row__meta">
          <span>${labels.turnover} ${turnover}</span>
          <span>${labels.volume} ${volumeText}</span>
          <span>${labels.amount} ${amountText}</span>
        </div>
      `;
      list.appendChild(li);
    });
    if (!series || !series.length) {
      const li = document.createElement("li");
      li.textContent = currentLang === "zh" ? "暂无数据" : "No data";
      list.appendChild(li);
    }
    block.appendChild(list);
    container.appendChild(block);
  });
}

function extractTimestamp(data) {
  if (!data) return null;
  return (
    data.generated_at ||
    data.generatedAt ||
    data.snapshot_date ||
    data.snapshotDate ||
    data.updated_at ||
    data.updatedAt ||
    data.created_at ||
    data.createdAt ||
    null
  );
}

function renderSourceInsight(target, source) {
  if (!target || !target.container || !target.body) {
    return;
  }
  const { container, body, time } = target;
  const emptyText = container.dataset && (currentLang === "zh" ? container.dataset.emptyZh : container.dataset.emptyEn);

  if (!source) {
    container.classList.add("is-empty");
    body.textContent = emptyText || "--";
    if (time) {
      time.textContent = "--";
    }
    return;
  }

  let content = source.summary_json || source.summary || source.raw_response || source.content || source.text;
  if (!content && Array.isArray(source.datasets)) {
    content = source.datasets
      .map((item) => `${item.titleKey || item.key}: ${toDisplayText(item.latest || item.summary)}`)
      .filter(Boolean)
      .join("\n");
  }
  let text = toDisplayText(content);
  if (!text) {
    text = emptyText || "--";
  }

  const isPlaceholder = !text || text === (emptyText || "") || text === "--";
  body.textContent = text;
  container.classList.toggle("is-empty", isPlaceholder);

  if (time) {
    const timestamp = extractTimestamp(source);
    time.textContent = timestamp ? formatDateTime(timestamp) : "--";
  }
}

function renderActivity(rows) {
  const body = elements.activityTable;
  if (!body) {
    return;
  }
  body.innerHTML = "";
  if (!rows || !rows.length) {
    const key = currentLang === "zh" ? "emptyZh" : "emptyEn";
    const message = body.dataset && body.dataset[key] ? body.dataset[key] : "--";
    const emptyRow = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 3;
    cell.className = "table-empty";
    cell.textContent = message;
    emptyRow.appendChild(cell);
    body.appendChild(emptyRow);
    return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    const metric = document.createElement("td");
    metric.textContent = item.metric || "--";
    const value = document.createElement("td");
    value.textContent = item.value_text || item.value_number || "--";
    const updated = document.createElement("td");
    updated.textContent = formatDate(item.updated_at);
    row.append(metric, value, updated);
    body.appendChild(row);
  });
}

function normaliseConfidence(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    if (numeric <= 1.2 && numeric >= 0) {
      return `${Math.round(numeric * 100)}%`;
    }
    return `${Math.round(numeric)}%`;
  }
  return String(value);
}

function toDisplayText(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value.trim();
  }
  if (Array.isArray(value)) {
    return value.map((item) => toDisplayText(item)).filter(Boolean).join("；");
  }
  if (typeof value === "object") {
    const keys = ["detail", "description", "value", "text", "content", "summary", "body"];
    for (const key of keys) {
      if (value[key]) {
        return toDisplayText(value[key]);
      }
    }
    const label = value.title || value.name || value.label;
    if (label) {
      return label;
    }
    try {
      return JSON.stringify(value, null, 2);
    } catch (error) {
      return String(value);
    }
  }
  return String(value);
}

function renderReasoningSnapshot(snapshot) {
  const container = elements.summaryContainer;
  if (!container) {
    return false;
  }
  container.innerHTML = "";
  const dict = getDict();
  const emptyText = container.dataset && (currentLang === "zh" ? container.dataset.emptyZh : container.dataset.emptyEn);
  if (!snapshot || !snapshot.summary) {
    if (emptyText) {
      const empty = document.createElement("p");
      empty.className = "market-overview-summary__empty";
      empty.textContent = emptyText;
      container.appendChild(empty);
    }
    return false;
  }

  const summary = snapshot.summary || {};
  const meta = document.createElement("div");
  meta.className = "market-overview-summary__meta";
  const biasPill = document.createElement("span");
  biasPill.className = "market-overview-bias";
  const biasKey = typeof summary.bias === "string" ? summary.bias.toLowerCase() : "neutral";
  biasPill.dataset.bias = biasKey || "neutral";
  const biasLabels = (dict.biasLabels || {});
  biasPill.textContent =
    biasLabels[biasKey] ||
    summary.bias ||
    (currentLang === "zh" ? "中性" : "Neutral");
  meta.appendChild(biasPill);

  const confidenceLabel = normaliseConfidence(summary.confidence);
  if (confidenceLabel) {
    const confidenceEl = document.createElement("span");
    confidenceEl.className = "market-overview-confidence";
    confidenceEl.textContent = `${dict.confidenceLabel || "Confidence"} ${confidenceLabel}`;
    meta.appendChild(confidenceEl);
  }
  container.appendChild(meta);

  const sections = [];
  const createSection = (title) => {
    const section = document.createElement("div");
    section.className = "market-overview-summary__section";
    if (title) {
      const heading = document.createElement("h4");
      heading.textContent = title;
      section.appendChild(heading);
    }
    sections.push(section);
    return section;
  };

  const summaryText = toDisplayText(summary.summary);
  if (summaryText) {
    const section = createSection(dict.summarySectionTitle || "Summary");
    const paragraph = document.createElement("p");
    paragraph.className = "market-overview-summary__text";
    paragraph.textContent = summaryText;
    section.appendChild(paragraph);
  }

  const signals = Array.isArray(summary.key_signals) ? summary.key_signals : [];
  if (signals.length) {
    const section = createSection(dict.signalsSectionTitle || "Key Signals");
    const list = document.createElement("ol");
    list.className = "market-overview-summary__signals";
    signals.forEach((item, index) => {
      const li = document.createElement("li");
      if (item && typeof item === "object" && !Array.isArray(item)) {
        const title =
          item.title ||
          item.name ||
          `${dict.signalLabel || "Signal"} ${index + 1}`;
        const detail = toDisplayText(item.detail || item.description || item.value);
        li.textContent = detail ? `${title} · ${detail}` : title;
      } else {
        li.textContent = toDisplayText(item);
      }
      list.appendChild(li);
    });
    section.appendChild(list);
  }

  const suggestionText = toDisplayText(summary.position_suggestion);
  if (suggestionText) {
    const section = createSection(dict.positionSectionTitle || "Position Suggestion");
    const suggestionEl = document.createElement("p");
    suggestionEl.className = "market-overview-summary__suggestion";
    suggestionEl.textContent = suggestionText;
    section.appendChild(suggestionEl);
  }

  const risks = Array.isArray(summary.risks) ? summary.risks : [];
  if (risks.length) {
    const section = createSection(dict.risksSectionTitle || "Risks");
    const list = document.createElement("ul");
    list.className = "market-overview-summary__risks";
    risks.forEach((item) => {
      const li = document.createElement("li");
      li.textContent = toDisplayText(item);
      list.appendChild(li);
    });
    section.appendChild(list);
  }

  sections.forEach((section) => container.appendChild(section));

  const hasMainContent = Boolean(summaryText || signals.length || suggestionText || risks.length);
  const hasBiasMeta = Boolean(summary.bias);
  const hasConfidenceMeta = summary.confidence !== undefined && summary.confidence !== null;
  if (!hasMainContent) {
    if (!hasBiasMeta && !hasConfidenceMeta && emptyText) {
      const empty = document.createElement("p");
      empty.className = "market-overview-summary__empty";
      empty.textContent = emptyText;
      container.appendChild(empty);
    }
    return hasBiasMeta || hasConfidenceMeta;
  }

  return true;
}

function applyReasoningSnapshot(snapshot) {
  const hasContent = renderReasoningSnapshot(snapshot);
  const dict = getDict();
  if (!isStreaming && elements.summaryContainer) {
    elements.summaryContainer.classList.remove("is-hidden");
  }
  if (!isStreaming && elements.streamContainer) {
    elements.streamContainer.classList.add("is-hidden");
    if (!hasContent) {
      const empty = elements.summaryContainer && elements.summaryContainer.querySelector(".market-overview-summary__empty");
      if (!empty) {
        const message =
          elements.summaryContainer &&
          (currentLang === "zh"
            ? elements.summaryContainer.dataset.emptyZh
            : elements.summaryContainer.dataset.emptyEn);
        if (message && elements.summaryContainer) {
          const placeholder = document.createElement("p");
          placeholder.className = "market-overview-summary__empty";
          placeholder.textContent = message;
          elements.summaryContainer.appendChild(placeholder);
        }
      }
    }
  }
  if (elements.reasonedAt) {
    elements.reasonedAt.textContent = snapshot && snapshot.generatedAt ? formatDateTime(snapshot.generatedAt) : "--";
  }
  if (elements.modelLabel) {
    if (snapshot && snapshot.model) {
      elements.modelLabel.textContent = snapshot.model;
    } else {
      elements.modelLabel.textContent = dict.modelReasoner || dict.modelDeepseek || "DeepSeek Reasoner";
    }
  }
}

async function loadOverview() {
  try {
    const response = await fetch(`${API_BASE}/market/overview`);
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    renderRealtime(payload.realtimeIndices || []);
    renderFundFlow(elements.marketFundFlow, payload.marketFundFlow || [], [
      { key: "trade_date" },
      { key: "main_net_inflow_amount", format: "number" },
      { key: "small_order_net_inflow_amount", format: "number" },
      { key: "medium_order_net_inflow_amount", format: "number" },
      { key: "large_order_net_inflow_amount", format: "number" },
    ]);
    renderFundFlow(elements.hsgtFundFlow, payload.hsgtFundFlow || [], [
      { key: "trade_date" },
      { key: "net_buy_amount", format: "number" },
      { key: "fund_inflow", format: "number" },
      { key: "market_value", format: "number" },
    ]);
    renderFundFlow(elements.marginAccount, payload.marginAccount || [], [
      { key: "trade_date" },
      { key: "financing_balance", format: "number" },
      { key: "financing_purchase_amount", format: "number" },
      { key: "participating_investor_count", format: "number" },
    ]);
    renderHistory(payload.indexHistory || {});
    renderSourceInsight(elements.sourceInsights.market, payload.marketInsight || null);
    renderSourceInsight(elements.sourceInsights.peripheral, payload.peripheralInsight || null);
    renderSourceInsight(elements.sourceInsights.macro, payload.macroInsight || null);
    renderActivity(payload.marketActivity || []);
    applyReasoningSnapshot(payload.latestReasoning);
  } catch (error) {
    console.error("Failed to load market overview", error);
  }
}

async function streamReasoning(runLLM = true) {
  if (!elements.streamContainer || !elements.reasonButton) {
    return;
  }
  const dict = getDict();
  isStreaming = true;
  elements.reasonButton.disabled = true;
  elements.reasonButton.classList.add("loading");
  if (elements.summaryContainer) {
    elements.summaryContainer.classList.add("is-hidden");
  }
  elements.streamContainer.classList.remove("is-hidden");
  elements.streamContainer.textContent = "";
  if (elements.modelLabel) {
    elements.modelLabel.textContent = dict.modelReasoner || dict.modelDeepseek || "DeepSeek Reasoner";
  }
  try {
    const response = await fetch(`${API_BASE}/market/overview/reason`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ runLLM }),
    });
    if (!response.ok || !response.body) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8");
    let done = false;
    let text = "";
    while (!done) {
      const result = await reader.read();
      done = result.done;
      if (result.value) {
        const chunk = decoder.decode(result.value, { stream: !done });
        text += chunk;
        elements.streamContainer.textContent += chunk;
        elements.streamContainer.scrollTop = elements.streamContainer.scrollHeight;
      }
    }
    elements.streamContainer.textContent = text.trim();
  } catch (error) {
    console.error("Failed to stream reasoning", error);
    const msg = currentLang === "zh" ? "推理失败，请稍后重试。" : "Reasoning failed. Please retry.";
    elements.streamContainer.textContent = msg;
  } finally {
    elements.reasonButton.disabled = false;
    elements.reasonButton.classList.remove("loading");
    isStreaming = false;
    try {
      await loadOverview();
    } catch (error) {
      console.error("Failed to refresh overview after reasoning", error);
    }
  }
}

function bindEvents() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const lang = btn.dataset.lang;
      if (lang && lang !== currentLang && translations[lang]) {
        currentLang = lang;
        persistLanguage(lang);
        applyTranslations();
        loadOverview();
      }
    });
  });
  if (elements.reasonButton) {
    elements.reasonButton.addEventListener("click", () => streamReasoning(true));
  }
}

function initialize() {
  applyTranslations();
  bindEvents();
  loadOverview();
  if (elements.streamContainer) {
    const empty = currentLang === "zh" ? elements.streamContainer.dataset.emptyZh : elements.streamContainer.dataset.emptyEn;
    if (empty) {
      elements.streamContainer.textContent = empty;
    }
    elements.streamContainer.classList.add("is-hidden");
  }
  if (elements.modelLabel) {
    const dict = getDict();
    elements.modelLabel.textContent = dict.modelReasoner || dict.modelDeepseek || "DeepSeek Reasoner";
  }
}

initialize();
