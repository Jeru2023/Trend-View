const translations = getTranslations("macroInsight");
const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);
const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  generatedAt: document.getElementById("macro-insight-generated"),
  summary: document.getElementById("macro-insight-summary"),
  model: document.getElementById("macro-insight-model"),
  highlights: document.getElementById("macro-insight-highlights"),
  datasets: document.getElementById("macro-insight-datasets"),
  warnings: document.getElementById("macro-insight-warnings"),
  refreshButton: document.getElementById("macro-insight-refresh"),
  history: document.getElementById("macro-insight-history"),
};

let currentLang = getInitialLanguage();
const state = {
  summary: null,
  generatedAt: null,
  model: null,
  datasets: [],
  warnings: [],
  history: [],
  fallbackSummary: null,
  fallbackGeneratedAt: null,
  snapshotDate: null,
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
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return String(value);
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return new Intl.NumberFormat(locale, options).format(number);
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "--";
  }
  return `${number.toFixed(digits)}%`;
}

function formatValue(value, format) {
  if (format === "percent") {
    return formatPercent(value);
  }
  if (format === "number") {
    return formatNumber(value, { maximumFractionDigits: 2 });
  }
  return formatNumber(value, { maximumFractionDigits: 2 });
}

function formatDate(value) {
  if (!value) {
    return "--";
  }
  if (typeof value === "string") {
    return value.slice(0, 10);
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

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  try {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    const locale = currentLang === "zh" ? "zh-CN" : "en-US";
    return `${date.toLocaleDateString(locale)} ${date.toLocaleTimeString(locale, {
      hour: "2-digit",
      minute: "2-digit",
    })}`;
  } catch (error) {
    return String(value);
  }
}

function renderSummary(summary, generatedAt, model, options = {}) {
  const { isFallback = false } = options;
  const dict = getDict();
  if (elements.generatedAt) {
    elements.generatedAt.textContent = generatedAt ? formatDateTime(generatedAt) : "--";
  }
  if (!elements.summary) {
    return;
  }
  elements.summary.innerHTML = "";
  if (!summary) {
    const emptyMsg = currentLang === "zh" ? elements.summary.dataset.emptyZh : elements.summary.dataset.emptyEn;
    elements.summary.textContent = emptyMsg || "--";
    if (elements.model) {
      elements.model.textContent = "";
    }
    if (elements.highlights) {
      elements.highlights.innerHTML = "";
      elements.highlights.classList.add("hidden");
    }
    return;
  }

  if (elements.model) {
    elements.model.textContent = model || "";
  }

  if (isFallback) {
    const fallbackNote = document.createElement("p");
    fallbackNote.className = "macro-fallback-note";
    fallbackNote.textContent = dict.fallbackSummaryNote || "Showing the most recent historical summary.";
    elements.summary.appendChild(fallbackNote);
  }

  const biasLabel = dict[`bias_${summary.market_bias}`] || summary.market_bias || "neutral";
  const bias = document.createElement("div");
  bias.className = "macro-bias";
  bias.dataset.bias = summary.market_bias || "neutral";
  bias.textContent = `${dict.biasLabel || "Bias"}: ${biasLabel || "--"}`;

  const overview = document.createElement("p");
  overview.className = "macro-overview";
  overview.textContent = summary.macro_overview || "--";

  const policy = document.createElement("div");
  policy.className = "macro-policy";
  const policyTitle = document.createElement("h4");
  policyTitle.textContent = dict.policyOutlook || "Policy outlook";
  const policyBody = document.createElement("p");
  policyBody.textContent = summary.policy_outlook || "--";
  policy.append(policyTitle, policyBody);

  elements.summary.append(bias, overview, policy);

  renderHighlights(summary.key_indicators || []);
  renderRiskAndWatch(summary);
}

function renderHighlights(indicators) {
  const container = elements.highlights;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!indicators || !indicators.length) {
    container.classList.add("hidden");
    return;
  }
  container.classList.remove("hidden");
  const dict = getDict();
  indicators.slice(0, 6).forEach((item) => {
    const card = document.createElement("div");
    card.className = "macro-highlight";

    const title = document.createElement("div");
    title.className = "macro-highlight__title";
    title.textContent = item.indicator || "--";

    const value = document.createElement("div");
    value.className = "macro-highlight__value";
    value.textContent = item.latest_value || "--";

    const comment = document.createElement("p");
    comment.className = "macro-highlight__comment";
    comment.textContent = item.trend_comment || dict.noTrendComment || "--";

    card.append(title, value, comment);
    container.appendChild(card);
  });
}

function renderRiskAndWatch(summary) {
  if (!elements.summary) {
    return;
  }
  const dict = getDict();

  const wrapper = document.createElement("div");
  wrapper.className = "macro-lists";

  const risks = Array.isArray(summary.risk_warnings) ? summary.risk_warnings : [];
  if (risks.length) {
    const riskBlock = document.createElement("div");
    riskBlock.className = "macro-list-block";
    const title = document.createElement("h4");
    title.textContent = dict.riskWarnings || "Risk warnings";
    const list = document.createElement("ul");
    risks.forEach((item) => {
      const li = document.createElement("li");
      li.textContent = item;
      list.appendChild(li);
    });
    riskBlock.append(title, list);
    wrapper.appendChild(riskBlock);
  }

  const watchPoints = Array.isArray(summary.watch_points) ? summary.watch_points : [];
  if (watchPoints.length) {
    const watchBlock = document.createElement("div");
    watchBlock.className = "macro-list-block";
    const title = document.createElement("h4");
    title.textContent = dict.watchPoints || "Watch points";
    const list = document.createElement("ul");
    watchPoints.forEach((item) => {
      const li = document.createElement("li");
      li.textContent = item;
      list.appendChild(li);
    });
    watchBlock.append(title, list);
    wrapper.appendChild(watchBlock);
  }

  if (wrapper.childElementCount) {
    elements.summary.appendChild(wrapper);
  }
}

function renderDatasets(datasets) {
  const container = elements.datasets;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!datasets || !datasets.length) {
    const empty = document.createElement("div");
    empty.className = "flow-card__empty";
    empty.textContent = currentLang === "zh" ? "暂无宏观数据。" : "No macro datasets.";
    container.appendChild(empty);
    return;
  }

  const dict = getDict();

  datasets.forEach((dataset) => {
    const panel = document.createElement("section");
    panel.className = "insight-panel";

    const heading = document.createElement("h3");
    heading.textContent = dict[dataset.titleKey] || dataset.titleKey || "Dataset";
    panel.appendChild(heading);

    const meta = document.createElement("div");
    meta.className = "macro-dataset-meta";
    if (dataset.updatedAt) {
      const metaLabel = document.createElement("span");
      metaLabel.textContent = `${dict.lastUpdated || "Last updated"}: ${formatDate(dataset.updatedAt)}`;
      meta.appendChild(metaLabel);
    }
    if (dataset.latest) {
      const latestLabel = document.createElement("span");
      latestLabel.textContent = `${dict.latestPeriod || "Latest"}: ${dataset.latest.period_label || formatDate(dataset.latest.period_date)}`;
      meta.appendChild(latestLabel);
    }
    if (meta.childElementCount) {
      panel.appendChild(meta);
    }

    const table = document.createElement("table");
    table.className = "data-table macro-table";
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");

    const periodLabelHeader = document.createElement("th");
    periodLabelHeader.textContent = dict.tablePeriodLabel || "Period";
    headerRow.appendChild(periodLabelHeader);

    const periodDateHeader = document.createElement("th");
    periodDateHeader.textContent = dict.tablePeriodDate || "Date";
    headerRow.appendChild(periodDateHeader);

    (dataset.fields || []).forEach((field) => {
      const th = document.createElement("th");
      th.textContent = dict[field.labelKey] || field.labelKey;
      headerRow.appendChild(th);
    });

    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    const series = Array.isArray(dataset.series) ? dataset.series : [];
    if (!series.length) {
      const emptyRow = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 2 + (dataset.fields || []).length;
      td.className = "table-empty";
      td.textContent = currentLang === "zh" ? "暂无数据" : "No data";
      emptyRow.appendChild(td);
      tbody.appendChild(emptyRow);
    } else {
      series.forEach((entry) => {
        const row = document.createElement("tr");
        const periodLabelCell = document.createElement("td");
        periodLabelCell.textContent = entry.period_label || "--";
        row.appendChild(periodLabelCell);

        const periodDateCell = document.createElement("td");
        periodDateCell.textContent = formatDate(entry.period_date);
        row.appendChild(periodDateCell);

        (dataset.fields || []).forEach((field) => {
          const cell = document.createElement("td");
          cell.textContent = formatValue(entry[field.key], field.format);
          row.appendChild(cell);
        });

        tbody.appendChild(row);
      });
    }

    table.appendChild(tbody);
    panel.appendChild(table);
    container.appendChild(panel);
  });
}

function renderWarnings(warnings) {
  const container = elements.warnings;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!warnings || !warnings.length) {
    container.classList.add("hidden");
    return;
  }
  container.classList.remove("hidden");
  const title = document.createElement("strong");
  title.textContent = currentLang === "zh" ? "提示" : "Warnings";
  container.appendChild(title);
  const list = document.createElement("ul");
  warnings.forEach((warning) => {
    const li = document.createElement("li");
    li.textContent = warning;
    list.appendChild(li);
  });
  container.appendChild(list);
}

function renderHistory(items) {
  const container = elements.history;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const dict = getDict();
  if (!items || !items.length) {
    const message =
      dict.historyEmpty ||
      (currentLang === "zh" ? container.dataset.emptyZh : container.dataset.emptyEn) ||
      "No historical macro insights.";
    const empty = document.createElement("p");
    empty.className = "insight-history__empty";
    empty.textContent = message;
    container.appendChild(empty);
    return;
  }

  items.forEach((item) => {
    const summary = item.summaryJson;
    if (!summary) {
      return;
    }
    const card = document.createElement("article");
    card.className = "concept-history-card";

    const header = document.createElement("header");
    header.className = "concept-history-card__header";
    const time = document.createElement("time");
    time.textContent = formatDateTime(item.generatedAt || item.snapshotDate);
    header.appendChild(time);
    card.appendChild(header);

    if (summary.market_bias) {
      const bias = document.createElement("p");
      bias.className = "macro-history-bias";
      const biasLabel = dict.biasLabel || "Bias";
      const biasValue = dict[`bias_${summary.market_bias}`] || summary.market_bias;
      bias.textContent = `${biasLabel}: ${biasValue || "--"}`;
      card.appendChild(bias);
    }

    if (summary.macro_overview) {
      const overview = document.createElement("p");
      overview.className = "macro-history-overview";
      overview.textContent = summary.macro_overview;
      card.appendChild(overview);
    }

    if (Array.isArray(summary.key_indicators) && summary.key_indicators.length) {
      const list = document.createElement("ul");
      list.className = "concept-history-card__concepts";
      summary.key_indicators.slice(0, 3).forEach((indicator) => {
        const li = document.createElement("li");
        const name = indicator.indicator || "--";
        const value = indicator.latest_value || "--";
        li.textContent = `${name} · ${value}`;
        list.appendChild(li);
      });
      card.appendChild(list);
    }

    container.appendChild(card);
  });
}

function renderAll() {
  const activeSummary = state.summary || state.fallbackSummary;
  const activeGeneratedAt = state.summary ? state.generatedAt : state.fallbackGeneratedAt;
  const isFallback = !state.summary && !!state.fallbackSummary;
  renderSummary(activeSummary, activeGeneratedAt, state.model, { isFallback });
  renderDatasets(state.datasets);
  renderWarnings(state.warnings);
  renderHistory(state.history);
}

async function loadMacroInsight(showLoading = false) {
  if (showLoading && elements.refreshButton) {
    elements.refreshButton.disabled = true;
    elements.refreshButton.classList.add("loading");
  }
  try {
    const insightPromise = fetch(`${API_BASE}/macro/insight`).then((res) => {
      if (res.status === 404) {
        return null;
      }
      if (!res.ok) {
        throw new Error(`Request failed with status ${res.status}`);
      }
      return res.json();
    });
    const historyPromise = fetch(`${API_BASE}/macro/insight/history?limit=6`).then((res) => {
      if (!res.ok) {
        throw new Error(`Failed to load macro insight history: ${res.status}`);
      }
      return res.json();
    });

    const [insightPayload, historyPayload] = await Promise.all([insightPromise, historyPromise]);
    if (insightPayload) {
      state.summary = insightPayload.summary || null;
      state.generatedAt = insightPayload.generatedAt || null;
      state.model = insightPayload.model || null;
      state.datasets = insightPayload.datasets || [];
      state.warnings = insightPayload.warnings || [];
      state.snapshotDate = insightPayload.snapshotDate || null;
    } else {
      state.summary = null;
      state.generatedAt = null;
      state.model = null;
      state.datasets = [];
      state.warnings = [];
      state.snapshotDate = null;
    }

    let historyItems = Array.isArray(historyPayload?.items) ? historyPayload.items : [];
    if (state.snapshotDate) {
      historyItems = historyItems.filter((item) => item.snapshotDate !== state.snapshotDate);
    }
    const historyWithSummary = historyItems.filter((item) => item?.summaryJson);
    state.history = historyWithSummary;
    state.fallbackSummary = null;
    state.fallbackGeneratedAt = null;
    if (!state.summary && historyWithSummary.length) {
      state.fallbackSummary = historyWithSummary[0].summaryJson;
      state.fallbackGeneratedAt = historyWithSummary[0].generatedAt;
      if (!state.model) {
        state.model = historyWithSummary[0].model || null;
      }
    }

    renderAll();
  } catch (error) {
    console.error("Failed to load macro insight", error);
    state.summary = null;
    state.generatedAt = null;
    state.model = null;
    state.datasets = [];
    state.warnings = [];
    state.history = [];
    state.fallbackSummary = null;
    state.fallbackGeneratedAt = null;
    state.snapshotDate = null;
    renderAll();
  } finally {
    if (elements.refreshButton) {
      elements.refreshButton.disabled = false;
      elements.refreshButton.classList.remove("loading");
    }
  }
}

async function triggerMacroInsightRefresh() {
  if (!elements.refreshButton) {
    return;
  }
  elements.refreshButton.disabled = true;
  elements.refreshButton.classList.add("loading");
  try {
    const response = await fetch(`${API_BASE}/control/sync/macro-insight`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ runLLM: true }),
    });
    if (!response.ok) {
      throw new Error(`Request failed with status ${response.status}`);
    }
    setTimeout(() => loadMacroInsight(), 1500);
  } catch (error) {
    console.error("Failed to trigger macro insight refresh", error);
    loadMacroInsight();
  }
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
  renderAll();
}

function bindEvents() {
  elements.langButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const lang = btn.dataset.lang;
      if (lang && lang !== currentLang && translations[lang]) {
        currentLang = lang;
        persistLanguage(lang);
        applyTranslations();
        loadMacroInsight();
      }
    });
  });
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", triggerMacroInsightRefresh);
  }
}

function initialize() {
  applyTranslations();
  bindEvents();
  loadMacroInsight();
}

initialize();
