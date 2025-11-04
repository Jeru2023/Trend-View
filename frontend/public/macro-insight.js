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

function renderSummary(summary, generatedAt, model) {
  const dict = getDict();
  if (elements.generatedAt) {
    elements.generatedAt.textContent = generatedAt ? formatDate(generatedAt) : "--";
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

async function loadMacroInsight(showLoading = false) {
  if (showLoading && elements.refreshButton) {
    elements.refreshButton.disabled = true;
    elements.refreshButton.classList.add("loading");
  }
  try {
    const response = await fetch(`${API_BASE}/macro/insight`);
    if (!response.ok) {
      if (response.status === 404) {
        renderSummary(null, null, null);
        renderDatasets([]);
        renderWarnings([]);
        return;
      }
      throw new Error(`Request failed with status ${response.status}`);
    }
    const payload = await response.json();
    renderSummary(payload.summary, payload.generatedAt, payload.model);
    renderDatasets(payload.datasets || []);
    renderWarnings(payload.warnings || []);
  } catch (error) {
    console.error("Failed to load macro insight", error);
    renderSummary(null, null, null);
    renderDatasets([]);
    renderWarnings([]);
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
