console.info("Market Insight bundle v20261110");

const translations = getTranslations("marketInsight");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const ARTICLE_LIMIT = 40;
const LANG_STORAGE_KEY = "trend-view-lang";

const elements = {
  summary: document.getElementById("market-insight-summary"),
  stages: document.getElementById("market-insight-stages"),
  refreshButton: document.getElementById("market-insight-refresh"),
  langButtons: document.querySelectorAll(".lang-btn"),
  status: document.getElementById("market-insight-status"),
  metaUpdated: document.getElementById("market-insight-updated"),
  metaElapsed: document.getElementById("market-insight-elapsed"),
  resetButton: document.getElementById("market-insight-reset"),
  exportImageButton: document.getElementById("market-insight-export-image"),
  exportPdfButton: document.getElementById("market-insight-export-pdf"),
  history: document.getElementById("market-insight-history"),
};

const state = {
  data: null,
  history: [],
};

const JOB_STATUS_INTERVAL = 5000;
const SUMMARY_POLL_INTERVAL = 4000;
let jobStatusTimer = null;
let summaryPollTimer = null;
let lastJobStatusValue = null;
let latestJobSnapshot = null;
let manualSyncPending = false;
let resettingJob = false;
let summaryPollingActive = false;
const EXPORT_SCALE = window.devicePixelRatio > 1 ? 2 : 1.5;
const EXPORT_MAX_WIDTH = 1080;
const EXPORT_MOBILE_MAX_WIDTH = 760;
const EXPORT_MOBILE_MIN_WIDTH = 540;
let exportInProgress = false;

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
  const htmlLang = document.documentElement.lang;
  if (htmlLang && translations[htmlLang]) {
    return htmlLang;
  }
  const browserLang = (navigator.language || "").toLowerCase();
  return translations[browserLang] ? browserLang : "zh";
}

function persistLanguage(lang) {
  try {
    window.localStorage.setItem(LANG_STORAGE_KEY, lang);
  } catch (error) {
    /* ignore */
  }
  document.documentElement.setAttribute("data-pref-lang", lang);
}

let currentLang = getInitialLanguage();
persistLanguage(currentLang);

function getDict() {
  return translations[currentLang] || translations.en;
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const dateValue = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(dateValue.getTime())) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return `${dateValue.toLocaleDateString(locale)} ${dateValue.toLocaleTimeString(locale, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })}`;
}

function formatNumber(value, digits = 0) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return numeric.toFixed(digits);
}

function formatPercent(value, digits = 0) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const percentage = Math.max(0, Math.min(numeric, 1)) * 100;
  return `${percentage.toFixed(digits)}%`;
}

function formatInteger(value) {
  if (value === null || value === undefined) {
    return "--";
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  const locale = currentLang === "zh" ? "zh-CN" : "en-US";
  return numeric.toLocaleString(locale);
}

function formatNarrativeText(value) {
  if (value === null || value === undefined) {
    return "";
  }
  let text = String(value);
  if (!text.trim()) {
    return "";
  }
  text = text.replace(/；/g, "；\n\n").replace(/;/g, ";\n\n");
  text = text.replace(/([。！？])(?!\s|\n)/g, "$1\n");
  text = text.replace(/(\.)(?=\s|$)/g, ".\n");
  text = text.replace(/\n{3,}/g, "\n\n");
  return text.trim();
}

function setNarrativeText(node, value, fallback = "--") {
  if (!node) return;
  const formatted = formatNarrativeText(value);
  node.textContent = formatted || fallback || "";
}

function getExportBannerInfo() {
  const dict = getDict();
  const summary = state.data?.summary;
  const title = dict.pageTitle || "Market Reasoning";
  const dateLabel = dict.generatedAtLabel || dict.metaUpdatedLabel || "Updated";
  const dateValue = summary?.generatedAt ? `${dateLabel}: ${formatDateTime(summary.generatedAt)}` : "";
  const footer =
    dict.exportDisclaimer ||
    (currentLang === "zh" ? "以上内容由AI推理，仅供参考" : "AI-generated content. For reference only.");
  return { title, date: dateValue, footer };
}

function injectExportBanners(root, info) {
  if (!root || !info) return;
  const top = document.createElement("div");
  top.className = "insight-export-banner insight-export-banner--top";
  const titleEl = document.createElement("strong");
  titleEl.textContent = info.title || "";
  top.appendChild(titleEl);
  if (info.date) {
    const dateEl = document.createElement("span");
    dateEl.textContent = info.date;
    top.appendChild(dateEl);
  }
  root.insertBefore(top, root.firstChild);

  const bottom = document.createElement("div");
  bottom.className = "insight-export-banner insight-export-banner--bottom";
  bottom.textContent = info.footer || "";
  root.appendChild(bottom);
}

function normalizeSummaryPayload(summary) {
  if (!summary) {
    return null;
  }
  if (typeof summary === "string") {
    try {
      return JSON.parse(summary);
    } catch (error) {
      return null;
    }
  }
  if (typeof summary === "object") {
    return summary;
  }
  return null;
}

function setRefreshLoading(isLoading) {
  const button = elements.refreshButton;
  if (!button) return;
  const label = button.querySelector(".btn__label");
  const dict = getDict();
  if (isLoading) {
    button.dataset.loading = "1";
    button.disabled = true;
    if (label) label.textContent = dict.refreshing || "Refreshing";
  } else {
    delete button.dataset.loading;
    button.disabled = false;
    if (label) label.textContent = dict.refreshButton || "Refresh";
  }
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

async function fetchJobStatusOnce() {
  try {
    const response = await fetch(`${API_BASE}/control/status`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    applyJobStatus(data?.jobs?.market_insight);
  } catch (error) {
    console.error("Failed to fetch job status", error);
  }
}

function scheduleJobStatusPolling() {
  if (jobStatusTimer !== null) {
    clearTimeout(jobStatusTimer);
  }
  const tick = async () => {
    await fetchJobStatusOnce();
    jobStatusTimer = window.setTimeout(tick, JOB_STATUS_INTERVAL);
  };
  tick();
}

function startSummaryPolling() {
  if (summaryPollingActive) {
    return;
  }
  summaryPollingActive = true;
  const tick = async () => {
    if (!summaryPollingActive) {
      return;
    }
    await fetchMarketInsight({ preserve: true, silent: true });
    if (!summaryPollingActive) {
      return;
    }
    summaryPollTimer = window.setTimeout(tick, SUMMARY_POLL_INTERVAL);
  };
  tick();
}

function stopSummaryPolling() {
  summaryPollingActive = false;
  if (summaryPollTimer !== null) {
    clearTimeout(summaryPollTimer);
    summaryPollTimer = null;
  }
}

function applyJobStatus(job) {
  latestJobSnapshot = job || null;
  if (!job) {
    if (!manualSyncPending) {
      setRefreshLoading(false);
    }
    return;
  }
  const statusValue = job.status || "idle";
  const previousStatus = lastJobStatusValue;
  lastJobStatusValue = statusValue;
  if (statusValue === "idle") {
    if (!manualSyncPending) {
      setRefreshLoading(false);
    }
    return;
  }
  const dict = getDict();
  const progressValue =
    typeof job.progress === "number" && Number.isFinite(job.progress) ? Math.round(job.progress * 100) : null;
  let message = job.message || "";
  if (statusValue === "failed" && job.error) {
    message = job.error;
  }
  if (!message) {
    if (statusValue === "running") {
      message = dict.statusRunning || dict.statusGenerating || "Running...";
    } else if (statusValue === "success") {
      message = dict.statusGenerated || "Completed.";
    } else if (statusValue === "failed") {
      message = dict.statusFailed || "Failed.";
    }
  }
  if (statusValue === "running" && progressValue !== null) {
    message = `${message} · ${progressValue}%`;
  }
  let tone = "";
  if (statusValue === "running") {
    tone = "info";
    manualSyncPending = false;
    setRefreshLoading(true);
    startSummaryPolling();
  } else if (statusValue === "failed") {
    tone = "error";
    manualSyncPending = false;
    setRefreshLoading(false);
    stopSummaryPolling();
  } else if (statusValue === "success") {
    tone = "success";
    manualSyncPending = false;
    setRefreshLoading(false);
    stopSummaryPolling();
  }
  if (elements.resetButton) {
    elements.resetButton.hidden = statusValue !== "running";
    elements.resetButton.disabled = resettingJob;
  }
  setStatus(message, tone);
  if (previousStatus === "running" && statusValue !== "running") {
    fetchMarketInsight();
  }
}

function updateToolbarMeta(meta) {
  const normalize = (value) => {
    if (value === null || value === undefined) return "--";
    const text = String(value).trim();
    return text ? text : "--";
  };
  if (elements.metaUpdated) {
    elements.metaUpdated.textContent = normalize(meta?.updated);
  }
  if (elements.metaElapsed) {
    elements.metaElapsed.textContent = normalize(meta?.elapsed);
  }
}

function getExportBaseFilename() {
  const generatedAt = state.data?.summary?.generatedAt;
  let timestamp = "latest";
  if (generatedAt) {
    const dateValue = new Date(generatedAt);
    if (!Number.isNaN(dateValue.getTime())) {
      const iso = dateValue.toISOString().replace(/[-:]/g, "").split(".")[0];
      timestamp = iso;
    }
  }
  return `market-insight-${timestamp}`;
}

function resolveExportWidth() {
  const viewportWidth = Math.max(
    window.innerWidth || 0,
    document.documentElement?.clientWidth || 0,
    EXPORT_MOBILE_MIN_WIDTH
  );
  if (viewportWidth > 900) {
    return EXPORT_MOBILE_MAX_WIDTH;
  }
  return Math.min(Math.max(viewportWidth, EXPORT_MOBILE_MIN_WIDTH), EXPORT_MOBILE_MAX_WIDTH);
}

function prepareExportClone(target, width) {
  const wrapper = document.createElement("div");
  wrapper.style.position = "fixed";
  wrapper.style.left = "-10000px";
  wrapper.style.top = "0";
  wrapper.style.width = `${width}px`;
  wrapper.style.background = "#ffffff";
  wrapper.style.padding = "0";
  wrapper.style.zIndex = "-1";

  const clone = target.cloneNode(true);
  clone.style.width = "100%";
  clone.setAttribute("data-export-mode", "mobile");
  clone.classList.add("insight-export-clone");
  stripHistorySectionForExport(clone);
  injectExportBanners(clone, getExportBannerInfo());
  wrapper.appendChild(clone);
  document.body.appendChild(wrapper);

  return {
    node: clone,
    cleanup: () => {
      document.body.removeChild(wrapper);
    },
  };
}

function stripHistorySectionForExport(root) {
  if (!root || typeof root.querySelector !== "function") return;
  const historyNode = root.querySelector("#market-insight-history");
  if (!historyNode) return;
  const section = historyNode.closest(".insight-section");
  if (section && section.parentNode) {
    section.remove();
  } else {
    historyNode.remove();
  }
}

function triggerDownload(dataUrl, filename) {
  const link = document.createElement("a");
  link.href = dataUrl;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

async function exportInsight(format) {
  if (exportInProgress) {
    return;
  }
  const dict = getDict();
  if (!window.html2canvas) {
    setStatus("html2canvas missing", "error");
    return;
  }
  if (format === "pdf" && !(window.jspdf && window.jspdf.jsPDF)) {
    setStatus("jsPDF missing", "error");
    return;
  }
  const target = document.getElementById("market-insight-root");
  if (!target) {
    setStatus(dict.exportFailed || "Export failed", "error");
    return;
  }
  exportInProgress = true;
  setStatus(dict.exporting || "Preparing export...", "info");
  let cleanupExport = null;
  try {
    const exportWidth = resolveExportWidth();
    const { node: exportNode, cleanup } = prepareExportClone(target, exportWidth);
    cleanupExport = cleanup;
    const canvas = await window.html2canvas(exportNode, {
      scale: EXPORT_SCALE,
      useCORS: true,
      backgroundColor: "#ffffff",
      scrollY: 0,
      scrollX: 0,
      width: exportWidth,
      windowWidth: exportWidth,
    });
    cleanupExport();
    cleanupExport = null;
    if (format === "image") {
      const dataUrl = canvas.toDataURL("image/png");
      triggerDownload(dataUrl, `${getExportBaseFilename()}.png`);
    } else {
      const { jsPDF } = window.jspdf;
      const pdf = new jsPDF("p", "pt", "a4");
      const pageWidth = pdf.internal.pageSize.getWidth();
      const pageHeight = pdf.internal.pageSize.getHeight();
      const imgWidth = pageWidth;
      const imgHeight = (canvas.height * imgWidth) / canvas.width;
      const imgData = canvas.toDataURL("image/png");
      let heightLeft = imgHeight;
      let position = 0;

      pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight);
      heightLeft -= pageHeight;

      while (heightLeft > 0) {
        position = heightLeft - imgHeight;
        pdf.addPage();
        pdf.addImage(imgData, "PNG", 0, position, imgWidth, imgHeight);
        heightLeft -= pageHeight;
      }

      pdf.save(`${getExportBaseFilename()}.pdf`);
    }
    setStatus(dict.exportReady || "Export ready.", "success");
  } catch (error) {
    console.error("Export failed", error);
    setStatus(error?.message || dict.exportFailed || "Export failed", "error");
  } finally {
    if (typeof cleanupExport === "function") {
      cleanupExport();
    }
    exportInProgress = false;
  }
}

function clearContainer(node, message) {
  if (!node) return;
  node.innerHTML = "";
  if (message) {
    const placeholder = document.createElement("div");
    placeholder.className = "empty-placeholder";
    placeholder.textContent = message;
    node.appendChild(placeholder);
  }
}

function renderStageResults(stageEntries) {
  const container = elements.stages;
  if (!container) return;
  container.innerHTML = "";
  const stages = Array.isArray(stageEntries) ? [...stageEntries] : [];
  stages.sort((a, b) => {
    const orderA = typeof a?.order === "number" ? a.order : 0;
    const orderB = typeof b?.order === "number" ? b.order : 0;
    return orderA - orderB;
  });
  if (!stages.length) {
    container.style.display = "none";
    return;
  }
  container.style.display = "";
  const dict = getDict();
  const heading = document.createElement("h2");
  heading.className = "stage-list__heading";
  heading.textContent = dict.stageSectionTitle || "Staged Reasoning";
  container.appendChild(heading);

  const statusLabels = {
    success: dict.stageStatusSuccess || "Ready",
    running: dict.stageStatusRunning || "Running",
    pending: dict.stageStatusPending || "Pending",
    failed: dict.stageStatusFailed || "Failed",
  };

  stages.forEach((stage) => {
    if (!stage || typeof stage !== "object") return;
    const card = document.createElement("article");
    const statusValue = (stage.status || "pending").toLowerCase();
    card.className = `stage-card stage-card--${statusValue}`;

    const header = document.createElement("header");
    header.className = "stage-card__header";
    const title = document.createElement("h3");
    title.className = "stage-card__title";
    title.textContent = stage.title || stage.stage || dict.stageSectionTitle || "Stage";
    header.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "stage-card__meta";
    const statusChip = document.createElement("span");
    statusChip.className = `stage-card__status-chip stage-card__status-chip--${statusValue}`;
    statusChip.textContent = statusLabels[statusValue] || statusValue;
    meta.appendChild(statusChip);

    header.appendChild(meta);
    card.appendChild(header);

    const sentimentLabelMap = {
      bullish: dict.sentimentBullish || "Bullish",
      bearish: dict.sentimentBearish || "Bearish",
      neutral: dict.sentimentNeutral || "Neutral",
    };

    if (statusValue === "success") {
      const biasValue = (stage.bias || "neutral").toLowerCase();
      const biasChip = document.createElement("span");
      biasChip.className = `stage-chip stage-chip--${["bullish", "bearish", "neutral"].includes(biasValue) ? biasValue : "neutral"}`;
      biasChip.textContent = sentimentLabelMap[biasValue] || sentimentLabelMap.neutral;
      meta.appendChild(biasChip);

      if (stage.confidence !== undefined && stage.confidence !== null && Number.isFinite(Number(stage.confidence))) {
        const confidence = document.createElement("span");
        confidence.className = "stage-card__confidence";
        confidence.textContent = `${dict.stageConfidenceLabel || "Confidence"}: ${formatPercent(stage.confidence, 0)}`;
        meta.appendChild(confidence);
      }

      if (stage.analysis) {
        const analysis = document.createElement("p");
        analysis.className = "stage-card__analysis";
        setNarrativeText(analysis, stage.analysis);
        card.appendChild(analysis);
      }

      const highlights = Array.isArray(stage.highlights) ? stage.highlights.filter(Boolean) : [];
      if (highlights.length) {
        const highlightsHeading = document.createElement("h4");
        highlightsHeading.className = "stage-card__subheading";
        highlightsHeading.textContent = dict.stageHighlightsLabel || "Highlights";
        card.appendChild(highlightsHeading);
        const list = document.createElement("ul");
        list.className = "stage-card__list";
        highlights.forEach((item) => {
          const li = document.createElement("li");
          setNarrativeText(li, item);
          list.appendChild(li);
        });
        card.appendChild(list);
      }

      const metrics = Array.isArray(stage.keyMetrics || stage.key_metrics)
        ? (stage.keyMetrics || stage.key_metrics).filter(Boolean)
        : [];
      if (metrics.length) {
        const metricsHeading = document.createElement("h4");
        metricsHeading.className = "stage-card__subheading";
        metricsHeading.textContent = dict.stageMetricsLabel || "Key Metrics";
        card.appendChild(metricsHeading);
        const metricList = document.createElement("div");
        metricList.className = "stage-card__metrics";
        metrics.forEach((metric) => {
          if (!metric || typeof metric !== "object") return;
          const row = document.createElement("div");
          row.className = "stage-card__metric-row";

          const headerRow = document.createElement("div");
          headerRow.className = "stage-card__metric-header";

          const label = document.createElement("span");
          label.className = "stage-card__metric-label";
          label.textContent = metric.label || dict.metricLabel || "Metric";
          headerRow.appendChild(label);

          const value = document.createElement("strong");
          value.className = "stage-card__metric-value";
          value.textContent = metric.value || "--";
          headerRow.appendChild(value);

          row.appendChild(headerRow);

          if (metric.insight) {
            const insight = document.createElement("span");
            insight.className = "stage-card__metric-insight";
            setNarrativeText(insight, metric.insight);
            row.appendChild(insight);
          }

          metricList.appendChild(row);
        });
        card.appendChild(metricList);
      }
    } else {
      const statusMessage = document.createElement("p");
      statusMessage.className = "stage-card__status-message";
      if (statusValue === "failed") {
        statusMessage.textContent = stage.error || dict.stageFailedDefault || "Stage failed.";
      } else {
        statusMessage.textContent = dict.stagePendingMessage || "Reasoning in progress…";
      }
      card.appendChild(statusMessage);
    }

    container.appendChild(card);
  });
}

function renderSummary() {
  const dict = getDict();
  const container = elements.summary;
  if (!container) return;

  const summaryWrapper = state.data?.summary;
  container.innerHTML = "";
  const stageList = Array.isArray(summaryWrapper?.stages) ? summaryWrapper.stages : [];
  renderStageResults(stageList);

  if (summaryWrapper) {
    const elapsedValue = Number(summaryWrapper.elapsedSeconds);
    updateToolbarMeta({
      updated: formatDateTime(summaryWrapper.generatedAt),
      elapsed: Number.isFinite(elapsedValue) ? `${elapsedValue.toFixed(2)}s` : "--",
    });
  } else {
    updateToolbarMeta();
  }

  if (!summaryWrapper) {
    const message = container.dataset[`empty${currentLang.toUpperCase()}`] || dict.emptySummary || "--";
    clearContainer(container, message);
    return;
  }

  const llmSummary = summaryWrapper?.summary;
  if (!llmSummary) {
    const placeholder = document.createElement("article");
    placeholder.className = "insight-card insight-card--placeholder";
    placeholder.textContent = dict.summaryPending || "Comprehensive reasoning is still running...";
    container.appendChild(placeholder);
    return;
  }

  const comprehensive = llmSummary.comprehensive_conclusion || {};

  const card = document.createElement("article");
  card.className = "insight-card";

  const header = document.createElement("div");
  header.className = "insight-card__header";
  const title = document.createElement("h2");
  title.className = "insight-card__title";
  title.textContent = dict.summarySectionTitle || "Market Insight";
  header.appendChild(title);

  const sentimentLabelMap = {
    bullish: dict.sentimentBullish || "Bullish",
    bearish: dict.sentimentBearish || "Bearish",
    neutral: dict.sentimentNeutral || "Neutral",
  };
  const sentimentValueRaw = (comprehensive.bias || "").toLowerCase();
  const sentimentValue = ["bullish", "bearish", "neutral"].includes(sentimentValueRaw)
    ? sentimentValueRaw
    : "neutral";
  const sentimentChip = document.createElement("span");
  sentimentChip.className = `sentiment-chip sentiment-chip--${sentimentValue}`;
  sentimentChip.textContent = sentimentLabelMap[sentimentValue] || dict.sentimentUnknown || "Unknown";

  const tokenHintParts = [];
  const promptTokens = summaryWrapper.promptTokens;
  const completionTokens = summaryWrapper.completionTokens;
  const totalTokens = summaryWrapper.totalTokens;
  if (promptTokens !== null && promptTokens !== undefined) {
    tokenHintParts.push(`${dict.promptTokensLabel || "Prompt"} ${formatInteger(promptTokens)}`);
  }
  if (completionTokens !== null && completionTokens !== undefined) {
    tokenHintParts.push(`${dict.completionTokensLabel || "Completion"} ${formatInteger(completionTokens)}`);
  }
  const tokenHint = tokenHintParts.length ? tokenHintParts.join(" · ") : null;

  const stats = document.createElement("div");
  stats.className = "insight-card__stats";
  const statsItems = [
    {
      label: dict.statsSentimentLabel || "市场情绪",
      content: (() => {
        const wrapper = document.createElement("div");
        wrapper.className = "insight-stat__sentiment";
        wrapper.appendChild(sentimentChip);
        return wrapper;
      })(),
    },
    {
      label: dict.statsConfidenceLabel || "置信度",
      value:
        comprehensive.confidence !== null && comprehensive.confidence !== undefined
          ? formatPercent(comprehensive.confidence, 0)
          : "--",
    },
  ];

  if (totalTokens !== null && totalTokens !== undefined) {
    statsItems.push({
      label: dict.statsTokenLabel || "Token",
      value: formatInteger(totalTokens),
      hint: tokenHint,
    });
  }

  statsItems.forEach((item) => {
    const stat = document.createElement("div");
    stat.className = "insight-stat";

    const label = document.createElement("div");
    label.className = "insight-stat__label";
    label.textContent = item.label;
    stat.appendChild(label);

    if (item.content) {
      const valueWrapper = document.createElement("div");
      valueWrapper.className = "insight-stat__value insight-stat__value--content";
      valueWrapper.appendChild(item.content);
      stat.appendChild(valueWrapper);
    } else {
      const value = document.createElement("div");
      value.className = "insight-stat__value";
      value.textContent = item.value ?? "--";
      stat.appendChild(value);
    }

    if (item.hint) {
      const hint = document.createElement("div");
      hint.className = "insight-stat__hint";
      hint.textContent = item.hint;
      stat.appendChild(hint);
    }

    stats.appendChild(stat);
  });

  const meta = document.createElement("div");
  meta.className = "insight-card__meta";
  meta.innerHTML = `
    <div>${dict.generatedAtLabel || "Generated"}: <strong>${formatDateTime(summaryWrapper.generatedAt)}</strong></div>
    <div>${dict.windowRangeLabel || "Window"}: ${formatDateTime(summaryWrapper.windowStart)} - ${formatDateTime(summaryWrapper.windowEnd)}</div>
    <div>${dict.elapsedLabel || "Latency"}: ${
      summaryWrapper.elapsedSeconds !== null && summaryWrapper.elapsedSeconds !== undefined
        ? `${summaryWrapper.elapsedSeconds.toFixed(2)}s`
        : "--"
    }</div>
    <div>${dict.modelLabel || "Model"}: ${summaryWrapper.modelUsed || "-"}</div>
  `;

  header.appendChild(meta);
  card.appendChild(header);
  card.appendChild(stats);

  const overviewBlock = document.createElement("section");
  overviewBlock.className = "insight-card__section";
  const overviewHeading = document.createElement("h3");
  overviewHeading.textContent =
    dict.comprehensiveSummaryLabel || dict.marketOverviewLabel || dict.summarySectionTitle || "Overview";
  overviewBlock.appendChild(overviewHeading);
  const overviewPara = document.createElement("p");
  setNarrativeText(overviewPara, comprehensive.summary, "--");
  overviewBlock.appendChild(overviewPara);
  card.appendChild(overviewBlock);

  const keySignals = Array.isArray(comprehensive.key_signals)
    ? comprehensive.key_signals.filter((item) => item && (item.title || item.detail))
    : [];
  if (keySignals.length) {
    const signalsSection = document.createElement("section");
    signalsSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.signalsSectionTitle || "Key Signals";
    signalsSection.appendChild(heading);
    const list = document.createElement("ul");
    list.className = "insight-card__detail-list";
    keySignals.forEach((signal) => {
      const li = document.createElement("li");
      const strong = document.createElement("strong");
      setNarrativeText(strong, signal.title || dict.signalLabel || "Signal", dict.signalLabel || "Signal");
      li.appendChild(strong);
      if (signal.detail) {
        const detail = document.createElement("p");
        setNarrativeText(detail, signal.detail);
        li.appendChild(detail);
      }
      if (Array.isArray(signal.supporting_analyses) && signal.supporting_analyses.length) {
        const sources = document.createElement("div");
        sources.className = "insight-card__detail-confidence";
        setNarrativeText(sources, `${dict.signalSupportLabel || "Based on"}: ${signal.supporting_analyses.join(", ")}`);
        li.appendChild(sources);
      }
      list.appendChild(li);
    });
    signalsSection.appendChild(list);
    card.appendChild(signalsSection);
  }

  const position = comprehensive.position_suggestion || {};
  const positionItems = [
    { label: dict.positionShortLabel || "Short term", value: position.short_term },
    { label: dict.positionMediumLabel || "Medium term", value: position.medium_term },
    { label: dict.positionRiskLabel || "Risk control", value: position.risk_control },
  ].filter((item) => item.value);
  if (positionItems.length) {
    const positionSection = document.createElement("section");
    positionSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.positionSectionTitle || "Position Suggestion";
    positionSection.appendChild(heading);
    const list = document.createElement("ul");
    list.className = "insight-card__list";
    positionItems.forEach((item) => {
      const li = document.createElement("li");
      setNarrativeText(li, `${item.label}: ${item.value}`);
      list.appendChild(li);
    });
    positionSection.appendChild(list);
    card.appendChild(positionSection);
  }

  const scenarios = Array.isArray(comprehensive.scenario_analysis)
    ? comprehensive.scenario_analysis.filter((item) => item && (item.scenario || item.conditions || item.target))
    : [];
  if (scenarios.length) {
    const scenarioSection = document.createElement("section");
    scenarioSection.className = "insight-card__section";
    const heading = document.createElement("h3");
    heading.textContent = dict.scenarioSectionTitle || "Scenario Analysis";
    scenarioSection.appendChild(heading);
    const list = document.createElement("ul");
    list.className = "insight-card__detail-list insight-card__scenario-list";
    scenarios.forEach((scenario) => {
      const li = document.createElement("li");
      if (scenario.scenario) {
        const strong = document.createElement("strong");
        setNarrativeText(strong, scenario.scenario, scenario.scenario);
        li.appendChild(strong);
      }
      if (scenario.conditions) {
        const conditions = document.createElement("p");
        conditions.className = "insight-card__scenario-meta";
        setNarrativeText(conditions, `${dict.scenarioConditionsLabel || "Conditions"}: ${scenario.conditions}`);
        li.appendChild(conditions);
      }
      if (scenario.target) {
        const target = document.createElement("p");
        target.className = "insight-card__detail-analysis";
        setNarrativeText(target, `${dict.scenarioTargetLabel || "Outcome"}: ${scenario.target}`);
        li.appendChild(target);
      }
      list.appendChild(li);
    });
    scenarioSection.appendChild(list);
    card.appendChild(scenarioSection);
  }


  container.appendChild(card);
}

function renderHistory(items = state.history) {
  const container = elements.history;
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const dict = getDict();
  if (!items || !items.length) {
    const langKey = `empty${currentLang.charAt(0).toUpperCase()}${currentLang.slice(1)}`;
    const message =
      container.dataset[langKey] ||
      dict.historyEmpty ||
      "No historical records.";
    const empty = document.createElement("div");
    empty.className = "insight-history__empty";
    empty.textContent = message;
    container.appendChild(empty);
    return;
  }

  items.forEach((entry) => {
    const card = document.createElement("article");
    card.className = "history-card";

    const header = document.createElement("div");
    header.className = "history-card__header";
    const headerParts = [];
    const generatedAt = entry.generatedAt || entry.generated_at;
    if (generatedAt) {
      headerParts.push(`${dict.historyGeneratedLabel || "Generated"}: ${formatDateTime(generatedAt)}`);
    }
    const windowStart = entry.windowStart || entry.window_start;
    const windowEnd = entry.windowEnd || entry.window_end;
    if (windowStart || windowEnd) {
      headerParts.push(
        `${dict.historyWindowLabel || "Window"}: ${formatDateTime(windowStart) || "--"} → ${
          formatDateTime(windowEnd) || "--"
        }`
      );
    }
    if (entry.modelUsed || entry.model_used) {
      headerParts.push(`${dict.modelLabel || "Model"}: ${entry.modelUsed || entry.model_used}`);
    }
    header.textContent = headerParts.join(" · ") || formatDateTime(generatedAt) || "--";
    card.appendChild(header);

    const summaryPayload = normalizeSummaryPayload(entry.summaryJson || entry.summary_json || entry.summary);
    const summaryData = summaryPayload?.summary ? summaryPayload.summary : summaryPayload;
    const comprehensive = summaryData?.comprehensive_conclusion || {};
    const summaryText = comprehensive.summary || summaryData?.summary || "";
    if (summaryText) {
      const summaryEl = document.createElement("p");
      summaryEl.className = "history-card__summary";
      setNarrativeText(summaryEl, summaryText);
      card.appendChild(summaryEl);
    }

    const drivers =
      (Array.isArray(comprehensive.drivers) && comprehensive.drivers) ||
      (Array.isArray(summaryData?.drivers) && summaryData.drivers) ||
      [];
    if (drivers && drivers.length) {
      const list = document.createElement("ul");
      list.className = "history-card__drivers";
      drivers.forEach((driver) => {
        if (!driver) return;
        const li = document.createElement("li");
        setNarrativeText(li, driver);
        list.appendChild(li);
      });
      card.appendChild(list);
    }

    const metaRow = document.createElement("div");
    metaRow.className = "history-card__meta";
    const biasValue = (comprehensive.bias || summaryData?.bias || "").toLowerCase();
    if (biasValue) {
      metaRow.appendChild(
        document.createTextNode(`${dict.biasLabel || "Bias"}: ${dict.biasLabels?.[biasValue] || biasValue}`)
      );
    }
    if (comprehensive.confidence !== undefined && comprehensive.confidence !== null) {
      const confidenceText = `${dict.confidenceLabel || "Confidence"}: ${formatPercent(comprehensive.confidence, 0)}`;
      metaRow.appendChild(document.createTextNode(confidenceText));
    }
    if (summaryData?.risk_level) {
      metaRow.appendChild(document.createTextNode(`${dict.riskLabel || "Risk"}: ${summaryData.risk_level}`));
    }
    if (metaRow.childNodes.length) {
      card.appendChild(metaRow);
    }

    container.appendChild(card);
  });
}

async function fetchMarketInsight(options = {}) {
  const { preserve = false, silent = false } = options;
  const dict = getDict();
  if (!preserve) {
    clearContainer(elements.summary, dict.loading || "Loading...");
    renderStageResults([]);
    updateToolbarMeta();
  }
  try {
    const response = await fetch(`${API_BASE}/market/market-insight`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    state.data = data;
    renderSummary();
    if (!silent) {
      fetchMarketInsightHistory();
    }
  } catch (error) {
    console.error("Failed to fetch market insight", error);
    if (!silent) {
      clearContainer(elements.summary, error?.message || "Failed to load insight");
      setStatus(error?.message || dict.statusFailed || dict.refreshFailed || "Request failed", "error");
    }
  }
}

async function fetchMarketInsightHistory(limit = 6) {
  if (!elements.history) {
    return;
  }
  try {
    const response = await fetch(`${API_BASE}/market/market-insight/history?limit=${limit}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    state.history = Array.isArray(payload.items) ? payload.items : [];
    renderHistory();
  } catch (error) {
    console.error("Failed to load market insight history", error);
    if (!state.history.length) {
      renderHistory([]);
    }
  }
}

async function triggerManualSync() {
  if (elements.refreshButton?.dataset.loading === "1") {
    return;
  }
  const dict = getDict();
  setRefreshLoading(true);
  setStatus(dict.statusGenerating || "Generating...", "info");
  let jobAccepted = false;
  try {
    const response = await fetch(`${API_BASE}/control/sync/market-insight`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lookbackHours: 24, articleLimit: ARTICLE_LIMIT }),
    });
    if (!response.ok) {
      let message = response.statusText || `HTTP ${response.status}`;
      try {
        const payload = await response.json();
        if (payload?.detail) {
          message = payload.detail;
        }
      } catch (parseError) {
        /* ignore */
      }
      if (response.status === 409) {
        jobAccepted = true;
        manualSyncPending = true;
        setStatus(dict.statusRunning || message, "warn");
        return;
      }
      throw new Error(message);
    }
    jobAccepted = true;
    manualSyncPending = true;
    startSummaryPolling();
  } catch (error) {
    console.error("Manual market insight generation failed", error);
    manualSyncPending = false;
    setStatus(error?.message || dict.statusFailed || dict.refreshFailed || "Request failed", "error");
  } finally {
    if (!jobAccepted) {
      manualSyncPending = false;
      setRefreshLoading(false);
    }
  }
}

async function resetMarketInsightJob() {
  if (resettingJob) {
    return;
  }
  resettingJob = true;
  if (elements.resetButton) {
    elements.resetButton.disabled = true;
  }
  const dict = getDict();
  setStatus(dict.statusResetting || "Resetting job...", "warn");
  try {
    const response = await fetch(`${API_BASE}/control/reset-job`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ job: "market_insight" }),
    });
    if (!response.ok) {
      let message = response.statusText || `HTTP ${response.status}`;
      try {
        const payload = await response.json();
        if (payload?.detail) {
          message = payload.detail;
        }
      } catch (parseError) {
        /* ignore */
      }
      throw new Error(message);
    }
    manualSyncPending = false;
    await fetchJobStatusOnce();
    setStatus(dict.statusResetOk || "Job reset.", "warn");
  } catch (error) {
    console.error("Failed to reset market insight job", error);
    setStatus(error?.message || dict.statusResetFailed || "Reset failed", "error");
  } finally {
    resettingJob = false;
    if (elements.resetButton) {
      elements.resetButton.disabled = false;
    }
  }
}

function bindLanguageButtons() {
  elements.langButtons.forEach((btn) => {
    btn.onclick = () => {
      const lang = btn.dataset.lang;
      if (lang && translations[lang]) {
        currentLang = lang;
        persistLanguage(lang);
        applyTranslations();
        fetchMarketInsight();
      }
    };
  });
}

function applyTranslations() {
  const dict = getDict();
  document.documentElement.lang = currentLang;
  document.title = dict.title || "Trend View - Market Insight";
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (key && dict[key]) {
      node.textContent = dict[key];
    }
  });
  document.querySelectorAll("[data-i18n-option]").forEach((node) => {
    const key = node.dataset.i18nOption;
    if (key && dict[key]) {
      node.textContent = dict[key];
    }
  });
  elements.langButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.lang === currentLang);
  });
  if (elements.summary?.dataset) {
    elements.summary.dataset.emptyEn = dict.emptySummary || "No insight yet.";
    elements.summary.dataset.emptyZh = dict.emptySummaryZh || "暂无推理结果。";
  }
  renderSummary();
  renderHistory();
  if (latestJobSnapshot) {
    applyJobStatus(latestJobSnapshot);
  }
}

function initialize() {
  setStatus("", "");
  bindLanguageButtons();
  applyTranslations();
  scheduleJobStatusPolling();
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", () => triggerManualSync());
  }
  if (elements.resetButton) {
    elements.resetButton.addEventListener("click", () => resetMarketInsightJob());
  }
  if (elements.exportImageButton) {
    elements.exportImageButton.addEventListener("click", () => exportInsight("image"));
  }
  if (elements.exportPdfButton) {
    elements.exportPdfButton.addEventListener("click", () => exportInsight("pdf"));
  }
  fetchMarketInsight();
  fetchMarketInsightHistory();
}

document.addEventListener("DOMContentLoaded", initialize);

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}
