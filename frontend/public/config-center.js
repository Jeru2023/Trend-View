const translations = getTranslations("controlPanel");

const API_BASE =
  window.API_BASE_URL ||
  (window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `${window.location.origin.replace(/:\d+$/, "")}:8000`);

const LANG_STORAGE_KEY = "trend-view-lang";

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

let currentLang = getInitialLanguage();
let toastTimer = null;

const configState = {
  includeST: false,
  includeDelisted: false,
  dailyTradeWindowDays: 365,
  peripheralAggregateTime: "06:00",
  globalFlashFrequencyMinutes: 180,
};

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  includeSt: document.getElementById("config-include-st"),
  includeDelisted: document.getElementById("config-include-delisted"),
  windowDays: document.getElementById("config-window"),
  windowSkeleton: document.querySelector("[data-skeleton=window]"),
  peripheralAggregateTime: document.getElementById("config-peripheral-aggregate-time"),
  peripheralSkeleton: document.querySelector("[data-skeleton=peripheral]"),
  globalFlashFrequency: document.getElementById("config-global-flash-frequency"),
  globalFlashSkeleton: document.querySelector("[data-skeleton=global-flash]"),
  toastContainer: document.getElementById("config-toast-container"),
  toast: document.getElementById("config-toast"),
  toastMessage: document.getElementById("config-toast-message"),
  saveButton: document.getElementById("save-config"),
};

function showSkeleton(skeleton, input, isLoading) {
  if (skeleton) {
    skeleton.classList.toggle("hidden", !isLoading);
  }
  if (input) {
    input.classList.toggle("hidden", isLoading);
  }
}

function renderToast(message, tone = "") {
  if (!elements.toastContainer || !elements.toast || !elements.toastMessage) {
    return;
  }
  if (toastTimer) {
    clearTimeout(toastTimer);
    toastTimer = null;
  }
  if (!message) {
    elements.toastContainer.classList.add("hidden");
    elements.toast.classList.remove("toast--success", "toast--error");
    elements.toastMessage.textContent = "";
    return;
  }
  elements.toastContainer.classList.remove("hidden");
  elements.toast.classList.remove("toast--success", "toast--error");
  if (tone === "success") {
    elements.toast.classList.add("toast--success");
  } else if (tone === "error") {
    elements.toast.classList.add("toast--error");
  }
  elements.toastMessage.textContent = message;
  toastTimer = window.setTimeout(() => renderToast(null), 3200);
}

function applyTranslations() {
  renderToast(null);
  document.documentElement.lang = currentLang;
  document.documentElement.setAttribute("data-pref-lang", currentLang);
  document.title = translations[currentLang].configTitle;

  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.dataset.i18n;
    if (key && translations[currentLang][key]) {
      node.textContent = translations[currentLang][key];
    }
  });
}

window.applyTranslations = applyTranslations;
if (window.__SIDEBAR_TRANSLATE_PENDING) {
  window.applyTranslations();
  window.__SIDEBAR_TRANSLATE_PENDING = false;
}

function setLang(lang) {
  if (!lang || !translations[lang]) {
    return;
  }
  persistLanguage(lang);
  currentLang = lang;
  applyTranslations();
}

async function loadConfig() {
  showSkeleton(elements.windowSkeleton, elements.windowDays, true);
  showSkeleton(elements.peripheralSkeleton, elements.peripheralAggregateTime, true);
  showSkeleton(elements.globalFlashSkeleton, elements.globalFlashFrequency, true);
  try {
    const response = await fetch(`${API_BASE}/control/status`);
    const data = await response.json();
    const config = data.config || {};
    configState.includeST = !!config.includeST;
    configState.includeDelisted = !!config.includeDelisted;
    elements.includeSt.checked = configState.includeST;
    elements.includeDelisted.checked = configState.includeDelisted;
    const windowDays = Number(config.dailyTradeWindowDays);
    if (Number.isFinite(windowDays) && windowDays > 0) {
      configState.dailyTradeWindowDays = windowDays;
      elements.windowDays.value = windowDays;
    } else {
      elements.windowDays.value = "";
    }
    if (elements.peripheralAggregateTime) {
      const scheduleValue =
        typeof config.peripheralAggregateTime === "string" && config.peripheralAggregateTime
          ? config.peripheralAggregateTime
          : configState.peripheralAggregateTime;
      configState.peripheralAggregateTime = scheduleValue || "06:00";
      elements.peripheralAggregateTime.value = scheduleValue || "";
    }
    if (elements.globalFlashFrequency) {
      const frequencyValue = Number(config.globalFlashFrequencyMinutes);
      if (Number.isFinite(frequencyValue) && frequencyValue >= 10) {
        configState.globalFlashFrequencyMinutes = Math.min(Math.max(frequencyValue, 10), 1440);
      }
      elements.globalFlashFrequency.value = configState.globalFlashFrequencyMinutes;
    }
  } catch (error) {
    console.error("Failed to load configuration", error);
    renderToast(translations[currentLang].toastConfigFailed || "Failed to save configuration", "error");
  } finally {
    showSkeleton(elements.windowSkeleton, elements.windowDays, false);
    showSkeleton(elements.peripheralSkeleton, elements.peripheralAggregateTime, false);
    showSkeleton(elements.globalFlashSkeleton, elements.globalFlashFrequency, false);
  }
}

async function saveConfig() {
  const windowDaysValue = Number(elements.windowDays.value);
  const dailyWindow =
    Number.isFinite(windowDaysValue) && windowDaysValue > 0
      ? windowDaysValue
      : configState.dailyTradeWindowDays;
  const scheduleValue =
    elements.peripheralAggregateTime?.value?.trim() ||
    configState.peripheralAggregateTime ||
    "06:00";
  const frequencyInput = Number(elements.globalFlashFrequency?.value);
  const globalFlashFrequency = Number.isFinite(frequencyInput) && frequencyInput >= 10
    ? Math.min(Math.max(frequencyInput, 10), 1440)
    : configState.globalFlashFrequencyMinutes;
  configState.includeST = elements.includeSt.checked;
  configState.includeDelisted = elements.includeDelisted.checked;
  configState.dailyTradeWindowDays = dailyWindow;
  configState.peripheralAggregateTime = scheduleValue;
  configState.globalFlashFrequencyMinutes = globalFlashFrequency;
  const payload = {
    includeST: elements.includeSt.checked,
    includeDelisted: elements.includeDelisted.checked,
    dailyTradeWindowDays: dailyWindow,
    peripheralAggregateTime: scheduleValue,
    globalFlashFrequencyMinutes: globalFlashFrequency,
  };
  elements.saveButton.disabled = true;
  try {
    const response = await fetch(`${API_BASE}/control/config`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      keepalive: true,
    });
    if (!response.ok) {
      const message = await response.text();
      throw new Error(message || `Request failed with status ${response.status}`);
    }
    renderToast(translations[currentLang].toastConfigSaved, "success");
    await loadConfig();
  } catch (error) {
    console.error("Failed to save configuration", error);
    const message =
      (error && error.message) || translations[currentLang].toastConfigFailed || "Failed to save configuration";
    renderToast(message, "error");
  } finally {
    elements.saveButton.disabled = false;
  }
}

function initLanguageSwitch() {
  elements.langButtons.forEach((btn) =>
    btn.addEventListener("click", () => setLang(btn.dataset.lang))
  );
}

function initActions() {
  elements.saveButton.addEventListener("click", saveConfig);
}

initLanguageSwitch();
initActions();
setLang(currentLang);
loadConfig();
