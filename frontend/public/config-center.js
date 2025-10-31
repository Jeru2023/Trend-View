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

const elements = {
  langButtons: document.querySelectorAll(".lang-btn"),
  includeSt: document.getElementById("config-include-st"),
  includeDelisted: document.getElementById("config-include-delisted"),
  windowDays: document.getElementById("config-window"),
  saveButton: document.getElementById("save-config"),
};

function applyTranslations() {
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
  try {
    const response = await fetch(`${API_BASE}/control/status`);
    const data = await response.json();
    const config = data.config || {};
    elements.includeSt.checked = !!config.includeST;
    elements.includeDelisted.checked = !!config.includeDelisted;
    const windowDays = Number(config.dailyTradeWindowDays);
    elements.windowDays.value = Number.isFinite(windowDays) && windowDays > 0 ? windowDays : 420;
  } catch (error) {
    console.error("Failed to load configuration", error);
  }
}

async function saveConfig() {
  const payload = {
    includeST: elements.includeSt.checked,
    includeDelisted: elements.includeDelisted.checked,
    dailyTradeWindowDays: Number(elements.windowDays.value) || 420,
  };
  try {
    await fetch(`${API_BASE}/control/config`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    alert(translations[currentLang].toastConfigSaved);
    loadConfig();
  } catch (error) {
    console.error("Failed to save configuration", error);
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
