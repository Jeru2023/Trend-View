const SIDEBAR_VERSION = "20260306";
const SIDEBAR_SCROLL_KEY = "trend-view-sidebar-scroll";

function getStoredScroll() {
  try {
    const stored = window.localStorage.getItem(SIDEBAR_SCROLL_KEY);
    if (stored === null) {
      return 0;
    }
    const value = Number(stored);
    return Number.isFinite(value) && value >= 0 ? value : 0;
  } catch (error) {
    return 0;
  }
}

function persistScroll(value) {
  try {
    window.localStorage.setItem(SIDEBAR_SCROLL_KEY, String(value));
  } catch (error) {
    /* no-op */
  }
}

function restoreScrollPosition(sidebarRoot) {
  const stored = getStoredScroll();
  if (stored < 0) {
    return;
  }
  sidebarRoot.scrollTop = stored;
  requestAnimationFrame(() => {
    sidebarRoot.scrollTop = stored;
  });
}

function highlightActiveNav(root) {
  if (!root) {
    return;
  }
  const activeKey = document.body.getAttribute("data-active-nav");
  if (!activeKey) {
    return;
  }
  const activeItem = root.querySelector(`[data-nav-key="${activeKey}"]`);
  if (activeItem) {
    activeItem.classList.add("nav__item--active");
  }
}

function ensureSidebarTranslations() {
  if (typeof window.applyTranslations === "function") {
    window.applyTranslations();
  } else {
    window.__SIDEBAR_TRANSLATE_PENDING = true;
  }
}

(function loadSidebar() {
  const container = document.querySelector("[data-sidebar-container]");
  if (!container) {
    return;
  }

  fetch(`sidebar.html?v=${SIDEBAR_VERSION}`)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`Failed to load sidebar (status ${response.status})`);
      }
      return response.text();
    })
    .then((html) => {
      container.innerHTML = html;
      const sidebarRoot = container.querySelector(".sidebar");
      if (sidebarRoot) {
        restoreScrollPosition(sidebarRoot);
        const persistHandler = () => persistScroll(sidebarRoot.scrollTop);
        sidebarRoot.addEventListener("scroll", persistHandler, { passive: true });
        sidebarRoot.addEventListener("click", persistHandler);
        window.addEventListener("beforeunload", persistHandler);
      }
      highlightActiveNav(sidebarRoot);
      ensureSidebarTranslations();
    })
    .catch((error) => {
      console.error("Sidebar injection failed:", error);
    });
})();

window.addEventListener("DOMContentLoaded", () => {
  if (window.__SIDEBAR_TRANSLATE_PENDING && typeof window.applyTranslations === "function") {
    window.applyTranslations();
    window.__SIDEBAR_TRANSLATE_PENDING = false;
  }
});
