const SIDEBAR_VERSION = "20251226";

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
