(function () {
  "use strict";

  function getAssetQuerySuffix() {
    try {
      const current = document.currentScript;
      if (!current || !current.src) return "";
      const parsed = new URL(current.src, window.location.origin);
      const version = String(parsed.searchParams.get("v") || "").trim();
      if (!version) return "";
      return `?v=${encodeURIComponent(version)}`;
    } catch (_error) {
      return "";
    }
  }

  function isRuntimeReady() {
    return !!(window.GameDenSite && typeof window.GameDenSite.fetchJson === "function");
  }

  function loadScriptSequentially(src) {
    return new Promise((resolve, reject) => {
      const targetHref = new URL(src, window.location.origin).href;
      const existingScript = Array.from(document.querySelectorAll("script[src]")).find((node) => {
        try {
          return new URL(node.src, window.location.origin).href === targetHref;
        } catch (_error) {
          return false;
        }
      });

      if (existingScript) {
        const loadedState = existingScript.getAttribute("data-gameden-loaded");
        if (loadedState === "true") {
          resolve();
          return;
        }
        existingScript.addEventListener("load", () => resolve(), { once: true });
        existingScript.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
        return;
      }

      const script = document.createElement("script");
      script.src = src;
      script.async = false;
      script.defer = false;
      script.setAttribute("data-gameden-loaded", "false");
      script.addEventListener(
        "load",
        () => {
          script.setAttribute("data-gameden-loaded", "true");
          resolve();
        },
        { once: true }
      );
      script.addEventListener(
        "error",
        () => {
          script.setAttribute("data-gameden-loaded", "error");
          reject(new Error(`Failed to load ${src}`));
        },
        { once: true }
      );
      (document.head || document.documentElement).appendChild(script);
    });
  }

  async function loadOptionalAuthBundle() {
    await loadScriptSequentially(`/runtime-config.js${suffix}`);
    await loadScriptSequentially("https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2");
    await loadScriptSequentially(`/supabase-client.js${suffix}`);
    await loadScriptSequentially(`/auth-session.js${suffix}`);
    await loadScriptSequentially(`/auth-state-listener.js${suffix}`);
    await loadScriptSequentially(`/account-state.js${suffix}`);
  }

  const suffix = getAssetQuerySuffix();
  const runtimeReady = (async function initRuntime() {
    if (isRuntimeReady()) return window.GameDenSite;

    await loadScriptSequentially(`/site-config.js${suffix}`);
    await loadScriptSequentially(`/site-branding.js${suffix}`);

    if (!isRuntimeReady()) {
      throw new Error("GameDen runtime failed to initialize. Ensure /site-branding.js is available.");
    }
    return window.GameDenSite;
  })();

  window.__GAMEDEN_RUNTIME_READY__ = runtimeReady;
  window.__GAMEDEN_AUTH_READY__ = runtimeReady.then(() => loadOptionalAuthBundle());
  window.getGameDenRuntime = function getGameDenRuntime() {
    return runtimeReady;
  };
  window.getGameDenAuthRuntime = function getGameDenAuthRuntime() {
    return window.__GAMEDEN_AUTH_READY__;
  };

  runtimeReady
    .then(() => {
      document.dispatchEvent(new CustomEvent("gameden:runtime-ready"));
    })
    .catch((error) => {
      document.dispatchEvent(new CustomEvent("gameden:runtime-error", { detail: error }));
      if (typeof console !== "undefined" && typeof console.error === "function") {
        console.error("[GameDenSite] runtime initialization failed", error);
      }
    });

  window.__GAMEDEN_AUTH_READY__
    .then(() => {
      document.dispatchEvent(new CustomEvent("gameden:auth-ready"));
    })
    .catch((error) => {
      document.dispatchEvent(new CustomEvent("gameden:auth-error", { detail: error }));
      if (typeof console !== "undefined" && typeof console.warn === "function") {
        console.warn("[GameDenSite] optional auth bundle failed to initialize", error);
      }
    });

})();
