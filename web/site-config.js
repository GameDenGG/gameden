(function () {
  "use strict";

  function isLocalHost(hostname) {
    const normalized = String(hostname || "").trim().toLowerCase();
    return (
      normalized === "localhost" ||
      normalized === "127.0.0.1" ||
      normalized === "::1" ||
      normalized.endsWith(".local")
    );
  }

  function defaultApiBase() {
    if (typeof window === "undefined") {
      return "https://gameden.onrender.com";
    }
    return isLocalHost(window.location.hostname) ? "" : "https://gameden.onrender.com";
  }

  // Shared static-site runtime config.
  // For a separate API host on Render static, set:
  // window.__GAMEDEN_SITE__.api_base = "https://your-api-host"
  const defaults = {
    site_name: "GameDen.gg",
    site_url: "https://gameden.gg",
    site_description:
      "Discover game deals, analytics, player trends, and price history on GameDen.gg.",
    // Production frontend is split from the API host by default.
    // For localhost development, keep same-origin requests by default.
    api_base: defaultApiBase(),
  };

  const existing =
    window.__GAMEDEN_SITE__ && typeof window.__GAMEDEN_SITE__ === "object"
      ? window.__GAMEDEN_SITE__
      : {};

  const merged = Object.assign({}, defaults, existing);
  if (typeof existing.api_base === "string") {
    const trimmed = existing.api_base.trim();
    if (trimmed) {
      merged.api_base = trimmed;
    } else if (!isLocalHost(window.location.hostname)) {
      // Prevent accidental production fallback to relative API requests.
      merged.api_base = defaults.api_base;
    }
  }

  window.__GAMEDEN_SITE__ = merged;
})();
