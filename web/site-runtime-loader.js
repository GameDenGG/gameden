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

  const suffix = getAssetQuerySuffix();
  document.write(`<script src="/site-config.js${suffix}"><\\/script>`);
  document.write(`<script src="/site-branding.js${suffix}"><\\/script>`);
})();
