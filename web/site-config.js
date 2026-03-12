(function () {
  "use strict";

  // Shared static-site runtime config.
  // For a separate API host on Render static, set:
  // window.__GAMEDEN_SITE__.api_base = "https://your-api-host"
  const defaults = {
    site_name: "GameDen.gg",
    site_url: "https://gameden.gg",
    site_description:
      "Discover game deals, analytics, player trends, and price history on GameDen.gg.",
    api_base: "https://gameden-web.onrender.com",
  };

  const existing =
    window.__GAMEDEN_SITE__ && typeof window.__GAMEDEN_SITE__ === "object"
      ? window.__GAMEDEN_SITE__
      : {};

  window.__GAMEDEN_SITE__ = Object.assign({}, defaults, existing);
})();