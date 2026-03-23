(function () {
  "use strict";

  const existing = window.GAMEDEN_CONFIG && typeof window.GAMEDEN_CONFIG === "object"
    ? window.GAMEDEN_CONFIG
    : {};
  const existingSupabase = existing.supabase && typeof existing.supabase === "object"
    ? existing.supabase
    : {};

  const supabaseUrl = String(existingSupabase.url || existing.SUPABASE_URL || "https://vmmqnyewvgohxqodxjau.supabase.co").trim();
  const supabaseAnonKey = String(existingSupabase.anonKey || existing.SUPABASE_ANON_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZtbXFueWV3dmdvaHhxb2R4amF1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQyMTE1MjcsImV4cCI6MjA4OTc4NzUyN30.ew8Ip6QWWRCiuZ_oQGF1UL0L-SaRdmICXdYrsp0kAcM").trim();

  window.GAMEDEN_CONFIG = Object.freeze({
    ...existing,
    // Keep legacy flat keys for compatibility with any pre-existing integrations.
    SUPABASE_URL: supabaseUrl,
    SUPABASE_ANON_KEY: supabaseAnonKey,
    // Canonical shape for new auth foundation code.
    supabase: Object.freeze({
      url: supabaseUrl,
      anonKey: supabaseAnonKey,
    }),
  });
})();
