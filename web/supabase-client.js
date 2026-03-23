(function () {
  "use strict";

  if (window.GameDenSupabaseClient && typeof window.GameDenSupabaseClient.getClient === "function") {
    return;
  }

  let cachedClient = null;
  let cachedError = null;
  let initCount = 0;

  function readConfig() {
    const config = window.GAMEDEN_CONFIG && typeof window.GAMEDEN_CONFIG === "object"
      ? window.GAMEDEN_CONFIG
      : {};
    const supabase = config.supabase && typeof config.supabase === "object"
      ? config.supabase
      : {};

    const url = String(supabase.url || config.SUPABASE_URL || "").trim();
    const anonKey = String(supabase.anonKey || config.SUPABASE_ANON_KEY || "").trim();
    return { url, anonKey };
  }

  function createConfigError(reason) {
    const error = new Error(reason);
    error.code = "AUTH_CONFIG_ERROR";
    return error;
  }

  function createRuntimeError(reason) {
    const error = new Error(reason);
    error.code = "AUTH_RUNTIME_ERROR";
    return error;
  }

  function ensureClient() {
    if (cachedClient) {
      return cachedClient;
    }
    if (cachedError) {
      return null;
    }

    const config = readConfig();
    if (!config.url || !config.anonKey) {
      cachedError = createConfigError(
        "Missing Supabase config. Ensure runtime-config.js sets GAMEDEN_CONFIG.supabase.url and anonKey.",
      );
      return null;
    }

    if (!window.supabase || typeof window.supabase.createClient !== "function") {
      cachedError = createRuntimeError(
        "Supabase SDK is not loaded. Include https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2 before auth scripts.",
      );
      return null;
    }

    try {
      cachedClient = window.supabase.createClient(config.url, config.anonKey, {
        auth: {
          persistSession: true,
          autoRefreshToken: true,
          detectSessionInUrl: true,
        },
      });
      initCount += 1;
      return cachedClient;
    } catch (error) {
      cachedError = error instanceof Error ? error : new Error(String(error || "Failed to initialize Supabase client."));
      return null;
    }
  }

  window.GameDenSupabaseClient = Object.freeze({
    getClient() {
      return ensureClient();
    },
    getConfig() {
      return readConfig();
    },
    getError() {
      if (cachedError) return cachedError;
      ensureClient();
      return cachedError;
    },
    getInitCount() {
      return initCount;
    },
  });
})();
