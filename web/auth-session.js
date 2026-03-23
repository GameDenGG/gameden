(function () {
  "use strict";

  if (window.GameDenAuthSession && typeof window.GameDenAuthSession.getSession === "function") {
    return;
  }

  let lastError = null;

  function resolveClient() {
    const registry = window.GameDenSupabaseClient;
    if (!registry || typeof registry.getClient !== "function") {
      lastError = new Error("GameDenSupabaseClient is unavailable. Load /supabase-client.js first.");
      return null;
    }
    const client = registry.getClient();
    if (!client) {
      lastError = registry.getError ? registry.getError() : new Error("Supabase client could not be initialized.");
      return null;
    }
    return client;
  }

  function normalizeAuthError(error, fallbackMessage) {
    if (error instanceof Error) return error;
    return new Error(fallbackMessage || String(error || "Auth request failed."));
  }

  async function getSession() {
    const client = resolveClient();
    if (!client) {
      return { session: null, error: lastError, ok: false };
    }
    try {
      const result = await client.auth.getSession();
      const error = result && result.error ? normalizeAuthError(result.error, "Failed to fetch auth session.") : null;
      lastError = error;
      return {
        session: result && result.data ? result.data.session : null,
        error,
        ok: !error,
      };
    } catch (error) {
      lastError = normalizeAuthError(error, "Failed to fetch auth session.");
      return { session: null, error: lastError, ok: false };
    }
  }

  async function getUser() {
    const sessionResult = await getSession();
    const session = sessionResult.session;
    const user = session && session.user ? session.user : null;
    return {
      user,
      session,
      error: sessionResult.error,
      ok: sessionResult.ok,
    };
  }

  async function isLoggedIn() {
    const userResult = await getUser();
    const loggedIn = !!(userResult.user && userResult.user.id);
    return {
      loggedIn,
      session: userResult.session,
      user: userResult.user,
      error: userResult.error,
      ok: userResult.ok,
    };
  }

  window.GameDenAuthSession = Object.freeze({
    async getSession() {
      return getSession();
    },
    async getUser() {
      return getUser();
    },
    async isLoggedIn() {
      return isLoggedIn();
    },
    async getState() {
      return isLoggedIn();
    },
    getLastError() {
      return lastError;
    },
  });
})();
