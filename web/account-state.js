(function () {
  "use strict";

  if (window.GameDenAccount && typeof window.GameDenAccount.getIdentity === "function") {
    return;
  }

  const USER_ID_STORAGE_KEY = "gameden.user_id";
  const AUTH_SYNC_STORAGE_KEY = "gameden.auth.sync.v1";
  const DASHBOARD_SNAPSHOT_STORAGE_KEY = "gameden.dashboard.snapshot.v1";
  const ANON_ID_RE = /^anon_[0-9a-f]{32}$/;
  const AUTH_ID_RE = /^acct_[0-9a-f-]{20,64}$/;

  function isAnonUserId(value) {
    return ANON_ID_RE.test(String(value || "").trim().toLowerCase());
  }

  function isAuthenticatedUserId(value) {
    return AUTH_ID_RE.test(String(value || "").trim().toLowerCase());
  }

  function normalizeStoredUserId(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (!normalized) return "";
    if (isAnonUserId(normalized) || isAuthenticatedUserId(normalized)) {
      return normalized;
    }
    return "";
  }

  function createAnonUserId() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return `anon_${String(window.crypto.randomUUID()).replaceAll("-", "").toLowerCase()}`;
    }
    const entropy = `${Date.now().toString(16)}${Math.floor(Math.random() * 1e12).toString(16)}`.slice(0, 32);
    return `anon_${entropy.padEnd(32, "0")}`;
  }

  function readStoredUserId() {
    try {
      return normalizeStoredUserId(window.localStorage.getItem(USER_ID_STORAGE_KEY));
    } catch (_error) {
      return "";
    }
  }

  function toAccountUserId(supabaseUserId) {
    const normalized = String(supabaseUserId || "").trim().toLowerCase();
    if (!normalized) return "";
    return normalizeStoredUserId(`acct_${normalized}`);
  }

  function dispatchAccountEvent(type, payload) {
    const detail = {
      type: String(type || "identity_changed"),
      ts: Date.now(),
      ...(payload && typeof payload === "object" ? payload : {}),
    };

    try {
      window.localStorage.setItem(AUTH_SYNC_STORAGE_KEY, JSON.stringify(detail));
    } catch (_error) {
      // Ignore storage failures.
    }

    window.dispatchEvent(new CustomEvent("gameden:account-sync", { detail }));
    return detail;
  }

  async function readSessionState() {
    if (!window.GameDenAuthSession || typeof window.GameDenAuthSession.getState !== "function") {
      return { loggedIn: false, user: null, session: null, error: null };
    }
    try {
      return await window.GameDenAuthSession.getState();
    } catch (error) {
      return { loggedIn: false, user: null, session: null, error: error instanceof Error ? error : new Error(String(error || "Auth state failed.")) };
    }
  }

  async function syncIdentityFromSession(options = {}) {
    const opts = options && typeof options === "object" ? options : {};
    const previousUserId = readStoredUserId();
    const state = await readSessionState();
    const loggedIn = !!(state && state.loggedIn && state.user && state.user.id);
    const runtime = window.GameDenSite;

    let nextUserId = previousUserId;
    if (loggedIn) {
      nextUserId = toAccountUserId(state.user.id);
      const guestCandidate = isAnonUserId(previousUserId) ? previousUserId : "";
      if (runtime && typeof runtime.setLastGuestViewerId === "function" && guestCandidate) {
        runtime.setLastGuestViewerId(guestCandidate);
      }
      if (runtime && typeof runtime.setViewerId === "function" && nextUserId) {
        runtime.setViewerId(nextUserId);
      } else if (nextUserId) {
        try {
          window.localStorage.setItem(USER_ID_STORAGE_KEY, nextUserId);
        } catch (_error) {
          // Ignore storage failures.
        }
      }
    } else if (isAuthenticatedUserId(previousUserId) && opts.allowDemoteToGuest === true) {
      nextUserId = "";
      if (runtime && typeof runtime.setViewerId === "function") {
        nextUserId = runtime.setViewerId(createAnonUserId()) || "";
      } else {
        try {
          nextUserId = createAnonUserId();
          window.localStorage.setItem(USER_ID_STORAGE_KEY, nextUserId);
        } catch (_error) {
          nextUserId = "";
        }
      }
    }

    nextUserId = normalizeStoredUserId(nextUserId) || readStoredUserId();
    const changed = previousUserId !== nextUserId;

    if (changed || opts.forceEvent) {
      dispatchAccountEvent("identity_changed", {
        previous_user_id: previousUserId || null,
        user_id: nextUserId || null,
        logged_in: !!loggedIn,
      });
    }

    return {
      loggedIn: !!loggedIn,
      previousUserId: previousUserId || null,
      userId: nextUserId || null,
      session: state && state.session ? state.session : null,
      user: state && state.user ? state.user : null,
      changed,
      error: state && state.error ? state.error : null,
    };
  }

  async function mergeGuestLists(guestUserId) {
    const guestId = normalizeStoredUserId(guestUserId);
    if (!guestId || !isAnonUserId(guestId)) {
      return { ok: true, skipped: true };
    }
    const runtime = window.GameDenSite;
    if (!runtime || typeof runtime.fetchJson !== "function") {
      throw new Error("Runtime fetch helper is unavailable.");
    }
    if (typeof window.getGameDenAuthRuntime === "function") {
      try {
        await window.getGameDenAuthRuntime();
      } catch (_error) {
        // Continue with best effort.
      }
    }

    let payload = null;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        payload = await runtime.fetchJson("/api/account/merge-guest-lists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ guest_user_id: guestId, clear_guest_data: true }),
        });
        break;
      } catch (error) {
        const status = Number(error && error.status ? error.status : 0);
        const retryable = status === 401 || status === 403;
        if (!retryable || attempt >= 1) {
          throw error;
        }
        await readSessionState();
        await new Promise((resolve) => setTimeout(resolve, 160));
      }
    }

    dispatchAccountEvent("merge_completed", {
      guest_user_id: guestId,
      user_id: payload && payload.user_id ? payload.user_id : null,
      merged_wishlist_count: Number(payload && payload.merged_wishlist_count ? payload.merged_wishlist_count : 0),
      merged_watchlist_count: Number(payload && payload.merged_watchlist_count ? payload.merged_watchlist_count : 0),
    });
    return payload;
  }

  function clearDashboardSnapshot() {
    try {
      window.sessionStorage.removeItem(DASHBOARD_SNAPSHOT_STORAGE_KEY);
    } catch (_error) {
      // Ignore storage failures.
    }
  }

  async function signUp(email, password) {
    if (!window.GameDenSupabaseClient || typeof window.GameDenSupabaseClient.getClient !== "function") {
      throw new Error("Supabase client helper is not loaded.");
    }
    const client = window.GameDenSupabaseClient.getClient();
    if (!client) {
      const clientError = window.GameDenSupabaseClient.getError && window.GameDenSupabaseClient.getError();
      throw clientError || new Error("Supabase client unavailable.");
    }
    const priorUserId = readStoredUserId();
    const result = await client.auth.signUp({ email, password });
    if (result.error) throw result.error;
    const identity = await syncIdentityFromSession();
    if (identity.loggedIn && isAnonUserId(priorUserId)) {
      await mergeGuestLists(priorUserId);
    }
    clearDashboardSnapshot();
    dispatchAccountEvent("signup_success", {
      previous_user_id: priorUserId || null,
      user_id: identity.userId || null,
      logged_in: !!identity.loggedIn,
    });
    return { result, identity };
  }

  async function signIn(email, password) {
    if (!window.GameDenSupabaseClient || typeof window.GameDenSupabaseClient.getClient !== "function") {
      throw new Error("Supabase client helper is not loaded.");
    }
    const client = window.GameDenSupabaseClient.getClient();
    if (!client) {
      const clientError = window.GameDenSupabaseClient.getError && window.GameDenSupabaseClient.getError();
      throw clientError || new Error("Supabase client unavailable.");
    }
    const priorUserId = readStoredUserId();
    const result = await client.auth.signInWithPassword({ email, password });
    if (result.error) throw result.error;
    const identity = await syncIdentityFromSession();
    if (identity.loggedIn && isAnonUserId(priorUserId)) {
      await mergeGuestLists(priorUserId);
    }
    clearDashboardSnapshot();
    dispatchAccountEvent("login_success", {
      previous_user_id: priorUserId || null,
      user_id: identity.userId || null,
      logged_in: !!identity.loggedIn,
    });
    return { result, identity };
  }

  async function signOut() {
    if (!window.GameDenSupabaseClient || typeof window.GameDenSupabaseClient.getClient !== "function") {
      throw new Error("Supabase client helper is not loaded.");
    }
    const client = window.GameDenSupabaseClient.getClient();
    if (!client) {
      const clientError = window.GameDenSupabaseClient.getError && window.GameDenSupabaseClient.getError();
      throw clientError || new Error("Supabase client unavailable.");
    }
    const priorUserId = readStoredUserId();
    const result = await client.auth.signOut();
    if (result.error) throw result.error;
    const identity = await syncIdentityFromSession({ allowDemoteToGuest: true });
    clearDashboardSnapshot();
    dispatchAccountEvent("logout_success", {
      previous_user_id: priorUserId || null,
      user_id: identity.userId || null,
      logged_in: !!identity.loggedIn,
    });
    return { result, identity };
  }

  window.GameDenAccount = Object.freeze({
    getIdentity() {
      const userId = readStoredUserId();
      return {
        userId,
        loggedIn: isAuthenticatedUserId(userId),
      };
    },
    isAuthenticatedUserId,
    isAnonymousUserId: isAnonUserId,
    syncIdentityFromSession,
    mergeGuestLists,
    clearDashboardSnapshot,
    dispatchAccountEvent,
    signUp,
    signIn,
    signOut,
  });

  if (window.GameDenAuthState && typeof window.GameDenAuthState.subscribe === "function") {
    window.GameDenAuthState.subscribe(function onAuthState(payload) {
      const eventName = String(payload && payload.event ? payload.event : "").trim().toUpperCase();
      const allowDemoteToGuest = eventName === "SIGNED_OUT";
      void syncIdentityFromSession({ allowDemoteToGuest });
    }, { emitCurrent: true });
  } else {
    void syncIdentityFromSession();
  }
})();
