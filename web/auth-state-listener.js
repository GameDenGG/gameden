(function () {
  "use strict";

  if (window.GameDenAuthState && typeof window.GameDenAuthState.subscribe === "function") {
    return;
  }

  const listeners = new Set();
  let underlyingSubscription = null;
  let currentState = Object.freeze({
    event: "INITIAL",
    session: null,
    user: null,
    at: Date.now(),
  });

  function getClient() {
    const registry = window.GameDenSupabaseClient;
    if (!registry || typeof registry.getClient !== "function") {
      return null;
    }
    return registry.getClient();
  }

  function safeInvoke(listener, payload) {
    try {
      listener(payload);
    } catch (error) {
      if (typeof console !== "undefined" && typeof console.error === "function") {
        console.error("[GameDenAuthState] listener error", error);
      }
    }
  }

  function emit(eventName, session) {
    const nextState = Object.freeze({
      event: String(eventName || "UNKNOWN"),
      session: session || null,
      user: session && session.user ? session.user : null,
      at: Date.now(),
    });
    currentState = nextState;
    listeners.forEach((listener) => safeInvoke(listener, nextState));
  }

  function ensureUnderlyingListener() {
    if (underlyingSubscription) {
      return underlyingSubscription;
    }
    const client = getClient();
    if (!client || !client.auth || typeof client.auth.onAuthStateChange !== "function") {
      return null;
    }
    const result = client.auth.onAuthStateChange(function onAuthStateChange(eventName, session) {
      emit(eventName, session);
    });
    if (result && result.data && result.data.subscription) {
      underlyingSubscription = result.data.subscription;
      return underlyingSubscription;
    }
    return null;
  }

  function teardownIfIdle() {
    if (listeners.size > 0 || !underlyingSubscription) {
      return;
    }
    if (typeof underlyingSubscription.unsubscribe === "function") {
      underlyingSubscription.unsubscribe();
    }
    underlyingSubscription = null;
  }

  function subscribe(listener, options) {
    if (typeof listener !== "function") {
      throw new Error("subscribe(listener): listener must be a function.");
    }
    listeners.add(listener);
    ensureUnderlyingListener();

    const opts = options && typeof options === "object" ? options : {};
    if (opts.emitCurrent !== false) {
      safeInvoke(listener, currentState);
    }

    return function unsubscribe() {
      listeners.delete(listener);
      teardownIfIdle();
    };
  }

  window.GameDenAuthState = Object.freeze({
    subscribe,
    getCurrentState() {
      return currentState;
    },
    getListenerCount() {
      return listeners.size;
    },
  });
})();
