import assert from "node:assert/strict";
import test from "node:test";

import { DEBUG_PRIORITY, DISCARD_REASON, INSIGHT_ACTION, TRIGGER_TYPE } from "./constants";
import {
  compareCandidates,
  filterCandidates,
  resolveConflictingCandidates,
  resolveDuplicateCandidates,
} from "./filtering";
import type { TransformedCandidate } from "./types";

const HOUR_MS = 60 * 60 * 1000;
const DUPLICATE_WINDOW_MS = 24 * HOUR_MS;

function makeCandidate(overrides: Partial<TransformedCandidate> & Pick<TransformedCandidate, "trigger" | "insight">): TransformedCandidate {
  return {
    trigger: overrides.trigger,
    insight: overrides.insight,
    magnitude: overrides.magnitude ?? 0.5,
    recency: overrides.recency ?? 0.5,
    user_relevance: overrides.user_relevance ?? 0.5,
    historical_context: overrides.historical_context ?? 0.5,
    score: overrides.score ?? 0.5,
    priority: overrides.priority ?? DEBUG_PRIORITY.MEDIUM,
  };
}

function priceCandidate(
  gameId: string,
  score: number,
  userRelevance: number,
  timestamp: number,
  action: string = INSIGHT_ACTION.BUY_NOW,
): TransformedCandidate {
  return makeCandidate({
    trigger: {
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId,
      timestamp,
      previous: 100,
      current: 75,
    },
    insight: {
      type: TRIGGER_TYPE.PRICE_CHANGE,
      gameId,
      title: "Price drop",
      reason: "Price fell from 100 to 75",
      action,
      score,
    },
    score,
    user_relevance: userRelevance,
  });
}

function reviewCandidate(
  gameId: string,
  score: number,
  userRelevance: number,
  timestamp: number,
  action: string = INSIGHT_ACTION.WAIT,
): TransformedCandidate {
  return makeCandidate({
    trigger: {
      type: TRIGGER_TYPE.REVIEW_CHANGE,
      gameId,
      timestamp,
      previous: "Mixed",
      current: "Mostly Positive",
    },
    insight: {
      type: TRIGGER_TYPE.REVIEW_CHANGE,
      gameId,
      title: "Reviews: Mixed → Mostly Positive",
      reason: "Shifted from Mixed to Mostly Positive",
      action,
      score,
    },
    score,
    user_relevance: userRelevance,
  });
}

function releaseCandidate(
  gameId: string,
  score: number,
  userRelevance: number,
  timestamp: number,
  action: string = INSIGHT_ACTION.WAIT,
): TransformedCandidate {
  return makeCandidate({
    trigger: {
      type: TRIGGER_TYPE.RELEASE_EVENT,
      gameId,
      timestamp,
      previous: null,
      current: "released",
    },
    insight: {
      type: TRIGGER_TYPE.RELEASE_EVENT,
      gameId,
      title: "Now fully released",
      reason: "Moved from pre-release to released",
      action,
      score,
    },
    score,
    user_relevance: userRelevance,
  });
}

function relevanceCandidate(
  gameId: string,
  score: number,
  userRelevance: number,
  timestamp: number,
  action: string = INSIGHT_ACTION.WAIT,
): TransformedCandidate {
  return makeCandidate({
    trigger: {
      type: TRIGGER_TYPE.RELEVANCE_INCREASE,
      gameId,
      timestamp,
      previous: 0.42,
      current: 0.61,
    },
    insight: {
      type: TRIGGER_TYPE.RELEVANCE_INCREASE,
      gameId,
      title: "New strong match for you",
      reason: "Relevance crossed the attention threshold",
      action,
      score,
    },
    score,
    user_relevance: userRelevance,
  });
}

function statusCandidate(
  gameId: string,
  score: number,
  userRelevance: number,
  timestamp: number,
  action: string = INSIGHT_ACTION.BUY_NOW,
): TransformedCandidate {
  return makeCandidate({
    trigger: {
      type: TRIGGER_TYPE.STATUS_CHANGE,
      gameId,
      timestamp,
      previous: "stable",
      current: "delisting",
    },
    insight: {
      type: TRIGGER_TYPE.STATUS_CHANGE,
      gameId,
      title: "Delisting soon",
      reason: "This game is heading for delisting",
      action,
      score,
    },
    score,
    user_relevance: userRelevance,
  });
}

test("compareCandidates orders by score, user relevance, recency, then type priority", () => {
  const baseTimestamp = 1_000_000;
  const price = priceCandidate("game-a", 0.9, 0.4, baseTimestamp);
  const lowerScore = priceCandidate("game-a", 0.8, 0.9, baseTimestamp);
  const lowerRelevance = priceCandidate("game-a", 0.9, 0.2, baseTimestamp);
  const older = priceCandidate("game-a", 0.9, 0.4, baseTimestamp - 1_000);
  const status = statusCandidate("game-a", 0.9, 0.4, baseTimestamp);

  assert.ok(compareCandidates(price, lowerScore) < 0);
  assert.ok(compareCandidates(price, lowerRelevance) < 0);
  assert.ok(compareCandidates(price, older) < 0);
  assert.ok(compareCandidates(price, status) < 0);
});

test("resolveDuplicateCandidates keeps the best candidate within a duplicate cluster", () => {
  const candidates = [
    priceCandidate("game-dup", 0.7, 0.5, 0),
    priceCandidate("game-dup", 0.9, 0.5, 2 * HOUR_MS),
    priceCandidate("game-dup", 0.6, 0.5, 4 * HOUR_MS),
  ];

  const result = resolveDuplicateCandidates(candidates);

  assert.equal(result.survivors.length, 1);
  assert.equal(result.discarded.length, 2);
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.DUPLICATE);
  assert.equal(result.discarded[0].score, 0.7);
  assert.equal(result.discarded[0].priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.discarded[1].discardReason, DISCARD_REASON.DUPLICATE);
  assert.equal(result.discarded[1].score, 0.6);
  assert.equal(result.survivors[0].score, 0.9);
});

test("resolveDuplicateCandidates allows multiple survivors across separate clusters", () => {
  const candidates = [
    priceCandidate("game-dup", 0.7, 0.5, 0),
    priceCandidate("game-dup", 0.9, 0.5, 2 * HOUR_MS),
    priceCandidate("game-dup", 0.6, 0.5, 25 * HOUR_MS),
  ];

  const result = resolveDuplicateCandidates(candidates);

  assert.equal(result.survivors.length, 2);
  assert.equal(result.discarded.length, 1);
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.DUPLICATE);
  assert.equal(result.discarded[0].score, 0.7);
  assert.equal(result.survivors[0].score, 0.9);
  assert.equal(result.survivors[1].score, 0.6);
});

test("resolveDuplicateCandidates treats exactly 24 hours as non-duplicate", () => {
  const candidates = [
    priceCandidate("game-boundary", 0.9, 0.5, 0),
    priceCandidate("game-boundary", 0.8, 0.5, DUPLICATE_WINDOW_MS),
  ];

  const result = resolveDuplicateCandidates(candidates);

  assert.equal(result.survivors.length, 2);
  assert.equal(result.discarded.length, 0);
});

test("resolveDuplicateCandidates leaves same-type candidates outside the window alone", () => {
  const candidates = [
    priceCandidate("game-open", 0.9, 0.5, 0),
    priceCandidate("game-open", 0.8, 0.5, DUPLICATE_WINDOW_MS + HOUR_MS),
  ];

  const result = resolveDuplicateCandidates(candidates);

  assert.equal(result.survivors.length, 2);
  assert.equal(result.discarded.length, 0);
});

test("resolveConflictingCandidates resolves Buy now versus Wait using the comparator", () => {
  const candidates = [
    priceCandidate("game-conflict", 0.72, 0.6, 0),
    reviewCandidate("game-conflict", 0.81, 0.6, 1_000),
  ];

  const result = resolveConflictingCandidates(candidates);

  assert.equal(result.survivors.length, 1);
  assert.equal(result.survivors[0].insight.action, INSIGHT_ACTION.WAIT);
  assert.equal(result.discarded.length, 1);
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.CONFLICT);
  assert.equal(result.discarded[0].score, 0.72);
});

test("resolveConflictingCandidates resolves Buy now versus Avoid using the comparator", () => {
  const candidates = [
    priceCandidate("game-avoid", 0.9, 0.6, 0),
    makeCandidate({
      trigger: {
        type: TRIGGER_TYPE.RELEASE_EVENT,
        gameId: "game-avoid",
        timestamp: 1_000,
        previous: null,
        current: "released",
      },
      insight: {
        type: TRIGGER_TYPE.RELEASE_EVENT,
        gameId: "game-avoid",
        title: "Now fully released",
        reason: "Moved from pre-release to released",
        action: INSIGHT_ACTION.AVOID,
        score: 0.7,
      },
      score: 0.7,
      user_relevance: 0.6,
    }),
  ];

  const result = resolveConflictingCandidates(candidates);

  assert.equal(result.survivors.length, 1);
  assert.equal(result.survivors[0].insight.action, INSIGHT_ACTION.BUY_NOW);
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.CONFLICT);
});

test("resolveConflictingCandidates does not treat Wait versus Avoid as a conflict", () => {
  const candidates = [
    reviewCandidate("game-quiet", 0.7, 0.5, 0, INSIGHT_ACTION.WAIT),
    releaseCandidate("game-quiet", 0.6, 0.5, 1_000, INSIGHT_ACTION.AVOID),
  ];

  const result = resolveConflictingCandidates(candidates);

  assert.equal(result.survivors.length, 2);
  assert.equal(result.discarded.length, 0);
});

test("filterCandidates applies duplicate resolution before conflict resolution", () => {
  const candidates = [
    priceCandidate("game-order", 0.9, 0.6, 0),
    priceCandidate("game-order", 0.5, 0.6, 2 * HOUR_MS),
    reviewCandidate("game-order", 0.8, 0.6, 4 * HOUR_MS),
  ];

  const result = filterCandidates(candidates);

  assert.equal(result.survivors.length, 1);
  assert.equal(result.survivors[0].insight.action, INSIGHT_ACTION.BUY_NOW);
  assert.equal(result.discarded.length, 2);
  assert.equal(result.discarded[0].discardReason, DISCARD_REASON.DUPLICATE);
  assert.equal(result.discarded[1].discardReason, DISCARD_REASON.CONFLICT);
});
