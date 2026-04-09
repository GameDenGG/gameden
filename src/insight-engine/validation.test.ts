import assert from "node:assert/strict";
import test from "node:test";

import {
  DEBUG_PRIORITY,
  DISCARD_REASON,
  TRIGGER_TYPE,
} from "./constants";
import {
  validatePriceChangeTrigger,
  validateReleaseEventTrigger,
  validateRelevanceIncreaseTrigger,
  validateReviewChangeTrigger,
  validateStatusChangeTrigger,
  validateTrigger,
} from "./validation";

function expectNullScoreFields(debug: {
  magnitude: number | null;
  recency: number | null;
  user_relevance: number | null;
  historical_context: number | null;
  score: number | null;
}) {
  assert.equal(debug.magnitude, null);
  assert.equal(debug.recency, null);
  assert.equal(debug.user_relevance, null);
  assert.equal(debug.historical_context, null);
  assert.equal(debug.score, null);
}

test("validateTrigger accepts a valid price_change trigger", () => {
  const result = validateTrigger({
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: "game-1",
    timestamp: 1710000000,
    previous: 59.99,
    current: 29.99,
  });

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.deepEqual(result.trigger, {
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: "game-1",
    timestamp: 1710000000,
    previous: 59.99,
    current: 29.99,
  });
  assert.equal(result.debug.priority, DEBUG_PRIORITY.LOW);
  assert.equal(result.debug.discardReason, null);
  expectNullScoreFields(result.debug);
});

test("validateReviewChangeTrigger accepts canonical review tiers", () => {
  const result = validateReviewChangeTrigger({
    type: TRIGGER_TYPE.REVIEW_CHANGE,
    gameId: "game-2",
    timestamp: 1710000001,
    previous: "Mostly Positive",
    current: "Very Positive",
  });

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.equal(result.trigger.type, TRIGGER_TYPE.REVIEW_CHANGE);
  assert.equal(result.debug.discardReason, null);
  expectNullScoreFields(result.debug);
});

test("validateReleaseEventTrigger accepts valid upstream release events", () => {
  const result = validateReleaseEventTrigger({
    type: TRIGGER_TYPE.RELEASE_EVENT,
    gameId: "game-3",
    timestamp: 1710000002,
    previous: null,
    current: "major_patch",
  });

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.equal(result.trigger.type, TRIGGER_TYPE.RELEASE_EVENT);
  assert.equal(result.debug.discardReason, null);
  expectNullScoreFields(result.debug);
});

test("validateRelevanceIncreaseTrigger accepts only threshold crossings", () => {
  const result = validateRelevanceIncreaseTrigger({
    type: TRIGGER_TYPE.RELEVANCE_INCREASE,
    gameId: "game-4",
    timestamp: 1710000003,
    previous: 0.42,
    current: 0.61,
  });

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.equal(result.trigger.type, TRIGGER_TYPE.RELEVANCE_INCREASE);
  assert.equal(result.debug.discardReason, null);
  expectNullScoreFields(result.debug);
});

test("validateStatusChangeTrigger accepts valid status transitions", () => {
  const result = validateStatusChangeTrigger({
    type: TRIGGER_TYPE.STATUS_CHANGE,
    gameId: "game-5",
    timestamp: 1710000004,
    previous: "stable",
    current: "delisting",
  });

  assert.equal(result.ok, true);
  if (!result.ok) {
    return;
  }

  assert.equal(result.trigger.type, TRIGGER_TYPE.STATUS_CHANGE);
  assert.equal(result.debug.discardReason, null);
  expectNullScoreFields(result.debug);
});

test("validateTrigger rejects non-object input", () => {
  const result = validateTrigger(null);

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "");
  assert.equal(result.debug.type, null);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateTrigger rejects a missing type field", () => {
  const result = validateTrigger({
    gameId: "game-6",
    timestamp: 1710000005,
    previous: 1,
    current: 2,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "game-6");
  assert.equal(result.debug.type, null);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateTrigger rejects an invalid type value", () => {
  const result = validateTrigger({
    type: "price-drop",
    gameId: "game-7",
    timestamp: 1710000006,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "game-7");
  assert.equal(result.debug.type, null);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateTrigger rejects missing gameId", () => {
  const result = validateTrigger({
    type: TRIGGER_TYPE.PRICE_CHANGE,
    timestamp: 1710000007,
    previous: 10,
    current: 8,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "");
  assert.equal(result.debug.type, TRIGGER_TYPE.PRICE_CHANGE);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateTrigger rejects missing timestamp", () => {
  const result = validateTrigger({
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: "game-8",
    previous: 10,
    current: 8,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "game-8");
  assert.equal(result.debug.type, TRIGGER_TYPE.PRICE_CHANGE);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateTrigger rejects wrong primitive types for common fields", () => {
  const result = validateTrigger({
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: 123,
    timestamp: "1710000008",
    previous: 10,
    current: 8,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "");
  assert.equal(result.debug.type, TRIGGER_TYPE.PRICE_CHANGE);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validatePriceChangeTrigger rejects missing numeric fields", () => {
  const result = validatePriceChangeTrigger({
    type: TRIGGER_TYPE.PRICE_CHANGE,
    gameId: "game-9",
    timestamp: 1710000009,
    current: 12,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.gameId, "game-9");
  assert.equal(result.debug.type, TRIGGER_TYPE.PRICE_CHANGE);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateReviewChangeTrigger rejects invalid review tiers", () => {
  const result = validateReviewChangeTrigger({
    type: TRIGGER_TYPE.REVIEW_CHANGE,
    gameId: "game-10",
    timestamp: 1710000010,
    previous: "Mixed",
    current: "Very Negative",
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.type, TRIGGER_TYPE.REVIEW_CHANGE);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateReleaseEventTrigger rejects non-null previous values", () => {
  const result = validateReleaseEventTrigger({
    type: TRIGGER_TYPE.RELEASE_EVENT,
    gameId: "game-11",
    timestamp: 1710000011,
    previous: "released",
    current: "released",
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.type, TRIGGER_TYPE.RELEASE_EVENT);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateReleaseEventTrigger rejects invalid current values", () => {
  const result = validateReleaseEventTrigger({
    type: TRIGGER_TYPE.RELEASE_EVENT,
    gameId: "game-12",
    timestamp: 1710000012,
    previous: null,
    current: "hotfix",
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.type, TRIGGER_TYPE.RELEASE_EVENT);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateRelevanceIncreaseTrigger rejects out-of-range values", () => {
  const result = validateRelevanceIncreaseTrigger({
    type: TRIGGER_TYPE.RELEVANCE_INCREASE,
    gameId: "game-13",
    timestamp: 1710000013,
    previous: -0.01,
    current: 0.8,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.type, TRIGGER_TYPE.RELEVANCE_INCREASE);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateStatusChangeTrigger rejects invalid enum values", () => {
  const result = validateStatusChangeTrigger({
    type: TRIGGER_TYPE.STATUS_CHANGE,
    gameId: "game-14",
    timestamp: 1710000014,
    previous: "delisting",
    current: "stable",
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.type, TRIGGER_TYPE.STATUS_CHANGE);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateRelevanceIncreaseTrigger rejects cases where previous is already above threshold", () => {
  const result = validateRelevanceIncreaseTrigger({
    type: TRIGGER_TYPE.RELEVANCE_INCREASE,
    gameId: "game-15",
    timestamp: 1710000015,
    previous: 0.6,
    current: 0.9,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.type, TRIGGER_TYPE.RELEVANCE_INCREASE);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});

test("validateRelevanceIncreaseTrigger rejects cases where current does not cross threshold", () => {
  const result = validateRelevanceIncreaseTrigger({
    type: TRIGGER_TYPE.RELEVANCE_INCREASE,
    gameId: "game-16",
    timestamp: 1710000016,
    previous: 0.2,
    current: 0.59,
  });

  assert.equal(result.ok, false);
  if (result.ok) {
    return;
  }

  assert.equal(result.debug.type, TRIGGER_TYPE.RELEVANCE_INCREASE);
  assert.equal(result.debug.priority, DEBUG_PRIORITY.DISCARDED);
  assert.equal(result.debug.discardReason, DISCARD_REASON.INVALID_TRIGGER);
  expectNullScoreFields(result.debug);
});
