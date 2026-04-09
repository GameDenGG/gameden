export const TRIGGER_TYPE = {
  PRICE_CHANGE: "price_change",
  REVIEW_CHANGE: "review_change",
  RELEASE_EVENT: "release_event",
  RELEVANCE_INCREASE: "relevance_increase",
  STATUS_CHANGE: "status_change",
} as const;

export const TRIGGER_TYPES = [
  TRIGGER_TYPE.PRICE_CHANGE,
  TRIGGER_TYPE.REVIEW_CHANGE,
  TRIGGER_TYPE.RELEASE_EVENT,
  TRIGGER_TYPE.RELEVANCE_INCREASE,
  TRIGGER_TYPE.STATUS_CHANGE,
] as const;

export type TriggerType = (typeof TRIGGER_TYPES)[number];

// Ordered ladder used for deterministic review-tier comparisons.
export const REVIEW_TIERS = [
  "Overwhelmingly Negative",
  "Mostly Negative",
  "Mixed",
  "Mostly Positive",
  "Very Positive",
  "Overwhelmingly Positive",
] as const;

export type ReviewTier = (typeof REVIEW_TIERS)[number];

export const RELEASE_EVENT_VALUE = {
  RELEASED: "released",
  EARLY_ACCESS: "early_access",
  MAJOR_PATCH: "major_patch",
} as const;

export const RELEASE_EVENT_VALUES = [
  RELEASE_EVENT_VALUE.RELEASED,
  RELEASE_EVENT_VALUE.EARLY_ACCESS,
  RELEASE_EVENT_VALUE.MAJOR_PATCH,
] as const;

export type ReleaseEventValue = (typeof RELEASE_EVENT_VALUES)[number];

export const STATUS_CHANGE_VALUE = {
  NONE: "none",
  STABLE: "stable",
  DELISTING: "delisting",
  LEAVING_SUBSCRIPTION: "leaving_subscription",
  PRICE_LOCK_ENDING: "price_lock_ending",
} as const;

export const STATUS_CHANGE_VALUES = [
  STATUS_CHANGE_VALUE.NONE,
  STATUS_CHANGE_VALUE.STABLE,
  STATUS_CHANGE_VALUE.DELISTING,
  STATUS_CHANGE_VALUE.LEAVING_SUBSCRIPTION,
  STATUS_CHANGE_VALUE.PRICE_LOCK_ENDING,
] as const;

export const STATUS_CHANGE_PREVIOUS_VALUES = [
  STATUS_CHANGE_VALUE.NONE,
  STATUS_CHANGE_VALUE.STABLE,
] as const;

export const STATUS_CHANGE_CURRENT_VALUES = [
  STATUS_CHANGE_VALUE.DELISTING,
  STATUS_CHANGE_VALUE.LEAVING_SUBSCRIPTION,
  STATUS_CHANGE_VALUE.PRICE_LOCK_ENDING,
] as const;

export type StatusChangeValue = (typeof STATUS_CHANGE_VALUES)[number];
export type StatusChangePreviousValue = (typeof STATUS_CHANGE_PREVIOUS_VALUES)[number];
export type StatusChangeCurrentValue = (typeof STATUS_CHANGE_CURRENT_VALUES)[number];

export const DEBUG_PRIORITY = {
  DISCARDED: "discarded",
  LOW: "low",
  MEDIUM: "medium",
  HIGH: "high",
} as const;

export const DEBUG_PRIORITIES = [
  DEBUG_PRIORITY.DISCARDED,
  DEBUG_PRIORITY.LOW,
  DEBUG_PRIORITY.MEDIUM,
  DEBUG_PRIORITY.HIGH,
] as const;

export type DebugPriority = (typeof DEBUG_PRIORITIES)[number];

export const DISCARD_REASON = {
  INVALID_TRIGGER: "invalid_trigger",
  DISMISSED_BY_USER: "dismissed_by_user",
  SCORE_BELOW_THRESHOLD: "score_below_threshold",
  DUPLICATE: "duplicate",
  CONFLICT: "conflict",
  OVERFLOW: "overflow",
} as const;

export const DISCARD_REASONS = [
  DISCARD_REASON.INVALID_TRIGGER,
  DISCARD_REASON.DISMISSED_BY_USER,
  DISCARD_REASON.SCORE_BELOW_THRESHOLD,
  DISCARD_REASON.DUPLICATE,
  DISCARD_REASON.CONFLICT,
  DISCARD_REASON.OVERFLOW,
] as const;

export type DiscardReason = (typeof DISCARD_REASONS)[number];
export type NullableDiscardReason = DiscardReason | null;

export const INSIGHT_ACTION = {
  BUY_NOW: "Buy now",
  WAIT: "Wait",
  AVOID: "Avoid",
} as const;

export const INSIGHT_ACTIONS = [
  INSIGHT_ACTION.BUY_NOW,
  INSIGHT_ACTION.WAIT,
  INSIGHT_ACTION.AVOID,
] as const;

export type InsightAction = (typeof INSIGHT_ACTIONS)[number];

export const SUPPORTING_MIN_SCORE = 0.4;
export const PRIMARY_MIN_SCORE = 0.7;
export const MAX_SUPPORTING_SIGNALS = 5;
export const MAX_PRIMARY_INSIGHTS = 1;
export const DUPLICATE_WINDOW_HOURS = 24;
export const RELEVANCE_TRIGGER_CROSSING_THRESHOLD = 0.6;

// Lower rank means higher priority in deterministic tie-breaks.
export const TRIGGER_TYPE_PRIORITY_ORDER = [
  TRIGGER_TYPE.PRICE_CHANGE,
  TRIGGER_TYPE.STATUS_CHANGE,
  TRIGGER_TYPE.REVIEW_CHANGE,
  TRIGGER_TYPE.RELEASE_EVENT,
  TRIGGER_TYPE.RELEVANCE_INCREASE,
] as const;

export const TRIGGER_TYPE_PRIORITY_RANK: Readonly<Record<TriggerType, number>> = Object.freeze(
  TRIGGER_TYPE_PRIORITY_ORDER.reduce((rankMap, triggerType, index) => {
    rankMap[triggerType] = index;
    return rankMap;
  }, {} as Record<TriggerType, number>),
);
