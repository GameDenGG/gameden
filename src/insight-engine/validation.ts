import {
  DEBUG_PRIORITY,
  DISCARD_REASON,
  RELEASE_EVENT_VALUES,
  RELEVANCE_TRIGGER_CROSSING_THRESHOLD,
  REVIEW_TIERS,
  STATUS_CHANGE_CURRENT_VALUES,
  STATUS_CHANGE_PREVIOUS_VALUES,
  TRIGGER_TYPE,
  TRIGGER_TYPES,
} from "./constants";
import type {
  DebugRecord,
  PriceChangeTrigger,
  ReleaseEventTrigger,
  RelevanceIncreaseTrigger,
  ReviewChangeTrigger,
  StatusChangeTrigger,
  TriggerType,
  ValidatedTrigger,
} from "./types";

export type TriggerValidationResult =
  | {
      ok: true;
      trigger: ValidatedTrigger;
      debug: DebugRecord;
    }
  | {
      ok: false;
      debug: DebugRecord;
    };

type RawTriggerObject = Record<string, unknown>;

type BaseValidationSuccess<TType extends TriggerType = TriggerType> = {
  raw: RawTriggerObject;
  type: TType;
  gameId: string;
  timestamp: number;
};

type BaseValidationResult<TType extends TriggerType = TriggerType> =
  | {
      ok: true;
      value: BaseValidationSuccess<TType>;
    }
  | {
      ok: false;
      debug: DebugRecord;
    };

function isObjectRecord(input: unknown): input is RawTriggerObject {
  return typeof input === "object" && input !== null && !Array.isArray(input);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNumber(value: unknown): value is number {
  return typeof value === "number";
}

function isTriggerType(value: unknown): value is TriggerType {
  return isString(value) && TRIGGER_TYPES.includes(value as TriggerType);
}

function isReviewTier(value: unknown): value is ReviewChangeTrigger["current"] {
  return isString(value) && REVIEW_TIERS.includes(value as ReviewChangeTrigger["current"]);
}

function isReleaseEventValue(value: unknown): value is ReleaseEventTrigger["current"] {
  return isString(value) && RELEASE_EVENT_VALUES.includes(value as ReleaseEventTrigger["current"]);
}

function isStatusChangePreviousValue(value: unknown): value is StatusChangeTrigger["previous"] {
  return isString(value) && STATUS_CHANGE_PREVIOUS_VALUES.includes(value as StatusChangeTrigger["previous"]);
}

function isStatusChangeCurrentValue(value: unknown): value is StatusChangeTrigger["current"] {
  return isString(value) && STATUS_CHANGE_CURRENT_VALUES.includes(value as StatusChangeTrigger["current"]);
}

function extractDebugGameId(input: unknown): string {
  return isObjectRecord(input) && isString(input.gameId) ? input.gameId : "";
}

function extractDebugType(input: unknown): TriggerType | null {
  if (isObjectRecord(input) && isTriggerType(input.type)) {
    return input.type;
  }
  return null;
}

function buildDebugRecord(
  priority: DebugRecord["priority"],
  discardReason: DebugRecord["discardReason"],
  input: unknown,
  overrides?: Partial<Pick<DebugRecord, "gameId" | "type">>,
): DebugRecord {
  return {
    gameId: overrides?.gameId ?? extractDebugGameId(input),
    type: overrides?.type ?? extractDebugType(input),
    magnitude: null,
    recency: null,
    user_relevance: null,
    historical_context: null,
    score: null,
    priority,
    discardReason,
  };
}

function invalidResult(
  input: unknown,
  overrides?: Partial<Pick<DebugRecord, "gameId" | "type">>,
): TriggerValidationResult {
  return {
    ok: false,
    debug: buildDebugRecord(DEBUG_PRIORITY.DISCARDED, DISCARD_REASON.INVALID_TRIGGER, input, overrides),
  };
}

function validResult(trigger: ValidatedTrigger): TriggerValidationResult {
  return {
    ok: true,
    trigger,
    debug: buildDebugRecord(DEBUG_PRIORITY.LOW, null, trigger, {
      gameId: trigger.gameId,
      type: trigger.type,
    }),
  };
}

function validateBaseTrigger<TType extends TriggerType = TriggerType>(
  input: unknown,
  expectedType?: TType,
): BaseValidationResult<TType> {
  if (!isObjectRecord(input)) {
    return invalidResult(input);
  }

  if (!isTriggerType(input.type)) {
    return invalidResult(input, {
      gameId: extractDebugGameId(input),
    });
  }

  if (expectedType && input.type !== expectedType) {
    return invalidResult(input, {
      gameId: extractDebugGameId(input),
      type: expectedType,
    });
  }

  if (!isString(input.gameId)) {
    return invalidResult(input, {
      type: input.type,
    });
  }

  if (!isNumber(input.timestamp)) {
    return invalidResult(input, {
      gameId: input.gameId,
      type: input.type,
    });
  }

  return {
    ok: true,
    value: {
      raw: input,
      type: (expectedType ?? input.type) as TType,
      gameId: input.gameId,
      timestamp: input.timestamp,
    },
  };
}

function validatePriceChangeBase(
  base: BaseValidationSuccess<typeof TRIGGER_TYPE.PRICE_CHANGE>,
): TriggerValidationResult {
  const { raw, gameId, timestamp, type } = base;
  if (!isNumber(raw.previous) || !isNumber(raw.current)) {
    return invalidResult(raw, { gameId, type });
  }

  const trigger: PriceChangeTrigger = {
    type,
    gameId,
    timestamp,
    previous: raw.previous,
    current: raw.current,
  };
  return validResult(trigger);
}

function validateReviewChangeBase(
  base: BaseValidationSuccess<typeof TRIGGER_TYPE.REVIEW_CHANGE>,
): TriggerValidationResult {
  const { raw, gameId, timestamp, type } = base;
  if (!isReviewTier(raw.previous) || !isReviewTier(raw.current)) {
    return invalidResult(raw, { gameId, type });
  }

  const trigger: ReviewChangeTrigger = {
    type,
    gameId,
    timestamp,
    previous: raw.previous,
    current: raw.current,
  };
  return validResult(trigger);
}

function validateReleaseEventBase(
  base: BaseValidationSuccess<typeof TRIGGER_TYPE.RELEASE_EVENT>,
): TriggerValidationResult {
  const { raw, gameId, timestamp, type } = base;
  if (raw.previous !== null || !isReleaseEventValue(raw.current)) {
    return invalidResult(raw, { gameId, type });
  }

  const trigger: ReleaseEventTrigger = {
    type,
    gameId,
    timestamp,
    previous: null,
    current: raw.current,
  };
  return validResult(trigger);
}

function validateRelevanceIncreaseBase(
  base: BaseValidationSuccess<typeof TRIGGER_TYPE.RELEVANCE_INCREASE>,
): TriggerValidationResult {
  const { raw, gameId, timestamp, type } = base;
  if (!isNumber(raw.previous) || !isNumber(raw.current)) {
    return invalidResult(raw, { gameId, type });
  }

  const previous = raw.previous;
  const current = raw.current;
  const inRange = previous >= 0 && previous <= 1 && current >= 0 && current <= 1;
  const crossedThreshold =
    previous < RELEVANCE_TRIGGER_CROSSING_THRESHOLD &&
    current >= RELEVANCE_TRIGGER_CROSSING_THRESHOLD;

  if (!inRange || !crossedThreshold) {
    return invalidResult(raw, { gameId, type });
  }

  const trigger: RelevanceIncreaseTrigger = {
    type,
    gameId,
    timestamp,
    previous,
    current,
  };
  return validResult(trigger);
}

function validateStatusChangeBase(
  base: BaseValidationSuccess<typeof TRIGGER_TYPE.STATUS_CHANGE>,
): TriggerValidationResult {
  const { raw, gameId, timestamp, type } = base;
  if (!isStatusChangePreviousValue(raw.previous) || !isStatusChangeCurrentValue(raw.current)) {
    return invalidResult(raw, { gameId, type });
  }

  const trigger: StatusChangeTrigger = {
    type,
    gameId,
    timestamp,
    previous: raw.previous,
    current: raw.current,
  };
  return validResult(trigger);
}

export function validatePriceChangeTrigger(input: unknown): TriggerValidationResult {
  const base = validateBaseTrigger(input, TRIGGER_TYPE.PRICE_CHANGE);
  return base.ok ? validatePriceChangeBase(base.value) : base;
}

export function validateReviewChangeTrigger(input: unknown): TriggerValidationResult {
  const base = validateBaseTrigger(input, TRIGGER_TYPE.REVIEW_CHANGE);
  return base.ok ? validateReviewChangeBase(base.value) : base;
}

export function validateReleaseEventTrigger(input: unknown): TriggerValidationResult {
  const base = validateBaseTrigger(input, TRIGGER_TYPE.RELEASE_EVENT);
  return base.ok ? validateReleaseEventBase(base.value) : base;
}

export function validateRelevanceIncreaseTrigger(input: unknown): TriggerValidationResult {
  const base = validateBaseTrigger(input, TRIGGER_TYPE.RELEVANCE_INCREASE);
  return base.ok ? validateRelevanceIncreaseBase(base.value) : base;
}

export function validateStatusChangeTrigger(input: unknown): TriggerValidationResult {
  const base = validateBaseTrigger(input, TRIGGER_TYPE.STATUS_CHANGE);
  return base.ok ? validateStatusChangeBase(base.value) : base;
}

export function validateTrigger(input: unknown): TriggerValidationResult {
  const base = validateBaseTrigger(input);
  if (!base.ok) {
    return base;
  }

  switch (base.value.type) {
    case TRIGGER_TYPE.PRICE_CHANGE:
      return validatePriceChangeBase(base.value as BaseValidationSuccess<typeof TRIGGER_TYPE.PRICE_CHANGE>);
    case TRIGGER_TYPE.REVIEW_CHANGE:
      return validateReviewChangeBase(base.value as BaseValidationSuccess<typeof TRIGGER_TYPE.REVIEW_CHANGE>);
    case TRIGGER_TYPE.RELEASE_EVENT:
      return validateReleaseEventBase(base.value as BaseValidationSuccess<typeof TRIGGER_TYPE.RELEASE_EVENT>);
    case TRIGGER_TYPE.RELEVANCE_INCREASE:
      return validateRelevanceIncreaseBase(
        base.value as BaseValidationSuccess<typeof TRIGGER_TYPE.RELEVANCE_INCREASE>,
      );
    case TRIGGER_TYPE.STATUS_CHANGE:
      return validateStatusChangeBase(base.value as BaseValidationSuccess<typeof TRIGGER_TYPE.STATUS_CHANGE>);
  }

  return invalidResult(base.value.raw, {
    gameId: base.value.gameId,
    type: base.value.type,
  });
}
