import {
  DEBUG_PRIORITY,
  DISCARD_REASON,
  MAX_SUPPORTING_SIGNALS,
} from "./constants";
import { compareCandidates } from "./filtering";
import type {
  DebugRecord,
  Insight,
  InsightEngineOutput,
  TransformedCandidate,
} from "./types";

export type FinalSelectionResult = {
  output: InsightEngineOutput;
  discarded: DebugRecord[];
};

function buildOverflowDiscardRecord(candidate: TransformedCandidate): DebugRecord {
  return {
    gameId: candidate.trigger.gameId,
    type: candidate.trigger.type,
    magnitude: candidate.magnitude,
    recency: candidate.recency,
    user_relevance: candidate.user_relevance,
    historical_context: candidate.historical_context,
    score: candidate.score,
    priority: DEBUG_PRIORITY.DISCARDED,
    discardReason: DISCARD_REASON.OVERFLOW,
  };
}

function isHighCandidate(candidate: TransformedCandidate): boolean {
  return candidate.priority === DEBUG_PRIORITY.HIGH;
}

function isMediumCandidate(candidate: TransformedCandidate): boolean {
  return candidate.priority === DEBUG_PRIORITY.MEDIUM;
}

export function selectFinalInsights(candidates: TransformedCandidate[]): FinalSelectionResult {
  const ordered = [...candidates].sort(compareCandidates);
  const supportingSignals: Insight[] = [];
  const discarded: DebugRecord[] = [];
  let primaryInsight: Insight | null = null;

  for (const candidate of ordered) {
    if (isHighCandidate(candidate)) {
      if (primaryInsight === null) {
        primaryInsight = candidate.insight;
      } else {
        discarded.push(buildOverflowDiscardRecord(candidate));
      }
      continue;
    }

    if (isMediumCandidate(candidate)) {
      if (supportingSignals.length < MAX_SUPPORTING_SIGNALS) {
        supportingSignals.push(candidate.insight);
      } else {
        discarded.push(buildOverflowDiscardRecord(candidate));
      }
    }
  }

  return {
    output: {
      primaryInsight,
      supportingSignals,
    },
    discarded,
  };
}
