import {
  DEBUG_PRIORITY,
  DISCARD_REASON,
  DUPLICATE_WINDOW_HOURS,
  INSIGHT_ACTION,
  TRIGGER_TYPE_PRIORITY_RANK,
} from "./constants";
import type {
  ConflictResolutionResult,
  DebugRecord,
  DuplicateResolutionResult,
  FilteringResult,
  TransformedCandidate,
} from "./types";

export type { ConflictResolutionResult, DuplicateResolutionResult, FilteringResult } from "./types";

const DUPLICATE_WINDOW_MS = DUPLICATE_WINDOW_HOURS * 60 * 60 * 1000;

function buildDiscardRecord(candidate: TransformedCandidate, discardReason: DebugRecord["discardReason"]): DebugRecord {
  return {
    gameId: candidate.trigger.gameId,
    type: candidate.trigger.type,
    magnitude: candidate.magnitude,
    recency: candidate.recency,
    user_relevance: candidate.user_relevance,
    historical_context: candidate.historical_context,
    score: candidate.score,
    priority: DEBUG_PRIORITY.DISCARDED,
    discardReason,
  };
}

function sortByComparator(a: TransformedCandidate, b: TransformedCandidate): number {
  return compareCandidates(a, b);
}

function sortByTimestampThenComparator(a: TransformedCandidate, b: TransformedCandidate): number {
  if (a.trigger.timestamp !== b.trigger.timestamp) {
    return a.trigger.timestamp - b.trigger.timestamp;
  }

  return compareCandidates(a, b);
}

function isOpposingActions(left: string, right: string): boolean {
  return (
    (left === INSIGHT_ACTION.BUY_NOW && (right === INSIGHT_ACTION.WAIT || right === INSIGHT_ACTION.AVOID)) ||
    (right === INSIGHT_ACTION.BUY_NOW && (left === INSIGHT_ACTION.WAIT || left === INSIGHT_ACTION.AVOID))
  );
}

function conflictsWithKeptCandidate(candidate: TransformedCandidate, kept: TransformedCandidate[]): boolean {
  return kept.some((other) => {
    return (
      other.trigger.gameId === candidate.trigger.gameId &&
      other.trigger.type !== candidate.trigger.type &&
      isOpposingActions(candidate.insight.action, other.insight.action)
    );
  });
}

function groupCandidatesByGameId(candidates: TransformedCandidate[]): Map<string, TransformedCandidate[]> {
  const grouped = new Map<string, TransformedCandidate[]>();

  for (const candidate of candidates) {
    const bucket = grouped.get(candidate.trigger.gameId);
    if (bucket) {
      bucket.push(candidate);
      continue;
    }
    grouped.set(candidate.trigger.gameId, [candidate]);
  }

  return grouped;
}

function groupCandidatesByGameAndType(candidates: TransformedCandidate[]): Map<string, Map<string, TransformedCandidate[]>> {
  const grouped = new Map<string, Map<string, TransformedCandidate[]>>();

  for (const candidate of candidates) {
    const gameBucket = grouped.get(candidate.trigger.gameId) ?? new Map<string, TransformedCandidate[]>();
    const typeBucket = gameBucket.get(candidate.trigger.type) ?? [];
    typeBucket.push(candidate);
    gameBucket.set(candidate.trigger.type, typeBucket);
    grouped.set(candidate.trigger.gameId, gameBucket);
  }

  return grouped;
}

function finalizeDuplicateCluster(
  cluster: TransformedCandidate[],
  survivors: TransformedCandidate[],
  discarded: DebugRecord[],
): void {
  if (cluster.length === 0) {
    return;
  }

  let winner = cluster[0];
  for (let index = 1; index < cluster.length; index += 1) {
    const candidate = cluster[index];
    if (compareCandidates(candidate, winner) < 0) {
      winner = candidate;
    }
  }

  survivors.push(winner);

  for (const candidate of cluster) {
    if (candidate !== winner) {
      discarded.push(buildDiscardRecord(candidate, DISCARD_REASON.DUPLICATE));
    }
  }
}

export function compareCandidates(a: TransformedCandidate, b: TransformedCandidate): number {
  if (a.score !== b.score) {
    return b.score - a.score;
  }
  if (a.user_relevance !== b.user_relevance) {
    return b.user_relevance - a.user_relevance;
  }
  if (a.trigger.timestamp !== b.trigger.timestamp) {
    return b.trigger.timestamp - a.trigger.timestamp;
  }

  return TRIGGER_TYPE_PRIORITY_RANK[a.trigger.type] - TRIGGER_TYPE_PRIORITY_RANK[b.trigger.type];
}

export function resolveDuplicateCandidates(candidates: TransformedCandidate[]): DuplicateResolutionResult {
  const survivors: TransformedCandidate[] = [];
  const discarded: DebugRecord[] = [];
  const grouped = groupCandidatesByGameAndType(candidates);
  const sortedGameIds = [...grouped.keys()].sort();

  for (const gameId of sortedGameIds) {
    const typeGroups = grouped.get(gameId);
    if (!typeGroups) {
      continue;
    }

    const sortedTypes = [...typeGroups.keys()].sort(
      (left, right) => TRIGGER_TYPE_PRIORITY_RANK[left as keyof typeof TRIGGER_TYPE_PRIORITY_RANK] - TRIGGER_TYPE_PRIORITY_RANK[right as keyof typeof TRIGGER_TYPE_PRIORITY_RANK],
    );

    for (const type of sortedTypes) {
      const typeCandidates = [...(typeGroups.get(type) ?? [])].sort(sortByTimestampThenComparator);

      let cluster: TransformedCandidate[] = [];
      let clusterStartTimestamp: number | null = null;

      for (const candidate of typeCandidates) {
        if (cluster.length === 0) {
          cluster = [candidate];
          clusterStartTimestamp = candidate.trigger.timestamp;
          continue;
        }

        if (
          clusterStartTimestamp !== null &&
          Math.abs(candidate.trigger.timestamp - clusterStartTimestamp) < DUPLICATE_WINDOW_MS
        ) {
          cluster.push(candidate);
          continue;
        }

        finalizeDuplicateCluster(cluster, survivors, discarded);
        cluster = [candidate];
        clusterStartTimestamp = candidate.trigger.timestamp;
      }

      finalizeDuplicateCluster(cluster, survivors, discarded);
    }
  }

  survivors.sort(sortByComparator);
  return {
    survivors,
    discarded,
  };
}

export function resolveConflictingCandidates(candidates: TransformedCandidate[]): ConflictResolutionResult {
  const survivors: TransformedCandidate[] = [];
  const discarded: DebugRecord[] = [];
  const grouped = groupCandidatesByGameId(candidates);
  const sortedGameIds = [...grouped.keys()].sort();

  for (const gameId of sortedGameIds) {
    const gameCandidates = [...(grouped.get(gameId) ?? [])].sort(sortByComparator);
    const keptForGame: TransformedCandidate[] = [];

    for (const candidate of gameCandidates) {
      if (conflictsWithKeptCandidate(candidate, keptForGame)) {
        discarded.push(buildDiscardRecord(candidate, DISCARD_REASON.CONFLICT));
        continue;
      }

      keptForGame.push(candidate);
    }

    survivors.push(...keptForGame);
  }

  survivors.sort(sortByComparator);
  return {
    survivors,
    discarded,
  };
}

export function filterCandidates(candidates: TransformedCandidate[]): FilteringResult {
  const duplicateResult = resolveDuplicateCandidates(candidates);
  const conflictResult = resolveConflictingCandidates(duplicateResult.survivors);

  return {
    survivors: conflictResult.survivors,
    discarded: [...duplicateResult.discarded, ...conflictResult.discarded],
  };
}
