/**
 * Engine ("research state") service — powers the public STATE home. Mock-backed in v1; the real
 * branch hits GET /state (the live engine snapshot) when NEXT_PUBLIC_USE_MOCKS is off.
 */
import type { ActivityFeed, EngineState } from "@/lib/types/domain";
import { MOCK_ACTIVITY_FEED, MOCK_ENGINE_STATE } from "@/lib/mocks/engine";
import { USE_MOCKS, mock, request } from "./config";

export async function state(signal?: AbortSignal): Promise<EngineState> {
  if (USE_MOCKS) return mock(MOCK_ENGINE_STATE);
  return request<EngineState>("/public/state", { signal });
}

/** The live engine activity feed — real reverse-chron events + an honest idle/online signal. */
export async function activity(signal?: AbortSignal): Promise<ActivityFeed> {
  if (USE_MOCKS) return mock(MOCK_ACTIVITY_FEED);
  return request<ActivityFeed>("/public/activity", { signal });
}
