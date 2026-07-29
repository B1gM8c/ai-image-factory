import "server-only";

type HealthResponse = { status: string };

export type ProviderProfileReadiness = {
  configured: number;
  active: number;
  draining: number;
  blocked: number;
};

type ReadinessResponse = {
  status: "ready" | "not_ready";
  provider_profiles: ProviderProfileReadiness | null;
};

export type GatewaySnapshot = {
  health: "ok" | "unreachable";
  readiness: "ready" | "not_ready" | "unreachable";
  providerProfiles: ProviderProfileReadiness | null;
  checkedAt: string;
};

const DEFAULT_GATEWAY_BASE_URL = "http://127.0.0.1:8787";

export async function getGatewaySnapshot(): Promise<GatewaySnapshot> {
  const [health, readiness] = await Promise.all([
    gatewayJson<HealthResponse>("/healthz"),
    gatewayJson<ReadinessResponse>("/readyz"),
  ]);

  return {
    health: health?.status === "ok" ? "ok" : "unreachable",
    readiness: readiness?.status ?? "unreachable",
    providerProfiles: readiness?.provider_profiles ?? null,
    checkedAt: new Date().toISOString(),
  };
}

async function gatewayJson<T>(path: string): Promise<T | null> {
  try {
    const response = await fetch(
      new URL(path, process.env.GATEWAY_BASE_URL || DEFAULT_GATEWAY_BASE_URL),
      { cache: "no-store", signal: AbortSignal.timeout(2500) },
    );
    if (!response.ok) return null;
    return (await response.json()) as T;
  } catch {
    return null;
  }
}
