import { gatewayPathFromSegments, proxyGatewayRequest } from "@/lib/gateway/client";

type RouteContext = {
  params: Promise<{ path?: string[] }>;
};

async function handler(request: Request, context: RouteContext) {
  const { path = [] } = await context.params;
  const { search } = new URL(request.url);
  return proxyGatewayRequest(gatewayPathFromSegments(path, search), request);
}

export const GET = handler;
export const POST = handler;
export const DELETE = handler;
