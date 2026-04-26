import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

function resolveBackendBaseUrl() {
  const value =
    process.env.PULSE_BACKEND_BASE_URL ??
    process.env.NEXT_PUBLIC_API_BASE_URL ??
    "http://localhost:8000";
  return value.replace(/\/$/, "");
}

function buildTargetUrl(request: NextRequest, pathSegments: string[]) {
  const incomingUrl = new URL(request.url);
  const target = new URL(
    `${resolveBackendBaseUrl()}/api/${pathSegments.join("/")}`,
  );

  if (incomingUrl.search) {
    target.search = incomingUrl.search;
  }

  return target;
}

function buildForwardHeaders(request: NextRequest) {
  const headers = new Headers();
  const contentType = request.headers.get("content-type");
  const accept = request.headers.get("accept");

  if (contentType) {
    headers.set("content-type", contentType);
  }
  if (accept) {
    headers.set("accept", accept);
  }

  return headers;
}

async function forward(request: NextRequest, pathSegments: string[]) {
  const targetUrl = buildTargetUrl(request, pathSegments);
  const method = request.method.toUpperCase();
  const hasBody = !["GET", "HEAD"].includes(method);
  const upstream = await fetch(targetUrl, {
    method,
    headers: buildForwardHeaders(request),
    body: hasBody ? await request.text() : undefined,
    cache: "no-store",
  });

  const contentType = upstream.headers.get("content-type") ?? "application/json";
  const body = await upstream.text();

  return new Response(body, {
    status: upstream.status,
    headers: {
      "content-type": contentType,
    },
  });
}

async function handle(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  try {
    const { path } = await context.params;
    return await forward(request, path);
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Proxy request to backend failed.";
    return new Response(message, {
      status: 502,
      headers: {
        "content-type": "text/plain; charset=utf-8",
      },
    });
  }
}

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  return handle(request, context);
}

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  return handle(request, context);
}
