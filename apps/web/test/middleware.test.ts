import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";
import { middleware } from "@/middleware";

describe("app route middleware", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("redirects unauthenticated app requests to login", async () => {
    const request = new NextRequest("http://localhost:4000/app");
    const response = await middleware(request);

    expect(response.status).toBe(307);
    expect(response.headers.get("location")).toBe("http://localhost:4000/login");
  });

  it("allows authenticated app requests with valid session", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ user: { id: "u1" } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      })
    );
    const request = new NextRequest("http://localhost:4000/app");
    request.cookies.set("sid", "session-token");
    const response = await middleware(request);

    expect(response.status).toBe(200);
    expect(response.headers.get("x-middleware-next")).toBe("1");
  });

  it("redirects when session cookie exists but auth check fails", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unauthorized", { status: 401 }));
    const request = new NextRequest("http://localhost:4000/app");
    request.cookies.set("sid", "stale-session");
    const response = await middleware(request);

    expect(response.status).toBe(307);
    expect(response.headers.get("location")).toBe("http://localhost:4000/login");
  });

  it("allows cross-origin api deployments without requiring sid cookie on web origin", async () => {
    const request = new NextRequest("http://localhost:3000/app");
    const response = await middleware(request);

    expect(response.status).toBe(200);
    expect(response.headers.get("x-middleware-next")).toBe("1");
  });
});
