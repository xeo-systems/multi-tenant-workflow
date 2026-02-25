"use client";

export type ApiError = {
  code: string;
  message: string;
  requestId?: string;
  status: number;
};

const baseUrl = process.env["NEXT_PUBLIC_API_BASE_URL"] || "http://localhost:4000";

export async function apiFetch<T>(path: string, options: RequestInit = {}): Promise<T> {
  const orgId = typeof window !== "undefined" ? window.localStorage.getItem("orgId") : null;
  const headers = new Headers(options.headers || {});
  if (!headers.has("Content-Type") && !(options.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }
  if (orgId && !headers.has("X-Org-Id")) {
    headers.set("X-Org-Id", orgId);
  }

  const res = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers,
    credentials: "include",
  });

  if (!res.ok) {
    const data = (await res.json().catch(() => null)) as { error?: { code?: string; message?: string; requestId?: string } } | null;
    const err: ApiError = {
      code: data?.error?.code || "UNKNOWN",
      message: data?.error?.message || "Request failed",
      requestId: data?.error?.requestId,
      status: res.status,
    };
    if (typeof window !== "undefined" && err.status === 401) {
      const path = window.location.pathname;
      if (!path.startsWith("/login") && !path.startsWith("/register")) {
        window.location.href = "/login";
      }
    }
    throw err;
  }

  return (await res.json()) as T;
}
