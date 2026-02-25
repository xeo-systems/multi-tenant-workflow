"use client";

export type ApiError = {
  code: string;
  message: string;
  requestId?: string;
  status: number;
};

const baseUrl = process.env["NEXT_PUBLIC_API_BASE_URL"] || "http://localhost:4000";

export type ApiFetchOptions = RequestInit & {
  skipOrgHeader?: boolean;
};

export async function apiFetch<T>(path: string, options: ApiFetchOptions = {}): Promise<T> {
  const { skipOrgHeader = false, ...requestOptions } = options;
  const orgId = typeof window !== "undefined" ? window.localStorage.getItem("orgId") : null;
  const headers = new Headers(requestOptions.headers || {});
  if (!headers.has("Content-Type") && !(requestOptions.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }
  if (!skipOrgHeader && orgId && !headers.has("X-Org-Id")) {
    headers.set("X-Org-Id", orgId);
  }

  let res: Response;
  try {
    res = await fetch(`${baseUrl}${path}`, {
      ...requestOptions,
      headers,
      credentials: "include",
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Network request failed";
    const err: ApiError = {
      code: "NETWORK_ERROR",
      message: `${message}. Unable to reach API at ${baseUrl}.`,
      status: 0,
    };
    throw err;
  }

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
