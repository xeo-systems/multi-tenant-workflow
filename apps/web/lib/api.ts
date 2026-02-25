"use client";

export type ApiError = {
  code: string;
  message: string;
  field?: string;
  requestId?: string;
  status: number;
};

export type ApiErrorKind = "unauthorized" | "forbidden" | "network" | "unknown";

const baseUrl = process.env["NEXT_PUBLIC_API_BASE_URL"] || "http://localhost:4000";
export const ORG_ID_STORAGE_KEY = "orgId";
export const DASHBOARD_CHECKLIST_DISMISSED_KEY = "dashboardChecklistDismissed";
export const ORG_CONTEXT_UPDATED_EVENT = "org-context-updated";

export type ApiFetchOptions = RequestInit & {
  skipOrgHeader?: boolean;
};

export function getStoredOrgId(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  return window.localStorage.getItem(ORG_ID_STORAGE_KEY);
}

export function setStoredOrgId(orgId: string) {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(ORG_ID_STORAGE_KEY, orgId);
  window.dispatchEvent(new Event(ORG_CONTEXT_UPDATED_EVENT));
}

export function clearStoredOrgId() {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.removeItem(ORG_ID_STORAGE_KEY);
  window.dispatchEvent(new Event(ORG_CONTEXT_UPDATED_EVENT));
}

export function isApiError(error: unknown): error is ApiError {
  return Boolean(
    error &&
      typeof error === "object" &&
      "status" in error &&
      "message" in error &&
      typeof (error as { status: unknown }).status === "number"
  );
}

export function isForbiddenError(error: unknown) {
  return isApiError(error) && error.status === 403;
}

export function isNetworkError(error: unknown) {
  return isApiError(error) && error.status === 0;
}

export function toApiError(error: unknown, fallbackMessage = "Request failed"): ApiError {
  if (isApiError(error)) {
    return error;
  }
  if (error instanceof Error) {
    return { code: "UNKNOWN", message: error.message || fallbackMessage, status: -1 };
  }
  return { code: "UNKNOWN", message: fallbackMessage, status: -1 };
}

export function getApiErrorKind(error: unknown): ApiErrorKind {
  const apiErr = toApiError(error);
  if (apiErr.status === 401) {
    return "unauthorized";
  }
  if (apiErr.status === 403) {
    return "forbidden";
  }
  if (apiErr.status === 0) {
    return "network";
  }
  return "unknown";
}

export async function apiFetch<T>(path: string, options: ApiFetchOptions = {}): Promise<T> {
  const { skipOrgHeader = false, ...requestOptions } = options;
  const orgId = getStoredOrgId();
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
    const data = (await res.json().catch(() => null)) as { error?: { code?: string; message?: string; field?: string; requestId?: string } } | null;
    const err: ApiError = {
      code: data?.error?.code || "UNKNOWN",
      message: data?.error?.message || "Request failed",
      field: data?.error?.field,
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
