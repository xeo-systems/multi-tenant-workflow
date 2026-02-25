import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import type { ReactNode } from "react";
import RegisterPage from "@/app/(auth)/register/page";
import LoginPage from "@/app/(auth)/login/page";
import DashboardPage from "@/app/(app)/app/page";
import MembersPage from "@/app/(app)/app/members/page";
import ApiKeysPage from "@/app/(app)/app/api-keys/page";
import UsagePage from "@/app/(app)/app/usage/page";
import BillingPage from "@/app/(app)/app/billing/page";
import SettingsPage from "@/app/(app)/app/settings/page";
import { AppShell } from "@/components/app-shell";
import { UserMenu } from "@/components/user-menu";
import { ToastProvider } from "@/lib/toast";

const pushMock = vi.fn();
const routerPushMock = vi.fn();
const routerReplaceMock = vi.fn();
const routerMock = { push: routerPushMock, replace: routerReplaceMock };
let mockPathname = "/app";
let apiKeysStore: Array<{
  id: string;
  name: string;
  prefix: string;
  createdAt: string;
  revokedAt: string | null;
  lastUsedAt: string | null;
}> = [];
let usageRequests = 120;
let flowEmail = "owner@example.com";
let flowPassword = "Password123!";
let latestSecret = "sk_test_secret_value";

const apiFetchMock = vi.fn(async (path: string, options?: RequestInit) => {
  if (path === "/auth/register" && options?.method === "POST") {
    const body = JSON.parse((options.body as string) || "{}") as { email: string; password: string };
    flowEmail = body.email;
    flowPassword = body.password;
    return { orgId: "org_test_123", userId: "usr_1" };
  }
  if (path === "/org") {
    return {
      org: { id: "org_test_123", name: "Test Org", plan: "free", planLimit: 1000 },
      role: "OWNER",
    };
  }
  if (path === "/org/members") {
    return [
      {
        id: "mem_1",
        role: "OWNER",
        createdAt: "2024-01-01T00:00:00.000Z",
        user: { id: "usr_1", email: "owner@example.com" },
      },
    ];
  }
  if (path === "/api-keys" && (!options?.method || options.method === "GET")) {
    return apiKeysStore;
  }
  if (path === "/api-keys" && options?.method === "POST") {
    const payload = JSON.parse((options.body as string) || "{}") as { name?: string };
    latestSecret = `sk_test_secret_value_${Date.now()}`;
    apiKeysStore = [
      {
        id: "key_2",
        name: payload.name || "Generated Key",
        prefix: "ak_654321",
        createdAt: "2024-01-02T00:00:00.000Z",
        revokedAt: null,
        lastUsedAt: null,
      },
      ...apiKeysStore,
    ];
    return { id: "key_2", secret: latestSecret };
  }
  if (path.startsWith("/api-keys/") && options?.method === "DELETE") {
    const id = path.split("/").at(-1);
    apiKeysStore = apiKeysStore.map((key) =>
      key.id === id ? { ...key, revokedAt: "2024-01-03T00:00:00.000Z" } : key
    );
    return { ok: true };
  }
  if (path === "/data" && options?.method === "GET") {
    const headers = new Headers(options.headers || {});
    const auth = headers.get("Authorization") || "";
    if (auth !== `Bearer ${latestSecret}`) {
      throw { status: 401, code: "UNAUTHORIZED", message: "Invalid API key" };
    }
    usageRequests += 1;
    return { orgId: "org_test_123", name: "Test Org" };
  }
  if (path === "/usage?days=30") {
    return [
      {
        id: "usage_1",
        orgId: "org_test_123",
        metric: "api_requests",
        date: "2024-01-01T00:00:00.000Z",
        quantity: usageRequests,
      },
    ];
  }
  if (path === "/billing/status") {
    return {
      billingConfigured: true,
      plan: "free",
      planLimit: 1000,
      subscription: {
        status: "active",
        currentPeriodStart: "2024-01-01T00:00:00.000Z",
        currentPeriodEnd: "2024-02-01T00:00:00.000Z",
      },
    };
  }
  if (path === "/auth/me") {
    return { user: { id: "usr_1", email: "owner@example.com" } };
  }
  if (path === "/auth/login") {
    const body = JSON.parse((options?.body as string) || "{}") as { email: string; password: string };
    if (body.email !== flowEmail || body.password !== flowPassword) {
      throw { status: 401, code: "UNAUTHORIZED", message: "Invalid credentials" };
    }
    return { userId: "usr_1", orgId: "org_test_123" };
  }
  return {};
});

vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return {
    ...actual,
    apiFetch: (...args: [string, RequestInit?]) => apiFetchMock(...args),
  };
});

vi.mock("@/lib/toast", async () => {
  const actual = await vi.importActual<typeof import("@/lib/toast")>("@/lib/toast");
  return {
    ...actual,
    useToast: () => ({ push: pushMock, remove: vi.fn() }),
  };
});

vi.mock("next/navigation", () => ({
  useRouter: () => routerMock,
  usePathname: () => mockPathname,
}));

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: ReactNode; href: string }) => <a href={href}>{children}</a>,
}));

beforeEach(() => {
  apiFetchMock.mockClear();
  pushMock.mockClear();
  routerPushMock.mockClear();
  routerReplaceMock.mockClear();
  mockPathname = "/app";
  window.localStorage.clear();
  usageRequests = 120;
  flowEmail = "owner@example.com";
  flowPassword = "Password123!";
  latestSecret = "sk_test_secret_value";
  apiKeysStore = [
    {
      id: "key_1",
      name: "Server Key",
      prefix: "ak_123456",
      createdAt: "2024-01-01T00:00:00.000Z",
      revokedAt: null,
      lastUsedAt: null,
    },
  ];

  if (!global.crypto) {
    // @ts-expect-error test polyfill
    global.crypto = {};
  }
  if (!global.crypto.randomUUID) {
    // @ts-expect-error test polyfill
    global.crypto.randomUUID = () => "test-uuid";
  }
});

describe("UI smoke", () => {
  it("renders login page", () => {
    render(
      <ToastProvider>
        <LoginPage />
      </ToastProvider>
    );
    expect(screen.getByRole("heading", { name: /sign in/i })).toBeInTheDocument();
  });

  it("covers core SaaS flow end-to-end fallback", async () => {
    usageRequests = 0;
    apiKeysStore = [];
    const email = "coreflow@example.com";
    const password = "Password123!";

    render(
      <ToastProvider>
        <RegisterPage />
      </ToastProvider>
    );
    fireEvent.change(screen.getByLabelText(/organization name/i), { target: { value: "Core Org" } });
    fireEvent.change(screen.getByLabelText(/^email$/i), { target: { value: email } });
    fireEvent.change(screen.getByLabelText(/password/i), { target: { value: password } });
    fireEvent.click(screen.getByRole("button", { name: /create account/i }));
    await waitFor(() => {
      expect(window.localStorage.getItem("orgId")).toBe("org_test_123");
    });

    render(
      <ToastProvider>
        <LoginPage />
      </ToastProvider>
    );
    fireEvent.change(screen.getAllByLabelText(/^email$/i).at(-1) as HTMLElement, { target: { value: email } });
    fireEvent.change(screen.getAllByLabelText(/password/i).at(-1) as HTMLElement, { target: { value: password } });
    fireEvent.click(screen.getAllByRole("button", { name: /sign in/i }).at(-1) as HTMLElement);
    await waitFor(() => {
      expect(window.localStorage.getItem("orgId")).toBe("org_test_123");
    });

    render(
      <ToastProvider>
        <DashboardPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /welcome back/i })).toBeInTheDocument();
    expect(screen.queryByText(/page not found/i)).not.toBeInTheDocument();

    render(
      <ToastProvider>
        <ApiKeysPage />
      </ToastProvider>
    );
    fireEvent.click(await screen.findByRole("button", { name: /new api key/i }));
    fireEvent.change(screen.getByLabelText(/key name/i), { target: { value: "CoreFlowKey" } });
    fireEvent.click(screen.getByRole("button", { name: /^create$/i }));
    expect(await screen.findByText(/secret shown once/i)).toBeInTheDocument();
    expect(await screen.findByText(latestSecret)).toBeInTheDocument();

    await apiFetchMock("/data", {
      method: "GET",
      headers: {
        "X-Org-Id": "org_test_123",
        Authorization: `Bearer ${latestSecret}`,
      },
    });

    render(
      <ToastProvider>
        <UsagePage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /usage/i })).toBeInTheDocument();
    expect(await screen.findByText(/activation success/i)).toBeInTheDocument();
    expect(usageRequests).toBe(1);
    expect(screen.queryByText(/page not found/i)).not.toBeInTheDocument();

    render(
      <ToastProvider>
        <MembersPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: "Members", level: 1 })).toBeInTheDocument();
    expect(screen.queryByText(/page not found/i)).not.toBeInTheDocument();

    render(
      <ToastProvider>
        <SettingsPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /settings/i })).toBeInTheDocument();
    expect(screen.queryByText(/page not found/i)).not.toBeInTheDocument();

    render(
      <ToastProvider>
        <BillingPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /billing/i })).toBeInTheDocument();
    expect(screen.queryByText(/page not found/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/unable to load/i)).not.toBeInTheDocument();
  });

  it("renders dashboard shell", async () => {
    render(
      <ToastProvider>
        <AppShell>
          <div>Dashboard</div>
        </AppShell>
      </ToastProvider>
    );
    expect(await screen.findByText("Test Org")).toBeInTheDocument();
    const main = screen.getByRole("main");
    expect(within(main).getByText("Dashboard")).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: /primary/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Dashboard" })).toHaveAttribute("href", "/app");
    expect(screen.getByRole("link", { name: "Members" })).toHaveAttribute("href", "/app/members");
    expect(screen.getByRole("link", { name: "API Keys" })).toHaveAttribute("href", "/app/api-keys");
    expect(screen.getByRole("link", { name: "Usage" })).toHaveAttribute("href", "/app/usage");
    expect(screen.getByRole("link", { name: "Billing" })).toHaveAttribute("href", "/app/billing");
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("href", "/app/settings");
  });

  it("renders /app dashboard page", async () => {
    render(
      <ToastProvider>
        <DashboardPage />
      </ToastProvider>
    );
    expect(await screen.findByText(/organization overview and plan status/i)).toBeInTheDocument();
    const checklist = screen.queryByRole("heading", { name: /activation checklist/i });
    const complete = screen.queryByRole("heading", { name: /activation complete/i });
    expect(Boolean(checklist || complete)).toBe(true);
  });

  it("renders /app/members page", async () => {
    render(
      <ToastProvider>
        <MembersPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: "Members", level: 1 })).toBeInTheDocument();
  });

  it("renders /app/api-keys page", async () => {
    render(
      <ToastProvider>
        <ApiKeysPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /api keys/i })).toBeInTheDocument();
  });

  it("creates and revokes api key with one-time secret UX", async () => {
    vi.spyOn(window, "confirm").mockReturnValue(true);

    render(
      <ToastProvider>
        <ApiKeysPage />
      </ToastProvider>
    );

    expect(await screen.findByText("Server Key")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /new api key/i }));
    fireEvent.change(screen.getByLabelText(/key name/i), { target: { value: "Integration_Key" } });
    fireEvent.click(screen.getByRole("button", { name: /^create$/i }));

    expect(await screen.findByText(/secret shown once/i)).toBeInTheDocument();
    expect(screen.getByText(latestSecret)).toBeInTheDocument();
    expect(await screen.findByText("Integration_Key")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /i stored it/i }));
    await waitFor(() => {
      expect(screen.queryByText(latestSecret)).not.toBeInTheDocument();
    });

    const row = screen.getByText("Integration_Key").closest("tr");
    expect(row).toBeTruthy();
    fireEvent.click(within(row as HTMLElement).getByRole("button", { name: /revoke/i }));

    await waitFor(() => {
      const updatedRow = screen.getByText("Integration_Key").closest("tr");
      expect(within(updatedRow as HTMLElement).getAllByText("Revoked").length).toBeGreaterThan(0);
      expect(within(updatedRow as HTMLElement).getByRole("button", { name: "Revoked" })).toBeDisabled();
    });
  });

  it("renders /app/usage page", async () => {
    render(
      <ToastProvider>
        <UsagePage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /usage/i })).toBeInTheDocument();
  });

  it("renders /app/billing page", async () => {
    render(
      <ToastProvider>
        <BillingPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /billing/i })).toBeInTheDocument();
  });

  it("renders /app/settings page", async () => {
    render(
      <ToastProvider>
        <SettingsPage />
      </ToastProvider>
    );
    expect(await screen.findByRole("heading", { name: /settings/i })).toBeInTheDocument();
  });

  it("logs out by clearing org context and replacing to login", async () => {
    window.localStorage.setItem("orgId", "org_test_123");
    render(
      <ToastProvider>
        <UserMenu />
      </ToastProvider>
    );

    fireEvent.click(screen.getByRole("button", { name: /user menu/i }));
    fireEvent.click(screen.getByRole("button", { name: /log out/i }));

    await waitFor(() => {
      expect(window.localStorage.getItem("orgId")).toBeNull();
      expect(routerReplaceMock).toHaveBeenCalledWith("/login");
    });
  });
});
