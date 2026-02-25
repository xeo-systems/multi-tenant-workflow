import { render, screen, within } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import type { ReactNode } from "react";
import LoginPage from "@/app/(auth)/login/page";
import DashboardPage from "@/app/(app)/app/page";
import MembersPage from "@/app/(app)/app/members/page";
import ApiKeysPage from "@/app/(app)/app/api-keys/page";
import UsagePage from "@/app/(app)/app/usage/page";
import BillingPage from "@/app/(app)/app/billing/page";
import SettingsPage from "@/app/(app)/app/settings/page";
import { AppShell } from "@/components/app-shell";
import { ToastProvider } from "@/lib/toast";

const pushMock = vi.fn();
let mockPathname = "/app";

const apiFetchMock = vi.fn(async (path: string) => {
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
  if (path === "/api-keys") {
    return [
      {
        id: "key_1",
        name: "Server Key",
        prefix: "ak_123456",
        createdAt: "2024-01-01T00:00:00.000Z",
        revokedAt: null,
        lastUsedAt: null,
      },
    ];
  }
  if (path === "/usage?days=30") {
    return [
      {
        id: "usage_1",
        orgId: "org_test_123",
        metric: "api_requests",
        date: "2024-01-01T00:00:00.000Z",
        quantity: 120,
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
    return { userId: "usr_1", orgId: "org_test_123" };
  }
  return {};
});

vi.mock("@/lib/api", () => ({
  apiFetch: (...args: [string]) => apiFetchMock(...args),
}));

vi.mock("@/lib/toast", async () => {
  const actual = await vi.importActual<typeof import("@/lib/toast")>("@/lib/toast");
  return {
    ...actual,
    useToast: () => ({ push: pushMock, remove: vi.fn() }),
  };
});

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn() }),
  usePathname: () => mockPathname,
}));

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: ReactNode; href: string }) => <a href={href}>{children}</a>,
}));

beforeEach(() => {
  apiFetchMock.mockClear();
  pushMock.mockClear();
  mockPathname = "/app";

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
  });

  it("renders /app dashboard page", async () => {
    render(
      <ToastProvider>
        <DashboardPage />
      </ToastProvider>
    );
    expect(await screen.findByText(/organization overview and plan status/i)).toBeInTheDocument();
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
});
