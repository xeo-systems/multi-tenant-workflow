import { render, screen, within } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import LoginPage from "@/app/(auth)/login/page";
import { AppShell } from "@/components/app-shell";
import { ToastProvider } from "@/lib/toast";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn() }),
  usePathname: () => "/app",
}));

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) => (
    <a href={href}>{children}</a>
  ),
}));

beforeEach(() => {
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

  it("renders dashboard shell", () => {
    render(
      <ToastProvider>
        <AppShell>
          <div>Dashboard</div>
        </AppShell>
      </ToastProvider>
    );
    const main = screen.getByRole("main");
    expect(within(main).getByText("Dashboard")).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: /primary/i })).toBeInTheDocument();
  });
});
