import { AppShell } from "@/components/app-shell";
import { cookies, headers } from "next/headers";
import { redirect } from "next/navigation";

const apiBaseUrl = process.env["NEXT_PUBLIC_API_BASE_URL"] || "http://localhost:4000";

export default async function AppLayout({ children }: { children: React.ReactNode }) {
  if (isCrossOriginApi()) {
    return <AppShell>{children}</AppShell>;
  }

  const sid = cookies().get("sid")?.value;
  if (!sid) {
    redirect("/login");
  }

  try {
    const res = await fetch(`${apiBaseUrl}/auth/me`, {
      method: "GET",
      headers: { cookie: `sid=${sid}` },
      cache: "no-store",
    });
    if (!res.ok) {
      redirect("/login");
    }
  } catch {
    redirect("/login");
  }

  return <AppShell>{children}</AppShell>;
}

function isCrossOriginApi() {
  try {
    const h = headers();
    const host = h.get("x-forwarded-host") || h.get("host");
    const proto = h.get("x-forwarded-proto") || "https";
    if (!host) {
      return false;
    }
    const webOrigin = `${proto}://${host}`;
    return new URL(apiBaseUrl).origin !== webOrigin;
  } catch {
    return false;
  }
}
