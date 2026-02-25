import { AppShell } from "@/components/app-shell";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";

const apiBaseUrl = process.env["NEXT_PUBLIC_API_BASE_URL"] || "http://localhost:4000";

export default async function AppLayout({ children }: { children: React.ReactNode }) {
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
