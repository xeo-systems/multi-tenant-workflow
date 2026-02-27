import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const apiBaseUrl = process.env["NEXT_PUBLIC_API_BASE_URL"] || "http://localhost:4000";

export function middleware(request: NextRequest) {
  return handleMiddleware(request);
}

async function handleMiddleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  if (!pathname.startsWith("/app")) {
    return NextResponse.next();
  }
  if (isCrossOriginApi(request.nextUrl.origin)) {
    return NextResponse.next();
  }

  const session = request.cookies.get("sid")?.value;
  if (!session) {
    const loginUrl = new URL("/login", request.url);
    return NextResponse.redirect(loginUrl);
  }

  try {
    const response = await fetch(`${apiBaseUrl}/auth/me`, {
      method: "GET",
      headers: {
        cookie: `sid=${session}`,
      },
      cache: "no-store",
    });
    if (!response.ok) {
      const loginUrl = new URL("/login", request.url);
      return NextResponse.redirect(loginUrl);
    }
  } catch {
    const loginUrl = new URL("/login", request.url);
    return NextResponse.redirect(loginUrl);
  }

  return NextResponse.next();
}

function isCrossOriginApi(webOrigin: string) {
  try {
    return new URL(apiBaseUrl).origin !== webOrigin;
  } catch {
    return false;
  }
}

export const config = {
  matcher: ["/app/:path*"],
};
