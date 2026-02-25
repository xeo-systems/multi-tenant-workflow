import { describe, expect, it } from "vitest";

describe("web security headers config", () => {
  it("includes baseline security headers and CSP", async () => {
    // @ts-ignore next config is ESM .mjs without TS declarations.
    const mod = await import("../next.config.mjs");
    const nextConfig = mod.default as { headers?: () => Promise<Array<{ headers: Array<{ key: string; value: string }> }>> };
    expect(typeof nextConfig.headers).toBe("function");

    const rules = await nextConfig.headers!();
    const rootRule = rules.find((rule) => rule.headers.some((h) => h.key === "Content-Security-Policy"));
    expect(rootRule).toBeTruthy();
    const headerMap = new Map((rootRule?.headers || []).map((h) => [h.key, h.value]));
    expect(headerMap.get("X-Content-Type-Options")).toBe("nosniff");
    expect(headerMap.get("Referrer-Policy")).toBe("strict-origin-when-cross-origin");
    expect(headerMap.get("X-Frame-Options")).toBe("DENY");
    const csp = headerMap.get("Content-Security-Policy") || "";
    expect(csp).toContain("default-src 'self'");
    expect(csp).toContain("frame-ancestors 'none'");
  });
});
