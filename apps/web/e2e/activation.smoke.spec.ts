import { expect, test } from "@playwright/test";

test("activation flow works across /app pages", async ({ page }) => {
  const seedEmail = process.env["E2E_EMAIL"];
  const seedPassword = process.env["E2E_PASSWORD"];

  if (seedEmail && seedPassword) {
    await page.goto("/login");
    await page.getByLabel("Email").fill(seedEmail);
    await page.getByLabel("Password").fill(seedPassword);
    await page.getByRole("button", { name: /sign in/i }).click();
  } else {
    const email = `e2e-${Date.now()}@example.com`;
    await page.goto("/register");
    await page.getByLabel("Organization name").fill("E2E Org");
    await page.getByLabel("Email").fill(email);
    await page.getByLabel("Password").fill("password123");
    await page.getByRole("button", { name: /create account/i }).click();
  }

  await expect(page).toHaveURL(/\/app$/);
  await expect(page.getByRole("heading", { name: /activation checklist/i })).toBeVisible();

  await page.goto("/app/api-keys");
  await expect(page.getByRole("heading", { name: /api keys/i })).toBeVisible();
  await page.getByRole("button", { name: /new api key/i }).click();
  await page.getByLabel("Key name").fill("E2E key");
  await page.getByRole("button", { name: /^create$/i }).click();
  await expect(page.getByText(/secret shown once/i)).toBeVisible();

  await page.goto("/app/usage");
  await expect(page.getByRole("heading", { name: /usage/i })).toBeVisible();
  await expect(page.getByText(/current period/i)).toBeVisible();

  await page.goto("/app/members");
  await expect(page.getByRole("heading", { name: /members/i })).toBeVisible();

  await page.goto("/app/billing");
  await expect(page.getByRole("heading", { name: /billing/i })).toBeVisible();

  await page.goto("/app/settings");
  await expect(page.getByRole("heading", { name: /settings/i })).toBeVisible();
});
