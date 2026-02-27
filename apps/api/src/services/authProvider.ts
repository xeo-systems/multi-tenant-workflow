import bcrypt from "bcryptjs";
import { Prisma } from "@saas/db";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";

const PASSWORD_COST = 12;
const DUMMY_BCRYPT_HASH = "$2a$12$Y7Qan/XVUPQQM4YVgjP7eOk7V/Wj34Y6PQxN4U2QjS4fRaI3hxLHS"; // hash for "dummy-password"

type RegisterInput = {
  email: string;
  password: string;
  orgName: string;
};

type RegisterResult = {
  orgId: string;
  userId: string;
};

type LoginInput = {
  email: string;
  password: string;
};

type LoginResult = {
  userId: string;
  orgId: string;
};

export interface AuthProvider {
  kind: "local" | "oidc" | "saml";
  register(input: RegisterInput): Promise<RegisterResult>;
  login(input: LoginInput): Promise<LoginResult>;
}

export function createAuthProvider(kind: string | undefined | null): AuthProvider {
  const normalized = (kind || "local").toLowerCase();
  if (normalized === "local") {
    return localAuthProvider;
  }
  return unsupportedAuthProvider(normalized === "saml" ? "saml" : "oidc");
}

const localAuthProvider: AuthProvider = {
  kind: "local",
  async register(input) {
    const existing = await prisma.user.findUnique({ where: { email: input.email } });
    if (existing) {
      throw new AppError("CONFLICT", 409, "Email already registered", { field: "email" });
    }

    const passwordHash = await bcrypt.hash(input.password, PASSWORD_COST);
    const result = await prisma.$transaction(async (tx: Prisma.TransactionClient) => {
      const org = await tx.organization.create({
        data: { name: input.orgName, plan: "free", planLimit: 1000 },
      });

      const user = await tx.user.create({
        data: { email: input.email, passwordHash },
      });

      await tx.membership.create({
        data: { orgId: org.id, userId: user.id, role: "OWNER" },
      });

      return { orgId: org.id, userId: user.id };
    });

    return result;
  },
  async login(input) {
    const user = await prisma.user.findUnique({ where: { email: input.email } });
    const passwordHash = user?.passwordHash || DUMMY_BCRYPT_HASH;
    const match = await bcrypt.compare(input.password, passwordHash);
    if (!user || !match) {
      throw new AppError("UNAUTHORIZED", 401, "Invalid credentials");
    }

    const membership = await prisma.membership.findFirst({
      where: { userId: user.id },
      orderBy: { createdAt: "asc" },
      select: { orgId: true },
    });
    if (!membership) {
      throw new AppError("FORBIDDEN", 403, "No organization membership found");
    }

    return { userId: user.id, orgId: membership.orgId };
  },
};

function unsupportedAuthProvider(kind: "oidc" | "saml"): AuthProvider {
  return {
    kind,
    async register() {
      throw new AppError("BAD_REQUEST", 400, `${kind.toUpperCase()} auth provider is not configured`);
    },
    async login() {
      throw new AppError("BAD_REQUEST", 400, `${kind.toUpperCase()} auth provider is not configured`);
    },
  };
}
