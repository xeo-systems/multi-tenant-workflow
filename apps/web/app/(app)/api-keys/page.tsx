"use client";

import { useEffect, useState } from "react";
import { ApiKeyCreateSchema } from "@saas/shared";
import { apiFetch, getApiErrorKind, toApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { EmptyState } from "@/components/empty-state";

type ApiKey = {
  id: string;
  name: string;
  prefix: string;
  createdAt: string;
  revokedAt?: string | null;
  lastUsedAt?: string | null;
};

const SECRET_TTL_MS = 2 * 60 * 1000;

export default function ApiKeysPage() {
  const { push } = useToast();
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [name, setName] = useState("");
  const [loading, setLoading] = useState(true);
  const [newSecret, setNewSecret] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [revokingId, setRevokingId] = useState<string | null>(null);
  const [forbidden, setForbidden] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [nameError, setNameError] = useState<string | null>(null);

  async function loadKeys() {
    setLoading(true);
    setForbidden(false);
    setErrorMessage(null);
    try {
      const data = await apiFetch<ApiKey[]>("/api-keys");
      setKeys(data);
    } catch (err) {
      const apiErr = toApiError(err, "Failed to load keys");
      const kind = getApiErrorKind(apiErr);
      if (kind === "forbidden") {
        setForbidden(true);
      } else {
        setErrorMessage(apiErr.message);
      }
      if (kind === "network") {
        push({ title: "Network error", description: "Could not reach API. Retry below.", variant: "destructive" });
      } else {
        push({ title: "Failed to load keys", description: apiErr.message, variant: "destructive" });
      }
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadKeys();
  }, []);

  useEffect(() => {
    if (!newSecret) {
      return;
    }
    const timeout = window.setTimeout(() => {
      setNewSecret(null);
      push({ title: "Secret cleared", description: "The one-time secret has been removed from view." });
    }, SECRET_TTL_MS);
    return () => window.clearTimeout(timeout);
  }, [newSecret, push]);

  useEffect(() => {
    return () => {
      setNewSecret(null);
    };
  }, []);

  async function createKey() {
    setNameError(null);
    const parsed = ApiKeyCreateSchema.safeParse({ name });
    if (!parsed.success) {
      setNameError(parsed.error.issues[0]?.message || "Provide a valid key name");
      push({ title: "Invalid name", description: "Fix the highlighted field.", variant: "destructive" });
      return;
    }

    setCreating(true);
    try {
      const data = await apiFetch<{ secret: string }>("/api-keys", {
        method: "POST",
        body: JSON.stringify(parsed.data),
      });
      setNewSecret(data.secret);
      setName("");
      setNameError(null);
      setCreateOpen(false);
      push({ title: "Key created" });
      await loadKeys();
    } catch (err) {
      const apiErr = toApiError(err, "Create failed");
      if (apiErr.field === "name") {
        setNameError(apiErr.message);
      }
      push({ title: "Create failed", description: apiErr.message, variant: "destructive" });
    } finally {
      setCreating(false);
    }
  }

  async function revokeKey(id: string, keyName: string) {
    if (typeof window !== "undefined" && !window.confirm(`Revoke API key '${keyName}'?`)) {
      return;
    }

    setRevokingId(id);
    try {
      await apiFetch(`/api-keys/${id}`, { method: "DELETE" });
      push({ title: "Key revoked" });
      await loadKeys();
    } catch (err) {
      const apiErr = toApiError(err, "Revoke failed");
      push({ title: "Revoke failed", description: apiErr.message, variant: "destructive" });
    } finally {
      setRevokingId(null);
    }
  }

  async function copySecret() {
    if (!newSecret || typeof navigator === "undefined") {
      return;
    }
    await navigator.clipboard.writeText(newSecret);
    push({ title: "Copied", description: "API key copied to clipboard." });
  }

  function clearSecret() {
    setNewSecret(null);
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <h1 className="page-title">API Keys</h1>
        <p className="page-description">Create and revoke keys used for programmatic access.</p>
      </div>

      {newSecret && (
        <Alert>
          <AlertTitle>Secret shown once</AlertTitle>
          <AlertDescription>
            Copy and store this key now. You will not be able to view it again.
            <code className="mt-2 block rounded bg-muted px-2 py-1 text-xs">{newSecret}</code>
            <div className="mt-3 flex items-center gap-2">
              <Button size="sm" onClick={copySecret}>
                Copy secret
              </Button>
              <Button size="sm" variant="outline" onClick={clearSecret}>
                I stored it
              </Button>
              <span className="text-xs text-muted-foreground">Treat this as a password.</span>
            </div>
          </AlertDescription>
        </Alert>
      )}

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle>Create key</CardTitle>
          <Button onClick={() => setCreateOpen(true)} disabled={forbidden}>
            New API key
          </Button>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">Use named keys for integrations and revoke any key instantly.</p>
        </CardContent>
      </Card>

      {createOpen && (
        <div className="fixed inset-0 z-50 grid place-items-center bg-black/40 p-4">
          <Card className="w-full max-w-lg">
            <CardHeader>
              <CardTitle>Create API key</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="key-name">Key name</Label>
                <Input
                  id="key-name"
                  value={name}
                  onChange={(e) => {
                    const value = e.currentTarget.value;
                    setName(value);
                    if (nameError) {
                      setNameError(null);
                    }
                  }}
                  placeholder="Production Integration"
                />
                {nameError && <p className="text-xs text-red-600">{nameError}</p>}
              </div>
              <div className="space-y-2">
                <Label htmlFor="key-scopes">Scopes (optional)</Label>
                <Input id="key-scopes" value="Not supported by current API" disabled />
              </div>
              <div className="flex justify-end gap-4">
                <Button variant="outline" onClick={() => setCreateOpen(false)} disabled={creating}>
                  Cancel
                </Button>
                <Button onClick={createKey} disabled={creating} aria-busy={creating}>
                  {creating ? "Creating..." : "Create"}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Existing keys</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="space-y-2">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : forbidden ? (
            <EmptyState title="Permissions required" description="You do not have permission to view API keys for this organization." />
          ) : errorMessage ? (
            <EmptyState title="Unable to load keys" description={errorMessage} actionLabel="Retry" onAction={loadKeys} />
          ) : keys.length === 0 ? (
            <EmptyState title="No keys" description="Create a key to start making API calls." actionLabel="Create key" onAction={() => setCreateOpen(true)} />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Prefix</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead>Last Used</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {keys.map((key) => {
                  const isRevoked = Boolean(key.revokedAt);
                  const isRevoking = revokingId === key.id;
                  return (
                    <TableRow key={key.id} className={isRevoked ? "opacity-70" : undefined}>
                      <TableCell>{key.name}</TableCell>
                      <TableCell>{key.prefix}</TableCell>
                      <TableCell>{new Date(key.createdAt).toLocaleDateString()}</TableCell>
                      <TableCell>{key.lastUsedAt ? new Date(key.lastUsedAt).toLocaleString() : "Never"}</TableCell>
                      <TableCell>{isRevoked ? "Revoked" : "Active"}</TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => revokeKey(key.id, key.name)}
                          disabled={isRevoked || isRevoking}
                        >
                          {isRevoked ? "Revoked" : isRevoking ? "Revoking..." : "Revoke"}
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
