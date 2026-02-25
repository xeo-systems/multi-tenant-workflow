"use client";

import { useEffect, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { ApiKeySchema } from "@/lib/validators";
import { useToast } from "@/lib/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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

export default function ApiKeysPage() {
  const { push } = useToast();
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [name, setName] = useState("");
  const [loading, setLoading] = useState(true);
  const [newSecret, setNewSecret] = useState<string | null>(null);

  async function loadKeys() {
    setLoading(true);
    try {
      const data = await apiFetch<ApiKey[]>("/api-keys");
      setKeys(data);
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Failed to load keys", description: apiErr.message, variant: "destructive" });
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadKeys();
  }, []);

  async function createKey() {
    const parsed = ApiKeySchema.safeParse({ name });
    if (!parsed.success) {
      push({ title: "Invalid name", description: "Provide a key name", variant: "destructive" });
      return;
    }
    try {
      const data = await apiFetch<{ secret: string }>("/api-keys", {
        method: "POST",
        body: JSON.stringify(parsed.data),
      });
      setNewSecret(data.secret);
      setName("");
      await loadKeys();
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Create failed", description: apiErr.message, variant: "destructive" });
    }
  }

  async function revokeKey(id: string) {
    try {
      await apiFetch(`/api-keys/${id}`, { method: "DELETE" });
      push({ title: "Key revoked" });
      await loadKeys();
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Revoke failed", description: apiErr.message, variant: "destructive" });
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">API Keys</h1>
        <p className="text-sm text-muted-foreground">Create and revoke keys used for programmatic access.</p>
      </div>

      {newSecret && (
        <Alert>
          <AlertTitle>Secret shown once</AlertTitle>
          <AlertDescription>
            Copy and store this key now. You will not be able to view it again.
            <code className="mt-2 block rounded bg-muted px-2 py-1 text-xs">{newSecret}</code>
          </AlertDescription>
        </Alert>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Create key</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap items-end gap-3">
          <div className="space-y-2">
            <Label htmlFor="key-name">Key name</Label>
            <Input id="key-name" value={name} onChange={(e) => setName(e.currentTarget.value)} />
          </div>
          <Button onClick={createKey}>Create</Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Existing keys</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <p className="text-sm text-muted-foreground">Loading keys...</p>
          ) : keys.length === 0 ? (
            <EmptyState
              title="No keys"
              description="Create a key to start making API calls."
              actionLabel="Create key"
              onAction={createKey}
            />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Prefix</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {keys.map((key) => (
                  <TableRow key={key.id}>
                    <TableCell>{key.name}</TableCell>
                    <TableCell>{key.prefix}</TableCell>
                    <TableCell>{key.revokedAt ? "Revoked" : "Active"}</TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => revokeKey(key.id)}
                        disabled={Boolean(key.revokedAt)}
                      >
                        Revoke
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
