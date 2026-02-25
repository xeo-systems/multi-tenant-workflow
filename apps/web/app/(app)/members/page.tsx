"use client";

import { useEffect, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { InviteMemberSchema } from "@/lib/validators";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { EmptyState } from "@/components/empty-state";

type Member = {
  id: string;
  role: string;
  user: { id: string; email: string };
  createdAt: string;
};

const roles = ["OWNER", "ADMIN", "MEMBER", "BILLING", "READONLY"] as const;

export default function MembersPage() {
  const { push } = useToast();
  const [members, setMembers] = useState<Member[]>([]);
  const [loading, setLoading] = useState(true);
  const [invite, setInvite] = useState({ email: "", role: "MEMBER" });

  async function loadMembers() {
    setLoading(true);
    try {
      const data = await apiFetch<Member[]>("/org/members");
      setMembers(data);
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Failed to load members", description: apiErr.message, variant: "destructive" });
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadMembers();
  }, []);

  async function inviteMember() {
    const parsed = InviteMemberSchema.safeParse(invite);
    if (!parsed.success) {
      push({ title: "Invalid input", description: "Check email and role", variant: "destructive" });
      return;
    }
    try {
      await apiFetch("/org/members", { method: "POST", body: JSON.stringify(parsed.data) });
      push({ title: "Invite sent", description: "Member added to org." });
      setInvite({ email: "", role: "MEMBER" });
      await loadMembers();
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Invite failed", description: apiErr.message, variant: "destructive" });
    }
  }

  async function updateRole(memberId: string, role: string) {
    try {
      await apiFetch(`/org/members/${memberId}`, { method: "PATCH", body: JSON.stringify({ role }) });
      push({ title: "Role updated" });
      await loadMembers();
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Update failed", description: apiErr.message, variant: "destructive" });
    }
  }

  async function removeMember(memberId: string) {
    try {
      await apiFetch(`/org/members/${memberId}`, { method: "DELETE" });
      push({ title: "Member removed" });
      await loadMembers();
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Remove failed", description: apiErr.message, variant: "destructive" });
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Members</h1>
        <p className="text-sm text-muted-foreground">Manage roles and access within your organization.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Invite member</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-3">
          <div className="space-y-2">
            <Label htmlFor="invite-email">Email</Label>
            <Input
              id="invite-email"
              value={invite.email}
              onChange={(e) => setInvite((prev) => ({ ...prev, email: e.currentTarget.value }))}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="invite-role">Role</Label>
            <select
              id="invite-role"
              className="h-10 w-full rounded-md border bg-background px-3 text-sm"
              value={invite.role}
              onChange={(e) => setInvite((prev) => ({ ...prev, role: e.currentTarget.value }))}
            >
              {roles.map((role) => (
                <option key={role} value={role}>
                  {role}
                </option>
              ))}
            </select>
          </div>
          <div className="flex items-end">
            <Button onClick={inviteMember}>Send invite</Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Current members</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <p className="text-sm text-muted-foreground">Loading members...</p>
          ) : members.length === 0 ? (
            <EmptyState
              title="No members yet"
              description="Invite your first teammate to get started."
              actionLabel="Send invite"
              onAction={inviteMember}
            />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Email</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {members.map((member) => (
                  <TableRow key={member.id}>
                    <TableCell>{member.user.email}</TableCell>
                    <TableCell>
                      <select
                        className="h-9 rounded-md border bg-background px-2 text-sm"
                        value={member.role}
                        onChange={(e) => updateRole(member.id, e.currentTarget.value)}
                        aria-label={`Change role for ${member.user.email}`}
                      >
                        {roles.map((role) => (
                          <option key={role} value={role}>
                            {role}
                          </option>
                        ))}
                      </select>
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => removeMember(member.id)}
                        aria-label={`Remove ${member.user.email}`}
                      >
                        Remove
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
