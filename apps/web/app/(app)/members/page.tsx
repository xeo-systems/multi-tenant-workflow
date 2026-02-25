"use client";

import { useEffect, useMemo, useState } from "react";
import { MembershipInviteSchema, Role, RoleSchema } from "@saas/shared";
import { apiFetch, ApiError, getApiErrorKind, toApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { EmptyState } from "@/components/empty-state";

type Member = {
  id: string;
  role: Role;
  createdAt?: string;
  user: { id: string; email: string };
};

type OrgResponse = {
  org: { id: string; name: string; plan: string; planLimit: number };
  role: Role;
};

const roles = RoleSchema.options;

export default function MembersPage() {
  const { push } = useToast();
  const [members, setMembers] = useState<Member[]>([]);
  const [loading, setLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [permissionDenied, setPermissionDenied] = useState(false);
  const [viewerRole, setViewerRole] = useState<Role | null>(null);
  const [invite, setInvite] = useState({ email: "", role: "MEMBER" as Role });
  const [inviteErrors, setInviteErrors] = useState<Record<string, string>>({});

  const canManage = useMemo(() => viewerRole === "OWNER" || viewerRole === "ADMIN", [viewerRole]);

  async function loadMembers() {
    setLoading(true);
    setErrorMessage(null);
    setPermissionDenied(false);

    try {
      const orgData = await apiFetch<OrgResponse>("/org");
      setViewerRole(orgData.role);

      const memberData = await apiFetch<Member[]>("/org/members");
      setMembers(memberData);
    } catch (err) {
      const apiErr = toApiError(err, "Failed to load members");
      if (getApiErrorKind(apiErr) === "forbidden") {
        setPermissionDenied(true);
      } else {
        setErrorMessage(apiErr.message);
      }
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadMembers();
  }, []);

  function handleForbidden(apiErr: ApiError, fallbackTitle: string) {
    if (getApiErrorKind(apiErr) === "forbidden") {
      push({ title: "Permission denied", description: "You do not have access to perform this action.", variant: "destructive" });
      return;
    }
    if (getApiErrorKind(apiErr) === "network") {
      push({ title: "Network error", description: "Could not reach API. Please retry.", variant: "destructive" });
      return;
    }
    push({ title: fallbackTitle, description: apiErr.message, variant: "destructive" });
  }

  async function inviteMember() {
    if (!canManage) {
      return;
    }
    setInviteErrors({});

    const parsed = MembershipInviteSchema.safeParse(invite);
    if (!parsed.success) {
      const nextErrors: Record<string, string> = {};
      parsed.error.issues.forEach((issue) => {
        const key = String(issue.path[0] || "email");
        nextErrors[key] = issue.message;
      });
      setInviteErrors(nextErrors);
      push({ title: "Invalid input", description: "Fix the highlighted fields.", variant: "destructive" });
      return;
    }

    try {
      await apiFetch("/org/members", { method: "POST", body: JSON.stringify(parsed.data) });
      push({ title: "Invite sent", description: "Member added to organization." });
      setInvite({ email: "", role: "MEMBER" });
      setInviteErrors({});
      await loadMembers();
    } catch (err) {
      const apiErr = toApiError(err, "Invite failed");
      if (apiErr.field === "email") {
        setInviteErrors((prev) => ({ ...prev, email: apiErr.message }));
      }
      handleForbidden(apiErr, "Invite failed");
    }
  }

  async function updateRole(memberId: string, role: Role) {
    if (!canManage) {
      return;
    }

    try {
      await apiFetch(`/org/members/${memberId}`, { method: "PATCH", body: JSON.stringify({ role }) });
      push({ title: "Role updated" });
      await loadMembers();
    } catch (err) {
      handleForbidden(toApiError(err, "Role update failed"), "Role update failed");
    }
  }

  async function removeMember(memberId: string, email: string) {
    if (!canManage) {
      return;
    }

    if (typeof window !== "undefined" && !window.confirm(`Remove ${email} from the organization?`)) {
      return;
    }

    try {
      await apiFetch(`/org/members/${memberId}`, { method: "DELETE" });
      push({ title: "Member removed" });
      await loadMembers();
    } catch (err) {
      handleForbidden(toApiError(err, "Remove failed"), "Remove failed");
    }
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <h1 className="page-title">Members</h1>
        <p className="page-description">Invite teammates and manage organization access.</p>
      </div>

      {!permissionDenied && !canManage && viewerRole && (
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm text-muted-foreground">
              You are signed in as {viewerRole}. This page is read-only for your role.
            </p>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Invite member</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-3">
          <div className="space-y-2 md:col-span-2">
            <Label htmlFor="invite-email">Email</Label>
            <Input
              id="invite-email"
              value={invite.email}
              onChange={(e) => {
                const value = e.currentTarget.value;
                setInvite((prev) => ({ ...prev, email: value }));
              }}
              disabled={!canManage || permissionDenied}
            />
            {inviteErrors["email"] && <p className="text-xs text-red-600">{inviteErrors["email"]}</p>}
          </div>
          <div className="space-y-2">
            <Label htmlFor="invite-role">Role</Label>
            <select
              id="invite-role"
              className="h-10 w-full rounded-md border bg-background px-4 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              value={invite.role}
              onChange={(e) => {
                const value = RoleSchema.parse(e.currentTarget.value);
                setInvite((prev) => ({ ...prev, role: value }));
              }}
              disabled={!canManage || permissionDenied}
            >
              {roles.map((role) => (
                <option key={role} value={role}>
                  {role}
                </option>
              ))}
            </select>
          </div>
          <div className="md:col-span-3 flex justify-end">
            <Button onClick={inviteMember} disabled={!canManage || permissionDenied}>
              Send invite
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Current members</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="space-y-2">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : errorMessage ? (
            <EmptyState title="Unable to load members" description={errorMessage} actionLabel="Retry" onAction={loadMembers} />
          ) : permissionDenied ? (
            <EmptyState
              title="Access denied"
              description="You do not have permission to view members for this organization."
            />
          ) : members.length === 0 ? (
            <EmptyState title="No members yet" description="Invite your first teammate to get started." actionLabel={canManage ? "Send invite" : undefined} onAction={canManage ? inviteMember : undefined} />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Email</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {members.map((member) => (
                  <TableRow key={member.id}>
                    <TableCell>{member.user.email}</TableCell>
                    <TableCell>
                      <select
                        className="h-10 rounded-md border bg-background px-4 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        value={member.role}
                        onChange={(e) => {
                          const value = RoleSchema.parse(e.currentTarget.value);
                          updateRole(member.id, value);
                        }}
                        aria-label={`Change role for ${member.user.email}`}
                        disabled={!canManage}
                      >
                        {roles.map((role) => (
                          <option key={role} value={role}>
                            {role}
                          </option>
                        ))}
                      </select>
                    </TableCell>
                    <TableCell>Active</TableCell>
                    <TableCell>
                      {member.createdAt ? new Date(member.createdAt).toLocaleDateString() : "Not available"}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => removeMember(member.id, member.user.email)}
                        aria-label={`Remove ${member.user.email}`}
                        disabled={!canManage}
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
