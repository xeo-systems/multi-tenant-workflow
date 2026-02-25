"use client";

import { useToast } from "@/lib/toast";
import { cn } from "@/lib/utils";

export function Toaster() {
  const { toasts, dismiss } = useToast();

  return (
    <div className="fixed right-6 top-6 z-50 flex w-80 flex-col gap-3">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          className={cn(
            "rounded-lg border bg-card p-4 shadow-lg",
            toast.variant === "destructive" && "border-red-500"
          )}
        >
          <div className="flex items-start justify-between gap-2">
            <div>
              <p className="text-sm font-semibold">{toast.title}</p>
              {toast.description && <p className="text-xs text-muted-foreground">{toast.description}</p>}
            </div>
            <button
              aria-label="Dismiss"
              className="text-xs text-muted-foreground hover:text-foreground"
              onClick={() => dismiss(toast.id)}
            >
              Close
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
