import * as React from "react";
import { cn } from "@/lib/utils";

export function Tooltip({ children, label }: { children: React.ReactNode; label: string }) {
  return (
    <span className="group relative inline-flex">
      {children}
      <span
        role="tooltip"
        className={cn(
          "pointer-events-none absolute left-1/2 top-full z-10 mt-2 w-max -translate-x-1/2 rounded-md bg-foreground px-2 py-1 text-xs text-background opacity-0 transition group-hover:opacity-100"
        )}
      >
        {label}
      </span>
    </span>
  );
}
