import "./globals.css";
import type { Metadata } from "next";
import { ToastProvider } from "@/lib/toast";
import { Toaster } from "@/components/toaster";

export const metadata: Metadata = {
  title: "Control Center",
  description: "Multi-tenant SaaS control center",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <ToastProvider>
          {children}
          <Toaster />
        </ToastProvider>
      </body>
    </html>
  );
}
