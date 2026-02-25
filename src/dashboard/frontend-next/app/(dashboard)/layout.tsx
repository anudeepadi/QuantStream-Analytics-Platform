"use client"

import { TooltipProvider } from "@/components/ui/tooltip"
import { Sidebar } from "@/components/layout/sidebar"
import { Header } from "@/components/layout/header"
import { AuthGuard } from "@/components/auth/auth-guard"

export default function DashboardLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <AuthGuard>
      <TooltipProvider>
        <div className="flex h-screen overflow-hidden">
          <Sidebar />
          <div className="flex flex-1 flex-col overflow-hidden">
            <Header />
            <main className="flex-1 overflow-auto p-6 pb-8">
              {children}
            </main>
          </div>
        </div>
      </TooltipProvider>
    </AuthGuard>
  )
}
