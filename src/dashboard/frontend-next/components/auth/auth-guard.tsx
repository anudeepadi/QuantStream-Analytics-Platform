"use client"

import { useAuth } from "@/lib/auth/auth-context"
import { useRouter } from "next/navigation"
import { useEffect } from "react"

export function AuthGuard({ children }: { readonly children: React.ReactNode }) {
  const { isAuthenticated, isLoading } = useAuth()
  const router = useRouter()

  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      router.replace("/login")
    }
  }, [isAuthenticated, isLoading, router])

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
      </div>
    )
  }

  if (!isAuthenticated) return null

  return <>{children}</>
}
