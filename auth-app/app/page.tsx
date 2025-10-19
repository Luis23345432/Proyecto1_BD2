"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { useAuth } from "@/lib/auth-context"
import { AuthForm } from "@/components/auth-form"

export default function LoginPage() {
  const router = useRouter()
  const { isAuthenticated, isLoading } = useAuth()

  useEffect(() => {
    if (!isLoading && isAuthenticated) {
      router.push("/dbms")
    }
  }, [isAuthenticated, isLoading, router])

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-alabaster to-timberwolf">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-cambridge-blue"></div>
          <p className="mt-4 text-ebony font-medium">Loading...</p>
        </div>
      </div>
    )
  }

  return <AuthForm />
}
