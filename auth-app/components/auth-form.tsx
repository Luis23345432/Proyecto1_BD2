"use client"

import type React from "react"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { useAuth } from "@/lib/auth-context"
import { register, login, type ApiError } from "@/lib/api-client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

type FormMode = "login" | "register"

interface FormError {
  field?: string
  message: string
}

export function AuthForm() {
  const router = useRouter()
  const { login: setAuth } = useAuth()
  const [mode, setMode] = useState<FormMode>("login")
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState<FormError | null>(null)
  const [isLoading, setIsLoading] = useState(false)

  const handleRegister = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    setIsLoading(true)

    try {
      await register({ username, password })
      setError(null)
      setUsername("")
      setPassword("")
      setMode("login")
    } catch (err: any) {
      const apiError: ApiError = err.error

      if (Array.isArray(apiError.detail)) {
        const passwordError = apiError.detail.find((e) => e.loc?.includes("password"))
        if (passwordError) {
          setError({
            field: "password",
            message: passwordError.msg,
          })
        }
      } else if (typeof apiError.detail === "string") {
        setError({
          message: apiError.detail,
        })
      }
    } finally {
      setIsLoading(false)
    }
  }

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    setIsLoading(true)

    try {
      const response = await login({ username, password })
      const tokenParts = response.access_token.split(".")
      const payload = JSON.parse(atob(tokenParts[1]))
      const userId = payload.sub || username

      setAuth(username, response.access_token, userId)
      router.push("/dbms")
    } catch (err: any) {
      const apiError: ApiError = err.error
      setError({
        message: typeof apiError.detail === "string" ? apiError.detail : "An error occurred",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const handleSubmit = mode === "login" ? handleLogin : handleRegister

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-alabaster to-timberwolf p-4">
      <Card className="w-full max-w-md shadow-lg">
        <CardHeader className="space-y-2">
          <CardTitle className="text-2xl text-center text-ebony">
            {mode === "login" ? "Welcome Back" : "Create Account"}
          </CardTitle>
          <CardDescription className="text-center text-davys-gray">
            {mode === "login" ? "Sign in to access the DBMS Manager" : "Register to get started"}
          </CardDescription>
        </CardHeader>

        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <label htmlFor="username" className="text-sm font-medium text-ebony">
                Username
              </label>
              <Input
                id="username"
                type="text"
                placeholder="Enter your username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                disabled={isLoading}
                className="border-timberwolf focus:border-cambridge-blue focus:ring-cambridge-blue"
              />
            </div>

            <div className="space-y-2">
              <label htmlFor="password" className="text-sm font-medium text-ebony">
                Password
              </label>
              <Input
                id="password"
                type="password"
                placeholder="Enter your password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                disabled={isLoading}
                className="border-timberwolf focus:border-cambridge-blue focus:ring-cambridge-blue"
              />
            </div>

            {error && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-md">
                <p className="text-sm text-red-700">{error.message}</p>
              </div>
            )}

            <Button
              type="submit"
              disabled={isLoading}
              className="w-full bg-cambridge-blue hover:bg-ebony text-alabaster font-medium py-2 transition-colors"
            >
              {isLoading ? "Loading..." : mode === "login" ? "Sign In" : "Create Account"}
            </Button>

            <div className="text-center text-sm">
              <span className="text-davys-gray">
                {mode === "login" ? "Don't have an account? " : "Already have an account? "}
              </span>
              <button
                type="button"
                onClick={() => {
                  setMode(mode === "login" ? "register" : "login")
                  setError(null)
                  setPassword("")
                }}
                className="text-cambridge-blue hover:text-ebony font-medium transition-colors"
              >
                {mode === "login" ? "Register" : "Sign In"}
              </button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
