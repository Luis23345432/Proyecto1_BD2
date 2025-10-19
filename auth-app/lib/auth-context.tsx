"use client"

import type React from "react"
import { createContext, useContext, useState, useEffect } from "react"

interface AuthContextType {
  username: string | null
  token: string | null
  isLoading: boolean
  login: (username: string, token: string) => void
  logout: () => void
  isAuthenticated: boolean
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [username, setUsername] = useState<string | null>(null)
  const [token, setToken] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  // Load auth data from localStorage on mount
  useEffect(() => {
    const storedUsername = localStorage.getItem("username")
    const storedToken = localStorage.getItem("token")

    if (storedUsername && storedToken) {
      setUsername(storedUsername)
      setToken(storedToken)
    }

    setIsLoading(false)
  }, [])

  const login = (newUsername: string, newToken: string) => {
    setUsername(newUsername)
    setToken(newToken)
    localStorage.setItem("username", newUsername)
    localStorage.setItem("token", newToken)
  }

  const logout = () => {
    setUsername(null)
    setToken(null)
    localStorage.removeItem("username")
    localStorage.removeItem("token")
  }

  return (
    <AuthContext.Provider
      value={{
        username,
        token,
        isLoading,
        login,
        logout,
        isAuthenticated: !!token,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider")
  }
  return context
}
