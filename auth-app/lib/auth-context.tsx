"use client"

import type React from "react"
import { createContext, useContext, useState, useEffect } from "react"

interface AuthContextType {
  username: string | null
  token: string | null
  userId: string | null
  isLoading: boolean
  login: (username: string, token: string, userId: string) => void
  logout: () => void
  isAuthenticated: boolean
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [username, setUsername] = useState<string | null>(null)
  const [token, setToken] = useState<string | null>(null)
  const [userId, setUserId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  // Load auth data from localStorage on mount
  useEffect(() => {
    const storedUsername = localStorage.getItem("username")
    const storedToken = localStorage.getItem("token")
    const storedUserId = localStorage.getItem("userId")

    if (storedUsername && storedToken && storedUserId) {
      setUsername(storedUsername)
      setToken(storedToken)
      setUserId(storedUserId)
    }

    setIsLoading(false)
  }, [])

  const login = (newUsername: string, newToken: string, newUserId: string) => {
    setUsername(newUsername)
    setToken(newToken)
    setUserId(newUserId)
    localStorage.setItem("username", newUsername)
    localStorage.setItem("token", newToken)
    localStorage.setItem("userId", newUserId)
  }

  const logout = () => {
    setUsername(null)
    setToken(null)
    setUserId(null)
    localStorage.removeItem("username")
    localStorage.removeItem("token")
    localStorage.removeItem("userId")
  }

  return (
    <AuthContext.Provider
      value={{
        username,
        token,
        userId,
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
