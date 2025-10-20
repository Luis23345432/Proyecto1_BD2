"use client"

import type React from "react"

import { useState } from "react"
import { createDatabase } from "@/lib/api-client"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { X } from "lucide-react"

interface CreateDatabaseModalProps {
  isOpen: boolean
  onClose: () => void
  userId: string
  token: string
  onDatabaseCreated: (dbName: string) => void
}

export function CreateDatabaseModal({ isOpen, onClose, userId, token, onDatabaseCreated }: CreateDatabaseModalProps) {
  const [dbName, setDbName] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    if (!dbName.trim()) {
      setError("Database name is required")
      return
    }

    setIsLoading(true)
    try {
      await createDatabase(userId, token, dbName)
      onDatabaseCreated(dbName)
      setDbName("")
      onClose()
    } catch (err: any) {
      const errorMessage = typeof err.error?.detail === "string" ? err.error.detail : "Failed to create database"
      setError(errorMessage)
      console.error("[v0] Error creating database:", err)
    } finally {
      setIsLoading(false)
    }
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-[#f1f2eb] rounded-lg shadow-xl max-w-md w-full">
        <div className="flex items-center justify-between p-6 border-b border-timberwolf">
          <h2 className="text-xl font-bold text-ebony">Create Database</h2>
          <button onClick={onClose} className="text-davys-gray hover:text-ebony transition-colors">
            <X size={24} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          <div className="space-y-2">
            <label htmlFor="dbName" className="text-sm font-medium text-ebony">
              Database Name
            </label>
            <Input
              id="dbName"
              type="text"
              placeholder="Enter database name"
              value={dbName}
              onChange={(e) => setDbName(e.target.value)}
              disabled={isLoading}
              className="border-timberwolf focus:border-cambridge-blue focus:ring-cambridge-blue"
            />
          </div>

          {error && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-md">
              <p className="text-sm text-red-700">{error}</p>
            </div>
          )}

          <div className="flex gap-3 pt-4">
            <Button
              type="button"
              onClick={onClose}
              disabled={isLoading}
              className="flex-1 bg-timberwolf hover:bg-davys-gray text-ebony font-medium"
            >
              Cancel
            </Button>
            <Button
              type="submit"
              disabled={isLoading}
              className="flex-1 bg-cambridge-blue hover:bg-ebony text-alabaster font-medium"
            >
              {isLoading ? "Creating..." : "Create"}
            </Button>
          </div>
        </form>
      </div>
    </div>
  )
}
