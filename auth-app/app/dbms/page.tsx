"use client"

import type React from "react"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { useAuth } from "@/lib/auth-context"
import { deleteDatabase, executeQuery } from "@/lib/api-client"
import { DatabaseSelector } from "@/components/database-selector"
import { CreateDatabaseModal } from "@/components/create-database-modal"
import { TablesSelector } from "@/components/tables-selector"
import { QueryResults } from "@/components/query-results"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Trash2 } from "lucide-react"

export default function DBMSManagerPage() {
  const router = useRouter()
  const { isAuthenticated, isLoading, username, userId, token, logout } = useAuth()
  const [isDropdownOpen, setIsDropdownOpen] = useState(false)
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null)
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)
  const [isDeleting, setIsDeleting] = useState(false)
  const [queryInput, setQueryInput] = useState("")
  const [queryResults, setQueryResults] = useState<any | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)
  const [queryError, setQueryError] = useState<string | null>(null)
  const [tablesRefreshKey, setTablesRefreshKey] = useState(0)

  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      router.push("/")
    }
  }, [isAuthenticated, isLoading, router])

  useEffect(() => {
    const storedDb = localStorage.getItem("selectedDatabase")
    if (storedDb) {
      setSelectedDatabase(storedDb)
    }
  }, [])

  const handleLogout = () => {
    logout()
    localStorage.removeItem("selectedDatabase")
    router.push("/")
  }

  const handleDatabaseSelect = (dbName: string) => {
    setSelectedDatabase(dbName)
    localStorage.setItem("selectedDatabase", dbName)
    setQueryResults(null)
    setQueryInput("")
  }

  const handleDatabaseCreated = (dbName: string) => {
    handleDatabaseSelect(dbName)
    setRefreshKey((prev) => prev + 1)
  }

  const handleDeleteDatabase = async () => {
    if (!selectedDatabase || !userId || !token) return

    if (!confirm(`Are you sure you want to delete "${selectedDatabase}"?`)) {
      return
    }

    setIsDeleting(true)
    try {
      await deleteDatabase(userId, token, selectedDatabase)
      setSelectedDatabase(null)
      localStorage.removeItem("selectedDatabase")
      setRefreshKey((prev) => prev + 1)
    } catch (err: any) {
      console.error("[v0] Error deleting database:", err)
      alert("Failed to delete database")
    } finally {
      setIsDeleting(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.shiftKey && e.key === "Enter") {
      e.preventDefault()
      handleExecuteQuery()
    }
  }

  const handleExecuteQuery = async () => {
    if (!queryInput.trim() || !selectedDatabase || !userId || !token) {
      setQueryError("Please enter a query and select a database")
      return
    }

    setIsExecuting(true)
    setQueryError(null)
    try {
      const results = await executeQuery(userId, token, selectedDatabase, queryInput)
      setQueryResults(results)

      if (queryInput.trim().toUpperCase().startsWith("CREATE TABLE")) {
        setTablesRefreshKey((prev) => prev + 1)
      }
    } catch (err: any) {
      console.error("[v0] Error executing query:", err)
      setQueryError(err.error?.detail || "Failed to execute query")
      setQueryResults(null)
    } finally {
      setIsExecuting(false)
    }
  }

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

  if (!isAuthenticated) {
    return null
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-alabaster to-timberwolf p-4">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8 pt-6">
          <div>
            <h1 className="text-3xl font-bold text-ebony">DBMS Manager</h1>
            <p className="text-davys-gray mt-1">Welcome, {username}</p>
          </div>

          <div className="relative">
            <button
              onClick={() => setIsDropdownOpen(!isDropdownOpen)}
              className="flex items-center justify-center w-10 h-10 rounded-full bg-cambridge-blue hover:bg-ebony text-alabaster transition-colors"
              aria-label="User menu"
            >
              <svg className="w-6 h-6" fill="currentColor" viewBox="0 0 24 24">
                <path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z" />
              </svg>
            </button>

            {/* Dropdown Menu */}
            {isDropdownOpen && (
              <div className="absolute right-0 mt-2 w-48 bg-alabaster border border-timberwolf rounded-lg shadow-lg z-10 overflow-hidden">
                <div className="p-3 border-b border-timberwolf bg-white rounded-t-lg">
                  <p className="text-sm font-medium text-ebony">{username}</p>
                </div>
                <button
                  onClick={handleLogout}
                  className="w-full text-left px-4 py-2 text-davys-gray bg-white hover:bg-timberwolf hover:text-ebony transition-colors text-sm font-medium rounded-b-lg"
                >
                  Logout
                </button>
              </div>
            )}
          </div>
        </div>

        {/* Main Content */}
        <Card className="shadow-lg">
          <CardHeader>
            <CardTitle className="text-ebony">Database Management</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {/* Database Selector and Controls */}
            <div className="space-y-4">
              <div className="flex items-center gap-4 flex-wrap">
                <div className="flex-1 min-w-xs">
                  <label className="block text-sm font-medium text-ebony mb-2">Select Database</label>
                  {userId && token && (
                    <DatabaseSelector
                      key={refreshKey}
                      userId={userId}
                      token={token}
                      selectedDatabase={selectedDatabase}
                      onDatabaseSelect={handleDatabaseSelect}
                      onRefresh={() => setRefreshKey((prev) => prev + 1)}
                    />
                  )}
                </div>

                <div className="flex gap-2 pt-6">
                  <Button
                    onClick={() => setIsCreateModalOpen(true)}
                    className="bg-cambridge-blue hover:bg-ebony text-alabaster font-medium"
                  >
                    New Database
                  </Button>

                  <Button
                    onClick={handleDeleteDatabase}
                    disabled={!selectedDatabase || isDeleting}
                    className="bg-red-600 hover:bg-red-700 text-alabaster font-medium disabled:opacity-50"
                  >
                    <Trash2 size={18} className="mr-2" />
                    Delete
                  </Button>
                </div>
              </div>

              {selectedDatabase && (
                <div className="p-4 bg-timberwolf rounded-lg">
                  <p className="text-sm text-ebony">
                    <span className="font-medium">Selected Database:</span> {selectedDatabase}
                  </p>
                </div>
              )}
            </div>

            {selectedDatabase && userId && token && (
              <div className="border-t border-timberwolf pt-6 space-y-4">
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                  {/* Tables Selector */}
                  <div className="lg:col-span-1">
                    <label className="block text-sm font-medium text-ebony mb-2">Tables</label>
                    <TablesSelector key={tablesRefreshKey} userId={userId} token={token} dbName={selectedDatabase} />
                  </div>

                  {/* Query Input */}
                  <div className="lg:col-span-2">
                    <label className="block text-sm font-medium text-ebony mb-2">Query Input</label>
                    <textarea
                      value={queryInput}
                      onChange={(e) => setQueryInput(e.target.value)}
                      onKeyDown={handleKeyDown}
                      placeholder="Enter your query here... (Press Shift+Enter to execute)"
                      className="w-full h-32 p-4 border border-timberwolf rounded-lg bg-white text-ebony placeholder-davys-gray focus:outline-none focus:ring-2 focus:ring-cambridge-blue resize-none"
                    />
                    <div className="mt-2 flex gap-2">
                      <Button
                        onClick={handleExecuteQuery}
                        disabled={isExecuting || !queryInput.trim()}
                        className="bg-cambridge-blue hover:bg-ebony text-alabaster font-medium disabled:opacity-50"
                      >
                        {isExecuting ? "Executing..." : "Execute Query"}
                      </Button>
                    </div>
                  </div>
                </div>

                {/* Query Error */}
                {queryError && (
                  <div className="p-4 bg-red-100 border border-red-300 rounded-lg">
                    <p className="text-sm text-red-700">{queryError}</p>
                  </div>
                )}

                {/* Query Results */}
                {queryResults && (
                  <div className="border-t border-timberwolf pt-6">
                    <label className="block text-sm font-medium text-ebony mb-2">Query Results</label>
                    <QueryResults response={queryResults} />
                  </div>
                )}
              </div>
            )}

            {!selectedDatabase && (
              <div className="border-t border-timberwolf pt-6">
                <p className="text-davys-gray text-center py-8">Select or create a database to get started</p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Create Database Modal */}
      {userId && token && (
        <CreateDatabaseModal
          isOpen={isCreateModalOpen}
          onClose={() => setIsCreateModalOpen(false)}
          userId={userId}
          token={token}
          onDatabaseCreated={handleDatabaseCreated}
        />
      )}
    </div>
  )
}
