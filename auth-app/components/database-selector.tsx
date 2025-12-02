/**
 * Selector de bases de datos con dropdown.
 * Carga y muestra las bases de datos del usuario, permitiendo seleccionar una activa.
 */
"use client"

import { useState, useEffect } from "react"
import { getDatabases, type Database } from "@/lib/api-client"
import { ChevronDown } from "lucide-react"

interface DatabaseSelectorProps {
  userId: string
  token: string
  selectedDatabase: string | null
  onDatabaseSelect: (dbName: string) => void
  onRefresh: () => void
}

export function DatabaseSelector({
  userId,
  token,
  selectedDatabase,
  onDatabaseSelect,
  onRefresh,
}: DatabaseSelectorProps) {
  const [databases, setDatabases] = useState<Database[]>([])
  const [isOpen, setIsOpen] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchDatabases()
  }, [])

  const fetchDatabases = async () => {
    setIsLoading(true)
    setError(null)
    try {
      const data = await getDatabases(userId, token)
      setDatabases(data)
    } catch (err: any) {
      setError("Failed to load databases")
      console.error("[v0] Error fetching databases:", err)
    } finally {
      setIsLoading(false)
    }
  }

  const handleSelect = (dbName: string) => {
    onDatabaseSelect(dbName)
    setIsOpen(false)
  }

  return (
    <div className="relative w-full max-w-xs">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full px-4 py-2 bg-alabaster border-2 border-cambridge-blue rounded-lg flex items-center justify-between hover:bg-timberwolf transition-colors text-ebony font-medium"
      >
        <span>{selectedDatabase || "Select a database"}</span>
        <ChevronDown size={20} className={`transition-transform ${isOpen ? "rotate-180" : ""}`} />
      </button>

      {isOpen && (
        <div className="absolute top-full left-0 right-0 mt-2 bg-white border-2 border-cambridge-blue rounded-lg shadow-lg z-20">
          {isLoading ? (
            <div className="p-3 text-center text-davys-gray">Loading...</div>
          ) : error ? (
            <div className="p-3 text-center text-red-600 text-sm">{error}</div>
          ) : databases.length === 0 ? (
            <div className="p-3 text-center text-davys-gray text-sm">No databases found</div>
          ) : (
            <div className="max-h-48 overflow-y-auto">
              {databases.map((db) => (
                <button
                  key={db.name}
                  onClick={() => handleSelect(db.name)}
                  className={`w-full text-left px-4 py-2 hover:bg-timberwolf transition-colors ${
                    selectedDatabase === db.name ? "bg-cambridge-blue text-alabaster font-medium" : "text-ebony"
                  }`}
                >
                  {db.name}
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
