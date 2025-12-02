/**
 * Selector de tablas para una base de datos.
 * Muestra la lista de tablas disponibles en la base de datos seleccionada.
 */
"use client"

import { useEffect, useState } from "react"
import { getTables } from "@/lib/api-client"

interface TablesSelectorProps {
  userId: string
  token: string
  dbName: string
}

export function TablesSelector({ userId, token, dbName }: TablesSelectorProps) {
  const [tables, setTables] = useState<string[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchTables = async () => {
      setIsLoading(true)
      setError(null)
      try {
        const tablesData = await getTables(userId, token, dbName)
        setTables(tablesData.map((t) => t.name))
      } catch (err: any) {
        console.error("[v0] Error fetching tables:", err)
        setError("Failed to load tables")
      } finally {
        setIsLoading(false)
      }
    }

    fetchTables()
  }, [userId, token, dbName])

  if (isLoading) {
    return (
      <div className="p-3 rounded-lg text-sm" style={{ backgroundColor: "#d8dad3", color: "#4a4a48" }}>
        Loading tables...
      </div>
    )
  }

  if (error) {
    return <div className="p-3 bg-red-100 rounded-lg text-red-700 text-sm">{error}</div>
  }

  return (
    <div className="border rounded-lg overflow-hidden bg-white" style={{ borderColor: "#d8dad3" }}>
      <div className="max-h-64 overflow-y-auto">
        {tables.length === 0 ? (
          <div className="p-4 text-center text-sm" style={{ color: "#4a4a48" }}>
            No tables found
          </div>
        ) : (
          <ul style={{ borderColor: "#d8dad3" }}>
            {tables.map((table) => (
              <li key={table} style={{ borderBottomColor: "#d8dad3", borderBottomWidth: "1px" }}>
                <div className="px-4 py-3 text-sm font-medium" style={{ color: "#566246" }}>
                  {table}
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
