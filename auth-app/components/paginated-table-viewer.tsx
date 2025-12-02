"use client"

import { useState, useEffect } from "react"
import { getTableRecords } from "@/lib/api-client"

interface Props {
  userId: string
  token: string
  dbName: string
  initialTableName?: string
}

export function PaginatedTableViewer({ userId, token, dbName, initialTableName }: Props) {
  const [tableName, setTableName] = useState("")
  const [limit, setLimit] = useState(100)
  const [offset, setOffset] = useState(0)
  const [data, setData] = useState<{
    rows: Record<string, any>[]
    count: number
    execution_time_ms: number
    metrics: { page_scans: number; disk_reads: number }
  } | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const canPrev = offset > 0
  const canNext = data ? offset + limit < data.count : false

  const fetchPage = async (nextOffset: number) => {
    if (!tableName.trim()) return
    setIsLoading(true)
    setError(null)
    try {
      const res = await getTableRecords(userId, token, dbName, tableName.trim(), limit, nextOffset)
      setData(res)
      setOffset(nextOffset)
    } catch (err: any) {
      console.error("[PaginatedTableViewer] fetch error", err)
      setError(err.error?.detail || "Failed to fetch records")
      setData(null)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    // reset paging when table changes
    setOffset(0)
    setData(null)
  }, [tableName, limit])

  useEffect(() => {
    if (initialTableName) {
      setTableName(initialTableName)
    }
  }, [initialTableName])

  useEffect(() => {
    if (initialTableName) {
      // auto-load first page when initial table provided
      fetchPage(0)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialTableName])

  const handleLoad = () => fetchPage(0)
  const handlePrev = () => fetchPage(Math.max(0, offset - limit))
  const handleNext = () => fetchPage(offset + limit)

  return (
    <div className="space-y-3">
      <div className="flex items-end gap-3 flex-wrap">
        <div className="flex-1 min-w-[220px]">
          <label className="block text-sm font-medium text-ebony mb-1">Table name</label>
          <input
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            placeholder="e.g. restaurants"
            className="w-full px-3 py-2 border rounded-lg"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-ebony mb-1">Page size</label>
          <select
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value))}
            className="px-3 py-2 border rounded-lg"
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>
        <button
          onClick={handleLoad}
          disabled={isLoading || !tableName.trim()}
          className="px-4 py-2 rounded-lg bg-cambridge-blue text-alabaster hover:bg-ebony disabled:opacity-50"
        >
          {isLoading ? "Loading..." : "Load"}
        </button>
      </div>

      {error && (
        <div className="p-3 bg-red-100 border border-red-300 rounded-lg text-sm text-red-700">{error}</div>
      )}

      {data && data.rows && data.rows.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center justify-between p-2 rounded-lg" style={{ backgroundColor: "#f1f2eb" }}>
            <div className="text-sm" style={{ color: "#566246" }}>
              Rows: {data.rows.length} / Total: {data.count} • Offset: {offset} • Exec: {data.execution_time_ms.toFixed(2)} ms
            </div>
            <div className="flex gap-2">
              <button
                onClick={handlePrev}
                disabled={!canPrev || isLoading}
                className="px-3 py-1 rounded bg-cambridge-blue text-alabaster disabled:opacity-50"
              >
                Prev
              </button>
              <button
                onClick={handleNext}
                disabled={!canNext || isLoading}
                className="px-3 py-1 rounded bg-cambridge-blue text-alabaster disabled:opacity-50"
              >
                Next
              </button>
            </div>
          </div>

          <div className="overflow-x-auto overflow-y-auto border rounded-lg" style={{ maxHeight: 400 }}>
            <table className="w-full border-collapse">
              <thead className="sticky top-0" style={{ backgroundColor: "#a4c2a5" }}>
                <tr>
                  {Object.keys(data.rows[0]).map((col) => (
                    <th key={col} className="px-4 py-2 text-left text-sm font-semibold" style={{ color: "#f1f2eb" }}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row, i) => (
                  <tr key={i} className="border-t">
                    {Object.keys(data.rows[0]).map((col) => (
                      <td key={`${i}-${col}`} className="px-4 py-2 text-sm" style={{ color: "#566246" }}>
                        {String(row[col])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
