"use client"

import { useState } from "react"
import { MetricsDetailsModal } from "./metrics-details-modal"

interface QueryResultsProps {
  response: {
    rows?: Record<string, any>[]
    count?: number
    ok?: boolean
    table?: string
    inserted?: number
    rid?: [number, number]
    execution_time_ms: number
    metrics: {
      total_disk_accesses: number
      disk_reads: number
      disk_writes: number
      indexes: Record<
        string,
        {
          type: string
          operations: Record<
            string,
            {
              count: number
              time_ms: number
            }
          >
        }
      >
    }
  }
}

export function QueryResults({ response }: QueryResultsProps) {
  const [isMetricsOpen, setIsMetricsOpen] = useState(false)

  if (response.rows && response.rows.length > 0) {
    const columns = Object.keys(response.rows[0])

    return (
      <div className="space-y-4">
        <div
          className="overflow-x-auto overflow-y-auto border rounded-lg"
          style={{ borderColor: "#d8dad3", maxHeight: "400px" }}
        >
          <table className="w-full border-collapse">
            <thead className="sticky top-0" style={{ backgroundColor: "#a4c2a5" }}>
              <tr>
                {columns.map((col) => (
                  <th
                    key={col}
                    className="px-4 py-3 text-left text-sm font-semibold"
                    style={{ color: "#f1f2eb", borderRight: "1px solid #d8dad3" }}
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {response.rows.map((row, rowIndex) => (
                <tr key={rowIndex} style={{ borderBottomColor: "#d8dad3", borderBottomWidth: "1px" }}>
                  {columns.map((col) => (
                    <td
                      key={`${rowIndex}-${col}`}
                      className="px-4 py-3 text-sm"
                      style={{ color: "#566246", borderRight: "1px solid #d8dad3" }}
                    >
                      {String(row[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div
          className="flex items-center justify-between p-3 rounded-lg"
          style={{ backgroundColor: "#f1f2eb", color: "#566246" }}
        >
          <div className="text-sm font-medium">Total Rows: {response.count}</div>
          <button
            onClick={() => setIsMetricsOpen(true)}
            className="text-sm font-medium px-3 py-1 rounded hover:opacity-70 transition-opacity"
            style={{ backgroundColor: "#a4c2a5", color: "#f1f2eb" }}
          >
            Execution Time: {response.execution_time_ms.toFixed(2)} ms
          </button>
        </div>
        <MetricsDetailsModal
          isOpen={isMetricsOpen}
          onClose={() => setIsMetricsOpen(false)}
          executionTimeMs={response.execution_time_ms}
          metrics={response.metrics}
        />
      </div>
    )
  }

  if (response.ok && response.table) {
    return (
      <div className="space-y-4">
        <div className="p-4 rounded-lg border" style={{ borderColor: "#a4c2a5", backgroundColor: "#f1f2eb" }}>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium" style={{ color: "#566246" }}>
                Table Created
              </span>
              <span
                className="text-sm px-3 py-1 rounded font-medium"
                style={{ backgroundColor: "#a4c2a5", color: "#f1f2eb" }}
              >
                Success
              </span>
            </div>
            <p className="text-lg font-bold" style={{ color: "#566246" }}>
              {response.table}
            </p>
            <p className="text-sm" style={{ color: "#4a4a48" }}>
              Rows Inserted: {response.inserted}
            </p>
          </div>
        </div>
        <div
          className="flex items-center justify-between p-3 rounded-lg"
          style={{ backgroundColor: "#f1f2eb", color: "#566246" }}
        >
          <div className="text-sm font-medium">Operation completed</div>
          <button
            onClick={() => setIsMetricsOpen(true)}
            className="text-sm font-medium px-3 py-1 rounded hover:opacity-70 transition-opacity"
            style={{ backgroundColor: "#a4c2a5", color: "#f1f2eb" }}
          >
            Execution Time: {response.execution_time_ms.toFixed(2)} ms
          </button>
        </div>
        <MetricsDetailsModal
          isOpen={isMetricsOpen}
          onClose={() => setIsMetricsOpen(false)}
          executionTimeMs={response.execution_time_ms}
          metrics={response.metrics}
        />
      </div>
    )
  }

  if (response.ok && response.rid) {
    return (
      <div className="space-y-4">
        <div className="p-4 rounded-lg border" style={{ borderColor: "#a4c2a5", backgroundColor: "#f1f2eb" }}>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium" style={{ color: "#566246" }}>
                Insert Successful
              </span>
              <span
                className="text-sm px-3 py-1 rounded font-medium"
                style={{ backgroundColor: "#a4c2a5", color: "#f1f2eb" }}
              >
                Success
              </span>
            </div>
            <p className="text-sm" style={{ color: "#4a4a48" }}>
              Record ID: [{response.rid[0]}, {response.rid[1]}]
            </p>
          </div>
        </div>
        <div
          className="flex items-center justify-between p-3 rounded-lg"
          style={{ backgroundColor: "#f1f2eb", color: "#566246" }}
        >
          <div className="text-sm font-medium">Operation completed</div>
          <button
            onClick={() => setIsMetricsOpen(true)}
            className="text-sm font-medium px-3 py-1 rounded hover:opacity-70 transition-opacity"
            style={{ backgroundColor: "#a4c2a5", color: "#f1f2eb" }}
          >
            Execution Time: {response.execution_time_ms.toFixed(2)} ms
          </button>
        </div>
        <MetricsDetailsModal
          isOpen={isMetricsOpen}
          onClose={() => setIsMetricsOpen(false)}
          executionTimeMs={response.execution_time_ms}
          metrics={response.metrics}
        />
      </div>
    )
  }

  return (
    <div className="p-4 text-center text-sm" style={{ color: "#4a4a48" }}>
      No results
    </div>
  )
}
