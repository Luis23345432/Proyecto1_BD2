"use client"

interface QueryResultsProps {
  rows: Record<string, any>[]
  count: number
}

export function QueryResults({ rows, count }: QueryResultsProps) {
  if (rows.length === 0) {
    return (
      <div className="p-4 text-center text-sm" style={{ color: "#4a4a48" }}>
        No results
      </div>
    )
  }

  const columns = Object.keys(rows[0])

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
            {rows.map((row, rowIndex) => (
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
      <div className="p-3 rounded-lg text-sm font-medium" style={{ backgroundColor: "#f1f2eb", color: "#566246" }}>
        Total Rows: {count}
      </div>
    </div>
  )
}
