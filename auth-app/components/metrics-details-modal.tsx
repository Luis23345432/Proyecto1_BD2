"use client"

interface IndexOperation {
  type: string
  operations: Record<
    string,
    {
      count: number
      time_ms: number
    }
  >
}

interface MetricsDetailsModalProps {
  isOpen: boolean
  onClose: () => void
  executionTimeMs: number
  metrics: {
    total_disk_accesses: number
    disk_reads: number
    disk_writes: number
    indexes: Record<string, IndexOperation>
  }
}

export function MetricsDetailsModal({ isOpen, onClose, executionTimeMs, metrics }: MetricsDetailsModalProps) {
  if (!isOpen) return null

  // Calculate total index time
  const totalIndexTime = Object.values(metrics.indexes).reduce((sum, index) => {
    return sum + Object.values(index.operations).reduce((opSum, op) => opSum + op.time_ms, 0)
  }, 0)

  return (
    <div className="fixed inset-0 flex items-center justify-center z-50 p-4">
      <div
        className="bg-[#f1f2eb] rounded-lg shadow-xl max-w-2xl w-full max-h-96 overflow-y-auto"
        style={{ backgroundColor: "#f1f2eb" }}
      >
        {/* Header */}
        <div className="sticky top-0 p-6 border-b" style={{ borderColor: "#d8dad3", backgroundColor: "#a4c2a5" }}>
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-bold" style={{ color: "#f1f2eb" }}>
              Query Execution Metrics
            </h2>
            <button onClick={onClose} className="text-lg font-bold hover:opacity-70" style={{ color: "#f1f2eb" }}>
              ✕
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="p-6 space-y-6">
          {/* Execution Time */}
          <div className="p-4 rounded-lg" style={{ backgroundColor: "#d8dad3" }}>
            <p className="text-sm font-medium" style={{ color: "#566246" }}>
              Total Execution Time
            </p>
            <p className="text-2xl font-bold mt-1" style={{ color: "#566246" }}>
              {executionTimeMs.toFixed(2)} ms
            </p>
          </div>

          {/* Disk Access Stats */}
          <div className="grid grid-cols-3 gap-4">
            <div className="p-4 rounded-lg" style={{ backgroundColor: "#d8dad3" }}>
              <p className="text-sm font-medium" style={{ color: "#566246" }}>
                Total Disk Accesses
              </p>
              <p className="text-2xl font-bold mt-1" style={{ color: "#566246" }}>
                {metrics.total_disk_accesses}
              </p>
            </div>
            <div className="p-4 rounded-lg" style={{ backgroundColor: "#d8dad3" }}>
              <p className="text-sm font-medium" style={{ color: "#566246" }}>
                Disk Reads
              </p>
              <p className="text-2xl font-bold mt-1" style={{ color: "#566246" }}>
                {metrics.disk_reads}
              </p>
            </div>
            <div className="p-4 rounded-lg" style={{ backgroundColor: "#d8dad3" }}>
              <p className="text-sm font-medium" style={{ color: "#566246" }}>
                Disk Writes
              </p>
              <p className="text-2xl font-bold mt-1" style={{ color: "#566246" }}>
                {metrics.disk_writes}
              </p>
            </div>
          </div>

          {/* Index Operations */}
          {Object.keys(metrics.indexes).length > 0 && (
            <div>
              <h3 className="text-lg font-semibold mb-3" style={{ color: "#566246" }}>
                Index Operations
              </h3>
              <div className="space-y-3">
                {Object.entries(metrics.indexes).map(([indexName, indexData]) => (
                  <div
                    key={indexName}
                    className="p-4 rounded-lg border"
                    style={{ borderColor: "#d8dad3", backgroundColor: "#f1f2eb" }}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <p className="font-medium" style={{ color: "#566246" }}>
                        {indexName}
                      </p>
                      <span
                        className="text-xs px-2 py-1 rounded"
                        style={{ backgroundColor: "#a4c2a5", color: "#f1f2eb" }}
                      >
                        {indexData.type}
                      </span>
                    </div>
                    <div className="space-y-1">
                      {Object.entries(indexData.operations).map(([opName, opData]) => (
                        <div key={opName} className="flex items-center justify-between text-sm">
                          <span style={{ color: "#4a4a48" }}>
                            {opName}: {opData.count} operation{opData.count !== 1 ? "s" : ""}
                          </span>
                          <span style={{ color: "#566246", fontWeight: "600" }}>{opData.time_ms.toFixed(3)} ms</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-3 p-3 rounded-lg" style={{ backgroundColor: "#d8dad3" }}>
                <p className="text-sm" style={{ color: "#566246" }}>
                  <span className="font-medium">Total Index Time:</span> {totalIndexTime.toFixed(3)} ms
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
