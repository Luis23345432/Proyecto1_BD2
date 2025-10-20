"use client"

import type React from "react"

import { useState } from "react"
import { uploadCSV, getTables } from "@/lib/api-client"
import type { Table } from "@/lib/api-client"
import { useEffect } from "react"

interface CSVImportModalProps {
  isOpen: boolean
  onClose: () => void
  userId: string
  token: string
  dbName: string
}

export function CSVImportModal({ isOpen, onClose, userId, token, dbName }: CSVImportModalProps) {
  const [tables, setTables] = useState<Table[]>([])
  const [selectedTable, setSelectedTable] = useState<string>("")
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [isUploading, setIsUploading] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [uploadSuccess, setUploadSuccess] = useState<{ inserted: number } | null>(null)

  useEffect(() => {
    if (isOpen) {
      loadTables()
      setSelectedFile(null)
      setUploadError(null)
      setUploadSuccess(null)
    }
  }, [isOpen])

  const loadTables = async () => {
    try {
      const tablesList = await getTables(userId, token, dbName)
      setTables(tablesList)
      if (tablesList.length > 0) {
        setSelectedTable(tablesList[0].name)
      }
    } catch (err: any) {
      console.error("[v0] Error loading tables:", err)
      setUploadError("Failed to load tables")
    }
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      if (!file.name.endsWith(".csv")) {
        setUploadError("Please select a CSV file")
        setSelectedFile(null)
        return
      }
      setSelectedFile(file)
      setUploadError(null)
    }
  }

  const handleUpload = async () => {
    if (!selectedTable || !selectedFile) {
      setUploadError("Please select a table and a CSV file")
      return
    }

    setIsUploading(true)
    setUploadError(null)
    setUploadSuccess(null)

    try {
      const result = await uploadCSV(userId, token, dbName, selectedTable, selectedFile)
      setUploadSuccess(result)
      setSelectedFile(null)
      setSelectedTable("")
    } catch (err: any) {
      console.error("[v0] Error uploading CSV:", err)
      setUploadError(err.error?.detail || "Failed to upload CSV")
    } finally {
      setIsUploading(false)
    }
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 flex items-center justify-center z-50">
      <div className="bg-[#f1f2eb] rounded-lg shadow-lg p-6 w-full max-w-md mx-4">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-bold text-ebony">Import CSV</h2>
          <button
            onClick={onClose}
            className="text-davys-gray hover:text-ebony transition-colors"
            aria-label="Close modal"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="space-y-4">
          {/* Table Selector */}
          <div>
            <label className="block text-sm font-medium text-ebony mb-2">Select Table</label>
            <select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              className="w-full px-3 py-2 border border-timberwolf rounded-lg bg-white text-ebony focus:outline-none focus:ring-2 focus:ring-cambridge-blue"
            >
              <option value="">Choose a table...</option>
              {tables.map((table) => (
                <option key={table.name} value={table.name}>
                  {table.name}
                </option>
              ))}
            </select>
          </div>

          {/* File Input */}
          <div>
            <label className="block text-sm font-medium text-ebony mb-2">Select CSV File</label>
            <input
              type="file"
              accept=".csv"
              onChange={handleFileChange}
              className="w-full px-3 py-2 border border-timberwolf rounded-lg bg-white text-ebony focus:outline-none focus:ring-2 focus:ring-cambridge-blue"
            />
            {selectedFile && <p className="text-sm text-cambridge-blue mt-1">Selected: {selectedFile.name}</p>}
          </div>

          {/* Error Message */}
          {uploadError && (
            <div className="p-3 bg-red-100 border border-red-300 rounded-lg">
              <p className="text-sm text-red-700">{uploadError}</p>
            </div>
          )}

          {/* Success Message */}
          {uploadSuccess && (
            <div className="p-3 bg-green-100 border border-green-300 rounded-lg">
              <p className="text-sm text-green-700">Successfully imported {uploadSuccess.inserted} rows!</p>
            </div>
          )}

          {/* Buttons */}
          <div className="flex gap-2 pt-4">
            <button
              onClick={handleUpload}
              disabled={isUploading || !selectedTable || !selectedFile}
              className="flex-1 px-4 py-2 rounded-lg font-medium text-alabaster transition-colors disabled:opacity-50"
              style={{ backgroundColor: "#a4c2a5" }}
            >
              {isUploading ? "Uploading..." : "Upload CSV"}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
