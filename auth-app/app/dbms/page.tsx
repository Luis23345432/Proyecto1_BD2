"use client";

import type React from "react";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/lib/auth-context";
import { deleteDatabase, executeQuery, spimiBuildIndex } from "@/lib/api-client";
import { DatabaseSelector } from "@/components/database-selector";
import { CreateDatabaseModal } from "@/components/create-database-modal";
import { TablesSelector } from "@/components/tables-selector";
import { QueryResults } from "@/components/query-results";
import MultimediaSearch from "@/components/multimedia-search";
import { CSVImportModal } from "@/components/csv-import-modal";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Trash2 } from "lucide-react";

export default function DBMSManagerPage() {
  const router = useRouter();
  const { isAuthenticated, isLoading, username, userId, token, logout } =
    useAuth();
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [isDeleting, setIsDeleting] = useState(false);
  const [queryInput, setQueryInput] = useState("");
  const [queryResults, setQueryResults] = useState<any | null>(null);
  const [topK, setTopK] = useState<number>(10);
  const [isExecuting, setIsExecuting] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [tablesRefreshKey, setTablesRefreshKey] = useState(0);
  const [isCSVModalOpen, setIsCSVModalOpen] = useState(false);
  const [spimiTable, setSpimiTable] = useState("");
  const [spimiColumns, setSpimiColumns] = useState("");
  const [spimiBuilding, setSpimiBuilding] = useState(false);
  const [spimiMessage, setSpimiMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      router.push("/");
    }
  }, [isAuthenticated, isLoading, router]);

  useEffect(() => {
    const storedDb = localStorage.getItem("selectedDatabase");
    if (storedDb) {
      setSelectedDatabase(storedDb);
    }
  }, []);

  const handleLogout = () => {
    logout();
    localStorage.removeItem("selectedDatabase");
    router.push("/");
  };

  const handleDatabaseSelect = (dbName: string) => {
    setSelectedDatabase(dbName);
    localStorage.setItem("selectedDatabase", dbName);
    setQueryResults(null);
    setQueryInput("");
  };

  const handleDatabaseCreated = (dbName: string) => {
    handleDatabaseSelect(dbName);
    setRefreshKey((prev) => prev + 1);
  };

  const handleDeleteDatabase = async () => {
    if (!selectedDatabase || !userId || !token) return;

    if (!confirm(`Are you sure you want to delete "${selectedDatabase}"?`)) {
      return;
    }

    setIsDeleting(true);
    try {
      await deleteDatabase(userId, token, selectedDatabase);
      setSelectedDatabase(null);
      localStorage.removeItem("selectedDatabase");
      setRefreshKey((prev) => prev + 1);
    } catch (err: any) {
      console.error("[v0] Error deleting database:", err);
      alert("Failed to delete database");
    } finally {
      setIsDeleting(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.shiftKey && e.key === "Enter") {
      e.preventDefault();
      handleExecuteQuery();
    }
  };

  const handleExecuteQuery = async () => {
    if (!queryInput.trim() || !selectedDatabase || !userId || !token) {
      setQueryError("Please enter a query and select a database");
      return;
    }

    setIsExecuting(true);
    setQueryError(null);
    try {
      // Ensure LIMIT Top-K is present if user didn't specify it
      let sql = queryInput.trim();
      const hasLimit = /\bLIMIT\b\s+\d+/i.test(sql);
      if (!hasLimit && topK > 0) {
        sql = `${sql}\nLIMIT ${topK}`;
      }
      const results = await executeQuery(
        userId,
        token,
        selectedDatabase,
        sql
      );
      setQueryResults(results);

      if (queryInput.trim().toUpperCase().startsWith("CREATE TABLE")) {
        setTablesRefreshKey((prev) => prev + 1);
      }
    } catch (err: any) {
      console.error("[v0] Error executing query:", err);
      setQueryError(err.error?.detail || "Failed to execute query");
      setQueryResults(null);
    } finally {
      setIsExecuting(false);
    }
  };

  const handleBuildSpimi = async () => {
    if (!userId || !token || !selectedDatabase) return;
    const table = spimiTable.trim();
    const cols = spimiColumns
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    if (!table || (!cols.length && queryInput.indexOf("@@") === -1)) {
      setSpimiMessage("Provide table and columns (comma-separated)");
      return;
    }
    setSpimiBuilding(true);
    setSpimiMessage(null);
    try {
      const res = await spimiBuildIndex(userId, token, selectedDatabase, table, {
        columns: cols.length ? cols : undefined,
      });
      setSpimiMessage(res.message || `Built index: ${res.total_documents} docs`);
    } catch (e: any) {
      setSpimiMessage(e?.error?.detail || "Failed to build SPIMI index");
    } finally {
      setSpimiBuilding(false);
    }
  };

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-alabaster to-timberwolf">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-cambridge-blue"></div>
          <p className="mt-4 text-ebony font-medium">Loading...</p>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return null;
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
          <CardContent className="space-y-6">
            {/* Database Selector and Controls */}
            <div className="space-y-4">
              <div className="flex items-center gap-4 flex-wrap">
                <div className="flex-1 min-w-xs">
                  <label className="block text-sm font-medium text-ebony mb-2">
                    Select Database
                  </label>
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
            </div>

            {selectedDatabase && userId && token && (
              <div className="border-t border-timberwolf pt-6 space-y-4">
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                  {/* Tables Selector */}
                  <div className="lg:col-span-1">
                    <label className="block text-sm font-medium text-ebony mb-2">
                      Tables
                    </label>
                    <TablesSelector
                      key={tablesRefreshKey}
                      userId={userId}
                      token={token}
                      dbName={selectedDatabase}
                    />
                  </div>

                  {/* Query Input */}
                  <div className="lg:col-span-2">
                    <label className="block text-sm font-medium text-ebony mb-2">
                      Query Input
                    </label>
                    <textarea
                      value={queryInput}
                      onChange={(e) => setQueryInput(e.target.value)}
                      onKeyDown={handleKeyDown}
                      placeholder="Enter your query here... (Press Shift+Enter to execute)"
                      className="w-full h-32 p-4 border border-timberwolf rounded-lg bg-white text-ebony placeholder-davys-gray focus:outline-none focus:ring-2 focus:ring-cambridge-blue resize-none"
                    />
                    <div className="mt-2 grid grid-cols-1 sm:grid-cols-3 gap-2 items-end">
                      <div>
                        <label className="block text-sm font-medium text-ebony mb-2">Top‑K</label>
                        <input
                          type="number"
                          value={topK}
                          inputMode="numeric"
                          step={1}
                          min={1}
                          max={100}
                          onChange={(e) => {
                            const v = e.currentTarget.value;
                            // Allow empty while typing; don't lock the field
                            if (v === "") {
                              // do not update to NaN; keep previous
                              return;
                            }
                            let n = Number(e.currentTarget.value);
                            if (!Number.isFinite(n)) {
                              return;
                            }
                            // Clamp
                            if (n < 1) n = 1;
                            if (n > 100) n = 100;
                            setTopK(Math.floor(n));
                          }}
                          onBlur={(e) => {
                            // If left empty, restore default 10
                            if (e.currentTarget.value === "") {
                              setTopK(10);
                            }
                          }}
                          className="w-full p-2 border rounded"
                        />
                      </div>
                      <div className="flex gap-2">
                        <Button
                          onClick={handleExecuteQuery}
                          disabled={isExecuting || !queryInput.trim()}
                          className="bg-cambridge-blue hover:bg-ebony text-alabaster font-medium disabled:opacity-50"
                        >
                          {isExecuting ? "Executing..." : "Execute Query"}
                        </Button>
                        <Button
                          onClick={() => setIsCSVModalOpen(true)}
                          className="bg-cambridge-blue hover:bg-ebony text-alabaster font-medium"
                        >
                          Import CSV
                        </Button>
                      </div>
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
                    <label className="block text-sm font-medium text-ebony mb-2">
                      Query Results
                    </label>
                    <QueryResults response={queryResults} />
                  </div>
                )}

                {/* Multimedia Search */}
                <div className="border-t border-timberwolf pt-6">
                  <MultimediaSearch />
                </div>

                {/* Full-Text (SPIMI) Builder */}
                <div className="border-t border-timberwolf pt-6">
                  <label className="block text-sm font-medium text-ebony mb-2">Build Full-Text Index (SPIMI)</label>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
                    <div>
                      <label className="block text-xs text-davys-gray mb-1">Table</label>
                      <input
                        type="text"
                        value={spimiTable}
                        onChange={(e) => setSpimiTable(e.target.value)}
                        placeholder="Table name"
                        className="w-full p-2 border rounded"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-davys-gray mb-1">Columns (comma-separated)</label>
                      <input
                        type="text"
                        value={spimiColumns}
                        onChange={(e) => setSpimiColumns(e.target.value)}
                        placeholder="e.g. title, description"
                        className="w-full p-2 border rounded"
                      />
                    </div>
                    <div className="flex gap-2">
                      <Button onClick={handleBuildSpimi} disabled={spimiBuilding} className="bg-cambridge-blue text-alabaster">
                        {spimiBuilding ? "Building..." : "Build SPIMI"}
                      </Button>
                    </div>
                  </div>
                  {spimiMessage && (
                    <div className="mt-3 p-3 bg-white border rounded text-sm text-ebony">{spimiMessage}</div>
                  )}
                </div>
              </div>
            )}

            {!selectedDatabase && (
              <div className="border-t border-timberwolf pt-6">
                <p className="text-davys-gray text-center py-8">
                  Select or create a database to get started
                </p>
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

      {/* CSV Import Modal */}
      {userId && token && selectedDatabase && (
        <CSVImportModal
          isOpen={isCSVModalOpen}
          onClose={() => setIsCSVModalOpen(false)}
          userId={userId}
          token={token}
          dbName={selectedDatabase}
        />
      )}
    </div>
  );
}
