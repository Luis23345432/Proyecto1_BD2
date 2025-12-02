"use client";

import React, { useEffect, useMemo, useState } from "react";
import { multimediaSearch, multimediaTrainCodebook, multimediaBuildIndex, multimediaStatus, API_BASE_URL, type MultimediaSearchResult } from "@/lib/api-client";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

export default function MultimediaSearch() {
  const [file, setFile] = useState<File | null>(null);
  const [modality, setModality] = useState<"image" | "audio">("image");
  const [strategy, setStrategy] = useState<"sequential" | "inverted">("inverted");
  const [k, setK] = useState<number>(10);
  const [isSearching, setIsSearching] = useState(false);
  const [results, setResults] = useState<MultimediaSearchResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [timingMs, setTimingMs] = useState<number | null>(null);
  const [dataRoot, setDataRoot] = useState<string>("");
    // Persist dataRoot in localStorage
    useEffect(() => {
      try {
        const saved = window.localStorage.getItem("multimedia.dataRoot");
        if (saved) setDataRoot(saved);
      } catch {}
    }, []);
    useEffect(() => {
      try {
        if (dataRoot) {
          window.localStorage.setItem("multimedia.dataRoot", dataRoot);
        } else {
          window.localStorage.removeItem("multimedia.dataRoot");
        }
      } catch {}
    }, [dataRoot]);
  const [indexType, setIndexType] = useState<"bow" | "inverted">("inverted");
  const [opMessage, setOpMessage] = useState<string | null>(null);
  const [isBuildingBow, setIsBuildingBow] = useState(false);
  const [statusObj, setStatusObj] = useState<{ codebook?: boolean; bow?: boolean; inverted?: boolean } | null>(null);

  const resultItems = useMemo(() => {
    if (!results) return [] as Array<{ doc_id: string; score: number; name: string }>;
    return (results?.results ?? []).map(r => {
      const name = r.doc_id.split(/[\\/]/).pop() || r.doc_id;
      return { ...r, name };
    });
  }, [results]);

  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0] || null;
    setFile(f);
  };

  const onSearch = async () => {
    if (!file) {
      setError("Select a file to search");
      return;
    }
    setIsSearching(true);
    setError(null);
    setResults(null);
    setTimingMs(null);
    const start = performance.now();
    try {
      const res = await multimediaSearch(file, modality, strategy, k);
      setResults(res);
    } catch (e: any) {
      setError(e?.error?.detail || "Search failed");
    } finally {
      const end = performance.now();
      setTimingMs(Math.round(end - start));
      setIsSearching(false);
    }
  };

  const onTrainCodebook = async () => {
    setOpMessage(null); setError(null);
    if (!dataRoot.trim()) { setError("Set Data Root"); return; }
    const resp = await multimediaTrainCodebook(modality, dataRoot, 512);
    if (!resp.ok) setError(resp.error || "Train failed"); else setOpMessage(`Codebook trained: ${resp.modality} k=${resp.k}`);
  };

  const onBuildIndex = async () => {
    setOpMessage(null); setError(null);
    if (!dataRoot.trim()) { setError("Set Data Root"); return; }
    const resp = await multimediaBuildIndex(modality, dataRoot, indexType);
    if (!resp.ok) setError(resp.error || "Index build failed"); else setOpMessage(`Index built: ${resp.index_type} count=${resp.count}`);
    // Refresh status after build
    try {
      const s = await multimediaStatus(modality);
      setStatusObj(s[modality] || null);
    } catch {}
  };

  const onBuildBowQuick = async () => {
    setOpMessage(null); setError(null);
    if (!dataRoot.trim()) { setError("Set Data Root"); return; }
    try {
      setIsBuildingBow(true);
      const resp = await multimediaBuildIndex(modality, dataRoot, "bow");
      if (!resp.ok) {
        setError(resp.error || "BoW build failed");
      } else {
        // Show a concise green status and encourage search
        setOpMessage(`BoW ready: count=${resp.count}`);
      }
    } finally {
      setIsBuildingBow(false);
    }
  };

  const onStatus = async () => {
    setOpMessage(null); setError(null);
    const resp = await multimediaStatus(modality);
    setStatusObj(resp[modality] || null);
    setOpMessage(JSON.stringify(resp[modality], null, 2));
  };

  return (
    <Card className="shadow-md">
      <CardHeader>
        <CardTitle>Multimedia Search (Image/Audio)</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Orquestación */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 items-end">
          <div>
            <label className="block text-sm font-medium mb-2">Data Root</label>
            <input
              type="text"
              value={dataRoot}
              onChange={(e) => setDataRoot(e.target.value)}
              placeholder="C:\\...\\datasets\\..."
              className="w-full p-2 border rounded"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-2">Index Type</label>
            <select
              value={indexType}
              onChange={(e) => setIndexType(e.target.value as any)}
              className="w-full p-2 border rounded"
            >
              <option value="inverted">Inverted</option>
              <option value="bow">BoW (Sequential)</option>
            </select>
          </div>
          <div className="flex gap-2 flex-wrap">
            <Button onClick={onTrainCodebook} className="bg-cambridge-blue text-alabaster">Train Codebook</Button>
            <Button onClick={onBuildIndex} className="bg-cambridge-blue text-alabaster">Build Index</Button>
            <Button onClick={onBuildBowQuick} disabled={isBuildingBow} className="bg-cambridge-blue text-alabaster">
              {isBuildingBow ? "Building BoW..." : "Build BoW"}
            </Button>
            <Button onClick={onStatus} className="bg-cambridge-blue text-alabaster">Check Status</Button>
          </div>
        </div>

        {opMessage && (
          <div className="p-3 bg-green-100 border border-green-300 rounded">
            <pre className="text-sm text-green-800 whitespace-pre-wrap">{opMessage}</pre>
          </div>
        )}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 items-end">
          <div>
            <label className="block text-sm font-medium mb-2">Modality</label>
            <select
              value={modality}
              onChange={(e) => setModality(e.target.value as any)}
              className="w-full p-2 border rounded"
            >
              <option value="image">Image</option>
              <option value="audio">Audio</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium mb-2">Strategy</label>
            <select
              value={strategy}
              onChange={(e) => setStrategy(e.target.value as any)}
              className="w-full p-2 border rounded"
            >
              <option value="inverted">Inverted Index</option>
              <option value="sequential">Sequential KNN</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium mb-2">Top‑K</label>
            <input
              type="number"
              value={k}
              inputMode="numeric"
              step={1}
              min={1}
              max={100}
              onChange={(e) => {
                const v = e.currentTarget.value;
                if (v === "") return; // allow empty while typing
                let n = Number(v);
                if (!Number.isFinite(n)) return;
                if (n < 1) n = 1; if (n > 100) n = 100;
                setK(Math.floor(n));
              }}
              onBlur={(e) => { if (e.currentTarget.value === "") setK(10); }}
              className="w-full p-2 border rounded"
            />
          </div>
        </div>

        {statusObj && (
          <div className="p-3 bg-green-50 border border-green-200 rounded">
            <span className="text-sm text-ebony font-medium mr-2">Status:</span>
            <span className={`inline-block px-2 py-1 text-xs rounded mr-2 ${statusObj.codebook ? 'bg-green-200 text-green-800' : 'bg-red-200 text-red-800'}`}>codebook: {String(!!statusObj.codebook)}</span>
            <span className={`inline-block px-2 py-1 text-xs rounded mr-2 ${statusObj.bow ? 'bg-green-200 text-green-800' : 'bg-red-200 text-red-800'}`}>bow: {String(!!statusObj.bow)}</span>
            <span className={`inline-block px-2 py-1 text-xs rounded ${statusObj.inverted ? 'bg-green-200 text-green-800' : 'bg-red-200 text-red-800'}`}>inverted: {String(!!statusObj.inverted)}</span>
          </div>
        )}

        <div className="flex items-center gap-3">
          <input type="file" onChange={onFileChange} className="border rounded p-2" />
          <Button onClick={onSearch} disabled={isSearching || !file} className="bg-cambridge-blue hover:bg-ebony text-alabaster">
            {isSearching ? "Searching..." : "Search"}
          </Button>
        </div>

        {error && (
          <div className="p-3 bg-red-100 border border-red-300 rounded">
            <p className="text-sm text-red-700">{error}</p>
          </div>
        )}

        {timingMs !== null && (
          <p className="text-sm text-davys-gray">Execution time: {timingMs} ms</p>
        )}

        {results && (
          <div>
            <label className="block text-sm font-medium mb-2">Results</label>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {resultItems.map((r, idx) => (
                <div key={idx} className="p-3 border rounded">
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-medium text-ebony truncate" title={r.doc_id}>{r.name}</span>
                    <span className="text-davys-gray">{r.score.toFixed(4)}</span>
                  </div>
                  {modality === "image" ? (
                    <img
                      src={`${API_BASE_URL}/multimedia/thumbnail?modality=${modality}&doc_id=${encodeURIComponent(r.doc_id)}`}
                      alt={r.name}
                      className="w-full h-32 object-cover rounded border"
                      onError={(e) => {
                        (e.target as HTMLImageElement).src = `${API_BASE_URL}/multimedia/preview?modality=${modality}&doc_id=${encodeURIComponent(r.doc_id)}`
                      }}
                    />
                  ) : (
                    <div className="bg-timberwolf h-20 rounded flex items-center justify-center text-davys-gray text-sm">
                      Audio preview requires serving files via API
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
