// Prefer a runtime-configurable base URL; fallback to current host:8000 when in browser
const envApiBase = (globalThis as any)?.process?.env?.NEXT_PUBLIC_API_BASE_URL as string | undefined
const BASE_URL = envApiBase
  ? envApiBase
  : (typeof window !== "undefined"
      ? `${window.location.protocol}//${window.location.hostname}:8000`
      : "http://127.0.0.1:8000")

export const API_BASE_URL = BASE_URL

export interface RegisterRequest {
  username: string
  password: string
}

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  access_token: string
  token_type: string
}

export interface RegisterResponse {
  username: string
}

export interface ApiError {
  detail:
    | string
    | Array<{
        type: string
        loc: string[]
        msg: string
        input: string
        ctx?: { min_length: number }
      }>
}

export interface Database {
  name: string
}

export interface Table {
  name: string
}

export interface QueryRequest {
  sql: string
}

export interface QueryResponse {
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

export async function register(data: RegisterRequest): Promise<RegisterResponse> {
  const response = await fetch(`${BASE_URL}/users/register`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }

  return response.json()
}

export async function login(data: LoginRequest): Promise<LoginResponse> {
  const response = await fetch(`${BASE_URL}/users/login`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }

  return response.json()
}

export async function getDatabases(userId: string, token: string): Promise<Database[]> {
  const response = await fetch(`${BASE_URL}/users/${userId}/databases`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }

  return response.json()
}

export async function createDatabase(userId: string, token: string, dbName: string): Promise<Database> {
  const response = await fetch(`${BASE_URL}/users/${userId}/databases`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ name: dbName }),
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }

  return response.json()
}

export async function deleteDatabase(userId: string, token: string, dbName: string): Promise<void> {
  const response = await fetch(`${BASE_URL}/users/${userId}/databases/${dbName}`, {
    method: "DELETE",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }
}

export async function getTables(userId: string, token: string, dbName: string): Promise<Table[]> {
  const response = await fetch(`${BASE_URL}/users/${userId}/databases/${dbName}/tables`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }

  return response.json()
}

export async function executeQuery(userId: string, token: string, dbName: string, sql: string): Promise<QueryResponse> {
  const response = await fetch(`${BASE_URL}/users/${userId}/databases/${dbName}/query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ sql }),
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }

  return response.json()
}

export async function uploadCSV(
  userId: string,
  token: string,
  dbName: string,
  tableName: string,
  file: File,
): Promise<{ ok: boolean; inserted: number }> {
  const formData = new FormData()
  formData.append("file", file)

  const response = await fetch(`${BASE_URL}/users/${userId}/databases/${dbName}/tables/${tableName}/load-csv`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
    },
    body: formData,
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw {
      status: response.status,
      error,
    }
  }

  return response.json()
}

export interface MultimediaSearchResult {
  ok?: boolean
  error?: string
  results?: Array<{ doc_id: string; score: number }>
}

export async function multimediaSearch(
  file: File,
  modality: "image" | "audio",
  strategy: "sequential" | "inverted" = "inverted",
  k: number = 10,
): Promise<MultimediaSearchResult> {
  const formData = new FormData()
  formData.append("file", file)
  const url = `${BASE_URL}/multimedia/search?modality=${modality}&strategy=${strategy}&k=${k}`
  const response = await fetch(url, {
    method: "POST",
    body: formData,
  })
  const data = await response.json()
  if (!response.ok || data?.ok === false) {
    const error: ApiError = (data && data.error) ? { detail: data.error } : data
    throw { status: response.status, error }
  }
  return data
}

export async function multimediaTrainCodebook(
  modality: "image" | "audio",
  dataRoot: string,
  k: number = 512,
  perObjectCap: number = 500,
  globalCap: number = 200000,
): Promise<{ ok: boolean; modality?: string; k?: number; paths?: number; error?: string }> {
  const response = await fetch(`${BASE_URL}/multimedia/train-codebook`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      modality,
      data_root: dataRoot,
      k,
      per_object_cap: perObjectCap,
      global_cap: globalCap,
    }),
  })
  return response.json()
}

export async function multimediaBuildIndex(
  modality: "image" | "audio",
  dataRoot: string,
  indexType: "bow" | "inverted" = "inverted",
): Promise<{ ok: boolean; modality?: string; index_type?: string; count?: number; error?: string }> {
  const response = await fetch(`${BASE_URL}/multimedia/index`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      modality,
      data_root: dataRoot,
      index_type: indexType,
    }),
  })
  return response.json()
}

export async function multimediaStatus(modality?: "image" | "audio") {
  const url = modality
    ? `${BASE_URL}/multimedia/status?modality=${modality}`
    : `${BASE_URL}/multimedia/status`
  const response = await fetch(url, { method: "GET" })
  return response.json()
}

// ---- SPIMI (Full-Text) helpers ----
export async function spimiBuildIndex(
  userId: string,
  token: string,
  dbName: string,
  tableName: string,
  opts: { column?: string; columns?: string[]; blockMaxDocs?: number } = {},
): Promise<{ ok: boolean; total_documents?: number; index_dir?: string; message?: string }> {
  const params = new URLSearchParams()
  if (opts.column) params.set("column", opts.column)
  if (opts.columns && opts.columns.length) {
    // FastAPI can parse repeated params as list: columns=col1&columns=col2
    opts.columns.forEach((c) => params.append("columns", c))
  }
  if (opts.blockMaxDocs) params.set("block_max_docs", String(opts.blockMaxDocs))
  const url = `${BASE_URL}/users/${userId}/databases/${dbName}/tables/${tableName}/spimi/build?${params.toString()}`
  const resp = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!resp.ok) {
    const error: ApiError = await resp.json()
    throw { status: resp.status, error }
  }
  return resp.json()
}
