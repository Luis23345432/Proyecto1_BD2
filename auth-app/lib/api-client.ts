// Prefer a runtime-configurable base URL; fallback to current host:8000 when in browser
const envApiBase = (globalThis as any)?.process?.env?.NEXT_PUBLIC_API_BASE_URL as string | undefined
const BASE_URL = envApiBase
  ? envApiBase
  : (typeof window !== "undefined"
      ? `${window.location.protocol}//${window.location.hostname}:8000`
      : "http://127.0.0.1:8000")

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

export async function executeQuery(
  userId: string,
  token: string,
  dbName: string,
  sql: string,
  options?: { limit?: number; offset?: number },
): Promise<QueryResponse> {
  const params = new URLSearchParams()
  if (options?.limit != null) params.set("limit", String(options.limit))
  if (options?.offset != null) params.set("offset", String(options.offset))
  const qs = params.toString()
  const url = `${BASE_URL}/users/${userId}/databases/${dbName}/query${qs ? `?${qs}` : ""}`

  const response = await fetch(url, {
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

// Fetch paginated table records from backend list endpoint
export async function getTableRecords(
  userId: string,
  token: string,
  dbName: string,
  tableName: string,
  limit: number,
  offset: number,
): Promise<{
  rows: Record<string, any>[]
  count: number
  execution_time_ms: number
  metrics: { page_scans: number; disk_reads: number }
}> {
  const url = `${BASE_URL}/users/${userId}/databases/${dbName}/tables/${tableName}/records?limit=${limit}&offset=${offset}`
  const response = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  })

  if (!response.ok) {
    const error: ApiError = await response.json()
    throw { status: response.status, error }
  }

  return response.json()
}
