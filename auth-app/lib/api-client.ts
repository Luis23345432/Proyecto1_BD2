const BASE_URL = "http://127.0.0.1:8000"

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
