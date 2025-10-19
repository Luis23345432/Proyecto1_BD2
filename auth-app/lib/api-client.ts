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
