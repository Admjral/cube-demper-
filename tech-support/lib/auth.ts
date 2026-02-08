import Cookies from "js-cookie"

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8010"
const TOKEN_KEY = "support_auth_token"
const USER_KEY = "support_auth_user"

export interface SupportUser {
  id: string
  email: string
  full_name: string | null
  role: string
  created_at: string
}

export interface SupportAuthResponse {
  access_token: string
  token_type: string
  user: SupportUser
}

class SupportAuthClient {
  private token: string | null = null
  private user: SupportUser | null = null

  constructor() {
    if (typeof window !== "undefined") {
      this.token = Cookies.get(TOKEN_KEY) || null
      const userJson = localStorage.getItem(USER_KEY)
      this.user = userJson && userJson !== "undefined" ? JSON.parse(userJson) : null
    }
  }

  getToken(): string | null {
    if (typeof window !== "undefined") {
      return Cookies.get(TOKEN_KEY) || null
    }
    return this.token
  }

  getUser(): SupportUser | null {
    if (typeof window !== "undefined") {
      const userJson = localStorage.getItem(USER_KEY)
      return userJson && userJson !== "undefined" ? JSON.parse(userJson) : null
    }
    return this.user
  }

  isAuthenticated(): boolean {
    return !!this.getToken()
  }

  private setAuth(token: string, user: SupportUser) {
    this.token = token
    this.user = user
    Cookies.set(TOKEN_KEY, token, {
      expires: 7,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
    })
    localStorage.setItem(USER_KEY, JSON.stringify(user))
  }

  private clearAuth() {
    this.token = null
    this.user = null
    Cookies.remove(TOKEN_KEY)
    localStorage.removeItem(USER_KEY)
  }

  async signIn(
    email: string,
    password: string,
  ): Promise<{ data: SupportAuthResponse | null; error: Error | null }> {
    try {
      const response = await fetch(`${API_URL}/support/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      })

      if (!response.ok) {
        const error = await response.json().catch(() => ({}))
        return { data: null, error: new Error(error.detail || "Ошибка входа") }
      }

      const tokenData = await response.json()

      const meResponse = await fetch(`${API_URL}/support/me`, {
        headers: {
          Authorization: `Bearer ${tokenData.access_token}`,
          "Content-Type": "application/json",
        },
      })

      if (!meResponse.ok) {
        return {
          data: null,
          error: new Error("Не удалось получить данные пользователя"),
        }
      }

      const user: SupportUser = await meResponse.json()

      // Проверка роли - только admin и support могут входить
      if (user.role !== "admin" && user.role !== "support") {
        return {
          data: null,
          error: new Error("Доступ запрещён. Требуется роль admin или support"),
        }
      }

      const data: SupportAuthResponse = {
        access_token: tokenData.access_token,
        token_type: tokenData.token_type,
        user,
      }

      this.setAuth(data.access_token, data.user)
      return { data, error: null }
    } catch (e) {
      return { data: null, error: e as Error }
    }
  }

  async signOut(): Promise<{ error: Error | null }> {
    this.clearAuth()
    return { error: null }
  }

  async getCurrentUser(): Promise<{ user: SupportUser | null; error: Error | null }> {
    const token = this.getToken()
    if (!token) {
      return { user: null, error: null }
    }

    try {
      const response = await fetch(`${API_URL}/support/me`, {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      })

      if (!response.ok) {
        this.clearAuth()
        return { user: null, error: new Error("Сессия истекла") }
      }

      const user: SupportUser = await response.json()
      this.user = user
      localStorage.setItem(USER_KEY, JSON.stringify(user))
      return { user, error: null }
    } catch (e) {
      return { user: null, error: e as Error }
    }
  }

  getSession(): { user: SupportUser | null; token: string | null } {
    return {
      user: this.getUser(),
      token: this.getToken(),
    }
  }
}

export const supportAuthClient = new SupportAuthClient()
