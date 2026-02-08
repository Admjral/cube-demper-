import Cookies from "js-cookie"

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8010"
const TOKEN_KEY = "partner_auth_token"
const PARTNER_KEY = "partner_auth_user"

export interface PartnerUser {
  id: string
  email: string
  full_name: string | null
  created_at: string
  updated_at: string
}

export interface PartnerAuthResponse {
  access_token: string
  token_type: string
  user: PartnerUser
}

class PartnerAuthClient {
  private token: string | null = null
  private user: PartnerUser | null = null

  constructor() {
    if (typeof window !== "undefined") {
      this.token = Cookies.get(TOKEN_KEY) || null
      const userJson = localStorage.getItem(PARTNER_KEY)
      this.user = userJson && userJson !== "undefined" ? JSON.parse(userJson) : null
    }
  }

  getToken(): string | null {
    if (typeof window !== "undefined") {
      return Cookies.get(TOKEN_KEY) || null
    }
    return this.token
  }

  getUser(): PartnerUser | null {
    if (typeof window !== "undefined") {
      const userJson = localStorage.getItem(PARTNER_KEY)
      return userJson && userJson !== "undefined" ? JSON.parse(userJson) : null
    }
    return this.user
  }

  isAuthenticated(): boolean {
    return !!this.getToken()
  }

  private setAuth(token: string, user: PartnerUser) {
    this.token = token
    this.user = user
    Cookies.set(TOKEN_KEY, token, {
      expires: 7,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
    })
    localStorage.setItem(PARTNER_KEY, JSON.stringify(user))
  }

  private clearAuth() {
    this.token = null
    this.user = null
    Cookies.remove(TOKEN_KEY)
    localStorage.removeItem(PARTNER_KEY)
  }

  async signIn(
    email: string,
    password: string,
  ): Promise<{ data: PartnerAuthResponse | null; error: Error | null }> {
    try {
      const response = await fetch(`${API_URL}/partner/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      })

      if (!response.ok) {
        const error = await response.json().catch(() => ({}))
        return { data: null, error: new Error(error.detail || "Ошибка входа") }
      }

      const tokenData = await response.json()

      const meResponse = await fetch(`${API_URL}/partner/me`, {
        headers: {
          Authorization: `Bearer ${tokenData.access_token}`,
          "Content-Type": "application/json",
        },
      })

      if (!meResponse.ok) {
        return {
          data: null,
          error: new Error("Не удалось получить данные партнёра"),
        }
      }

      const user: PartnerUser = await meResponse.json()
      const data: PartnerAuthResponse = {
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

  async getCurrentPartner(): Promise<{ user: PartnerUser | null; error: Error | null }> {
    const token = this.getToken()
    if (!token) {
      return { user: null, error: null }
    }

    try {
      const response = await fetch(`${API_URL}/partner/me`, {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
      })

      if (!response.ok) {
        this.clearAuth()
        return { user: null, error: new Error("Сессия истекла") }
      }

      const user: PartnerUser = await response.json()
      this.user = user
      localStorage.setItem(PARTNER_KEY, JSON.stringify(user))
      return { user, error: null }
    } catch (e) {
      return { user: null, error: e as Error }
    }
  }

  getSession(): { user: PartnerUser | null; token: string | null } {
    return {
      user: this.getUser(),
      token: this.getToken(),
    }
  }
}

export const partnerAuthClient = new PartnerAuthClient()

