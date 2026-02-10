import Cookies from 'js-cookie'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8010'
const TOKEN_KEY = 'auth_token'
const USER_KEY = 'auth_user'

export interface User {
  id: string
  email: string
  full_name: string | null
  phone: string | null
  phone_verified: boolean
  avatar_url: string | null
  role: 'user' | 'admin'
  created_at: string
}

export interface AuthResponse {
  access_token: string
  token_type: string
  user: User
}

class AuthClient {
  private token: string | null = null
  private user: User | null = null

  constructor() {
    if (typeof window !== 'undefined') {
      this.token = Cookies.get(TOKEN_KEY) || null
      const userJson = localStorage.getItem(USER_KEY)
      this.user = (userJson && userJson !== 'undefined') ? JSON.parse(userJson) : null
    }
  }

  getToken(): string | null {
    if (typeof window !== 'undefined') {
      return Cookies.get(TOKEN_KEY) || null
    }
    return this.token
  }

  getUser(): User | null {
    if (typeof window !== 'undefined') {
      const userJson = localStorage.getItem(USER_KEY)
      return (userJson && userJson !== 'undefined') ? JSON.parse(userJson) : null
    }
    return this.user
  }

  isAuthenticated(): boolean {
    return !!this.getToken()
  }

  private setAuth(token: string, user: User) {
    this.token = token
    this.user = user
    Cookies.set(TOKEN_KEY, token, {
      expires: 7,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax'
    })
    localStorage.setItem(USER_KEY, JSON.stringify(user))
  }

  private clearAuth() {
    this.token = null
    this.user = null
    Cookies.remove(TOKEN_KEY)
    localStorage.removeItem(USER_KEY)
  }

  async signIn(email: string, password: string): Promise<{ data: AuthResponse | null, error: Error | null }> {
    try {
      const response = await fetch(`${API_URL}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      })

      if (!response.ok) {
        const error = await response.json()
        return { data: null, error: new Error(error.detail || 'Ошибка входа') }
      }

      const tokenData = await response.json()

      // Fetch user data using the token
      const userResponse = await fetch(`${API_URL}/auth/me`, {
        headers: {
          'Authorization': `Bearer ${tokenData.access_token}`,
          'Content-Type': 'application/json'
        }
      })

      if (!userResponse.ok) {
        return { data: null, error: new Error('Не удалось получить данные пользователя') }
      }

      const user: User = await userResponse.json()
      const data: AuthResponse = {
        access_token: tokenData.access_token,
        token_type: tokenData.token_type,
        user
      }

      this.setAuth(data.access_token, data.user)
      return { data, error: null }
    } catch (e) {
      return { data: null, error: e as Error }
    }
  }

  async signUp(
    email: string,
    password: string,
    metadata?: { full_name?: string; phone?: string; ref_code?: string }
  ): Promise<{ data: AuthResponse | null, error: Error | null }> {
    try {
      const response = await fetch(`${API_URL}/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email,
          password,
          full_name: metadata?.full_name,
          phone: metadata?.phone,
          ref_code: metadata?.ref_code || undefined,
        })
      })

      if (!response.ok) {
        const error = await response.json()
        return { data: null, error: new Error(error.detail || 'Ошибка регистрации') }
      }

      // Backend now returns Token (access_token + token_type)
      const tokenData = await response.json()

      // Fetch user data using the token
      const userResponse = await fetch(`${API_URL}/auth/me`, {
        headers: {
          'Authorization': `Bearer ${tokenData.access_token}`,
          'Content-Type': 'application/json'
        }
      })

      if (!userResponse.ok) {
        return { data: null, error: new Error('Не удалось получить данные пользователя') }
      }

      const user: User = await userResponse.json()
      const data: AuthResponse = {
        access_token: tokenData.access_token,
        token_type: tokenData.token_type,
        user
      }

      this.setAuth(data.access_token, data.user)
      return { data, error: null }
    } catch (e) {
      return { data: null, error: e as Error }
    }
  }

  async sendOtp(): Promise<{ error: Error | null }> {
    const token = this.getToken()
    if (!token) return { error: new Error('Not authenticated') }

    try {
      const response = await fetch(`${API_URL}/auth/send-otp`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      })

      if (!response.ok) {
        const error = await response.json()
        return { error: new Error(error.detail || 'Ошибка отправки кода') }
      }

      return { error: null }
    } catch (e) {
      return { error: e as Error }
    }
  }

  async verifyOtp(code: string): Promise<{ error: Error | null }> {
    const token = this.getToken()
    if (!token) return { error: new Error('Not authenticated') }

    try {
      const response = await fetch(`${API_URL}/auth/verify-otp`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ code })
      })

      if (!response.ok) {
        const error = await response.json()
        return { error: new Error(error.detail || 'Неверный код') }
      }

      // Update local user data
      const user = this.getUser()
      if (user) {
        user.phone_verified = true
        this.user = user
        localStorage.setItem(USER_KEY, JSON.stringify(user))
      }

      return { error: null }
    } catch (e) {
      return { error: e as Error }
    }
  }

  async signOut(): Promise<{ error: Error | null }> {
    this.clearAuth()
    return { error: null }
  }

  async resetPassword(email: string): Promise<{ error: Error | null }> {
    try {
      const response = await fetch(`${API_URL}/auth/forgot-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email })
      })

      if (!response.ok) {
        const error = await response.json()
        return { error: new Error(error.detail || 'Ошибка сброса пароля') }
      }

      return { error: null }
    } catch (e) {
      return { error: e as Error }
    }
  }

  async updatePassword(token: string, newPassword: string): Promise<{ error: Error | null }> {
    try {
      const response = await fetch(`${API_URL}/auth/reset-password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token, new_password: newPassword })
      })

      if (!response.ok) {
        const error = await response.json()
        return { error: new Error(error.detail || 'Ошибка обновления пароля') }
      }

      return { error: null }
    } catch (e) {
      return { error: e as Error }
    }
  }

  async getCurrentUser(): Promise<{ user: User | null, error: Error | null }> {
    const token = this.getToken()
    if (!token) {
      return { user: null, error: null }
    }

    try {
      const response = await fetch(`${API_URL}/auth/me`, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      })

      if (!response.ok) {
        this.clearAuth()
        return { user: null, error: new Error('Сессия истекла') }
      }

      const user: User = await response.json()
      this.user = user
      localStorage.setItem(USER_KEY, JSON.stringify(user))
      return { user, error: null }
    } catch (e) {
      return { user: null, error: e as Error }
    }
  }

  getSession(): { user: User | null, token: string | null } {
    return {
      user: this.getUser(),
      token: this.getToken()
    }
  }
}

export const authClient = new AuthClient()
