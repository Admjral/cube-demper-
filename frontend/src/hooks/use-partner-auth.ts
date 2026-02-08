'use client'

import { useEffect, useState, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import Cookies from 'js-cookie'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8010'
const PARTNER_TOKEN_KEY = 'partner_token'
const PARTNER_KEY = 'partner_user'

export interface Partner {
  id: string
  email: string
  full_name: string | null
  created_at: string
  updated_at: string
}

export interface PartnerAuthState {
  partner: Partner | null
  loading: boolean
}

class PartnerAuthClient {
  private token: string | null = null
  private partner: Partner | null = null

  constructor() {
    if (typeof window !== 'undefined') {
      this.token = Cookies.get(PARTNER_TOKEN_KEY) || null
      const partnerJson = localStorage.getItem(PARTNER_KEY)
      this.partner = (partnerJson && partnerJson !== 'undefined') ? JSON.parse(partnerJson) : null
    }
  }

  getToken(): string | null {
    if (typeof window !== 'undefined') {
      return Cookies.get(PARTNER_TOKEN_KEY) || null
    }
    return this.token
  }

  getPartner(): Partner | null {
    if (typeof window !== 'undefined') {
      const partnerJson = localStorage.getItem(PARTNER_KEY)
      return (partnerJson && partnerJson !== 'undefined') ? JSON.parse(partnerJson) : null
    }
    return this.partner
  }

  private setAuth(token: string, partner: Partner) {
    this.token = token
    this.partner = partner
    Cookies.set(PARTNER_TOKEN_KEY, token, {
      expires: 7,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax'
    })
    localStorage.setItem(PARTNER_KEY, JSON.stringify(partner))
  }

  private clearAuth() {
    this.token = null
    this.partner = null
    Cookies.remove(PARTNER_TOKEN_KEY)
    localStorage.removeItem(PARTNER_KEY)
  }

  async signIn(email: string, password: string): Promise<{ success: boolean, error: string | null }> {
    try {
      const response = await fetch(`${API_URL}/partner/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
      })

      if (!response.ok) {
        const error = await response.json()
        return { success: false, error: error.detail || 'Ошибка входа' }
      }

      const tokenData = await response.json()

      // Fetch partner data using the token
      const partnerResponse = await fetch(`${API_URL}/partner/me`, {
        headers: {
          'Authorization': `Bearer ${tokenData.access_token}`,
          'Content-Type': 'application/json'
        }
      })

      if (!partnerResponse.ok) {
        return { success: false, error: 'Не удалось получить данные партнёра' }
      }

      const partner: Partner = await partnerResponse.json()
      this.setAuth(tokenData.access_token, partner)
      return { success: true, error: null }
    } catch (e) {
      return { success: false, error: 'Ошибка сети' }
    }
  }

  async signOut(): Promise<void> {
    this.clearAuth()
  }

  async getCurrentPartner(): Promise<Partner | null> {
    const token = this.getToken()
    if (!token) {
      return null
    }

    try {
      const response = await fetch(`${API_URL}/partner/me`, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      })

      if (!response.ok) {
        this.clearAuth()
        return null
      }

      const partner: Partner = await response.json()
      this.partner = partner
      localStorage.setItem(PARTNER_KEY, JSON.stringify(partner))
      return partner
    } catch (e) {
      return null
    }
  }
}

const partnerAuthClient = new PartnerAuthClient()

export function usePartnerAuth() {
  const router = useRouter()
  const [state, setState] = useState<PartnerAuthState>({
    partner: null,
    loading: true,
  })

  useEffect(() => {
    const partner = partnerAuthClient.getPartner()
    setState({ partner, loading: false })

    if (partner) {
      partnerAuthClient.getCurrentPartner().then((verifiedPartner) => {
        setState({ partner: verifiedPartner, loading: false })
      })
    }
  }, [])

  const signIn = useCallback(async (email: string, password: string) => {
    const result = await partnerAuthClient.signIn(email, password)
    if (result.success) {
      const partner = partnerAuthClient.getPartner()
      setState({ partner, loading: false })
    }
    return result
  }, [])

  const signOut = useCallback(async () => {
    await partnerAuthClient.signOut()
    setState({ partner: null, loading: false })
    router.push('/partner/login')
  }, [router])

  return {
    ...state,
    signIn,
    signOut,
  }
}

// API helper for partner endpoints
export async function partnerApi<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const token = partnerAuthClient.getToken()

  const response = await fetch(`${API_URL}${endpoint}`, {
    ...options,
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({}))
    throw new Error(error.detail || 'Request failed')
  }

  return response.json()
}
