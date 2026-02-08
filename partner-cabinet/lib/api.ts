import { partnerAuthClient } from "./auth"

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8010"

async function fetchWithAuth<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const token = partnerAuthClient.getToken()

  const response = await fetch(`${API_URL}${endpoint}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...options.headers,
    },
  })

  if (!response.ok) {
    if (response.status === 401) {
      partnerAuthClient.signOut()
      window.location.href = "/login"
    }
    const error = await response.json().catch(() => ({}))
    throw new Error(error.detail || "Ошибка запроса")
  }

  return response.json()
}

// Types
export interface PartnerStats {
  clicks: number
  registrations: number
  paid_users: number
  total_earned: number
  available_balance: number
  total_withdrawn: number
}

export interface PartnerLead {
  id: string
  email: string
  full_name: string | null
  registered_at: string
  status: "registered" | "paid"
  partner_earned: number
}

export interface PartnerTransaction {
  id: string
  type: "income" | "payout"
  amount: number
  description: string
  status: "pending" | "completed" | "rejected"
  created_at: string
}

export interface PromoCodeData {
  promo_code: string | null
  referral_link: string | null
}

// API functions
export async function getPartnerStats(): Promise<PartnerStats> {
  return fetchWithAuth<PartnerStats>("/partner/stats")
}

export async function getPartnerLeads(
  limit = 50,
  offset = 0
): Promise<{ leads: PartnerLead[]; total: number }> {
  return fetchWithAuth(`/partner/leads?limit=${limit}&offset=${offset}`)
}

export async function getPartnerTransactions(
  limit = 50,
  offset = 0
): Promise<{ transactions: PartnerTransaction[]; total: number }> {
  return fetchWithAuth(`/partner/transactions?limit=${limit}&offset=${offset}`)
}

export async function requestPayout(
  amount: number,
  requisites: string
): Promise<{ success: boolean; payout_id: string; message: string }> {
  return fetchWithAuth("/partner/payout", {
    method: "POST",
    body: JSON.stringify({ amount, requisites }),
  })
}

export async function getPromoCode(): Promise<PromoCodeData> {
  return fetchWithAuth<PromoCodeData>("/partner/promo-code")
}
