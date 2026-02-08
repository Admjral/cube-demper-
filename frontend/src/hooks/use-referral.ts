'use client'

import { api } from '@/lib/api'

// Types
export interface ReferralStats {
  clicks: number
  registrations: number
  paid_users: number
  total_earned: number
  available_balance: number
  total_withdrawn: number
}

export interface ReferralLead {
  id: string
  email: string
  full_name: string | null
  registered_at: string
  status: 'registered' | 'paid'
  partner_earned: number
}

export interface ReferralLeadsResponse {
  leads: ReferralLead[]
  total: number
}

export interface ReferralTransaction {
  id: string
  type: 'income' | 'payout'
  amount: number
  description: string
  status: 'pending' | 'completed' | 'rejected'
  created_at: string
}

export interface ReferralTransactionsResponse {
  transactions: ReferralTransaction[]
  total: number
}

export interface ReferralLink {
  promo_code: string | null
  referral_link: string | null
}

// API functions
export async function getReferralStats(): Promise<ReferralStats> {
  return api.get<ReferralStats>('/referral/stats')
}

export async function getReferralLeads(limit = 100): Promise<ReferralLeadsResponse> {
  return api.get<ReferralLeadsResponse>(`/referral/leads?limit=${limit}`)
}

export async function getReferralTransactions(limit = 100): Promise<ReferralTransactionsResponse> {
  return api.get<ReferralTransactionsResponse>(`/referral/transactions?limit=${limit}`)
}

export async function getReferralLink(): Promise<ReferralLink> {
  return api.get<ReferralLink>('/referral/link')
}

export async function requestPayout(amount: number, requisites: string): Promise<{ success: boolean; message: string }> {
  return api.post('/referral/payout', { amount, requisites })
}
