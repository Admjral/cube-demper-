'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useAuth } from '@/hooks/use-auth'
import type { Subscription, PricingPlan } from '@/types/api'

// Query keys
export const billingKeys = {
  all: ['billing'] as const,
  subscription: () => [...billingKeys.all, 'subscription'] as const,
  plans: () => [...billingKeys.all, 'plans'] as const,
  history: () => [...billingKeys.all, 'history'] as const,
}

// Get current subscription
export function useSubscription() {
  const { user } = useAuth()

  return useQuery({
    queryKey: billingKeys.subscription(),
    queryFn: () => api.get<Subscription | null>('/billing/subscription'),
    enabled: !!user?.id,
  })
}

// Get pricing plans
export function usePricingPlans() {
  return useQuery({
    queryKey: billingKeys.plans(),
    queryFn: () => api.get<PricingPlan[]>('/billing/plans'),
  })
}

// Create payment (TipTop Pay)
export function useCreatePayment() {
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<{ payment_url: string; payment_id: string }>('/billing/subscribe', {
        plan_id: planId,
      }),
  })
}

// Get payment history
export function usePaymentHistory() {
  const { user } = useAuth()

  return useQuery({
    queryKey: billingKeys.history(),
    queryFn: () =>
      api.get<Array<{
        id: string
        amount: number
        status: string
        plan: string
        created_at: string
      }>>('/billing/payments'),
    enabled: !!user?.id,
  })
}

// Cancel subscription
export function useCancelSubscription() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: () =>
      api.post<{ success: boolean }>('/billing/cancel'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: billingKeys.all })
    },
  })
}
