'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { AdminStats, AdminUser, AdminPayment } from '@/types/api'

// Query keys
export const adminKeys = {
  all: ['admin'] as const,
  stats: () => [...adminKeys.all, 'stats'] as const,
  users: () => [...adminKeys.all, 'users'] as const,
  payments: () => [...adminKeys.all, 'payments'] as const,
  subscriptions: () => [...adminKeys.all, 'subscriptions'] as const,
  stores: () => [...adminKeys.all, 'stores'] as const,
}

// Get admin stats
export function useAdminStats() {
  return useQuery({
    queryKey: adminKeys.stats(),
    queryFn: () => api.get<AdminStats>('/admin/stats'),
  })
}

// Get all users
export function useAdminUsers() {
  return useQuery({
    queryKey: adminKeys.users(),
    queryFn: () => api.get<AdminUser[]>('/admin/users'),
  })
}

// Update user
export function useUpdateUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ userId, data }: { userId: string; data: Partial<AdminUser> }) =>
      api.patch<AdminUser>(`/admin/users/${userId}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
    },
  })
}

// Get all payments
export function useAdminPayments() {
  return useQuery({
    queryKey: adminKeys.payments(),
    queryFn: () => api.get<AdminPayment[]>('/admin/payments'),
  })
}

// Get all subscriptions
export function useAdminSubscriptions() {
  return useQuery({
    queryKey: adminKeys.subscriptions(),
    queryFn: () =>
      api.get<Array<{
        id: string
        user_id: string
        user_email: string
        plan: string
        status: string
        current_period_end: string
        created_at: string
      }>>('/admin/subscriptions'),
  })
}

// Update subscription
export function useUpdateSubscription() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ subscriptionId, data }: { subscriptionId: string; data: { status?: string; plan?: string; period_end?: string } }) =>
      api.patch(`/admin/subscriptions/${subscriptionId}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.subscriptions() })
    },
  })
}

// Get all stores
export function useAdminStores() {
  return useQuery({
    queryKey: adminKeys.stores(),
    queryFn: () =>
      api.get<Array<{
        id: string
        user_id: string
        user_email: string
        name: string
        merchant_id: string
        products_count: number
        is_active: boolean
        created_at: string
      }>>('/admin/stores'),
  })
}
