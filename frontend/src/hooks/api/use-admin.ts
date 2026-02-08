'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type {
  AdminStats,
  AdminUser,
  AdminUserListResponse,
  AdminStore,
  AdminStoreListResponse,
  AdminPayment,
  AdminPaymentListResponse,
} from '@/types/api'
import type { Plan, Addon } from '@/hooks/api/use-features'

// Types for subscription details
export interface UserSubscriptionDetails {
  user_id: string
  subscription: {
    id: string | null
    plan_id: string | null
    plan_code: string | null
    plan_name: string | null
    status: string | null
    analytics_limit: number
    demping_limit: number
    is_trial: boolean
    trial_ends_at: string | null
    ends_at: string | null
    notes: string | null
  } | null
  addons: Array<{
    id: string
    code: string
    name: string
    quantity: number
    status: string
    starts_at: string
    expires_at: string | null
  }>
  computed_features: string[]
  computed_limits: {
    analytics_limit: number
    demping_limit: number
  }
}

// Query keys
export const adminKeys = {
  all: ['admin'] as const,
  stats: () => [...adminKeys.all, 'stats'] as const,
  users: (page?: number) => [...adminKeys.all, 'users', page] as const,
  stores: (page?: number) => [...adminKeys.all, 'stores', page] as const,
  payments: (page?: number) => [...adminKeys.all, 'payments', page] as const,
  subscriptions: () => [...adminKeys.all, 'subscriptions'] as const,
}

// Get admin stats
export function useAdminStats() {
  return useQuery({
    queryKey: adminKeys.stats(),
    queryFn: () => api.get<AdminStats>('/admin/stats'),
  })
}

// Get all users
export function useAdminUsers(page: number = 1, pageSize: number = 50) {
  return useQuery({
    queryKey: adminKeys.users(page),
    queryFn: () => api.get<AdminUserListResponse>(`/admin/users?page=${page}&page_size=${pageSize}`),
  })
}

// Update user role
export function useUpdateUserRole() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ userId, role }: { userId: string; role: string }) =>
      api.patch('/admin/users/role', { user_id: userId, role }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
    },
  })
}

// Block user
export function useBlockUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ userId, reason }: { userId: string; reason?: string }) =>
      api.post(`/admin/users/${userId}/block`, { reason }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
      queryClient.invalidateQueries({ queryKey: adminKeys.stats() })
    },
  })
}

// Unblock user
export function useUnblockUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (userId: string) => api.post(`/admin/users/${userId}/unblock`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
      queryClient.invalidateQueries({ queryKey: adminKeys.stats() })
    },
  })
}

// Delete user
export function useDeleteUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (userId: string) => api.delete(`/admin/users/${userId}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
      queryClient.invalidateQueries({ queryKey: adminKeys.stats() })
    },
  })
}

// Get all stores
export function useAdminStores(page: number = 1, pageSize: number = 50) {
  return useQuery({
    queryKey: adminKeys.stores(page),
    queryFn: () => api.get<AdminStoreListResponse>(`/admin/stores?page=${page}&page_size=${pageSize}`),
  })
}

// Get all payments
export function useAdminPayments(page: number = 1, pageSize: number = 50) {
  return useQuery({
    queryKey: adminKeys.payments(page),
    queryFn: () => api.get<AdminPaymentListResponse>(`/admin/payments?page=${page}&page_size=${pageSize}`),
  })
}

// Get all subscriptions (uses users with subscription info)
export function useAdminSubscriptions() {
  return useQuery({
    queryKey: adminKeys.subscriptions(),
    queryFn: () => api.get<AdminUserListResponse>('/admin/users?page=1&page_size=100'),
    select: (data) => {
      // Filter only users with active subscriptions
      return data.users.filter(u => u.subscription_status === 'active')
    },
  })
}

// Extend subscription
export function useExtendSubscription() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ subscriptionId, days }: { subscriptionId: string; days: number }) =>
      api.post(`/admin/subscriptions/${subscriptionId}/extend`, { days }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
      queryClient.invalidateQueries({ queryKey: adminKeys.subscriptions() })
    },
  })
}

// Get user subscription details
export function useUserSubscriptionDetails(userId: string, enabled = true) {
  return useQuery({
    queryKey: [...adminKeys.all, 'user-subscription', userId],
    queryFn: () => api.get<UserSubscriptionDetails>(`/admin/users/${userId}/subscription-details`),
    enabled: enabled && !!userId,
  })
}

// Assign subscription to user
export function useAssignSubscription() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({
      userId,
      planCode,
      days,
      isTrial,
      notes
    }: {
      userId: string
      planCode: string
      days?: number
      isTrial?: boolean
      notes?: string
    }) =>
      api.post(`/admin/users/${userId}/subscription`, {
        plan_code: planCode,
        days: days ?? 30,
        is_trial: isTrial ?? false,
        notes,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
      queryClient.invalidateQueries({ queryKey: adminKeys.subscriptions() })
      queryClient.invalidateQueries({ queryKey: [...adminKeys.all, 'user-subscription'] })
    },
  })
}

// Assign add-on to user
export function useAssignAddon() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({
      userId,
      addonCode,
      quantity,
      days
    }: {
      userId: string
      addonCode: string
      quantity?: number
      days?: number
    }) =>
      api.post(`/admin/users/${userId}/addon`, {
        addon_code: addonCode,
        quantity: quantity ?? 1,
        days: days ?? 30,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
      queryClient.invalidateQueries({ queryKey: [...adminKeys.all, 'user-subscription'] })
    },
  })
}

// Remove add-on from user
export function useRemoveAddon() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ userId, addonCode }: { userId: string; addonCode: string }) =>
      api.delete(`/admin/users/${userId}/addon/${addonCode}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: adminKeys.users() })
      queryClient.invalidateQueries({ queryKey: [...adminKeys.all, 'user-subscription'] })
    },
  })
}
