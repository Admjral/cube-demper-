'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useAuth } from '@/hooks/use-auth'
import type {
  KaspiStore,
  KaspiAuthRequest,
  KaspiAuthResponse,
  SyncStoreResponse,
} from '@/types/api'

// Query keys
export const storeKeys = {
  all: ['stores'] as const,
  list: (userId: string) => [...storeKeys.all, 'list', userId] as const,
  detail: (storeId: string) => [...storeKeys.all, 'detail', storeId] as const,
}

// Get all stores for user
export function useStores() {
  const { user } = useAuth()

  return useQuery({
    queryKey: storeKeys.list(user?.id || ''),
    queryFn: () =>
      api.get<KaspiStore[]>('/kaspi/stores'),
    enabled: !!user?.id,
  })
}

// Connect Kaspi account (authenticate and create store)
export function useKaspiAuth() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (data: KaspiAuthRequest) =>
      api.post<KaspiAuthResponse>('/kaspi/auth', data),
    onSuccess: () => {
      // Invalidate stores list to refetch
      queryClient.invalidateQueries({ queryKey: storeKeys.all })
    },
  })
}

// Delete store
export function useDeleteStore() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (storeId: string) =>
      api.delete<{ success: boolean }>(`/kaspi/stores/${storeId}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: storeKeys.all })
    },
  })
}

// Sync store products
export function useSyncStore() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (storeId: string) =>
      api.post<SyncStoreResponse>(`/kaspi/stores/${storeId}/sync`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: storeKeys.all })
    },
  })
}

// Update store API token
export function useUpdateStoreApiToken() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ storeId, apiToken }: { storeId: string; apiToken: string }) =>
      api.patch<{ status: string; message: string }>(`/kaspi/stores/${storeId}/api-token`, { api_token: apiToken }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: storeKeys.all })
    },
  })
}
