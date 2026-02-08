'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { SalesAnalytics, TopProduct } from '@/types/api'

// Query keys
export const analyticsKeys = {
  all: ['analytics'] as const,
  sales: (storeId: string, period: string) => [...analyticsKeys.all, 'sales', storeId, period] as const,
  topProducts: (storeId: string) => [...analyticsKeys.all, 'top-products', storeId] as const,
}

// Sync orders response
export interface SyncOrdersResponse {
  status: string
  message: string
  store_id: string
}

// Get sales analytics
export function useSalesAnalytics(
  storeId: string | undefined,
  period: '7d' | '30d' | '90d' = '7d'
) {
  return useQuery({
    queryKey: analyticsKeys.sales(storeId || '', period),
    queryFn: () => api.get<SalesAnalytics>(`/kaspi/stores/${storeId}/analytics?period=${period}`),
    enabled: !!storeId,
    staleTime: 60000, // Cache for 1 minute
  })
}

// Get top products
export function useTopProducts(storeId: string | undefined, limit: number = 10) {
  return useQuery({
    queryKey: analyticsKeys.topProducts(storeId || ''),
    queryFn: () => api.get<TopProduct[]>(`/kaspi/stores/${storeId}/top-products?limit=${limit}`),
    enabled: !!storeId,
    staleTime: 60000,
  })
}

// Sync orders from Kaspi
export function useSyncOrders() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ storeId, daysBack = 30 }: { storeId: string; daysBack?: number }) =>
      api.post<SyncOrdersResponse>(`/kaspi/stores/${storeId}/sync-orders?days_back=${daysBack}`),
    onSuccess: (_, { storeId }) => {
      // Invalidate analytics after a delay to allow sync to complete
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: analyticsKeys.all })
      }, 5000)
    },
  })
}
