'use client'

import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { StoreStats } from '@/types/api'

// Query keys
export const dashboardKeys = {
  all: ['dashboard'] as const,
  stats: (storeId: string) => [...dashboardKeys.all, 'stats', storeId] as const,
}

// Get store statistics for dashboard
export function useStoreStats(storeId: string | undefined) {
  return useQuery({
    queryKey: dashboardKeys.stats(storeId || ''),
    queryFn: () => api.get<StoreStats>(`/kaspi/stores/${storeId}/stats`),
    enabled: !!storeId,
    staleTime: 30000, // Cache for 30 seconds
    refetchInterval: 60000, // Refetch every minute
  })
}
