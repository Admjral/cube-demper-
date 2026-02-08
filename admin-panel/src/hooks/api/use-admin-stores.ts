"use client"

import { useQuery } from "@tanstack/react-query"
import { api } from "@/lib/api"
import type { StoreListResponse } from "@/types/admin"

export function useAdminStores(page: number = 1, pageSize: number = 50) {
  return useQuery({
    queryKey: ["admin", "stores", page, pageSize],
    queryFn: () =>
      api.get<StoreListResponse>(`/admin/stores?page=${page}&page_size=${pageSize}`),
  })
}
