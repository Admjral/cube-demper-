"use client"

import { useQuery } from "@tanstack/react-query"
import { api } from "@/lib/api"
import type { AdminStats } from "@/types/admin"

export function useAdminStats() {
  return useQuery({
    queryKey: ["admin", "stats"],
    queryFn: () => api.get<AdminStats>("/admin/stats"),
  })
}
