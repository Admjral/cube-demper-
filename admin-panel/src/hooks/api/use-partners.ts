"use client"

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { api } from "@/lib/api"
import type { PartnerListResponse, PartnerStats, PartnerCreateRequest } from "@/types/admin"
import { toast } from "sonner"

export function usePartners() {
  return useQuery({
    queryKey: ["admin", "partners"],
    queryFn: () => api.get<PartnerListResponse>("/admin/partners"),
  })
}

export function usePartnerStats(partnerId: string) {
  return useQuery({
    queryKey: ["admin", "partners", partnerId, "stats"],
    queryFn: () => api.get<PartnerStats>(`/admin/partners/${partnerId}/stats`),
    enabled: !!partnerId,
  })
}

export function useCreatePartner() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (data: PartnerCreateRequest) =>
      api.post("/admin/partners", data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", "partners"] })
      toast.success("Партнер создан")
    },
    onError: (error: any) => {
      toast.error(error.message || "Ошибка при создании партнера")
    },
  })
}

export function useDeletePartner() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (partnerId: string) => api.delete(`/admin/partners/${partnerId}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", "partners"] })
      toast.success("Партнер удален")
    },
    onError: (error: any) => {
      toast.error(error.message || "Ошибка при удалении партнера")
    },
  })
}
