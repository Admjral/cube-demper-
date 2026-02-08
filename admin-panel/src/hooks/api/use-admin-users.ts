"use client"

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query"
import { api } from "@/lib/api"
import type { UserListResponse, UserDetailsResponse, ExtendSubscriptionRequest } from "@/types/admin"
import { toast } from "sonner"

export function useAdminUsers(page: number = 1, pageSize: number = 50) {
  return useQuery({
    queryKey: ["admin", "users", page, pageSize],
    queryFn: () =>
      api.get<UserListResponse>(`/admin/users?page=${page}&page_size=${pageSize}`),
  })
}

export function useUserDetails(userId: string) {
  return useQuery({
    queryKey: ["admin", "users", userId, "details"],
    queryFn: () => api.get<UserDetailsResponse>(`/admin/users/${userId}/details`),
    enabled: !!userId,
  })
}

export function useBlockUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ userId, reason }: { userId: string; reason?: string }) =>
      api.post(`/admin/users/${userId}/block`, { reason }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", "users"] })
      queryClient.invalidateQueries({ queryKey: ["admin", "stats"] })
      toast.success("Пользователь заблокирован")
    },
    onError: (error: any) => {
      toast.error(error.message || "Ошибка при блокировке пользователя")
    },
  })
}

export function useUnblockUser() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (userId: string) => api.post(`/admin/users/${userId}/unblock`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", "users"] })
      queryClient.invalidateQueries({ queryKey: ["admin", "stats"] })
      toast.success("Пользователь разблокирован")
    },
    onError: (error: any) => {
      toast.error(error.message || "Ошибка при разблокировке пользователя")
    },
  })
}

export function useExtendSubscription() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ subscriptionId, days }: { subscriptionId: string; days: number }) =>
      api.post(`/admin/subscriptions/${subscriptionId}/extend`, { days } as ExtendSubscriptionRequest),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["admin", "users"] })
      toast.success("Подписка продлена")
    },
    onError: (error: any) => {
      toast.error(error.message || "Ошибка при продлении подписки")
    },
  })
}
