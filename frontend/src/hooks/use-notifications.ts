import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'

export interface Notification {
  id: string
  type: string
  title: string
  message: string | null
  data: Record<string, unknown>
  is_read: boolean
  created_at: string
}

export interface NotificationsResponse {
  notifications: Notification[]
  total: number
  unread_count: number
}

export interface UnreadCountResponse {
  unread_count: number
}

// Fetch notifications
export function useNotifications(limit = 20, offset = 0, unreadOnly = false) {
  return useQuery({
    queryKey: ['notifications', { limit, offset, unreadOnly }],
    queryFn: async () => {
      const params = new URLSearchParams({
        limit: limit.toString(),
        offset: offset.toString(),
        unread_only: unreadOnly.toString(),
      })
      return api.get<NotificationsResponse>(`/notifications?${params}`)
    },
    staleTime: 30 * 1000, // 30 seconds
  })
}

// Fetch unread count (for polling)
export function useUnreadCount() {
  return useQuery({
    queryKey: ['notifications', 'unread-count'],
    queryFn: async () => {
      return api.get<UnreadCountResponse>('/notifications/unread-count')
    },
    staleTime: 30 * 1000, // 30 seconds
    refetchInterval: 30 * 1000, // Poll every 30 seconds
  })
}

// Mark single notification as read
export function useMarkAsRead() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (notificationId: string) => {
      return api.post(`/notifications/${notificationId}/read`)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['notifications'] })
    },
  })
}

// Mark all notifications as read
export function useMarkAllAsRead() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async () => {
      return api.post('/notifications/read-all')
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['notifications'] })
    },
  })
}

// Delete a notification
export function useDeleteNotification() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (notificationId: string) => {
      return api.delete(`/notifications/${notificationId}`)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['notifications'] })
    },
  })
}

// Notification type icons and colors
export function getNotificationMeta(type: string): {
  icon: string
  color: string
  bgColor: string
} {
  switch (type) {
    // Demping
    case 'demping_price_changed':
      return { icon: 'TrendingDown', color: 'text-green-500', bgColor: 'bg-green-500/10' }
    case 'demping_competitor_found':
      return { icon: 'Users', color: 'text-yellow-500', bgColor: 'bg-yellow-500/10' }
    case 'demping_min_reached':
      return { icon: 'AlertTriangle', color: 'text-orange-500', bgColor: 'bg-orange-500/10' }
    case 'demping_session_expired':
      return { icon: 'KeyRound', color: 'text-red-500', bgColor: 'bg-red-500/10' }

    // Orders
    case 'order_new':
      return { icon: 'ShoppingCart', color: 'text-blue-500', bgColor: 'bg-blue-500/10' }
    case 'order_status_changed':
      return { icon: 'Package', color: 'text-purple-500', bgColor: 'bg-purple-500/10' }

    // Referral
    case 'referral_signup':
      return { icon: 'UserPlus', color: 'text-green-500', bgColor: 'bg-green-500/10' }
    case 'referral_paid':
      return { icon: 'Wallet', color: 'text-emerald-500', bgColor: 'bg-emerald-500/10' }
    case 'referral_payout_completed':
      return { icon: 'CreditCard', color: 'text-green-500', bgColor: 'bg-green-500/10' }

    // System
    case 'system_subscription_expiring':
      return { icon: 'Clock', color: 'text-yellow-500', bgColor: 'bg-yellow-500/10' }
    case 'system_subscription_expired':
      return { icon: 'AlertCircle', color: 'text-red-500', bgColor: 'bg-red-500/10' }
    case 'system_store_sync_completed':
      return { icon: 'RefreshCw', color: 'text-green-500', bgColor: 'bg-green-500/10' }
    case 'system_store_sync_failed':
      return { icon: 'XCircle', color: 'text-red-500', bgColor: 'bg-red-500/10' }

    default:
      return { icon: 'Bell', color: 'text-gray-500', bgColor: 'bg-gray-500/10' }
  }
}
