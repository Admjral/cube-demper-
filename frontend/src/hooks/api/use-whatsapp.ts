'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'

// ===== Types =====

export interface WhatsAppSession {
  id: string
  user_id: string
  session_name: string
  phone_number: string | null
  status: 'disconnected' | 'connecting' | 'connected' | 'qr_pending' | 'failed'
  last_seen: string | null
  created_at: string
  updated_at: string
}

export interface WhatsAppTemplate {
  id: string
  user_id: string
  name: string
  name_en: string | null
  message: string
  variables: string[]
  is_active: boolean
  trigger_event: string | null
  created_at: string
  updated_at: string
}

export interface WhatsAppMessage {
  id: string
  session_id: string
  template_id: string | null
  recipient_phone: string
  message_content: string
  status: 'pending' | 'sent' | 'delivered' | 'read' | 'failed'
  waha_message_id: string | null
  error_message: string | null
  sent_at: string | null
  created_at: string
}

export interface WhatsAppSettings {
  id: string
  user_id: string
  daily_limit: number
  interval_seconds: number
  work_hours_start: string
  work_hours_end: string
  work_days: number[]
  auto_reply_enabled: boolean
  created_at: string
  updated_at: string
}

export interface SessionQRResponse {
  qr_code: string | null
  status: string
}

// ===== Query Keys =====

export const whatsappKeys = {
  all: ['whatsapp'] as const,
  sessions: () => [...whatsappKeys.all, 'sessions'] as const,
  session: (id: string) => [...whatsappKeys.all, 'session', id] as const,
  sessionQr: (id: string) => [...whatsappKeys.all, 'session', id, 'qr'] as const,
  templates: () => [...whatsappKeys.all, 'templates'] as const,
  settings: () => [...whatsappKeys.all, 'settings'] as const,
}

// ===== Sessions Hooks =====

export function useWhatsAppSessions() {
  return useQuery({
    queryKey: whatsappKeys.sessions(),
    queryFn: () => api.get<WhatsAppSession[]>('/whatsapp/sessions'),
  })
}

export function useCreateSession() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (name: string) =>
      api.post<WhatsAppSession>('/whatsapp/sessions', { name }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeys.sessions() })
    },
  })
}

export function useSessionQRCode(sessionId: string, enabled: boolean = true) {
  return useQuery({
    queryKey: whatsappKeys.sessionQr(sessionId),
    queryFn: () => api.get<SessionQRResponse>(`/whatsapp/sessions/${sessionId}/qr`),
    enabled,
    refetchInterval: (query) => {
      // Stop polling when connected
      if (query.state.data?.status === 'connected') {
        return false
      }
      return 3000 // Poll every 3 seconds
    },
  })
}

export interface PairPhoneResponse {
  code: string
  status: string
}

export function usePairByPhone() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ sessionId, phoneNumber }: { sessionId: string; phoneNumber: string }) =>
      api.post<PairPhoneResponse>(`/whatsapp/sessions/${sessionId}/pair-phone`, {
        phone_number: phoneNumber,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeys.sessions() })
    },
  })
}

export function useDeleteSession() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (sessionId: string) =>
      api.delete<{ success: boolean }>(`/whatsapp/sessions/${sessionId}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeys.sessions() })
    },
  })
}

// ===== Templates Hooks =====

export function useWhatsAppTemplates() {
  return useQuery({
    queryKey: whatsappKeys.templates(),
    queryFn: () => api.get<WhatsAppTemplate[]>('/whatsapp/templates'),
  })
}

export function useCreateTemplate() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (data: {
      name: string
      name_en?: string
      message: string
      variables?: string[]
      is_active?: boolean
      trigger_event?: string
    }) => api.post<WhatsAppTemplate>('/whatsapp/templates', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeys.templates() })
    },
  })
}

export function useUpdateTemplate() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ id, ...data }: { id: string } & Partial<WhatsAppTemplate>) =>
      api.patch<WhatsAppTemplate>(`/whatsapp/templates/${id}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeys.templates() })
    },
  })
}

export function useDeleteTemplate() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (templateId: string) =>
      api.delete<{ success: boolean }>(`/whatsapp/templates/${templateId}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeys.templates() })
    },
  })
}

// ===== Messages Hooks =====

export function useSendWhatsAppMessage() {
  return useMutation({
    mutationFn: (data: {
      session_id: string
      phone: string
      message?: string
      template_id?: string
      variables?: Record<string, string>
    }) => api.post<WhatsAppMessage>('/whatsapp/send', data),
  })
}

// ===== Settings Hooks =====

export function useWhatsAppSettings() {
  return useQuery({
    queryKey: whatsappKeys.settings(),
    queryFn: () => api.get<WhatsAppSettings>('/whatsapp/settings'),
  })
}

export function useUpdateWhatsAppSettings() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (data: Partial<WhatsAppSettings>) =>
      api.patch<WhatsAppSettings>('/whatsapp/settings', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeys.settings() })
    },
  })
}

// ===== Message History Types =====

export interface WhatsAppMessageHistory {
  id: string
  recipient_phone: string
  recipient_name: string | null
  message_content: string
  message_type: string
  status: 'pending' | 'sent' | 'delivered' | 'read' | 'failed'
  template_name: string | null
  error_message: string | null
  sent_at: string | null
  delivered_at: string | null
  read_at: string | null
  created_at: string
}

export interface MessageHistoryResponse {
  messages: WhatsAppMessageHistory[]
  total: number
  page: number
  per_page: number
}

export interface MessageHistoryFilters {
  status_filter?: string
  phone?: string
  date_from?: string
  date_to?: string
  page?: number
  per_page?: number
}

// ===== Statistics Types =====

export interface WhatsAppStats {
  total_sent: number
  total_delivered: number
  total_read: number
  total_failed: number
  total_pending: number
  delivery_rate: number
  read_rate: number
  today_sent: number
  today_limit: number
  messages_by_day: {
    date: string
    sent: number
    delivered: number
    read: number
    failed: number
  }[]
}

// ===== AI Salesman Types =====

export interface AISalesmanSettings {
  store_id: string
  store_name: string
  ai_enabled: boolean
  ai_tone: string | null
  ai_discount_percent: number | null
  ai_promo_code: string | null
  ai_review_bonus: string | null
  ai_send_delay_minutes: number
  ai_max_messages_per_day: number
}

export interface AISalesmanMessage {
  id: string
  order_id: string | null
  customer_phone: string
  customer_name: string | null
  trigger: string
  text: string
  products_suggested: string[]
  created_at: string
}

export interface AISalesmanStats {
  total_messages: number
  by_trigger: Record<string, number>
  by_day: { date: string; count: number }[]
  top_products: { product: string; count: number }[]
}

// ===== Extended Query Keys =====

export const whatsappKeysExtended = {
  ...whatsappKeys,
  messages: (filters?: MessageHistoryFilters) => [...whatsappKeys.all, 'messages', filters] as const,
  stats: (days?: number) => [...whatsappKeys.all, 'stats', days] as const,
  salesmanSettings: () => [...whatsappKeys.all, 'salesman', 'settings'] as const,
  salesmanHistory: (limit?: number) => [...whatsappKeys.all, 'salesman', 'history', limit] as const,
  salesmanStats: (days?: number) => [...whatsappKeys.all, 'salesman', 'stats', days] as const,
}

// ===== Message History Hooks =====

export function useWhatsAppMessages(filters: MessageHistoryFilters = {}) {
  const params = new URLSearchParams()
  if (filters.status_filter) params.set('status_filter', filters.status_filter)
  if (filters.phone) params.set('phone', filters.phone)
  if (filters.date_from) params.set('date_from', filters.date_from)
  if (filters.date_to) params.set('date_to', filters.date_to)
  if (filters.page) params.set('page', String(filters.page))
  if (filters.per_page) params.set('per_page', String(filters.per_page))

  const queryString = params.toString()
  const url = queryString ? `/whatsapp/messages?${queryString}` : '/whatsapp/messages'

  return useQuery({
    queryKey: whatsappKeysExtended.messages(filters),
    queryFn: () => api.get<MessageHistoryResponse>(url),
  })
}

// ===== Statistics Hooks =====

export function useWhatsAppStats(days: number = 7) {
  return useQuery({
    queryKey: whatsappKeysExtended.stats(days),
    queryFn: () => api.get<WhatsAppStats>(`/whatsapp/stats?days=${days}`),
  })
}

// ===== AI Salesman Hooks =====

export function useAISalesmanSettings() {
  return useQuery({
    queryKey: whatsappKeysExtended.salesmanSettings(),
    queryFn: () => api.get<AISalesmanSettings[]>('/ai/salesman/settings'),
  })
}

export function useUpdateAISalesmanSettings() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ storeId, ...data }: { storeId: string } & Partial<AISalesmanSettings>) =>
      api.put<AISalesmanSettings>(`/ai/salesman/settings/${storeId}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeysExtended.salesmanSettings() })
    },
  })
}

export function useAISalesmanHistory(limit: number = 50) {
  return useQuery({
    queryKey: whatsappKeysExtended.salesmanHistory(limit),
    queryFn: () => api.get<{ messages: AISalesmanMessage[]; total: number }>(`/ai/salesman/history?limit=${limit}`),
  })
}

export function useAISalesmanStats(days: number = 7) {
  return useQuery({
    queryKey: whatsappKeysExtended.salesmanStats(days),
    queryFn: () => api.get<AISalesmanStats>(`/ai/salesman/stats?days=${days}`),
  })
}

export function useProcessOrderSalesman() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (data: { order_id: string; send_message?: boolean }) =>
      api.post<{ text: string; trigger: string; products_suggested: string[]; generated_at: string }>(
        '/ai/salesman/process-order',
        data
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: whatsappKeysExtended.salesmanHistory() })
    },
  })
}

// ===== Orders Polling Types =====

export interface OrdersPollingStatus {
  store_id: string
  store_name: string
  orders_polling_enabled: boolean
  last_orders_sync: string | null
}

// ===== Orders Polling Keys =====

export const ordersPollingKeys = {
  all: ['orders-polling'] as const,
  status: (storeId: string) => [...ordersPollingKeys.all, 'status', storeId] as const,
}

// ===== Orders Polling Hooks =====

export function useOrdersPollingStatus(storeId: string | undefined) {
  return useQuery({
    queryKey: ordersPollingKeys.status(storeId || ''),
    queryFn: () => api.get<OrdersPollingStatus>(`/kaspi/stores/${storeId}/orders-polling`),
    enabled: !!storeId,
  })
}

export function useToggleOrdersPolling() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ storeId, enabled }: { storeId: string; enabled: boolean }) =>
      api.patch<OrdersPollingStatus>(`/kaspi/stores/${storeId}/orders-polling`, { enabled }),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ordersPollingKeys.status(data.store_id) })
    },
  })
}


// ===== Customer Contacts =====

export interface CustomerContact {
  id: string
  phone: string
  name: string | null
  store_name: string | null
  orders_count: number
  first_order_code: string | null
  last_order_code: string | null
  is_blocked: boolean
  created_at: string
  updated_at: string
}

export interface ContactsListResponse {
  contacts: CustomerContact[]
  total: number
  page: number
  per_page: number
}

export const contactsKeys = {
  all: ['whatsapp', 'contacts'] as const,
  list: (filters: Record<string, string | number>) => [...contactsKeys.all, filters] as const,
}

export function useCustomerContacts(filters: { store_id?: string; search?: string; page?: number; per_page?: number } = {}) {
  const params = new URLSearchParams()
  if (filters.store_id) params.set('store_id', filters.store_id)
  if (filters.search) params.set('search', filters.search)
  if (filters.page) params.set('page', String(filters.page))
  if (filters.per_page) params.set('per_page', String(filters.per_page))
  const query = params.toString()

  return useQuery({
    queryKey: contactsKeys.list(filters),
    queryFn: () => api.get<ContactsListResponse>(`/whatsapp/contacts${query ? `?${query}` : ''}`),
  })
}

export function useBlockContact() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (contactId: string) => api.patch(`/whatsapp/contacts/${contactId}/block`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: contactsKeys.all }),
  })
}

export function useUnblockContact() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (contactId: string) => api.patch(`/whatsapp/contacts/${contactId}/unblock`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: contactsKeys.all }),
  })
}


// ===== Broadcast Campaigns =====

export interface BroadcastCampaign {
  id: string
  name: string
  message_text: string
  status: 'draft' | 'sending' | 'completed' | 'failed' | 'cancelled'
  scheduled_at: string | null
  started_at: string | null
  completed_at: string | null
  total_recipients: number
  sent_count: number
  failed_count: number
  filter_min_orders: number
  created_at: string
}

export const broadcastKeys = {
  all: ['whatsapp', 'broadcasts'] as const,
  list: () => [...broadcastKeys.all, 'list'] as const,
}

export function useBroadcasts() {
  return useQuery({
    queryKey: broadcastKeys.list(),
    queryFn: () => api.get<BroadcastCampaign[]>('/whatsapp/broadcasts'),
  })
}

export function useCreateBroadcast() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (data: {
      name: string
      message_text: string
      store_id?: string
      template_id?: string
      filter_min_orders?: number
      filter_store_id?: string
    }) => api.post<BroadcastCampaign>('/whatsapp/broadcasts', data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: broadcastKeys.all }),
  })
}

export function useStartBroadcast() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (campaignId: string) => api.post(`/whatsapp/broadcasts/${campaignId}/start`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: broadcastKeys.all }),
  })
}

export function useCancelBroadcast() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (campaignId: string) => api.post(`/whatsapp/broadcasts/${campaignId}/cancel`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: broadcastKeys.all }),
  })
}
