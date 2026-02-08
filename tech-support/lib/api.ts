import { supportAuthClient } from "./auth"

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8010"

async function fetchWithAuth(url: string, options: RequestInit = {}) {
  const token = supportAuthClient.getToken()
  const headers: HeadersInit = {
    "Content-Type": "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...options.headers,
  }

  const response = await fetch(`${API_URL}${url}`, {
    ...options,
    headers,
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({}))
    throw new Error(error.detail || `HTTP error ${response.status}`)
  }

  return response.json()
}

// Types
export interface SupportChat {
  id: string
  user_id: string
  user_email: string
  user_name: string | null
  status: "open" | "closed" | "pending"
  assigned_to: string | null
  assigned_name: string | null
  last_message: string | null
  last_message_at: string | null
  unread_count: number
  created_at: string
  updated_at: string
}

export interface SupportMessage {
  id: string
  chat_id: string
  sender_id: string
  sender_type: "user" | "support"
  sender_name: string | null
  content: string
  is_read: boolean
  created_at: string
}

export interface ChatsResponse {
  chats: SupportChat[]
  total: number
}

export interface MessagesResponse {
  messages: SupportMessage[]
  total: number
}

// API functions
export async function getSupportChats(
  status?: string,
  limit: number = 50,
  offset: number = 0
): Promise<ChatsResponse> {
  const params = new URLSearchParams()
  if (status) params.append("status", status)
  params.append("limit", limit.toString())
  params.append("offset", offset.toString())

  return fetchWithAuth(`/support/chats?${params.toString()}`)
}

export async function getSupportChat(chatId: string): Promise<SupportChat> {
  return fetchWithAuth(`/support/chats/${chatId}`)
}

export async function getChatMessages(
  chatId: string,
  limit: number = 100,
  offset: number = 0
): Promise<MessagesResponse> {
  const params = new URLSearchParams()
  params.append("limit", limit.toString())
  params.append("offset", offset.toString())

  return fetchWithAuth(`/support/chats/${chatId}/messages?${params.toString()}`)
}

export async function sendMessage(
  chatId: string,
  content: string
): Promise<SupportMessage> {
  return fetchWithAuth(`/support/chats/${chatId}/messages`, {
    method: "POST",
    body: JSON.stringify({ content }),
  })
}

export async function assignChat(chatId: string): Promise<SupportChat> {
  return fetchWithAuth(`/support/chats/${chatId}/assign`, {
    method: "POST",
  })
}

export async function closeChat(chatId: string): Promise<SupportChat> {
  return fetchWithAuth(`/support/chats/${chatId}/close`, {
    method: "POST",
  })
}

export async function reopenChat(chatId: string): Promise<SupportChat> {
  return fetchWithAuth(`/support/chats/${chatId}/reopen`, {
    method: "POST",
  })
}

export async function markMessagesAsRead(chatId: string): Promise<{ success: boolean }> {
  return fetchWithAuth(`/support/chats/${chatId}/read`, {
    method: "POST",
  })
}

// WebSocket connection for real-time messages
export function createChatWebSocket(
  chatId: string,
  onMessage: (message: SupportMessage) => void,
  onError?: (error: Event) => void
): WebSocket | null {
  const token = supportAuthClient.getToken()
  if (!token) return null

  const wsUrl = API_URL.replace("http", "ws")
  const ws = new WebSocket(`${wsUrl}/support/ws/${chatId}?token=${token}`)

  ws.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data)
      onMessage(message)
    } catch (e) {
      console.error("Failed to parse WebSocket message:", e)
    }
  }

  ws.onerror = (error) => {
    console.error("WebSocket error:", error)
    onError?.(error)
  }

  return ws
}
