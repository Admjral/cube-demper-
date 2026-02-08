import { authClient } from './auth'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8010'

export interface SupportMessage {
  id: string
  chat_id: string
  sender_id: string
  sender_type: 'user' | 'support'
  sender_name: string | null
  content: string
  is_read: boolean
  created_at: string
}

export interface SupportChat {
  id: string
  status: 'open' | 'closed' | 'pending'
  created_at: string
}

export interface MessagesResponse {
  messages: SupportMessage[]
  total: number
}

async function fetchWithAuth(url: string, options: RequestInit = {}) {
  const token = authClient.getToken()
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
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

export async function getUserChat(): Promise<SupportChat> {
  return fetchWithAuth('/support/user/chat')
}

export async function getUserMessages(
  limit: number = 100,
  offset: number = 0
): Promise<MessagesResponse> {
  const params = new URLSearchParams()
  params.append('limit', limit.toString())
  params.append('offset', offset.toString())
  return fetchWithAuth(`/support/user/chat/messages?${params.toString()}`)
}

export async function sendUserMessage(content: string): Promise<SupportMessage> {
  return fetchWithAuth('/support/user/chat/messages', {
    method: 'POST',
    body: JSON.stringify({ content }),
  })
}

export function createUserChatWebSocket(
  chatId: string,
  onMessage: (message: SupportMessage) => void,
  onError?: (error: Event) => void
): WebSocket | null {
  const token = authClient.getToken()
  if (!token) return null

  const wsUrl = API_URL.replace('http', 'ws')
  const ws = new WebSocket(`${wsUrl}/support/ws/${chatId}?token=${token}`)

  ws.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data)
      onMessage(message)
    } catch (e) {
      console.error('Failed to parse WebSocket message:', e)
    }
  }

  ws.onerror = (error) => {
    console.error('WebSocket error:', error)
    onError?.(error)
  }

  return ws
}
