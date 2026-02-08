'use client'

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useAuth } from '@/hooks/use-auth'
import type { ChatMessage, ChatRequest, ChatResponse } from '@/types/api'

export type AssistantType = 'lawyer' | 'salesman'

// Query keys
export const aiChatKeys = {
  all: ['ai-chat'] as const,
  history: (type: AssistantType) =>
    [...aiChatKeys.all, 'history', type] as const,
}

// Get chat history
export function useChatHistory(assistantType: AssistantType) {
  const { user } = useAuth()

  return useQuery({
    queryKey: aiChatKeys.history(assistantType),
    queryFn: () =>
      api.get<ChatMessage[]>(`/ai/history/${assistantType}`),
    enabled: !!user?.id,
  })
}

// Send message to AI
export function useSendMessage(assistantType: AssistantType) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (message: string) =>
      api.post<ChatResponse>('/ai/chat', {
        assistant_type: assistantType,
        message,
      }),
    onSuccess: () => {
      // Invalidate chat history to refetch
      queryClient.invalidateQueries({
        queryKey: aiChatKeys.history(assistantType),
      })
    },
  })
}

// Clear chat history
export function useClearChat(assistantType: AssistantType) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: () =>
      api.post<{ success: boolean }>('/ai/clear-history', {
        assistant_type: assistantType,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: aiChatKeys.history(assistantType),
      })
    },
  })
}
