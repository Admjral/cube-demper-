'use client'

import { useState, useRef, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { useSendMessage, useChatHistory, type AssistantType } from '@/hooks/api/use-ai-chat'
import { useAuth } from '@/hooks/use-auth'
import { Send, Loader2, User, Bot, Trash2 } from 'lucide-react'
import { cn } from '@/lib/utils'

interface ChatInterfaceProps {
  assistantType: AssistantType
  assistantName: string
  assistantDescription: string
  placeholder?: string
  systemPromptPreview?: string
}

interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  created_at: string
}

export function ChatInterface({
  assistantType,
  assistantName,
  assistantDescription,
  placeholder = 'Введите ваш вопрос...',
  systemPromptPreview,
}: ChatInterfaceProps) {
  const { user } = useAuth()
  const [input, setInput] = useState('')
  const [localMessages, setLocalMessages] = useState<Message[]>([])
  const scrollRef = useRef<HTMLDivElement>(null)

  const { data: history, isLoading: historyLoading } = useChatHistory(assistantType)
  const { mutate: sendMessage, isPending: sending } = useSendMessage(assistantType)

  // Sync history with local messages
  useEffect(() => {
    if (history) {
      setLocalMessages(history)
    }
  }, [history])

  // Scroll to bottom on new message
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [localMessages])

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || sending) return

    const userMessage: Message = {
      id: `temp-${Date.now()}`,
      role: 'user',
      content: input.trim(),
      created_at: new Date().toISOString(),
    }

    setLocalMessages(prev => [...prev, userMessage])
    const messageToSend = input.trim()
    setInput('')

    sendMessage(messageToSend, {
      onSuccess: (response) => {
        const assistantMessage: Message = {
          id: `assistant-${Date.now()}`,
          role: 'assistant',
          content: response.message,
          created_at: new Date().toISOString(),
        }
        setLocalMessages(prev => [...prev, assistantMessage])
      },
      onError: () => {
        // Remove the user message on error
        setLocalMessages(prev => prev.filter(m => m.id !== userMessage.id))
      },
    })
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit(e)
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="p-4 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-primary/10 flex items-center justify-center">
            <Bot className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h3 className="font-semibold text-foreground">{assistantName}</h3>
            <p className="text-sm text-muted-foreground">{assistantDescription}</p>
          </div>
        </div>
        {systemPromptPreview && (
          <p className="mt-3 text-xs text-muted-foreground bg-muted/50 rounded-lg p-2">
            {systemPromptPreview}
          </p>
        )}
      </div>

      {/* Messages */}
      <ScrollArea ref={scrollRef} className="flex-1 p-4">
        {historyLoading ? (
          <div className="flex items-center justify-center h-32">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : localMessages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-center">
            <Bot className="h-12 w-12 text-muted-foreground/50 mb-3" />
            <p className="text-muted-foreground">
              Начните диалог с {assistantName}
            </p>
            <p className="text-sm text-muted-foreground/70 mt-1">
              Задайте любой вопрос по теме
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            {localMessages.map((message) => (
              <div
                key={message.id}
                className={cn(
                  'flex gap-3',
                  message.role === 'user' && 'flex-row-reverse'
                )}
              >
                <Avatar className="h-8 w-8 shrink-0">
                  <AvatarFallback className={cn(
                    message.role === 'user' ? 'bg-primary text-primary-foreground' : 'bg-muted'
                  )}>
                    {message.role === 'user' ? (
                      <User className="h-4 w-4" />
                    ) : (
                      <Bot className="h-4 w-4" />
                    )}
                  </AvatarFallback>
                </Avatar>
                <div
                  className={cn(
                    'rounded-2xl px-4 py-2 max-w-[80%]',
                    message.role === 'user'
                      ? 'bg-primary text-primary-foreground'
                      : 'bg-muted text-foreground'
                  )}
                >
                  <p className="text-sm whitespace-pre-wrap">{message.content}</p>
                </div>
              </div>
            ))}
            {sending && (
              <div className="flex gap-3">
                <Avatar className="h-8 w-8 shrink-0">
                  <AvatarFallback className="bg-muted">
                    <Bot className="h-4 w-4" />
                  </AvatarFallback>
                </Avatar>
                <div className="rounded-2xl px-4 py-2 bg-muted">
                  <Loader2 className="h-4 w-4 animate-spin" />
                </div>
              </div>
            )}
          </div>
        )}
      </ScrollArea>

      {/* Input */}
      <form onSubmit={handleSubmit} className="p-4 border-t border-border">
        <div className="flex gap-2">
          <Textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={placeholder}
            className="min-h-[44px] max-h-32 resize-none"
            disabled={sending || !user}
          />
          <Button
            type="submit"
            size="icon"
            className="h-11 w-11 shrink-0"
            disabled={!input.trim() || sending || !user}
          >
            {sending ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Send className="h-4 w-4" />
            )}
          </Button>
        </div>
        {!user && (
          <p className="text-xs text-muted-foreground mt-2 text-center">
            Войдите в аккаунт для использования ИИ-ассистента
          </p>
        )}
      </form>
    </div>
  )
}
