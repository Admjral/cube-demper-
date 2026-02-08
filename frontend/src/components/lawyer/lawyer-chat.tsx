'use client'

import { useState, useRef, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { Badge } from '@/components/ui/badge'
import { 
  useLawyerChat, 
  useLawyerChatHistory, 
  useClearLawyerChat,
  useSubmitFeedback,
  type LawyerLanguage,
  type LawyerChatMessage 
} from '@/hooks/api/use-lawyer'
import { useAuth } from '@/hooks/use-auth'
import { Send, Loader2, User, Scale, Trash2, ThumbsUp, ThumbsDown, ExternalLink } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import { cn } from '@/lib/utils'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

interface LawyerChatProps {
  language: LawyerLanguage
}

interface Message extends LawyerChatMessage {
  sources?: Array<{
    article_number: string
    title: string
    document_title: string
    source_url?: string
    similarity: number
  }>
}

export function LawyerChat({ language }: LawyerChatProps) {
  const { user } = useAuth()
  const [input, setInput] = useState('')
  const [localMessages, setLocalMessages] = useState<Message[]>([])
  const scrollRef = useRef<HTMLDivElement>(null)

  const { data: history, isLoading: historyLoading } = useLawyerChatHistory()
  const { mutate: sendMessage, isPending: sending } = useLawyerChat()
  const { mutate: clearHistory } = useClearLawyerChat()
  const { mutate: submitFeedback } = useSubmitFeedback()

  // Sync history with local messages
  useEffect(() => {
    if (history) {
      setLocalMessages(history as Message[])
    }
  }, [history])

  // Scroll to bottom on new message
  useEffect(() => {
    if (scrollRef.current) {
      const viewport = scrollRef.current.querySelector('[data-radix-scroll-area-viewport]')
      if (viewport) {
        viewport.scrollTop = viewport.scrollHeight
      }
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

    sendMessage({ message: messageToSend, language }, {
      onSuccess: (response) => {
        const assistantMessage: Message = {
          id: `assistant-${Date.now()}`,
          role: 'assistant',
          content: response.message,
          created_at: new Date().toISOString(),
          sources: response.sources,
        }
        setLocalMessages(prev => [...prev, assistantMessage])
      },
      onError: () => {
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

  const handleClearHistory = () => {
    clearHistory(undefined, {
      onSuccess: () => {
        setLocalMessages([])
      }
    })
  }

  const handleFeedback = (messageId: string, rating: -1 | 1) => {
    submitFeedback({ message_id: messageId, rating })
  }

  return (
    <div className="flex flex-col h-full min-h-0 overflow-hidden">
      {/* Header */}
      <div className="p-4 border-b border-border flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-primary/10 flex items-center justify-center">
            <Scale className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h3 className="font-semibold text-foreground">
              {language === 'kk' ? 'ИИ Заңгер' : 'ИИ Юрист'}
            </h3>
            <p className="text-sm text-muted-foreground">
              {language === 'kk' 
                ? 'ҚР заңнамасы бойынша кеңес' 
                : 'Консультации по законодательству РК'}
            </p>
          </div>
        </div>
        {localMessages.length > 0 && (
          <Button variant="ghost" size="sm" onClick={handleClearHistory}>
            <Trash2 className="h-4 w-4 mr-1" />
            Очистить
          </Button>
        )}
      </div>

      {/* Messages */}
      <ScrollArea ref={scrollRef} className="flex-1 min-h-0 p-4">
        {historyLoading ? (
          <div className="flex items-center justify-center h-32">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : localMessages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-center">
            <Scale className="h-12 w-12 text-muted-foreground/50 mb-3" />
            <p className="text-muted-foreground">
              {language === 'kk' 
                ? 'Заң сұрағыңызды қойыңыз'
                : 'Задайте юридический вопрос'}
            </p>
            <p className="text-sm text-muted-foreground/70 mt-1">
              {language === 'kk'
                ? 'Мысалы: ЖК қандай салықтар төлейді?'
                : 'Например: Какие налоги платит ИП?'}
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            {localMessages.map((message) => (
              <div key={message.id}>
                <div
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
                        <Scale className="h-4 w-4" />
                      )}
                    </AvatarFallback>
                  </Avatar>
                  <div className={cn(
                    'rounded-2xl px-4 py-2 max-w-[85%]',
                    message.role === 'user'
                      ? 'bg-primary text-primary-foreground'
                      : 'bg-muted text-foreground'
                  )}>
                    {message.role === 'user' ? (
                      <p className="text-sm whitespace-pre-wrap">{message.content}</p>
                    ) : (
                      <div className="text-sm prose prose-sm dark:prose-invert max-w-none prose-p:my-1 prose-headings:my-2 prose-ul:my-1 prose-ol:my-1 prose-li:my-0.5 prose-hr:my-2 prose-strong:text-foreground">
                        <ReactMarkdown>{message.content}</ReactMarkdown>
                      </div>
                    )}
                  </div>
                </div>
                
                {/* Sources and feedback for assistant messages */}
                {message.role === 'assistant' && (
                  <div className="ml-11 mt-2 space-y-2">
                    {/* Sources */}
                    {message.sources && message.sources.length > 0 && (
                      <div className="flex flex-wrap gap-1">
                        {message.sources.slice(0, 3).map((source, idx) => (
                          <TooltipProvider key={idx}>
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Badge 
                                  variant="outline" 
                                  className="text-xs cursor-pointer hover:bg-muted"
                                  onClick={() => source.source_url && window.open(source.source_url, '_blank')}
                                >
                                  {source.document_title?.substring(0, 20)}...
                                  <ExternalLink className="h-3 w-3 ml-1" />
                                </Badge>
                              </TooltipTrigger>
                              <TooltipContent>
                                <p className="max-w-xs">
                                  Статья {source.article_number}: {source.title}
                                </p>
                              </TooltipContent>
                            </Tooltip>
                          </TooltipProvider>
                        ))}
                      </div>
                    )}
                    
                    {/* Feedback buttons */}
                    <div className="flex gap-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-6 px-2"
                        onClick={() => handleFeedback(message.id, 1)}
                      >
                        <ThumbsUp className="h-3 w-3" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-6 px-2"
                        onClick={() => handleFeedback(message.id, -1)}
                      >
                        <ThumbsDown className="h-3 w-3" />
                      </Button>
                    </div>
                  </div>
                )}
              </div>
            ))}
            {sending && (
              <div className="flex gap-3">
                <Avatar className="h-8 w-8 shrink-0">
                  <AvatarFallback className="bg-muted">
                    <Scale className="h-4 w-4" />
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
            placeholder={language === 'kk' 
              ? 'Заң сұрағыңызды жазыңыз...'
              : 'Задайте юридический вопрос...'}
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
            Войдите в аккаунт для использования ИИ-юриста
          </p>
        )}
      </form>
    </div>
  )
}
