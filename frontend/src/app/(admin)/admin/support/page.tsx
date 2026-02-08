'use client'

import { useState, useEffect, useRef } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { api } from '@/lib/api'
import {
  Loader2,
  Send,
  MessageSquare,
  User,
  Headphones,
  Check,
  X,
  RefreshCw,
} from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'
import { ru } from 'date-fns/locale'
import { cn } from '@/lib/utils'

interface SupportChat {
  id: string
  user_id: string
  user_email: string
  user_name: string | null
  status: 'open' | 'closed' | 'pending'
  assigned_to: string | null
  assigned_name: string | null
  last_message: string | null
  last_message_at: string | null
  unread_count: number
  created_at: string
}

interface SupportMessage {
  id: string
  chat_id: string
  sender_id: string
  sender_type: 'user' | 'support'
  sender_name: string | null
  content: string
  is_read: boolean
  created_at: string
}

export default function AdminSupportPage() {
  const [chats, setChats] = useState<SupportChat[]>([])
  const [selectedChat, setSelectedChat] = useState<SupportChat | null>(null)
  const [messages, setMessages] = useState<SupportMessage[]>([])
  const [newMessage, setNewMessage] = useState('')
  const [loading, setLoading] = useState(true)
  const [messagesLoading, setMessagesLoading] = useState(false)
  const [sending, setSending] = useState(false)
  const [statusFilter, setStatusFilter] = useState<string>('')

  const messagesEndRef = useRef<HTMLDivElement>(null)

  // Fetch chats
  const fetchChats = async () => {
    try {
      const params = new URLSearchParams({ limit: '50' })
      if (statusFilter) params.append('status', statusFilter)
      const data = await api.get<{ chats: SupportChat[] }>(`/support/chats?${params}`)
      setChats(data.chats)
    } catch (error) {
      console.error('Failed to fetch chats:', error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchChats()
  }, [statusFilter])

  // Fetch messages when chat selected
  useEffect(() => {
    if (!selectedChat) return

    const fetchMessages = async () => {
      setMessagesLoading(true)
      try {
        const data = await api.get<{ messages: SupportMessage[] }>(
          `/support/chats/${selectedChat.id}/messages`
        )
        setMessages(data.messages)
        // Mark as read
        await api.post(`/support/chats/${selectedChat.id}/read`)
        // Update unread count in list
        setChats(prev =>
          prev.map(c => (c.id === selectedChat.id ? { ...c, unread_count: 0 } : c))
        )
      } catch (error) {
        console.error('Failed to fetch messages:', error)
      } finally {
        setMessagesLoading(false)
      }
    }

    fetchMessages()
  }, [selectedChat?.id])

  // Scroll to bottom when messages change
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // Send message
  const handleSend = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!newMessage.trim() || !selectedChat || sending) return

    setSending(true)
    try {
      const message = await api.post<SupportMessage>(
        `/support/chats/${selectedChat.id}/messages`,
        { content: newMessage }
      )
      setMessages(prev => [...prev, message])
      setNewMessage('')
      // Update last message in list
      setChats(prev =>
        prev.map(c =>
          c.id === selectedChat.id
            ? { ...c, last_message: newMessage, last_message_at: new Date().toISOString() }
            : c
        )
      )
    } catch (error) {
      console.error('Failed to send message:', error)
    } finally {
      setSending(false)
    }
  }

  // Assign chat to self
  const handleAssign = async () => {
    if (!selectedChat) return
    try {
      const updated = await api.post<SupportChat>(`/support/chats/${selectedChat.id}/assign`)
      setSelectedChat(updated)
      setChats(prev => prev.map(c => (c.id === updated.id ? updated : c)))
    } catch (error) {
      console.error('Failed to assign chat:', error)
    }
  }

  // Close chat
  const handleClose = async () => {
    if (!selectedChat) return
    try {
      const updated = await api.post<SupportChat>(`/support/chats/${selectedChat.id}/close`)
      setSelectedChat(updated)
      setChats(prev => prev.map(c => (c.id === updated.id ? updated : c)))
    } catch (error) {
      console.error('Failed to close chat:', error)
    }
  }

  // Reopen chat
  const handleReopen = async () => {
    if (!selectedChat) return
    try {
      const updated = await api.post<SupportChat>(`/support/chats/${selectedChat.id}/reopen`)
      setSelectedChat(updated)
      setChats(prev => prev.map(c => (c.id === updated.id ? updated : c)))
    } catch (error) {
      console.error('Failed to reopen chat:', error)
    }
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'open':
        return <Badge variant="default">Открыт</Badge>
      case 'pending':
        return <Badge variant="secondary">Ожидает</Badge>
      case 'closed':
        return <Badge variant="outline">Закрыт</Badge>
      default:
        return <Badge variant="outline">{status}</Badge>
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Поддержка</h1>
          <p className="text-muted-foreground">Чаты с пользователями</p>
        </div>
        <div className="flex gap-2">
          <select
            value={statusFilter}
            onChange={e => setStatusFilter(e.target.value)}
            className="px-3 py-2 rounded-md border bg-background text-sm"
          >
            <option value="">Все чаты</option>
            <option value="pending">Ожидают</option>
            <option value="open">Открытые</option>
            <option value="closed">Закрытые</option>
          </select>
          <Button variant="outline" size="icon" onClick={fetchChats}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 h-[calc(100vh-220px)]">
        {/* Chat list */}
        <Card className="glass-card lg:col-span-1">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              Чаты ({chats.length})
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <ScrollArea className="h-[calc(100vh-320px)]">
              {chats.length === 0 ? (
                <div className="p-4 text-center text-muted-foreground">
                  <MessageSquare className="h-8 w-8 mx-auto mb-2 opacity-50" />
                  <p className="text-sm">Нет чатов</p>
                </div>
              ) : (
                chats.map(chat => (
                  <div
                    key={chat.id}
                    onClick={() => setSelectedChat(chat)}
                    className={cn(
                      'p-3 border-b cursor-pointer hover:bg-muted/50 transition-colors',
                      selectedChat?.id === chat.id && 'bg-muted'
                    )}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0 flex-1">
                        <p className="font-medium text-sm truncate">
                          {chat.user_email}
                        </p>
                        <p className="text-xs text-muted-foreground truncate">
                          {chat.last_message || 'Нет сообщений'}
                        </p>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        {getStatusBadge(chat.status)}
                        {chat.unread_count > 0 && (
                          <Badge variant="destructive" className="h-5 w-5 p-0 flex items-center justify-center text-xs">
                            {chat.unread_count}
                          </Badge>
                        )}
                      </div>
                    </div>
                    {chat.last_message_at && (
                      <p className="text-xs text-muted-foreground mt-1">
                        {formatDistanceToNow(new Date(chat.last_message_at), {
                          addSuffix: true,
                          locale: ru,
                        })}
                      </p>
                    )}
                  </div>
                ))
              )}
            </ScrollArea>
          </CardContent>
        </Card>

        {/* Messages */}
        <Card className="glass-card lg:col-span-2 flex flex-col">
          {!selectedChat ? (
            <div className="flex-1 flex items-center justify-center text-muted-foreground">
              <div className="text-center">
                <MessageSquare className="h-12 w-12 mx-auto mb-2 opacity-50" />
                <p>Выберите чат</p>
              </div>
            </div>
          ) : (
            <>
              {/* Chat header */}
              <CardHeader className="pb-2 border-b">
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="text-base">{selectedChat.user_email}</CardTitle>
                    <p className="text-xs text-muted-foreground">
                      {selectedChat.user_name || 'Без имени'} • {getStatusBadge(selectedChat.status)}
                    </p>
                  </div>
                  <div className="flex gap-2">
                    {!selectedChat.assigned_to && selectedChat.status !== 'closed' && (
                      <Button size="sm" variant="outline" onClick={handleAssign}>
                        <Check className="h-4 w-4 mr-1" />
                        Взять
                      </Button>
                    )}
                    {selectedChat.status !== 'closed' ? (
                      <Button size="sm" variant="outline" onClick={handleClose}>
                        <X className="h-4 w-4 mr-1" />
                        Закрыть
                      </Button>
                    ) : (
                      <Button size="sm" variant="outline" onClick={handleReopen}>
                        <RefreshCw className="h-4 w-4 mr-1" />
                        Открыть
                      </Button>
                    )}
                  </div>
                </div>
              </CardHeader>

              {/* Messages list */}
              <ScrollArea className="flex-1 p-4">
                {messagesLoading ? (
                  <div className="flex items-center justify-center h-full">
                    <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                  </div>
                ) : messages.length === 0 ? (
                  <div className="text-center text-muted-foreground py-8">
                    <p>Нет сообщений</p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {messages.map(msg => (
                      <div
                        key={msg.id}
                        className={cn(
                          'flex gap-2',
                          msg.sender_type === 'support' && 'flex-row-reverse'
                        )}
                      >
                        <div
                          className={cn(
                            'h-8 w-8 rounded-full flex items-center justify-center shrink-0',
                            msg.sender_type === 'support'
                              ? 'bg-primary text-primary-foreground'
                              : 'bg-muted'
                          )}
                        >
                          {msg.sender_type === 'support' ? (
                            <Headphones className="h-4 w-4" />
                          ) : (
                            <User className="h-4 w-4" />
                          )}
                        </div>
                        <div
                          className={cn(
                            'max-w-[70%] rounded-lg px-3 py-2',
                            msg.sender_type === 'support'
                              ? 'bg-primary text-primary-foreground'
                              : 'bg-muted'
                          )}
                        >
                          <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                          <p
                            className={cn(
                              'text-xs mt-1',
                              msg.sender_type === 'support'
                                ? 'text-primary-foreground/70'
                                : 'text-muted-foreground'
                            )}
                          >
                            {formatDistanceToNow(new Date(msg.created_at), {
                              addSuffix: true,
                              locale: ru,
                            })}
                          </p>
                        </div>
                      </div>
                    ))}
                    <div ref={messagesEndRef} />
                  </div>
                )}
              </ScrollArea>

              {/* Input */}
              {selectedChat.status !== 'closed' && (
                <form onSubmit={handleSend} className="p-4 border-t">
                  <div className="flex gap-2">
                    <Input
                      value={newMessage}
                      onChange={e => setNewMessage(e.target.value)}
                      placeholder="Введите ответ..."
                      disabled={sending}
                      className="flex-1"
                    />
                    <Button type="submit" size="icon" disabled={sending || !newMessage.trim()}>
                      {sending ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Send className="h-4 w-4" />
                      )}
                    </Button>
                  </div>
                </form>
              )}
            </>
          )}
        </Card>
      </div>
    </div>
  )
}
