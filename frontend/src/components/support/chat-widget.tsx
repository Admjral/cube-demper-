"use client"

import { useState, useEffect, useRef } from "react"
import { usePathname } from "next/navigation"
import { formatDistanceToNow } from "date-fns"
import { ru } from "date-fns/locale"
import { MessageCircle, X, Send, Loader2, User, Headphones } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { cn } from "@/lib/utils"
import {
  getUserChat,
  getUserMessages,
  sendUserMessage,
  createUserChatWebSocket,
  SupportMessage,
  SupportChat,
} from "@/lib/support"
import { authClient } from "@/lib/auth"
import { useT } from "@/lib/i18n"

export function SupportChatWidget() {
  const [isOpen, setIsOpen] = useState(false)
  const [chat, setChat] = useState<SupportChat | null>(null)
  const [messages, setMessages] = useState<SupportMessage[]>([])
  const [newMessage, setNewMessage] = useState("")
  const [loading, setLoading] = useState(false)
  const [sending, setSending] = useState(false)
  const [unreadCount, setUnreadCount] = useState(0)

  const messagesEndRef = useRef<HTMLDivElement>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const t = useT()

  const pathname = usePathname()

  // Don't show widget if not authenticated
  const isAuthenticated = authClient.isAuthenticated()

  useEffect(() => {
    if (!isAuthenticated || !isOpen) return

    async function loadChat() {
      setLoading(true)
      try {
        const chatData = await getUserChat()
        setChat(chatData)

        const messagesData = await getUserMessages()
        setMessages(messagesData.messages)

        // Setup WebSocket
        if (chatData.id) {
          wsRef.current?.close()
          const ws = createUserChatWebSocket(
            chatData.id,
            (message) => {
              setMessages((prev) => [...prev, message])
              if (!isOpen && message.sender_type === "support") {
                setUnreadCount((c) => c + 1)
              }
            }
          )
          wsRef.current = ws
        }
      } catch (e) {
        console.error("Failed to load chat:", e)
      } finally {
        setLoading(false)
      }
    }

    loadChat()

    return () => {
      wsRef.current?.close()
    }
  }, [isAuthenticated, isOpen])

  useEffect(() => {
    // Scroll to bottom when messages change
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages])

  useEffect(() => {
    // Clear unread count when opening
    if (isOpen) {
      setUnreadCount(0)
    }
  }, [isOpen])

  const handleSend = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!newMessage.trim() || sending) return

    setSending(true)
    try {
      const message = await sendUserMessage(newMessage)
      setMessages((prev) => [...prev, message])
      setNewMessage("")

      // Refresh chat status (in case it was created)
      if (!chat) {
        const chatData = await getUserChat()
        setChat(chatData)

        // Setup WebSocket if not already done
        if (chatData.id && !wsRef.current) {
          const ws = createUserChatWebSocket(chatData.id, (msg) => {
            setMessages((prev) => [...prev, msg])
          })
          wsRef.current = ws
        }
      }
    } catch (e) {
      console.error("Failed to send message:", e)
    } finally {
      setSending(false)
    }
  }

  if (!isAuthenticated) return null

  // Hide floating widget on dedicated support page
  if (pathname === '/dashboard/support') return null

  return (
    <>
      {/* Floating button */}
      <Button
        onClick={() => setIsOpen(!isOpen)}
        className={cn(
          "fixed bottom-20 right-4 lg:bottom-6 z-50 h-14 w-14 rounded-full shadow-lg",
          isOpen && "bg-muted text-muted-foreground hover:bg-muted"
        )}
        size="icon"
      >
        {isOpen ? (
          <X className="h-6 w-6" />
        ) : (
          <>
            <MessageCircle className="h-6 w-6" />
            {unreadCount > 0 && (
              <span className="absolute -top-1 -right-1 h-5 w-5 rounded-full bg-red-500 text-xs text-white flex items-center justify-center">
                {unreadCount}
              </span>
            )}
          </>
        )}
      </Button>

      {/* Chat panel */}
      {isOpen && (
        <div className="fixed bottom-36 right-4 lg:bottom-24 z-50 w-[calc(100vw-2rem)] max-w-sm bg-background border rounded-lg shadow-xl flex flex-col h-[60vh] max-h-[500px]">
          {/* Header */}
          <div className="flex items-center gap-3 p-4 border-b bg-primary text-primary-foreground rounded-t-lg">
            <div className="h-10 w-10 rounded-full bg-primary-foreground/20 flex items-center justify-center">
              <Headphones className="h-5 w-5" />
            </div>
            <div>
              <h3 className="font-semibold">{t("support.team")}</h3>
              <p className="text-xs opacity-80">{t("support.responseTime")}</p>
            </div>
          </div>

          {/* Messages */}
          <ScrollArea className="flex-1 p-4">
            {loading ? (
              <div className="flex items-center justify-center h-full">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : messages.length === 0 ? (
              <div className="text-center text-muted-foreground py-8">
                <MessageCircle className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>{t("support.writeToUs")}</p>
              </div>
            ) : (
              <div className="space-y-4">
                {messages.map((msg) => (
                  <div
                    key={msg.id}
                    className={cn(
                      "flex gap-2",
                      msg.sender_type === "user" && "flex-row-reverse"
                    )}
                  >
                    <div
                      className={cn(
                        "h-8 w-8 rounded-full flex items-center justify-center shrink-0",
                        msg.sender_type === "user"
                          ? "bg-primary text-primary-foreground"
                          : "bg-muted"
                      )}
                    >
                      {msg.sender_type === "user" ? (
                        <User className="h-4 w-4" />
                      ) : (
                        <Headphones className="h-4 w-4" />
                      )}
                    </div>
                    <div
                      className={cn(
                        "max-w-[75%] rounded-lg px-3 py-2",
                        msg.sender_type === "user"
                          ? "bg-primary text-primary-foreground"
                          : "bg-muted"
                      )}
                    >
                      <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                      <p
                        className={cn(
                          "text-xs mt-1",
                          msg.sender_type === "user"
                            ? "text-primary-foreground/70"
                            : "text-muted-foreground"
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
          <form onSubmit={handleSend} className="p-4 border-t">
            <div className="flex gap-2">
              <Input
                value={newMessage}
                onChange={(e) => setNewMessage(e.target.value)}
                placeholder={t("support.enterMessage")}
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
        </div>
      )}
    </>
  )
}
