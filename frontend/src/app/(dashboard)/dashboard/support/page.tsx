"use client"

import { useState, useEffect, useRef } from "react"
import { formatDistanceToNow } from "date-fns"
import { Send, Loader2, User, Headphones, MessageCircle } from "lucide-react"

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
import { useTranslation, getDateLocale } from "@/lib/i18n"

export default function SupportPage() {
  const { t, locale } = useTranslation()
  const [chat, setChat] = useState<SupportChat | null>(null)
  const [messages, setMessages] = useState<SupportMessage[]>([])
  const [newMessage, setNewMessage] = useState("")
  const [loading, setLoading] = useState(true)
  const [sending, setSending] = useState(false)

  const messagesEndRef = useRef<HTMLDivElement>(null)
  const wsRef = useRef<WebSocket | null>(null)

  useEffect(() => {
    async function loadChat() {
      setLoading(true)
      try {
        const chatData = await getUserChat()
        setChat(chatData)

        const messagesData = await getUserMessages()
        setMessages(messagesData.messages)

        if (chatData.id) {
          wsRef.current?.close()
          const ws = createUserChatWebSocket(chatData.id, (message) => {
            setMessages((prev) => [...prev, message])
          })
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
  }, [])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [messages])

  const handleSend = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!newMessage.trim() || sending) return

    setSending(true)
    try {
      const message = await sendUserMessage(newMessage)
      setMessages((prev) => [...prev, message])
      setNewMessage("")

      if (!chat) {
        const chatData = await getUserChat()
        setChat(chatData)

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

  return (
    <div className="flex flex-col h-[calc(100vh-8rem)] lg:h-[calc(100vh-4rem)]">
      {/* Header */}
      <div className="flex items-center gap-3 p-4 border-b">
        <div className="h-10 w-10 rounded-full bg-primary/10 flex items-center justify-center">
          <Headphones className="h-5 w-5 text-primary" />
        </div>
        <div>
          <h1 className="font-semibold text-lg">{t("support.pageTitle")}</h1>
          <p className="text-sm text-muted-foreground">{t("support.pageSubtitle")}</p>
        </div>
      </div>

      {/* Messages */}
      <ScrollArea className="flex-1 p-4">
        {loading ? (
          <div className="flex items-center justify-center h-full py-16">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center text-center py-16">
            <MessageCircle className="h-16 w-16 text-muted-foreground/30 mb-4" />
            <p className="text-muted-foreground">{t("support.emptyChat")}</p>
          </div>
        ) : (
          <div className="space-y-4 max-w-2xl mx-auto">
            {messages.map((msg) => (
              <div
                key={msg.id}
                className={cn(
                  "flex gap-3",
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
                    "max-w-[75%] rounded-2xl px-4 py-2.5",
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
                      locale: getDateLocale(locale),
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
        <div className="flex gap-2 max-w-2xl mx-auto">
          <Input
            value={newMessage}
            onChange={(e) => setNewMessage(e.target.value)}
            placeholder={t("support.inputPlaceholder")}
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
  )
}
