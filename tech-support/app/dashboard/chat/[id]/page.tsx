"use client"

import { useEffect, useState, useRef, use } from "react"
import { useRouter } from "next/navigation"
import { formatDistanceToNow } from "date-fns"
import { ru } from "date-fns/locale"
import {
    ArrowLeft,
    Loader2,
    Send,
    User,
    CheckCircle,
    RotateCcw,
    UserPlus,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
    getSupportChat,
    getChatMessages,
    sendMessage,
    assignChat,
    closeChat,
    reopenChat,
    markMessagesAsRead,
    createChatWebSocket,
    SupportChat,
    SupportMessage,
} from "@/lib/api"
import { supportAuthClient } from "@/lib/auth"
import { cn } from "@/lib/utils"

export default function ChatPage({ params }: { params: Promise<{ id: string }> }) {
    const resolvedParams = use(params)
    const chatId = resolvedParams.id
    const router = useRouter()
    const messagesEndRef = useRef<HTMLDivElement>(null)
    const wsRef = useRef<WebSocket | null>(null)

    const [chat, setChat] = useState<SupportChat | null>(null)
    const [messages, setMessages] = useState<SupportMessage[]>([])
    const [newMessage, setNewMessage] = useState("")
    const [loading, setLoading] = useState(true)
    const [sending, setSending] = useState(false)
    const [actionLoading, setActionLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)

    const currentUser = supportAuthClient.getUser()

    useEffect(() => {
        async function loadData() {
            try {
                const [chatData, messagesData] = await Promise.all([
                    getSupportChat(chatId),
                    getChatMessages(chatId),
                ])
                setChat(chatData)
                setMessages(messagesData.messages)

                // Mark messages as read
                await markMessagesAsRead(chatId)
            } catch (e) {
                setError(e instanceof Error ? e.message : "Ошибка загрузки")
            } finally {
                setLoading(false)
            }
        }
        loadData()

        // Setup WebSocket for real-time messages
        const ws = createChatWebSocket(
            chatId,
            (message) => {
                setMessages((prev) => [...prev, message])
                // Mark as read immediately
                markMessagesAsRead(chatId)
            },
            (error) => {
                console.error("WebSocket error:", error)
            }
        )
        wsRef.current = ws

        return () => {
            ws?.close()
        }
    }, [chatId])

    useEffect(() => {
        // Scroll to bottom when messages change
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
    }, [messages])

    const handleSend = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!newMessage.trim() || sending) return

        setSending(true)
        try {
            const message = await sendMessage(chatId, newMessage)
            setMessages((prev) => [...prev, message])
            setNewMessage("")
        } catch (e) {
            alert(e instanceof Error ? e.message : "Ошибка отправки")
        } finally {
            setSending(false)
        }
    }

    const handleAssign = async () => {
        setActionLoading(true)
        try {
            const updatedChat = await assignChat(chatId)
            setChat(updatedChat)
        } catch (e) {
            alert(e instanceof Error ? e.message : "Ошибка")
        } finally {
            setActionLoading(false)
        }
    }

    const handleClose = async () => {
        setActionLoading(true)
        try {
            const updatedChat = await closeChat(chatId)
            setChat(updatedChat)
        } catch (e) {
            alert(e instanceof Error ? e.message : "Ошибка")
        } finally {
            setActionLoading(false)
        }
    }

    const handleReopen = async () => {
        setActionLoading(true)
        try {
            const updatedChat = await reopenChat(chatId)
            setChat(updatedChat)
        } catch (e) {
            alert(e instanceof Error ? e.message : "Ошибка")
        } finally {
            setActionLoading(false)
        }
    }

    if (loading) {
        return (
            <div className="flex items-center justify-center h-[calc(100vh-8rem)]">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        )
    }

    if (error || !chat) {
        return (
            <div className="text-center py-8">
                <p className="text-red-500 mb-4">{error || "Чат не найден"}</p>
                <Button variant="outline" onClick={() => router.push("/dashboard")}>
                    <ArrowLeft className="mr-2 h-4 w-4" />
                    Назад
                </Button>
            </div>
        )
    }

    return (
        <div className="flex flex-col h-[calc(100vh-8rem)]">
            {/* Header */}
            <div className="flex items-center justify-between pb-4 border-b">
                <div className="flex items-center gap-4">
                    <Button variant="ghost" size="icon" onClick={() => router.push("/dashboard")}>
                        <ArrowLeft className="h-4 w-4" />
                    </Button>
                    <div>
                        <div className="font-medium flex items-center gap-2">
                            {chat.user_name || chat.user_email}
                            <StatusBadge status={chat.status} />
                        </div>
                        <div className="text-sm text-muted-foreground">
                            {chat.user_email}
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    {!chat.assigned_to && chat.status !== "closed" && (
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={handleAssign}
                            disabled={actionLoading}
                        >
                            <UserPlus className="mr-2 h-4 w-4" />
                            Взять в работу
                        </Button>
                    )}
                    {chat.status === "open" && (
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={handleClose}
                            disabled={actionLoading}
                        >
                            <CheckCircle className="mr-2 h-4 w-4" />
                            Закрыть
                        </Button>
                    )}
                    {chat.status === "closed" && (
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={handleReopen}
                            disabled={actionLoading}
                        >
                            <RotateCcw className="mr-2 h-4 w-4" />
                            Открыть снова
                        </Button>
                    )}
                </div>
            </div>

            {/* Messages */}
            <ScrollArea className="flex-1 py-4">
                <div className="space-y-4 px-1">
                    {messages.length === 0 ? (
                        <div className="text-center text-muted-foreground py-8">
                            Нет сообщений
                        </div>
                    ) : (
                        messages.map((msg) => (
                            <div
                                key={msg.id}
                                className={cn(
                                    "flex gap-3",
                                    msg.sender_type === "support" && "flex-row-reverse"
                                )}
                            >
                                <Avatar className="h-8 w-8">
                                    <AvatarFallback>
                                        <User className="h-4 w-4" />
                                    </AvatarFallback>
                                </Avatar>
                                <div
                                    className={cn(
                                        "max-w-[70%] rounded-lg px-4 py-2",
                                        msg.sender_type === "support"
                                            ? "bg-primary text-primary-foreground"
                                            : "bg-muted"
                                    )}
                                >
                                    {msg.sender_type === "support" && msg.sender_name && (
                                        <div className="text-xs opacity-70 mb-1">
                                            {msg.sender_name}
                                        </div>
                                    )}
                                    <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                                    <div
                                        className={cn(
                                            "text-xs mt-1",
                                            msg.sender_type === "support"
                                                ? "text-primary-foreground/70"
                                                : "text-muted-foreground"
                                        )}
                                    >
                                        {formatDistanceToNow(new Date(msg.created_at), {
                                            addSuffix: true,
                                            locale: ru,
                                        })}
                                    </div>
                                </div>
                            </div>
                        ))
                    )}
                    <div ref={messagesEndRef} />
                </div>
            </ScrollArea>

            {/* Input */}
            {chat.status !== "closed" ? (
                <form onSubmit={handleSend} className="flex gap-2 pt-4 border-t">
                    <Input
                        value={newMessage}
                        onChange={(e) => setNewMessage(e.target.value)}
                        placeholder="Введите сообщение..."
                        disabled={sending}
                        className="flex-1"
                    />
                    <Button type="submit" disabled={sending || !newMessage.trim()}>
                        {sending ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                            <Send className="h-4 w-4" />
                        )}
                    </Button>
                </form>
            ) : (
                <div className="text-center text-muted-foreground py-4 border-t">
                    Чат закрыт. Откройте снова, чтобы продолжить общение.
                </div>
            )}
        </div>
    )
}

function StatusBadge({ status }: { status: string }) {
    switch (status) {
        case "open":
            return <Badge variant="success">Открыт</Badge>
        case "pending":
            return <Badge variant="warning">Ожидание</Badge>
        case "closed":
            return <Badge variant="secondary">Закрыт</Badge>
        default:
            return <Badge variant="outline">{status}</Badge>
    }
}
