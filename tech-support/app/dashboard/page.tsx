"use client"

import { useEffect, useState, Suspense } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { formatDistanceToNow } from "date-fns"
import { ru } from "date-fns/locale"
import { Loader2, MessageSquare, User } from "lucide-react"

import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { getSupportChats, SupportChat } from "@/lib/api"
import { cn } from "@/lib/utils"

function ChatList() {
    const router = useRouter()
    const searchParams = useSearchParams()
    const status = searchParams.get("status") || undefined

    const [chats, setChats] = useState<SupportChat[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        async function loadChats() {
            setLoading(true)
            try {
                const data = await getSupportChats(status)
                setChats(data.chats)
            } catch (e) {
                setError(e instanceof Error ? e.message : "Ошибка загрузки")
            } finally {
                setLoading(false)
            }
        }
        loadChats()

        // Polling for new chats every 10 seconds
        const interval = setInterval(loadChats, 10000)
        return () => clearInterval(interval)
    }, [status])

    if (loading && chats.length === 0) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        )
    }

    if (error) {
        return (
            <div className="text-center text-red-500 py-8">
                {error}
            </div>
        )
    }

    if (chats.length === 0) {
        return (
            <Card className="p-8 text-center text-muted-foreground">
                <MessageSquare className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>Нет чатов{status ? ` со статусом "${status}"` : ""}</p>
            </Card>
        )
    }

    return (
        <div className="space-y-2">
            {chats.map((chat) => (
                <Card
                    key={chat.id}
                    className={cn(
                        "p-4 cursor-pointer hover:bg-muted/50 transition-colors",
                        chat.unread_count > 0 && "border-primary/50"
                    )}
                    onClick={() => router.push(`/dashboard/chat/${chat.id}`)}
                >
                    <div className="flex items-start gap-4">
                        <Avatar>
                            <AvatarFallback>
                                <User className="h-4 w-4" />
                            </AvatarFallback>
                        </Avatar>
                        <div className="flex-1 min-w-0">
                            <div className="flex items-center justify-between gap-2">
                                <div className="font-medium truncate">
                                    {chat.user_name || chat.user_email}
                                </div>
                                <div className="flex items-center gap-2">
                                    <StatusBadge status={chat.status} />
                                    {chat.unread_count > 0 && (
                                        <Badge variant="default" className="rounded-full">
                                            {chat.unread_count}
                                        </Badge>
                                    )}
                                </div>
                            </div>
                            <div className="text-sm text-muted-foreground truncate mt-1">
                                {chat.last_message || "Нет сообщений"}
                            </div>
                            <div className="flex items-center justify-between mt-2 text-xs text-muted-foreground">
                                <span>{chat.user_email}</span>
                                {chat.last_message_at && (
                                    <span>
                                        {formatDistanceToNow(new Date(chat.last_message_at), {
                                            addSuffix: true,
                                            locale: ru,
                                        })}
                                    </span>
                                )}
                            </div>
                            {chat.assigned_name && (
                                <div className="text-xs text-muted-foreground mt-1">
                                    Назначен: {chat.assigned_name}
                                </div>
                            )}
                        </div>
                    </div>
                </Card>
            ))}
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

export default function DashboardPage() {
    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h1 className="text-2xl font-bold">Чаты поддержки</h1>
            </div>
            <Suspense fallback={
                <div className="flex items-center justify-center h-64">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
            }>
                <ChatList />
            </Suspense>
        </div>
    )
}
