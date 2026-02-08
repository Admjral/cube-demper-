'use client'

import { useState } from 'react'
import { Bell, Check, Loader2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  useNotifications,
  useUnreadCount,
  useMarkAsRead,
  useMarkAllAsRead,
  getNotificationMeta,
  type Notification,
} from '@/hooks/use-notifications'
import { formatDistanceToNow } from 'date-fns'
import { ru } from 'date-fns/locale'
import {
  TrendingDown,
  Users,
  AlertTriangle,
  KeyRound,
  ShoppingCart,
  Package,
  UserPlus,
  Wallet,
  CreditCard,
  Clock,
  AlertCircle,
  RefreshCw,
  XCircle,
} from 'lucide-react'

const iconMap: Record<string, React.ElementType> = {
  TrendingDown,
  Users,
  AlertTriangle,
  KeyRound,
  ShoppingCart,
  Package,
  UserPlus,
  Wallet,
  CreditCard,
  Clock,
  AlertCircle,
  RefreshCw,
  XCircle,
  Bell,
}

function NotificationItem({
  notification,
  onRead,
}: {
  notification: Notification
  onRead: (id: string) => void
}) {
  const meta = getNotificationMeta(notification.type)
  const IconComponent = iconMap[meta.icon] || Bell

  return (
    <div
      className={`flex gap-3 p-3 border-b last:border-0 cursor-pointer hover:bg-muted/50 transition-colors ${
        !notification.is_read ? 'bg-primary/5' : ''
      }`}
      onClick={() => !notification.is_read && onRead(notification.id)}
    >
      <div className={`p-2 rounded-full shrink-0 ${meta.bgColor}`}>
        <IconComponent className={`h-4 w-4 ${meta.color}`} />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-start justify-between gap-2">
          <p className={`text-sm font-medium ${!notification.is_read ? 'text-foreground' : 'text-muted-foreground'}`}>
            {notification.title}
          </p>
          {!notification.is_read && (
            <div className="w-2 h-2 bg-primary rounded-full shrink-0 mt-1.5" />
          )}
        </div>
        {notification.message && (
          <p className="text-xs text-muted-foreground mt-0.5 line-clamp-2">
            {notification.message}
          </p>
        )}
        <p className="text-xs text-muted-foreground mt-1">
          {formatDistanceToNow(new Date(notification.created_at), {
            addSuffix: true,
            locale: ru,
          })}
        </p>
      </div>
    </div>
  )
}

export function NotificationBell() {
  const [open, setOpen] = useState(false)
  const { data: unreadData } = useUnreadCount()
  const { data: notificationsData, isLoading } = useNotifications(20, 0, false)
  const markAsRead = useMarkAsRead()
  const markAllAsRead = useMarkAllAsRead()

  const unreadCount = unreadData?.unread_count ?? 0
  const notifications = notificationsData?.notifications ?? []

  const handleMarkAsRead = (id: string) => {
    markAsRead.mutate(id)
  }

  const handleMarkAllAsRead = () => {
    markAllAsRead.mutate()
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="ghost" size="icon" className="touch-target relative">
          <Bell className="h-5 w-5" />
          {unreadCount > 0 && (
            <Badge
              className="absolute -top-1 -right-1 h-5 w-5 p-0 flex items-center justify-center text-xs"
              variant="destructive"
            >
              {unreadCount > 9 ? '9+' : unreadCount}
            </Badge>
          )}
          <span className="sr-only">Уведомления</span>
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80 p-0" align="end">
        <div className="flex items-center justify-between px-4 py-3 border-b">
          <h3 className="font-semibold">Уведомления</h3>
          {unreadCount > 0 && (
            <Button
              variant="ghost"
              size="sm"
              className="h-auto py-1 px-2 text-xs"
              onClick={handleMarkAllAsRead}
              disabled={markAllAsRead.isPending}
            >
              {markAllAsRead.isPending ? (
                <Loader2 className="h-3 w-3 animate-spin mr-1" />
              ) : (
                <Check className="h-3 w-3 mr-1" />
              )}
              Прочитать все
            </Button>
          )}
        </div>

        <ScrollArea className="h-[400px]">
          {isLoading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : notifications.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-8 text-center">
              <Bell className="h-10 w-10 text-muted-foreground mb-2" />
              <p className="text-sm text-muted-foreground">Нет уведомлений</p>
            </div>
          ) : (
            notifications.map((notification) => (
              <NotificationItem
                key={notification.id}
                notification={notification}
                onRead={handleMarkAsRead}
              />
            ))
          )}
        </ScrollArea>
      </PopoverContent>
    </Popover>
  )
}
