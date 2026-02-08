"use client"

import { useState } from "react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { formatPrice } from "@/lib/utils"
import { ExtendSubscriptionDialog } from "./extend-subscription-dialog"
import { useExtendSubscription } from "@/hooks/api/use-admin-users"
import { Calendar } from "lucide-react"
import type { UserDetailsResponse } from "@/types/admin"

interface UserDetailsDialogProps {
  user: UserDetailsResponse
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function UserDetailsDialog({ user, open, onOpenChange }: UserDetailsDialogProps) {
  const [extendDialogOpen, setExtendDialogOpen] = useState(false)
  const extendSubscription = useExtendSubscription()

  const handleExtend = (days: number) => {
    if (user.subscription) {
      extendSubscription.mutate({ subscriptionId: user.subscription.id, days })
      setExtendDialogOpen(false)
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Детали пользователя</DialogTitle>
            <DialogDescription>{user.email}</DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div>
              <h3 className="font-semibold mb-2">Основная информация</h3>
              <div className="space-y-1 text-sm">
                <p><strong>Имя:</strong> {user.full_name || "—"}</p>
                <p><strong>Email:</strong> {user.email}</p>
                <p><strong>Роль:</strong> {user.role}</p>
                <p><strong>Статус:</strong> {user.is_blocked ? "Заблокирован" : "Активен"}</p>
                {user.partner_name && (
                  <p><strong>Партнер:</strong> {user.partner_name}</p>
                )}
              </div>
            </div>

            {user.subscription && (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <h3 className="font-semibold">Подписка</h3>
                  {user.subscription.status === 'active' && (
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => setExtendDialogOpen(true)}
                    >
                      <Calendar className="h-4 w-4 mr-2" />
                      Продлить
                    </Button>
                  )}
                </div>
                <div className="space-y-1 text-sm">
                  <p><strong>Тариф:</strong> {user.subscription.plan}</p>
                  <p><strong>Статус:</strong> {user.subscription.status}</p>
                  <p><strong>Лимит продуктов:</strong> {user.subscription.products_limit}</p>
                  <p><strong>Начало:</strong> {new Date(user.subscription.current_period_start).toLocaleString("ru-RU")}</p>
                  <p><strong>Окончание:</strong> {new Date(user.subscription.current_period_end).toLocaleString("ru-RU")}</p>
                </div>
              </div>
            )}

          {user.stores.length > 0 && (
            <div>
              <h3 className="font-semibold mb-2">Магазины ({user.stores.length})</h3>
              <div className="space-y-2">
                {user.stores.map((store) => (
                  <div key={store.id} className="p-2 border rounded text-sm">
                    <p><strong>{store.name}</strong></p>
                    <p className="text-muted-foreground">Продуктов: {store.products_count}</p>
                    <p className="text-muted-foreground">Статус: {store.is_active ? "Активен" : "Неактивен"}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {user.payments.length > 0 && (
            <div>
              <h3 className="font-semibold mb-2">Платежи ({user.payments.length})</h3>
              <div className="space-y-2">
                {user.payments.map((payment) => (
                  <div key={payment.id} className="p-2 border rounded text-sm">
                    <p><strong>{formatPrice(payment.amount)}</strong></p>
                    <p className="text-muted-foreground">Статус: {payment.status}</p>
                    <p className="text-muted-foreground">Дата: {new Date(payment.created_at).toLocaleString("ru-RU")}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
          </div>
        </DialogContent>
      </Dialog>

      {user.subscription && (
        <ExtendSubscriptionDialog
          subscriptionId={user.subscription.id}
          open={extendDialogOpen}
          onOpenChange={setExtendDialogOpen}
          onExtend={handleExtend}
        />
      )}
    </>
  )
}
