"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Package,
  Plus,
  Calendar,
  Clock,
  Search,
  MoreHorizontal,
  Store,
  Construction,
} from "lucide-react"
import Link from "next/link"

interface PreOrder {
  id: string
  product: string
  customer: string
  phone: string
  quantity: number
  deposit: number
  expectedDate: string
  status: "pending" | "confirmed" | "ready" | "completed" | "cancelled"
  notes: string
}

function NoStoreSelected({ locale }: { locale: string }) {
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Store className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {locale === "ru" ? "Выберите магазин" : "Select a store"}
        </h3>
        <p className="text-muted-foreground mb-4">
          {locale === "ru"
            ? "Для управления предзаказами выберите магазин или добавьте новый"
            : "Select a store or add a new one to manage pre-orders"}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {locale === "ru" ? "Добавить магазин" : "Add store"}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

function ComingSoon({ locale }: { locale: string }) {
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Construction className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {locale === "ru" ? "В разработке" : "Coming soon"}
        </h3>
        <p className="text-muted-foreground">
          {locale === "ru"
            ? "Функционал предзаказов скоро будет доступен"
            : "Pre-orders feature will be available soon"}
        </p>
      </CardContent>
    </Card>
  )
}

export default function PreOrdersPage() {
  const { locale, selectedStore } = useStore()
  const [searchQuery, setSearchQuery] = useState("")
  const [dialogOpen, setDialogOpen] = useState(false)

  // Pre-orders will be loaded from API when backend is ready
  const preorders: PreOrder[] = []

  const getStatusBadge = (status: string) => {
    const variants: Record<string, { label: string; labelEn: string; variant: "default" | "secondary" | "outline" }> = {
      pending: { label: "Ожидает", labelEn: "Pending", variant: "secondary" },
      confirmed: { label: "Подтверждён", labelEn: "Confirmed", variant: "default" },
      ready: { label: "Готов", labelEn: "Ready", variant: "outline" },
      completed: { label: "Завершён", labelEn: "Completed", variant: "outline" },
      cancelled: { label: "Отменён", labelEn: "Cancelled", variant: "secondary" },
    }
    const { label, labelEn, variant } = variants[status] || variants.pending
    return (
      <Badge variant={variant}>
        {locale === "ru" ? label : labelEn}
      </Badge>
    )
  }

  const formatPrice = (price: number) => {
    return new Intl.NumberFormat("ru-RU").format(price) + " ₸"
  }

  const filteredPreorders = preorders.filter(
    (p) =>
      p.product.toLowerCase().includes(searchQuery.toLowerCase()) ||
      p.customer.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const pendingCount = preorders.filter((p) => p.status === "pending").length
  const readyCount = preorders.filter((p) => p.status === "ready").length

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">
            {locale === "ru" ? "Предзаказы" : "Pre-orders"}
          </h1>
          <p className="text-muted-foreground">
            {locale === "ru"
              ? "Управление предзаказами клиентов"
              : "Manage customer pre-orders"}
          </p>
        </div>
        {selectedStore && (
          <Button className="touch-target" disabled>
            <Plus className="h-4 w-4 mr-2" />
            {locale === "ru" ? "Новый предзаказ" : "New pre-order"}
          </Button>
        )}
      </div>

      {/* No store selected */}
      {!selectedStore && <NoStoreSelected locale={locale} />}

      {/* Coming soon message when store is selected */}
      {selectedStore && <ComingSoon locale={locale} />}

      {/* Stats and list - will be shown when backend is ready and preorders exist */}
      {selectedStore && preorders.length > 0 && (
        <>
          <div className="grid gap-4 sm:grid-cols-3">
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru" ? "Всего предзаказов" : "Total pre-orders"}
                  </p>
                  <Package className="h-4 w-4 text-muted-foreground" />
                </div>
                <p className="text-2xl font-semibold mt-2">{preorders.length}</p>
              </CardContent>
            </Card>
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru" ? "Ожидают" : "Pending"}
                  </p>
                  <Clock className="h-4 w-4 text-yellow-500" />
                </div>
                <p className="text-2xl font-semibold mt-2">{pendingCount}</p>
              </CardContent>
            </Card>
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {locale === "ru" ? "Готовы к выдаче" : "Ready"}
                  </p>
                  <Calendar className="h-4 w-4 text-green-500" />
                </div>
                <p className="text-2xl font-semibold mt-2">{readyCount}</p>
              </CardContent>
            </Card>
          </div>

          <Card className="glass-card">
            <CardContent className="p-4">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder={locale === "ru" ? "Поиск по товару или клиенту..." : "Search by product or customer..."}
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                />
              </div>
            </CardContent>
          </Card>

          <div className="space-y-4">
            {filteredPreorders.map((preorder) => (
              <Card key={preorder.id} className="glass-card">
                <CardContent className="p-4">
                  <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-4">
                    <div className="flex-1">
                      <div className="flex items-start justify-between">
                        <div>
                          <h3 className="font-semibold">{preorder.product}</h3>
                          <p className="text-sm text-muted-foreground">
                            {preorder.customer} • {preorder.phone}
                          </p>
                        </div>
                        {getStatusBadge(preorder.status)}
                      </div>
                      <div className="mt-4 grid grid-cols-2 sm:grid-cols-4 gap-4">
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {locale === "ru" ? "Количество" : "Quantity"}
                          </p>
                          <p className="font-medium">{preorder.quantity} шт</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {locale === "ru" ? "Предоплата" : "Deposit"}
                          </p>
                          <p className="font-medium">{formatPrice(preorder.deposit)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {locale === "ru" ? "Ожидаемая дата" : "Expected date"}
                          </p>
                          <p className="font-medium">
                            {new Date(preorder.expectedDate).toLocaleDateString(
                              locale === "ru" ? "ru-RU" : "en-US"
                            )}
                          </p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">
                            {locale === "ru" ? "Примечания" : "Notes"}
                          </p>
                          <p className="font-medium truncate">{preorder.notes}</p>
                        </div>
                      </div>
                    </div>
                    <Button variant="ghost" size="icon" className="touch-target shrink-0">
                      <MoreHorizontal className="h-5 w-5" />
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </>
      )}
    </div>
  )
}
