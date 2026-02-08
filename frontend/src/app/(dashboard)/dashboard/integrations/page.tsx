"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Store,
  Plus,
  RefreshCw,
  Trash2,
  CheckCircle,
  XCircle,
  Loader2,
  ShoppingBag,
  Clock,
  Package,
} from "lucide-react"
import { toast } from "sonner"
import {
  useStores,
  useKaspiAuth,
  useDeleteStore,
  useSyncStore,
} from "@/hooks/api/use-stores"

export default function IntegrationsPage() {
  const { locale } = useStore()
  const [showAddDialog, setShowAddDialog] = useState(false)
  const [kaspiEmail, setKaspiEmail] = useState("")
  const [kaspiPassword, setKaspiPassword] = useState("")

  // API hooks
  const { data: stores, isLoading: storesLoading } = useStores()
  const kaspiAuth = useKaspiAuth()
  const deleteStore = useDeleteStore()
  const syncStore = useSyncStore()

  const connectedStores = stores?.filter((s) => s.is_active) || []
  const totalProducts = stores?.reduce((acc, s) => acc + (s.products_count || 0), 0) || 0

  const handleConnectKaspi = async () => {
    if (!kaspiEmail || !kaspiPassword) {
      toast.error(locale === "ru" ? "Заполните все поля" : "Fill all fields")
      return
    }

    try {
      await kaspiAuth.mutateAsync({
        email: kaspiEmail,
        password: kaspiPassword,
      })
      toast.success(
        locale === "ru"
          ? "Магазин успешно подключён!"
          : "Store connected successfully!"
      )
      setShowAddDialog(false)
      setKaspiEmail("")
      setKaspiPassword("")
    } catch (error: any) {
      toast.error(
        error?.message ||
          (locale === "ru" ? "Ошибка подключения" : "Connection error")
      )
    }
  }

  const handleDeleteStore = async (storeId: string, storeName: string) => {
    if (
      !confirm(
        locale === "ru"
          ? `Удалить магазин "${storeName}"?`
          : `Delete store "${storeName}"?`
      )
    ) {
      return
    }

    try {
      await deleteStore.mutateAsync(storeId)
      toast.success(locale === "ru" ? "Магазин удалён" : "Store deleted")
    } catch {
      toast.error(locale === "ru" ? "Ошибка удаления" : "Delete error")
    }
  }

  const handleSyncStore = async (storeId: string) => {
    try {
      const result = await syncStore.mutateAsync(storeId)
      toast.success(
        locale === "ru"
          ? `Синхронизировано ${result.products_count} товаров`
          : `Synced ${result.products_count} products`
      )
    } catch {
      toast.error(locale === "ru" ? "Ошибка синхронизации" : "Sync error")
    }
  }

  const formatDate = (dateString: string | null) => {
    if (!dateString) return locale === "ru" ? "Никогда" : "Never"
    const date = new Date(dateString)
    return date.toLocaleDateString(locale === "ru" ? "ru-RU" : "en-US", {
      day: "numeric",
      month: "short",
      hour: "2-digit",
      minute: "2-digit",
    })
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">
            {locale === "ru" ? "Магазины Kaspi" : "Kaspi Stores"}
          </h1>
          <p className="text-muted-foreground">
            {locale === "ru"
              ? "Подключите свои магазины для синхронизации"
              : "Connect your stores for synchronization"}
          </p>
        </div>
        <Dialog open={showAddDialog} onOpenChange={setShowAddDialog}>
          <DialogTrigger asChild>
            <Button>
              <Plus className="h-4 w-4 mr-2" />
              {locale === "ru" ? "Добавить магазин" : "Add store"}
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>
                {locale === "ru"
                  ? "Подключить магазин Kaspi"
                  : "Connect Kaspi Store"}
              </DialogTitle>
              <DialogDescription>
                {locale === "ru"
                  ? "Введите данные от личного кабинета продавца Kaspi"
                  : "Enter your Kaspi seller account credentials"}
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 pt-4">
              <div className="space-y-2">
                <Label htmlFor="kaspi-email">Email</Label>
                <Input
                  id="kaspi-email"
                  type="email"
                  placeholder="seller@example.com"
                  value={kaspiEmail}
                  onChange={(e) => setKaspiEmail(e.target.value)}
                  disabled={kaspiAuth.isPending}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="kaspi-password">
                  {locale === "ru" ? "Пароль" : "Password"}
                </Label>
                <Input
                  id="kaspi-password"
                  type="password"
                  placeholder="••••••••"
                  value={kaspiPassword}
                  onChange={(e) => setKaspiPassword(e.target.value)}
                  disabled={kaspiAuth.isPending}
                />
              </div>
              <p className="text-xs text-muted-foreground">
                {locale === "ru"
                  ? "Ваши данные надёжно защищены и используются только для синхронизации"
                  : "Your credentials are securely protected and used only for synchronization"}
              </p>
              <Button
                onClick={handleConnectKaspi}
                disabled={kaspiAuth.isPending}
                className="w-full"
              >
                {kaspiAuth.isPending ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    {locale === "ru" ? "Подключение..." : "Connecting..."}
                  </>
                ) : (
                  <>
                    <Store className="h-4 w-4 mr-2" />
                    {locale === "ru" ? "Подключить" : "Connect"}
                  </>
                )}
              </Button>
            </div>
          </DialogContent>
        </Dialog>
      </div>

      {/* Stats */}
      <div className="grid gap-4 sm:grid-cols-3">
        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <p className="text-sm text-muted-foreground">
                {locale === "ru" ? "Всего магазинов" : "Total stores"}
              </p>
              <ShoppingBag className="h-4 w-4 text-muted-foreground" />
            </div>
            <p className="text-2xl font-semibold mt-2">{stores?.length || 0}</p>
          </CardContent>
        </Card>
        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <p className="text-sm text-muted-foreground">
                {locale === "ru" ? "Активных" : "Active"}
              </p>
              <CheckCircle className="h-4 w-4 text-green-500" />
            </div>
            <p className="text-2xl font-semibold mt-2">{connectedStores.length}</p>
          </CardContent>
        </Card>
        <Card className="glass-card">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <p className="text-sm text-muted-foreground">
                {locale === "ru" ? "Товаров" : "Products"}
              </p>
              <Package className="h-4 w-4 text-muted-foreground" />
            </div>
            <p className="text-2xl font-semibold mt-2">{totalProducts}</p>
          </CardContent>
        </Card>
      </div>

      {/* Stores list */}
      {storesLoading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : stores && stores.length > 0 ? (
        <div className="space-y-4">
          {stores.map((store) => (
            <Card key={store.id} className="glass-card">
              <CardContent className="p-6">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                  <div className="flex items-center gap-4">
                    <div
                      className={`p-3 rounded-xl ${
                        store.is_active ? "bg-green-500/10" : "bg-muted"
                      }`}
                    >
                      <Store
                        className={`h-6 w-6 ${
                          store.is_active
                            ? "text-green-500"
                            : "text-muted-foreground"
                        }`}
                      />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <h3 className="font-semibold">{store.name}</h3>
                        <Badge
                          variant={store.is_active ? "default" : "secondary"}
                        >
                          {store.is_active
                            ? locale === "ru"
                              ? "Активен"
                              : "Active"
                            : locale === "ru"
                            ? "Неактивен"
                            : "Inactive"}
                        </Badge>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        Merchant ID: {store.merchant_id}
                      </p>
                      <div className="flex items-center gap-4 mt-1 text-xs text-muted-foreground">
                        <span className="flex items-center gap-1">
                          <Package className="h-3 w-3" />
                          {store.products_count || 0}{" "}
                          {locale === "ru" ? "товаров" : "products"}
                        </span>
                        <span className="flex items-center gap-1">
                          <Clock className="h-3 w-3" />
                          {locale === "ru" ? "Синхр:" : "Sync:"}{" "}
                          {formatDate(store.last_sync)}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => handleSyncStore(store.id)}
                      disabled={syncStore.isPending}
                    >
                      {syncStore.isPending ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <RefreshCw className="h-4 w-4" />
                      )}
                      <span className="ml-2 hidden sm:inline">
                        {locale === "ru" ? "Синхронизировать" : "Sync"}
                      </span>
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="text-destructive"
                      onClick={() => handleDeleteStore(store.id, store.name)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : (
        <Card className="glass-card border-dashed">
          <CardContent className="p-6">
            <div className="flex flex-col items-center justify-center text-center py-8">
              <div className="p-4 rounded-full bg-muted mb-4">
                <Store className="h-8 w-8 text-muted-foreground" />
              </div>
              <h3 className="font-semibold mb-2">
                {locale === "ru" ? "Нет подключённых магазинов" : "No connected stores"}
              </h3>
              <p className="text-sm text-muted-foreground mb-4">
                {locale === "ru"
                  ? "Подключите свой первый магазин Kaspi для начала работы"
                  : "Connect your first Kaspi store to get started"}
              </p>
              <Button onClick={() => setShowAddDialog(true)}>
                <Plus className="h-4 w-4 mr-2" />
                {locale === "ru" ? "Добавить магазин" : "Add store"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
