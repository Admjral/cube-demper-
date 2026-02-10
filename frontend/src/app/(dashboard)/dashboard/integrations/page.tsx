"use client"

import { useState } from "react"
import { useT } from "@/lib/i18n"
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
  const t = useT()
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
      toast.error(t("integrations.connectError"))
      return
    }

    try {
      await kaspiAuth.mutateAsync({
        email: kaspiEmail,
        password: kaspiPassword,
      })
      toast.success(t("integrations.connectSuccess"))
      setShowAddDialog(false)
      setKaspiEmail("")
      setKaspiPassword("")
    } catch (error: any) {
      toast.error(
        error?.message || t("integrations.connectError")
      )
    }
  }

  const handleDeleteStore = async (storeId: string, storeName: string) => {
    if (
      !confirm(
        `${t("integrations.deleteStore")} "${storeName}"?`
      )
    ) {
      return
    }

    try {
      await deleteStore.mutateAsync(storeId)
      toast.success(t("integrations.storeDeleted"))
    } catch {
      toast.error(t("integrations.deleteError"))
    }
  }

  const handleSyncStore = async (storeId: string) => {
    try {
      const result = await syncStore.mutateAsync(storeId)
      toast.success(
        `${t("integrations.synced")} ${result.products_count} ${t("common.products")}`
      )
    } catch {
      toast.error(t("integrations.syncError"))
    }
  }

  const formatDate = (dateString: string | null) => {
    if (!dateString) return t("integrations.never")
    const date = new Date(dateString)
    return date.toLocaleDateString("ru-RU", {
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
            {t("integrations.title")}
          </h1>
          <p className="text-muted-foreground">
            {t("integrations.subtitle")}
          </p>
        </div>
        <Dialog open={showAddDialog} onOpenChange={setShowAddDialog}>
          <DialogTrigger asChild>
            <Button>
              <Plus className="h-4 w-4 mr-2" />
              {t("integrations.addStore")}
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>
                {t("integrations.connectStore")}
              </DialogTitle>
              <DialogDescription>
                {t("integrations.connectDesc")}
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
                  {t("integrations.password")}
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
                {t("integrations.securityNote")}
              </p>
              <Button
                onClick={handleConnectKaspi}
                disabled={kaspiAuth.isPending}
                className="w-full"
              >
                {kaspiAuth.isPending ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    {t("integrations.connecting")}
                  </>
                ) : (
                  <>
                    <Store className="h-4 w-4 mr-2" />
                    {t("integrations.connect")}
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
                {t("integrations.totalStores")}
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
                {t("integrations.activeStores")}
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
                {t("integrations.productsCount")}
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
                            ? t("common.active")
                            : t("common.inactive")}
                        </Badge>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        Merchant ID: {store.merchant_id}
                      </p>
                      <div className="flex items-center gap-4 mt-1 text-xs text-muted-foreground">
                        <span className="flex items-center gap-1">
                          <Package className="h-3 w-3" />
                          {store.products_count || 0}{" "}
                          {t("common.products")}
                        </span>
                        <span className="flex items-center gap-1">
                          <Clock className="h-3 w-3" />
                          {t("integrations.syncLabel")}{" "}
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
                        {t("integrations.sync")}
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
                {t("integrations.noStores")}
              </h3>
              <p className="text-sm text-muted-foreground mb-4">
                {t("integrations.noStoresDesc")}
              </p>
              <Button onClick={() => setShowAddDialog(true)}>
                <Plus className="h-4 w-4 mr-2" />
                {t("integrations.addStore")}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
