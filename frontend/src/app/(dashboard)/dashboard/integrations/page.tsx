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
  Key,
  Eye,
  EyeOff,
  AlertTriangle,
  ChevronDown,
} from "lucide-react"
import { toast } from "sonner"
import {
  useStores,
  useKaspiAuth,
  useDeleteStore,
  useSyncStore,
  useUpdateStoreApiToken,
} from "@/hooks/api/use-stores"

export default function IntegrationsPage() {
  const t = useT()
  const [showAddDialog, setShowAddDialog] = useState(false)
  const [kaspiEmail, setKaspiEmail] = useState("")
  const [kaspiPassword, setKaspiPassword] = useState("")
  const [apiTokenInputs, setApiTokenInputs] = useState<Record<string, string>>({})
  const [showTokenInputs, setShowTokenInputs] = useState<Record<string, boolean>>({})

  // API hooks
  const { data: stores, isLoading: storesLoading } = useStores()
  const kaspiAuth = useKaspiAuth()
  const deleteStore = useDeleteStore()
  const syncStore = useSyncStore()
  const updateApiToken = useUpdateStoreApiToken()

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

  const handleSaveApiToken = async (storeId: string) => {
    const token = apiTokenInputs[storeId]
    if (!token) return
    try {
      await updateApiToken.mutateAsync({ storeId, apiToken: token })
      toast.success(t("integrations.apiTokenSaved"))
      setApiTokenInputs((prev) => ({ ...prev, [storeId]: "" }))
    } catch {
      toast.error(t("integrations.apiTokenSaveError"))
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

      {/* Invalid token alert */}
      {stores?.some((s) => s.api_key_set && !s.api_key_valid) && (
        <div className="rounded-lg border border-yellow-500/50 bg-yellow-500/10 p-4">
          <div className="flex items-start gap-3">
            <AlertTriangle className="h-5 w-5 text-yellow-500 shrink-0 mt-0.5" />
            <div className="space-y-1">
              {stores
                .filter((s) => s.api_key_set && !s.api_key_valid)
                .map((s) => (
                  <p key={s.id} className="text-sm">
                    {t("integrations.apiTokenExpiredAlert").replace("{name}", s.name)}
                  </p>
                ))}
              <p className="text-xs text-muted-foreground">
                {t("integrations.apiTokenExpiredWarning")}
              </p>
            </div>
          </div>
        </div>
      )}

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

                {/* API Token Section */}
                <div className="border-t pt-4 mt-4 space-y-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Key className="h-4 w-4 text-muted-foreground" />
                      <span className="text-sm font-medium">{t("integrations.apiToken")}</span>
                    </div>
                    <Badge
                      variant={
                        store.api_key_set && store.api_key_valid
                          ? "default"
                          : store.api_key_set && !store.api_key_valid
                          ? "destructive"
                          : "secondary"
                      }
                    >
                      {store.api_key_set && store.api_key_valid
                        ? t("integrations.apiTokenActive")
                        : store.api_key_set && !store.api_key_valid
                        ? t("integrations.apiTokenInvalid")
                        : t("integrations.apiTokenNotSet")}
                    </Badge>
                  </div>

                  <p className="text-xs text-muted-foreground">
                    {t("integrations.apiTokenDesc")}
                  </p>

                  {/* Token input */}
                  <div className="flex gap-2">
                    <div className="relative flex-1">
                      <Input
                        type={showTokenInputs[store.id] ? "text" : "password"}
                        placeholder={t("integrations.apiTokenPlaceholder")}
                        value={apiTokenInputs[store.id] || ""}
                        onChange={(e) =>
                          setApiTokenInputs((prev) => ({ ...prev, [store.id]: e.target.value }))
                        }
                        disabled={updateApiToken.isPending}
                      />
                      <button
                        type="button"
                        className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                        onClick={() =>
                          setShowTokenInputs((prev) => ({ ...prev, [store.id]: !prev[store.id] }))
                        }
                      >
                        {showTokenInputs[store.id] ? (
                          <EyeOff className="h-4 w-4" />
                        ) : (
                          <Eye className="h-4 w-4" />
                        )}
                      </button>
                    </div>
                    <Button
                      size="sm"
                      onClick={() => handleSaveApiToken(store.id)}
                      disabled={!apiTokenInputs[store.id] || updateApiToken.isPending}
                    >
                      {updateApiToken.isPending ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        t("integrations.apiTokenSave")
                      )}
                    </Button>
                  </div>

                  {/* How to get token - collapsible instructions */}
                  <details className="text-xs">
                    <summary className="cursor-pointer text-muted-foreground hover:text-foreground flex items-center gap-1">
                      <ChevronDown className="h-3 w-3" />
                      {t("integrations.apiTokenHowTo")}
                    </summary>
                    <ol className="mt-2 ml-4 space-y-1 text-muted-foreground list-decimal">
                      <li>{t("integrations.apiTokenStep1")}</li>
                      <li>{t("integrations.apiTokenStep2")}</li>
                      <li>{t("integrations.apiTokenStep3")}</li>
                      <li>{t("integrations.apiTokenStep4")}</li>
                    </ol>
                    <p className="mt-2 text-yellow-600 dark:text-yellow-500">
                      {t("integrations.apiTokenExpiredWarning")}
                    </p>
                  </details>
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
