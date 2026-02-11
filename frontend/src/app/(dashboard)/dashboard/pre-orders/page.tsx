"use client"

import { useStore } from "@/store/use-store"
import { useT } from "@/lib/i18n"
import { SubscriptionGate } from "@/components/shared/subscription-gate"
import { useProducts } from "@/hooks/api/use-products"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Package,
  Calendar,
  Store,
  Bot,
} from "lucide-react"
import Link from "next/link"
import { FeatureGate } from "@/components/shared/feature-gate"
import { formatPrice } from "@/lib/utils"
import { ProductDempingDialog } from "@/components/shared/product-demping-dialog"
import { useState } from "react"

function NoStoreSelected() {
  const t = useT()
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Store className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {t("preOrders.selectStore")}
        </h3>
        <p className="text-muted-foreground mb-4">
          {t("preOrders.selectStoreDesc")}
        </p>
        <Link href="/dashboard/integrations">
          <Button>
            {t("preOrders.addStore")}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

function NoActivePreOrders() {
  const t = useT()
  return (
    <Card className="glass-card">
      <CardContent className="p-8 text-center">
        <Package className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
        <h3 className="text-lg font-semibold mb-2">
          {t("preOrders.noActive")}
        </h3>
        <p className="text-muted-foreground mb-4">
          {t("preOrders.noActiveDesc")}
        </p>
        <Link href="/dashboard/price-bot">
          <Button>
            <Bot className="h-4 w-4 mr-2" />
            {t("preOrders.goToDemping")}
          </Button>
        </Link>
      </CardContent>
    </Card>
  )
}

export default function PreOrdersPage() {
  const { selectedStore } = useStore()
  const t = useT()
  const [selectedProductId, setSelectedProductId] = useState<string | null>(null)

  const {
    data: products,
    isLoading,
  } = useProducts(selectedStore?.id)

  // Filter only products with active pre-orders
  const preOrderProducts = products?.filter((p) => p.pre_order_days > 0) || []

  return (
    <SubscriptionGate>
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold">
          {t("preOrders.title")}
        </h1>
        <p className="text-muted-foreground">
          {t("preOrders.activePreOrdersDesc")}
        </p>
      </div>

      {/* No store selected */}
      {!selectedStore && <NoStoreSelected />}

      {/* Feature gate for preorder access */}
      {selectedStore && (
        <FeatureGate feature="preorder">
          {/* Loading */}
          {isLoading && (
            <div className="space-y-4">
              {[1, 2, 3].map((i) => (
                <Card key={i} className="glass-card">
                  <CardContent className="p-4">
                    <Skeleton className="h-16" />
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          {/* Stats */}
          {!isLoading && preOrderProducts.length > 0 && (
            <Card className="glass-card">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    {t("preOrders.activePreOrders")}
                  </p>
                  <Package className="h-4 w-4 text-orange-500" />
                </div>
                <p className="text-2xl font-semibold mt-2">{preOrderProducts.length}</p>
              </CardContent>
            </Card>
          )}

          {/* No active pre-orders */}
          {!isLoading && preOrderProducts.length === 0 && (
            <NoActivePreOrders />
          )}

          {/* Products with active pre-orders */}
          {!isLoading && preOrderProducts.length > 0 && (
            <div className="space-y-3">
              {preOrderProducts.map((product) => (
                <Card
                  key={product.id}
                  className="glass-card cursor-pointer hover:border-orange-500/50 transition-colors"
                  onClick={() => setSelectedProductId(product.id)}
                >
                  <CardContent className="p-4">
                    <div className="flex items-start justify-between gap-4">
                      <div className="flex-1 min-w-0">
                        <h3 className="font-medium truncate">{product.name}</h3>
                        <p className="text-sm text-muted-foreground">{product.kaspi_sku}</p>
                      </div>
                      <Badge className="bg-orange-500/20 text-orange-600 hover:bg-orange-500/30 shrink-0">
                        {product.pre_order_days} {t("preOrder.daysUnit")}
                      </Badge>
                    </div>
                    <div className="mt-3 grid grid-cols-2 sm:grid-cols-3 gap-4">
                      <div>
                        <p className="text-xs text-muted-foreground">
                          {t("priceBot.currentPrice")}
                        </p>
                        <p className="font-semibold">{formatPrice(product.price)}</p>
                      </div>
                      <div>
                        <p className="text-xs text-muted-foreground">
                          {t("preOrder.deliveryBy")}
                        </p>
                        <p className="font-medium flex items-center gap-1">
                          <Calendar className="h-3.5 w-3.5" />
                          {new Date(Date.now() + product.pre_order_days * 86400000).toLocaleDateString("ru-RU", {
                            day: "numeric",
                            month: "short",
                          })}
                        </p>
                      </div>
                      <div className="hidden sm:block">
                        <p className="text-xs text-muted-foreground">
                          {t("priceBot.demping")}
                        </p>
                        <Badge variant={product.bot_active ? "default" : "secondary"} className="mt-1">
                          {product.bot_active ? t("priceBot.dempingOn") : t("priceBot.dempingOff")}
                        </Badge>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </FeatureGate>
      )}
    </div>

    {/* Product demping dialog (reuse from price-bot) */}
    <ProductDempingDialog
      productId={selectedProductId}
      open={!!selectedProductId}
      onOpenChange={(open) => !open && setSelectedProductId(null)}
    />
    </SubscriptionGate>
  )
}
