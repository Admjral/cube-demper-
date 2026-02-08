"use client"

import { useEffect } from "react"
import { useStore } from "@/store/use-store"
import { useStores } from "@/hooks/api/use-stores"
import { useAuth } from "@/hooks/use-auth"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Store, ChevronDown, Check, Loader2, Plus } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import Link from "next/link"

export function StoreSelector() {
  const { user } = useAuth()
  const { selectedStore, stores, setSelectedStore, initStoresFromApi, locale } = useStore()
  const { data: apiStores, isLoading } = useStores()

  // Sync API stores with global store
  useEffect(() => {
    if (apiStores) {
      initStoresFromApi(apiStores as any)
    }
  }, [apiStores, initStoresFromApi])

  const hasStores = stores.length > 0

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          className="touch-target gap-2 px-3 h-10"
        >
          {isLoading ? (
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          ) : (
            <Store className="h-4 w-4 text-muted-foreground" />
          )}
          <span className="max-w-[150px] truncate text-sm font-medium">
            {selectedStore?.name || (locale === 'ru' ? 'Выберите магазин' : 'Select store')}
          </span>
          {selectedStore?.products_count !== undefined && selectedStore.products_count > 0 && (
            <Badge variant="secondary" className="text-xs">
              {selectedStore.products_count}
            </Badge>
          )}
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-[260px] glass-card">
        <DropdownMenuLabel className="text-muted-foreground">
          {locale === 'ru' ? 'Ваши магазины' : 'Your stores'}
        </DropdownMenuLabel>
        <DropdownMenuSeparator />
        {!hasStores ? (
          <div className="px-2 py-4 text-center">
            <p className="text-sm text-muted-foreground mb-3">
              {locale === 'ru' ? 'Нет подключённых магазинов' : 'No connected stores'}
            </p>
            <Link href="/dashboard/integrations">
              <Button size="sm" variant="outline" className="w-full">
                <Plus className="h-4 w-4 mr-2" />
                {locale === 'ru' ? 'Добавить магазин' : 'Add store'}
              </Button>
            </Link>
          </div>
        ) : (
          <>
            {stores.map((store) => (
              <DropdownMenuItem
                key={store.id}
                onClick={() => setSelectedStore(store)}
                className="touch-target flex items-center justify-between"
              >
                <div className="flex items-center gap-2 flex-1 min-w-0">
                  <Store className={`h-4 w-4 flex-shrink-0 ${store.is_active ? 'text-green-500' : 'text-muted-foreground'}`} />
                  <span className="truncate">{store.name}</span>
                </div>
                <div className="flex items-center gap-2 flex-shrink-0">
                  {store.products_count !== undefined && store.products_count > 0 && (
                    <Badge variant="outline" className="text-xs">
                      {store.products_count}
                    </Badge>
                  )}
                  {selectedStore?.id === store.id && (
                    <Check className="h-4 w-4 text-primary" />
                  )}
                </div>
              </DropdownMenuItem>
            ))}
            <DropdownMenuSeparator />
            <Link href="/dashboard/integrations">
              <DropdownMenuItem className="touch-target">
                <Plus className="h-4 w-4 mr-2 text-muted-foreground" />
                <span className="text-muted-foreground">
                  {locale === 'ru' ? 'Добавить магазин' : 'Add store'}
                </span>
              </DropdownMenuItem>
            </Link>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
