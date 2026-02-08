'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { useAdminStores } from '@/hooks/api/use-admin'
import {
  Store,
  Search,
  Loader2,
  Calendar,
  Package,
  CheckCircle,
  XCircle,
  RefreshCw,
} from 'lucide-react'
import { format } from 'date-fns'
import { ru } from 'date-fns/locale'

export default function AdminStoresPage() {
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const { data, isLoading } = useAdminStores(page, 50)

  const stores = data?.stores ?? []
  const total = data?.total ?? 0

  const filteredStores = stores.filter(
    (store) =>
      store.name.toLowerCase().includes(search.toLowerCase()) ||
      store.user_email.toLowerCase().includes(search.toLowerCase()) ||
      store.merchant_id.toLowerCase().includes(search.toLowerCase())
  )

  const totalProducts = stores.reduce((sum, s) => sum + (s.products_count ?? 0), 0)
  const activeStores = stores.filter((s) => s.is_active).length

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Store className="h-6 w-6" />
            Магазины
          </h1>
          <p className="text-muted-foreground">
            Всего: {total} магазинов
          </p>
        </div>
        <div className="text-right space-y-1">
          <p className="text-sm text-muted-foreground">
            {activeStores} активных / {stores.length} на странице
          </p>
          <p className="text-sm text-muted-foreground">
            {totalProducts.toLocaleString()} товаров
          </p>
        </div>
      </div>

      {/* Search */}
      <div className="relative max-w-md">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Поиск по названию, email или merchant ID..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-10"
        />
      </div>

      {/* Stores table */}
      <Card className="glass-card">
        <CardContent className="p-0">
          {isLoading ? (
            <div className="flex items-center justify-center h-64">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : filteredStores.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-64">
              <Store className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground">Магазины не найдены</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Магазин
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Владелец
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Merchant ID
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Товаров
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Статус
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Последняя синхр.
                    </th>
                    <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                      Подключён
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredStores.map((store) => (
                    <tr
                      key={store.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="p-4">
                        <span className="font-medium text-foreground">
                          {store.name}
                        </span>
                      </td>
                      <td className="p-4">
                        <div>
                          <p className="text-sm">{store.user_name || 'Без имени'}</p>
                          <p className="text-xs text-muted-foreground">
                            {store.user_email}
                          </p>
                        </div>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-mono text-muted-foreground">
                          {store.merchant_id}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm flex items-center gap-1">
                          <Package className="h-3 w-3 text-muted-foreground" />
                          {(store.products_count ?? 0).toLocaleString()}
                        </span>
                      </td>
                      <td className="p-4">
                        {store.is_active ? (
                          <Badge className="bg-green-500/10 text-green-500 border-0">
                            <CheckCircle className="h-3 w-3 mr-1" />
                            Активен
                          </Badge>
                        ) : (
                          <Badge variant="secondary">
                            <XCircle className="h-3 w-3 mr-1" />
                            Неактивен
                          </Badge>
                        )}
                      </td>
                      <td className="p-4">
                        {store.last_sync ? (
                          <span className="text-sm text-muted-foreground flex items-center gap-1">
                            <RefreshCw className="h-3 w-3" />
                            {format(new Date(store.last_sync), 'd MMM, HH:mm', { locale: ru })}
                          </span>
                        ) : (
                          <span className="text-sm text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="p-4">
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {store.created_at
                            ? format(new Date(store.created_at), 'd MMM yyyy', { locale: ru })
                            : '-'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Pagination */}
      {total > 50 && (
        <div className="flex items-center justify-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page === 1}
            onClick={() => setPage(page - 1)}
          >
            Назад
          </Button>
          <span className="text-sm text-muted-foreground">
            Страница {page} из {Math.ceil(total / 50)}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= Math.ceil(total / 50)}
            onClick={() => setPage(page + 1)}
          >
            Вперёд
          </Button>
        </div>
      )}
    </div>
  )
}
