'use client'

import { useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
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
} from 'lucide-react'

export default function AdminStoresPage() {
  const [search, setSearch] = useState('')
  const { data: stores, isLoading } = useAdminStores()

  // Mock data for demo
  const mockStores = stores || [
    { id: '1', user_id: '1', user_email: 'seller1@mail.kz', name: 'Магазин Электроники', merchant_id: 'M123456', products_count: 450, is_active: true, created_at: '2024-01-20' },
    { id: '2', user_id: '1', user_email: 'seller1@mail.kz', name: 'Аксессуары KZ', merchant_id: 'M123457', products_count: 120, is_active: true, created_at: '2024-01-22' },
    { id: '3', user_id: '2', user_email: 'shop2@gmail.com', name: 'Tech Store', merchant_id: 'M234567', products_count: 890, is_active: true, created_at: '2024-01-15' },
    { id: '4', user_id: '4', user_email: 'store3@inbox.kz', name: 'Gadget Zone', merchant_id: 'M345678', products_count: 230, is_active: false, created_at: '2024-01-10' },
    { id: '5', user_id: '6', user_email: 'trial@test.kz', name: 'New Shop', merchant_id: 'M456789', products_count: 45, is_active: true, created_at: '2024-01-25' },
  ]

  const filteredStores = mockStores.filter(
    (store) =>
      store.name.toLowerCase().includes(search.toLowerCase()) ||
      store.user_email.toLowerCase().includes(search.toLowerCase()) ||
      store.merchant_id.toLowerCase().includes(search.toLowerCase())
  )

  const totalProducts = mockStores.reduce((sum, s) => sum + s.products_count, 0)
  const activeStores = mockStores.filter((s) => s.is_active).length

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Store className="h-6 w-6" />
            Магазины
          </h1>
          <p className="text-muted-foreground">Все подключённые магазины Kaspi</p>
        </div>
        <div className="text-right space-y-1">
          <p className="text-sm text-muted-foreground">
            {activeStores} активных / {mockStores.length} всего
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
                        <span className="text-sm text-muted-foreground">
                          {store.user_email}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm font-mono text-muted-foreground">
                          {store.merchant_id}
                        </span>
                      </td>
                      <td className="p-4">
                        <span className="text-sm flex items-center gap-1">
                          <Package className="h-3 w-3 text-muted-foreground" />
                          {store.products_count.toLocaleString()}
                        </span>
                      </td>
                      <td className="p-4">
                        {store.is_active ? (
                          <Badge className="bg-success/10 text-success border-0">
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
                        <span className="text-sm text-muted-foreground flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {new Date(store.created_at).toLocaleDateString('ru-RU')}
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
    </div>
  )
}
