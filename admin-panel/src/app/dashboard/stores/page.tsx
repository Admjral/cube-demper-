"use client"

import { useState } from "react"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { useAdminStores } from "@/hooks/api/use-admin-stores"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"

export default function StoresPage() {
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState("")
  const { data, isLoading } = useAdminStores(page, 50)

  const filteredStores = data?.stores.filter(
    (store) =>
      store.name.toLowerCase().includes(search.toLowerCase()) ||
      store.user_email.toLowerCase().includes(search.toLowerCase()) ||
      store.merchant_id.toLowerCase().includes(search.toLowerCase())
  ) || []

  if (isLoading) {
    return <div className="text-center py-8">Загрузка...</div>
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Магазины пользователей</h1>
        <p className="text-muted-foreground">Все подключенные магазины</p>
      </div>

      <div className="flex items-center gap-4">
        <Input
          placeholder="Поиск по названию, email или merchant_id..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="max-w-sm"
        />
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Название</TableHead>
            <TableHead>Владелец</TableHead>
            <TableHead>Merchant ID</TableHead>
            <TableHead>Продуктов</TableHead>
            <TableHead>Статус</TableHead>
            <TableHead>Последняя синхронизация</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {filteredStores.map((store) => (
            <TableRow key={store.id}>
              <TableCell className="font-medium">{store.name}</TableCell>
              <TableCell>
                <div>
                  <div>{store.user_email}</div>
                  {store.user_name && (
                    <div className="text-sm text-muted-foreground">{store.user_name}</div>
                  )}
                </div>
              </TableCell>
              <TableCell className="font-mono text-sm">{store.merchant_id}</TableCell>
              <TableCell>{store.products_count}</TableCell>
              <TableCell>
                {store.is_active ? (
                  <Badge variant="default">Активен</Badge>
                ) : (
                  <Badge variant="secondary">Неактивен</Badge>
                )}
              </TableCell>
              <TableCell>
                {store.last_sync
                  ? new Date(store.last_sync).toLocaleString("ru-RU")
                  : "—"}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {data && data.total > 50 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            Показано {filteredStores.length} из {data.total}
          </p>
          <div className="flex gap-2">
            <Button
              variant="outline"
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
            >
              Назад
            </Button>
            <Button
              variant="outline"
              onClick={() => setPage((p) => p + 1)}
              disabled={page * 50 >= data.total}
            >
              Вперед
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
