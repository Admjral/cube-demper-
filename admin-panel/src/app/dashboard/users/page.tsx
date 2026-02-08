"use client"

import { useState } from "react"
import { UsersTable } from "@/components/admin/users-table"
import { useAdminUsers } from "@/hooks/api/use-admin-users"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"

export default function UsersPage() {
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState("")
  const { data, isLoading } = useAdminUsers(page, 50)

  const filteredUsers = data?.users.filter(
    (user) =>
      user.email.toLowerCase().includes(search.toLowerCase()) ||
      (user.full_name && user.full_name.toLowerCase().includes(search.toLowerCase()))
  ) || []

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Пользователи</h1>
        <p className="text-muted-foreground">Управление пользователями системы</p>
      </div>

      <div className="flex items-center gap-4">
        <Input
          placeholder="Поиск по email или имени..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="max-w-sm"
        />
      </div>

      <UsersTable users={filteredUsers} isLoading={isLoading} />

      {data && data.total > 50 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            Показано {filteredUsers.length} из {data.total}
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
