"use client"

import { useState } from "react"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useBlockUser, useUnblockUser, useUserDetails } from "@/hooks/api/use-admin-users"
import { UserDetailsDialog } from "./user-details-dialogs"
import { Ban, Unlock, Eye } from "lucide-react"
import type { AdminUser } from "@/types/admin"

interface UsersTableProps {
  users: AdminUser[]
  isLoading?: boolean
}

export function UsersTable({ users, isLoading }: UsersTableProps) {
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null)

  const blockUser = useBlockUser()
  const unblockUser = useUnblockUser()
  const { data: userDetails } = useUserDetails(selectedUserId || "")

  const handleBlock = (userId: string) => {
    if (confirm("Заблокировать пользователя?")) {
      blockUser.mutate({ userId })
    }
  }

  const handleUnblock = (userId: string) => {
    if (confirm("Разблокировать пользователя?")) {
      unblockUser.mutate(userId)
    }
  }


  if (isLoading) {
    return <div className="text-center py-8">Загрузка...</div>
  }

  return (
    <>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Имя</TableHead>
            <TableHead>Email</TableHead>
            <TableHead>Магазин</TableHead>
            <TableHead>Тариф</TableHead>
            <TableHead>Подключен</TableHead>
            <TableHead>Заканчивается</TableHead>
            <TableHead>Статус</TableHead>
            <TableHead>Действия</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {users.map((user) => (
            <TableRow key={user.id}>
              <TableCell>{user.full_name || "—"}</TableCell>
              <TableCell>{user.email}</TableCell>
              <TableCell>{user.stores_count}</TableCell>
              <TableCell>
                {user.subscription_plan ? (
                  <Badge variant="outline">{user.subscription_plan}</Badge>
                ) : (
                  "—"
                )}
              </TableCell>
              <TableCell>
                {user.created_at ? new Date(user.created_at).toLocaleDateString("ru-RU") : "—"}
              </TableCell>
              <TableCell>
                {user.subscription_end_date
                  ? new Date(user.subscription_end_date).toLocaleDateString("ru-RU")
                  : "—"}
              </TableCell>
              <TableCell>
                {user.is_blocked ? (
                  <Badge variant="destructive">Заблокирован</Badge>
                ) : (
                  <Badge variant="default">Активен</Badge>
                )}
              </TableCell>
              <TableCell>
                <div className="flex items-center gap-2">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => setSelectedUserId(user.id)}
                  >
                    <Eye className="h-4 w-4" />
                  </Button>
                  {user.is_blocked ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => handleUnblock(user.id)}
                    >
                      <Unlock className="h-4 w-4" />
                    </Button>
                  ) : (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => handleBlock(user.id)}
                    >
                      <Ban className="h-4 w-4" />
                    </Button>
                  )}
                </div>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {selectedUserId && userDetails && (
        <UserDetailsDialog
          user={userDetails}
          open={!!selectedUserId}
          onOpenChange={(open) => !open && setSelectedUserId(null)}
        />
      )}
    </>
  )
}
