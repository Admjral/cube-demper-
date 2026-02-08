"use client"

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Button } from "@/components/ui/button"
import { Trash2 } from "lucide-react"
import type { Partner } from "@/types/admin"

interface PartnersTableProps {
  partners: Partner[]
  onDelete: (partnerId: string) => void
  isLoading?: boolean
}

export function PartnersTable({ partners, onDelete, isLoading }: PartnersTableProps) {
  if (isLoading) {
    return <div className="text-center py-8">Загрузка...</div>
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Email</TableHead>
          <TableHead>Имя</TableHead>
          <TableHead>Привлечено пользователей</TableHead>
          <TableHead>Создан</TableHead>
          <TableHead>Действия</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {partners.map((partner) => (
          <TableRow key={partner.id}>
            <TableCell className="font-medium">{partner.email}</TableCell>
            <TableCell>{partner.full_name || "—"}</TableCell>
            <TableCell>{partner.referred_users_count}</TableCell>
            <TableCell>
              {new Date(partner.created_at).toLocaleDateString("ru-RU")}
            </TableCell>
            <TableCell>
              <Button
                size="sm"
                variant="destructive"
                onClick={() => {
                  if (confirm("Удалить партнера?")) {
                    onDelete(partner.id)
                  }
                }}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}
