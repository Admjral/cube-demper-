"use client"

import { useState } from "react"
import { PartnersTable } from "@/components/admin/partners-table"
import { PartnerForm } from "@/components/admin/partner-form"
import { usePartners, useCreatePartner, useDeletePartner } from "@/hooks/api/use-partners"
import { Button } from "@/components/ui/button"
import { Plus } from "lucide-react"
import type { PartnerCreateRequest } from "@/types/admin"

export default function PartnersPage() {
  const [formOpen, setFormOpen] = useState(false)
  const { data, isLoading } = usePartners()
  const createPartner = useCreatePartner()
  const deletePartner = useDeletePartner()

  const handleCreate = (data: PartnerCreateRequest) => {
    createPartner.mutate(data)
  }

  const handleDelete = (partnerId: string) => {
    deletePartner.mutate(partnerId)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Партнеры</h1>
          <p className="text-muted-foreground">Управление партнерами и их статистикой</p>
        </div>
        <Button onClick={() => setFormOpen(true)}>
          <Plus className="h-4 w-4 mr-2" />
          Создать партнера
        </Button>
      </div>

      <PartnersTable
        partners={data?.partners || []}
        onDelete={handleDelete}
        isLoading={isLoading}
      />

      <PartnerForm
        open={formOpen}
        onOpenChange={setFormOpen}
        onSubmit={handleCreate}
      />
    </div>
  )
}
