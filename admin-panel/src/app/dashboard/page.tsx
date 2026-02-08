"use client"

import { StatsCards } from "@/components/admin/stats-cards"
import { useAdminStats } from "@/hooks/api/use-admin-stats"

export default function DashboardPage() {
  const { data: stats, isLoading } = useAdminStats()

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Общая статистика</h1>
        <p className="text-muted-foreground">Обзор системы</p>
      </div>

      <StatsCards stats={stats} isLoading={isLoading} />
    </div>
  )
}
