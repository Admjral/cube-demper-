"use client"

import { useEffect, useState } from "react"
import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    CardDescription,
} from "@/components/ui/card"
import { DollarSign, MousePointerClick, Users, CreditCard, Loader2 } from "lucide-react"
import { EarningsChart } from "@/components/dashboard/earnings-chart"
import { PromoCodeCard } from "@/components/dashboard/promo-code-card"
import { getPartnerStats, PartnerStats } from "@/lib/api"

export default function DashboardPage() {
    const [stats, setStats] = useState<PartnerStats | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        async function loadStats() {
            try {
                const data = await getPartnerStats()
                setStats(data)
            } catch (e) {
                setError(e instanceof Error ? e.message : "Ошибка загрузки")
            } finally {
                setLoading(false)
            }
        }
        loadStats()
    }, [])

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        )
    }

    if (error) {
        return (
            <div className="text-center text-red-500 py-8">
                {error}
            </div>
        )
    }

    const conversionRate = stats && stats.registrations > 0
        ? Math.round((stats.paid_users / stats.registrations) * 100)
        : 0

    return (
        <div className="grid gap-4 md:gap-8">
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Клики по ссылке</CardTitle>
                        <MousePointerClick className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats?.clicks.toLocaleString() || 0}</div>
                        <p className="text-xs text-muted-foreground">Всего переходов</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Регистрации</CardTitle>
                        <Users className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats?.registrations.toLocaleString() || 0}</div>
                        <p className="text-xs text-muted-foreground">
                            {stats && stats.clicks > 0
                                ? `${Math.round((stats.registrations / stats.clicks) * 100)}% конверсия`
                                : "Приведённых пользователей"
                            }
                        </p>
                    </CardContent>
                </Card>
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Оплатившие</CardTitle>
                        <CreditCard className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats?.paid_users || 0}</div>
                        <p className="text-xs text-muted-foreground">{conversionRate}% от регистраций</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Заработано</CardTitle>
                        <DollarSign className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{stats?.total_earned.toLocaleString() || 0} ₸</div>
                        <p className="text-xs text-muted-foreground">
                            Доступно: {stats?.available_balance.toLocaleString() || 0} ₸
                        </p>
                    </CardContent>
                </Card>
            </div>

            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
                <div className="col-span-4">
                    <Card className="h-full">
                        <CardHeader>
                            <CardTitle>Динамика дохода</CardTitle>
                            <CardDescription>Статистика за последние 30 дней</CardDescription>
                        </CardHeader>
                        <CardContent className="pl-2">
                            <EarningsChart />
                        </CardContent>
                    </Card>
                </div>
                <div className="col-span-3">
                    <PromoCodeCard />
                </div>
            </div>
        </div>
    )
}
