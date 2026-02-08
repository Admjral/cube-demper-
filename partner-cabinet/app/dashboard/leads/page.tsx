"use client"

import { useEffect, useState } from "react"
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"
import { Loader2 } from "lucide-react"
import { getPartnerLeads, PartnerLead } from "@/lib/api"

export default function LeadsPage() {
    const [leads, setLeads] = useState<PartnerLead[]>([])
    const [total, setTotal] = useState(0)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        async function loadLeads() {
            try {
                const data = await getPartnerLeads(50, 0)
                setLeads(data.leads)
                setTotal(data.total)
            } catch (e) {
                setError(e instanceof Error ? e.message : "Ошибка загрузки")
            } finally {
                setLoading(false)
            }
        }
        loadLeads()
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

    return (
        <div className="grid gap-6">
            <Card>
                <CardHeader>
                    <CardTitle>Мои Клиенты</CardTitle>
                    <CardDescription>
                        Список пользователей, перешедших по вашей ссылке или использовавших промокод.
                        Всего: {total}
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    {leads.length === 0 ? (
                        <div className="text-center text-muted-foreground py-8">
                            Пока нет приведённых клиентов
                        </div>
                    ) : (
                        <Table>
                            <TableHeader>
                                <TableRow>
                                    <TableHead>Email</TableHead>
                                    <TableHead>Имя</TableHead>
                                    <TableHead>Дата регистрации</TableHead>
                                    <TableHead>Статус</TableHead>
                                    <TableHead className="text-right">Ваш доход</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {leads.map((lead) => (
                                    <TableRow key={lead.id}>
                                        <TableCell className="font-medium">{lead.email}</TableCell>
                                        <TableCell>{lead.full_name || "—"}</TableCell>
                                        <TableCell>
                                            {lead.registered_at
                                                ? new Date(lead.registered_at).toLocaleDateString("ru-RU")
                                                : "—"
                                            }
                                        </TableCell>
                                        <TableCell>
                                            <StatusBadge status={lead.status} />
                                        </TableCell>
                                        <TableCell className={cn(
                                            "text-right font-bold",
                                            lead.partner_earned > 0 ? "text-green-600" : "text-muted-foreground"
                                        )}>
                                            {lead.partner_earned > 0 ? `+${lead.partner_earned.toLocaleString()} ₸` : "0 ₸"}
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    )}
                </CardContent>
            </Card>
        </div>
    )
}

function StatusBadge({ status }: { status: string }) {
    if (status === 'paid') {
        return (
            <span className="inline-flex items-center rounded-full border border-transparent bg-green-500/15 px-2.5 py-0.5 text-xs font-semibold text-green-600 transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2">
                Оплатил тариф
            </span>
        )
    }
    return (
        <span className="inline-flex items-center rounded-full border border-transparent bg-blue-500/15 px-2.5 py-0.5 text-xs font-semibold text-blue-600 transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2">
            Зарегистрирован
        </span>
    )
}
