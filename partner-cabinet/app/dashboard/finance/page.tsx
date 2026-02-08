"use client"

import { useEffect, useState } from "react"
import { Plus, Wallet, ArrowUpRight, ArrowDownLeft, Loader2 } from "lucide-react"

import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"
import {
    getPartnerStats,
    getPartnerTransactions,
    requestPayout,
    PartnerStats,
    PartnerTransaction,
} from "@/lib/api"

export default function FinancePage() {
    const [stats, setStats] = useState<PartnerStats | null>(null)
    const [transactions, setTransactions] = useState<PartnerTransaction[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    const [isPayoutOpen, setIsPayoutOpen] = useState(false)
    const [payoutLoading, setPayoutLoading] = useState(false)
    const [amount, setAmount] = useState("")
    const [requisites, setRequisites] = useState("")

    useEffect(() => {
        async function loadData() {
            try {
                const [statsData, transactionsData] = await Promise.all([
                    getPartnerStats(),
                    getPartnerTransactions(50, 0),
                ])
                setStats(statsData)
                setTransactions(transactionsData.transactions)
            } catch (e) {
                setError(e instanceof Error ? e.message : "Ошибка загрузки")
            } finally {
                setLoading(false)
            }
        }
        loadData()
    }, [])

    const handleRequestPayout = async (e: React.FormEvent) => {
        e.preventDefault()
        setPayoutLoading(true)

        try {
            const result = await requestPayout(Number(amount), requisites)
            alert(result.message)
            setIsPayoutOpen(false)
            setAmount("")
            setRequisites("")
            // Reload data
            const [statsData, transactionsData] = await Promise.all([
                getPartnerStats(),
                getPartnerTransactions(50, 0),
            ])
            setStats(statsData)
            setTransactions(transactionsData.transactions)
        } catch (e) {
            alert(e instanceof Error ? e.message : "Ошибка при запросе выплаты")
        } finally {
            setPayoutLoading(false)
        }
    }

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
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Доступный баланс</CardTitle>
                        <Wallet className="h-4 w-4 text-muted-foreground" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">
                            {stats?.available_balance.toLocaleString() || 0} ₸
                        </div>
                        <p className="text-xs text-muted-foreground">Минимальный вывод: 5 000 ₸</p>
                    </CardContent>
                </Card>
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Всего заработано</CardTitle>
                        <ArrowUpRight className="h-4 w-4 text-green-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">
                            {stats?.total_earned.toLocaleString() || 0} ₸
                        </div>
                    </CardContent>
                </Card>
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Всего выведено</CardTitle>
                        <ArrowDownLeft className="h-4 w-4 text-red-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">
                            {stats?.total_withdrawn.toLocaleString() || 0} ₸
                        </div>
                    </CardContent>
                </Card>
            </div>

            <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold md:text-2xl">История транзакций</h2>

                <Dialog open={isPayoutOpen} onOpenChange={setIsPayoutOpen}>
                    <DialogTrigger asChild>
                        <Button disabled={(stats?.available_balance || 0) < 5000}>
                            <Plus className="mr-2 h-4 w-4" />
                            Запросить выплату
                        </Button>
                    </DialogTrigger>
                    <DialogContent className="sm:max-w-[425px]">
                        <DialogHeader>
                            <DialogTitle>Запрос выплаты</DialogTitle>
                            <DialogDescription>
                                Укажите сумму и реквизиты для получения средств. Обработка занимает до 24 часов.
                                <br />
                                Доступно: <strong>{stats?.available_balance.toLocaleString() || 0} ₸</strong>
                            </DialogDescription>
                        </DialogHeader>
                        <form onSubmit={handleRequestPayout}>
                            <div className="grid gap-4 py-4">
                                <div className="grid grid-cols-4 items-center gap-4">
                                    <Label htmlFor="amount" className="text-right">
                                        Сумма
                                    </Label>
                                    <Input
                                        id="amount"
                                        value={amount}
                                        onChange={(e) => setAmount(e.target.value)}
                                        placeholder="5000"
                                        className="col-span-3"
                                        type="number"
                                        min="5000"
                                        max={stats?.available_balance || 0}
                                        required
                                    />
                                </div>
                                <div className="grid grid-cols-4 items-center gap-4">
                                    <Label htmlFor="req" className="text-right">
                                        Реквизиты
                                    </Label>
                                    <Input
                                        id="req"
                                        value={requisites}
                                        onChange={(e) => setRequisites(e.target.value)}
                                        placeholder="Kaspi Gold: +7 7..."
                                        className="col-span-3"
                                        required
                                    />
                                </div>
                            </div>
                            <DialogFooter>
                                <Button type="submit" disabled={payoutLoading}>
                                    {payoutLoading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                                    Отправить запрос
                                </Button>
                            </DialogFooter>
                        </form>
                    </DialogContent>
                </Dialog>
            </div>

            <Card>
                {transactions.length === 0 ? (
                    <div className="text-center text-muted-foreground py-8">
                        Пока нет транзакций
                    </div>
                ) : (
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>ID</TableHead>
                                <TableHead>Дата</TableHead>
                                <TableHead>Описание</TableHead>
                                <TableHead className="text-right">Сумма</TableHead>
                                <TableHead>Статус</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {transactions.map((t) => (
                                <TableRow key={t.id}>
                                    <TableCell className="font-medium font-mono text-xs">
                                        {t.id.slice(0, 8)}...
                                    </TableCell>
                                    <TableCell>
                                        {t.created_at
                                            ? new Date(t.created_at).toLocaleDateString("ru-RU")
                                            : "—"
                                        }
                                    </TableCell>
                                    <TableCell>{t.description}</TableCell>
                                    <TableCell className={cn(
                                        "text-right font-bold",
                                        t.amount > 0 ? "text-green-600" : "text-black"
                                    )}>
                                        {t.amount > 0 ? "+" : ""}{t.amount.toLocaleString()} ₸
                                    </TableCell>
                                    <TableCell>
                                        <StatusBadge status={t.status} />
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                )}
            </Card>
        </div>
    )
}

function StatusBadge({ status }: { status: string }) {
    if (status === 'completed') {
        return (
            <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold border-transparent bg-green-500/15 text-green-600">
                Выполнено
            </span>
        )
    }
    if (status === 'rejected') {
        return (
            <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold border-transparent bg-red-500/15 text-red-600">
                Отклонено
            </span>
        )
    }
    return (
        <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold border-transparent bg-secondary text-secondary-foreground">
            В обработке
        </span>
    )
}
