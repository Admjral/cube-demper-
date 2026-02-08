"use client"

import { useEffect, useState } from "react"
import { Check, Copy, Loader2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { getPromoCode, PromoCodeData } from "@/lib/api"

export function PromoCodeCard() {
    const [copied, setCopied] = useState(false)
    const [data, setData] = useState<PromoCodeData | null>(null)
    const [loading, setLoading] = useState(true)

    useEffect(() => {
        async function loadPromoCode() {
            try {
                const result = await getPromoCode()
                setData(result)
            } catch (e) {
                console.error("Failed to load promo code:", e)
            } finally {
                setLoading(false)
            }
        }
        loadPromoCode()
    }, [])

    const onCopy = () => {
        if (data?.referral_link) {
            navigator.clipboard.writeText(data.referral_link)
            setCopied(true)
            setTimeout(() => setCopied(false), 2000)
        }
    }

    const onCopyCode = () => {
        if (data?.promo_code) {
            navigator.clipboard.writeText(data.promo_code)
            setCopied(true)
            setTimeout(() => setCopied(false), 2000)
        }
    }

    if (loading) {
        return (
            <Card className="h-full">
                <CardHeader>
                    <CardTitle>Ваш Промокод</CardTitle>
                </CardHeader>
                <CardContent className="flex items-center justify-center h-32">
                    <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                </CardContent>
            </Card>
        )
    }

    if (!data?.promo_code) {
        return (
            <Card className="h-full">
                <CardHeader>
                    <CardTitle>Ваш Промокод</CardTitle>
                    <CardDescription>
                        Промокод ещё не назначен
                    </CardDescription>
                </CardHeader>
                <CardContent className="text-center text-muted-foreground py-8">
                    Обратитесь к администратору для получения промокода
                </CardContent>
            </Card>
        )
    }

    return (
        <Card className="h-full">
            <CardHeader>
                <CardTitle>Ваш Промокод</CardTitle>
                <CardDescription>
                    Клиент получает скидку, а вы — 5000 ₸
                </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
                <div
                    className="flex items-center justify-center rounded-lg border-2 border-dashed bg-muted/50 p-6 cursor-pointer hover:bg-muted/70 transition-colors"
                    onClick={onCopyCode}
                    title="Нажмите, чтобы скопировать"
                >
                    <span className="font-mono text-3xl font-bold tracking-[0.2em] text-primary">
                        {data.promo_code}
                    </span>
                </div>
                {data.referral_link && (
                    <div className="flex space-x-2">
                        <Input
                            value={data.referral_link}
                            readOnly
                            className="bg-muted/50 font-mono text-xs"
                        />
                        <Button variant="outline" size="icon" onClick={onCopy} className="shrink-0">
                            {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                        </Button>
                    </div>
                )}
            </CardContent>
        </Card>
    )
}
