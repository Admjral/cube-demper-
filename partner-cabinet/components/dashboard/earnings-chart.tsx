"use client"

import { useTheme } from "next-themes"
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts"

const data = [
    { name: "Jan", total: 45000 },
    { name: "Feb", total: 110000 },
    { name: "Mar", total: 155000 },
    { name: "Apr", total: 210000 },
    { name: "May", total: 180000 },
    { name: "Jun", total: 260000 },
    { name: "Jul", total: 310000 },
]

export function EarningsChart() {
    const { theme } = useTheme()

    return (
        <ResponsiveContainer width="100%" height={350}>
            <AreaChart data={data}>
                <defs>
                    <linearGradient id="colorTotal" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.3} />
                        <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0} />
                    </linearGradient>
                </defs>
                <XAxis
                    dataKey="name"
                    stroke="#888888"
                    fontSize={12}
                    tickLine={false}
                    axisLine={false}
                />
                <YAxis
                    stroke="#888888"
                    fontSize={12}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(value) => `${value / 1000}k`}
                />
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                <Tooltip
                    contentStyle={{ borderRadius: '8px', border: '1px solid hsl(var(--border))', backgroundColor: 'hsl(var(--popover))' }}
                    itemStyle={{ color: 'hsl(var(--popover-foreground))' }}
                />
                <Area
                    type="monotone"
                    dataKey="total"
                    stroke="hsl(var(--primary))"
                    fillOpacity={1}
                    fill="url(#colorTotal)"
                />
            </AreaChart>
        </ResponsiveContainer>
    )
}
