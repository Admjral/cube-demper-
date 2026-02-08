"use client"

import { useState } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import {
    LayoutDashboard,
    Wallet,
    Users,
    Megaphone,
    LogOut,
    Menu,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

const sidebarItems = [
    { href: "/dashboard", icon: LayoutDashboard, label: "Обзор" },
    { href: "/dashboard/leads", icon: Users, label: "Клиенты" },
    { href: "/dashboard/finance", icon: Wallet, label: "Финансы" },
    { href: "/dashboard/marketing", icon: Megaphone, label: "Маркетинг" },
]

export default function DashboardLayout({
    children,
}: {
    children: React.ReactNode
}) {
    const pathname = usePathname()
    const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

    return (
        <div className="flex min-h-screen w-full bg-muted/40">
            {/* Sidebar (Desktop) */}
            <aside className="hidden w-64 flex-col border-r bg-background md:flex">
                <div className="flex h-14 items-center border-b px-4 lg:h-[60px] lg:px-6">
                    <Link href="/dashboard" className="flex items-center gap-2 font-semibold">
                        <span className="">Partner Cabinet</span>
                    </Link>
                </div>
                <div className="flex flex-1 flex-col gap-2 p-4">
                    <nav className="grid items-start px-2 text-sm font-medium lg:px-4">
                        {sidebarItems.map((item) => (
                            <Link
                                key={item.href}
                                href={item.href}
                                className={cn(
                                    "flex items-center gap-3 rounded-lg px-3 py-2 transition-all hover:text-primary",
                                    pathname === item.href
                                        ? "bg-muted text-primary"
                                        : "text-muted-foreground"
                                )}
                            >
                                <item.icon className="h-4 w-4" />
                                {item.label}
                            </Link>
                        ))}
                    </nav>
                </div>
                <div className="mt-auto p-4">
                    <Button variant="outline" className="w-full justify-start gap-2" asChild>
                        <Link href="/login">
                            <LogOut className="h-4 w-4" />
                            Выйти
                        </Link>
                    </Button>
                </div>
            </aside>

            {/* Main Content */}
            <div className="flex flex-col sm:gap-4 sm:py-4 sm:pl-14 md:pl-0 w-full">
                {/* Header (Mobile Trigger + User) */}
                <header className="sticky top-0 z-30 flex h-14 items-center gap-4 border-b bg-background px-4 sm:static sm:h-auto sm:border-0 sm:bg-transparent sm:px-6 md:hidden">
                    <Button variant="outline" size="icon" className="md:hidden">
                        <Menu className="h-5 w-5" />
                        <span className="sr-only">Toggle Menu</span>
                    </Button>
                    <div className="w-full flex-1">
                        <span className="font-semibold">Partner Cabinet</span>
                    </div>
                </header>
                <main className="grid flex-1 items-start gap-4 p-4 sm:px-6 sm:py-0 md:gap-8">
                    {children}
                </main>
            </div>
        </div>
    )
}
