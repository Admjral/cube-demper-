"use client"

import { useState, useEffect } from "react"
import Link from "next/link"
import { usePathname, useRouter } from "next/navigation"
import {
    MessageSquare,
    LogOut,
    Menu,
    Inbox,
    CheckCircle,
    Clock,
} from "lucide-react"

import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import { supportAuthClient } from "@/lib/auth"

const sidebarItems = [
    { href: "/dashboard", icon: Inbox, label: "Все чаты" },
    { href: "/dashboard?status=open", icon: MessageSquare, label: "Открытые" },
    { href: "/dashboard?status=pending", icon: Clock, label: "В ожидании" },
    { href: "/dashboard?status=closed", icon: CheckCircle, label: "Закрытые" },
]

export default function DashboardLayout({
    children,
}: {
    children: React.ReactNode
}) {
    const pathname = usePathname()
    const router = useRouter()
    const [user, setUser] = useState(supportAuthClient.getUser())
    const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

    useEffect(() => {
        if (!supportAuthClient.isAuthenticated()) {
            router.replace("/login")
        }
    }, [router])

    const handleLogout = async () => {
        await supportAuthClient.signOut()
        router.push("/login")
    }

    return (
        <div className="flex min-h-screen w-full bg-muted/40">
            {/* Sidebar (Desktop) */}
            <aside className="hidden w-64 flex-col border-r bg-background md:flex">
                <div className="flex h-14 items-center border-b px-4 lg:h-[60px] lg:px-6">
                    <Link href="/dashboard" className="flex items-center gap-2 font-semibold">
                        <MessageSquare className="h-5 w-5 text-primary" />
                        <span>Tech Support</span>
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
                                    pathname + (typeof window !== 'undefined' ? window.location.search : '') === item.href
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
                <div className="mt-auto p-4 space-y-2">
                    {user && (
                        <div className="px-3 py-2 text-sm text-muted-foreground">
                            {user.full_name || user.email}
                        </div>
                    )}
                    <Button
                        variant="outline"
                        className="w-full justify-start gap-2"
                        onClick={handleLogout}
                    >
                        <LogOut className="h-4 w-4" />
                        Выйти
                    </Button>
                </div>
            </aside>

            {/* Main Content */}
            <div className="flex flex-col sm:gap-4 sm:py-4 sm:pl-14 md:pl-0 w-full">
                {/* Header (Mobile Trigger + User) */}
                <header className="sticky top-0 z-30 flex h-14 items-center gap-4 border-b bg-background px-4 sm:static sm:h-auto sm:border-0 sm:bg-transparent sm:px-6 md:hidden">
                    <Button
                        variant="outline"
                        size="icon"
                        className="md:hidden"
                        onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
                    >
                        <Menu className="h-5 w-5" />
                        <span className="sr-only">Toggle Menu</span>
                    </Button>
                    <div className="w-full flex-1">
                        <span className="font-semibold">Tech Support</span>
                    </div>
                </header>
                <main className="flex-1 p-4 sm:px-6 sm:py-0 md:gap-8">
                    {children}
                </main>
            </div>
        </div>
    )
}
