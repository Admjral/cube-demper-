"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { useStore } from "@/store/use-store"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Button } from "@/components/ui/button"
import {
  LayoutDashboard,
  Bot,
  BarChart3,
  Calculator,
  Package,
  MessageSquare,
  Plug,
  Receipt,
  Scale,
  User,
  Settings,
  X,
  FileStack,
} from "lucide-react"

const navigation = [
  {
    name: "Dashboard",
    nameRu: "Главная",
    href: "/dashboard",
    icon: LayoutDashboard,
  },
  {
    name: "Price Bot",
    nameRu: "Демпинг цен",
    href: "/dashboard/price-bot",
    icon: Bot,
  },
  {
    name: "Sales",
    nameRu: "Продажи",
    href: "/dashboard/sales",
    icon: BarChart3,
  },
  {
    name: "Invoice Merger",
    nameRu: "Склейка накладных",
    href: "/dashboard/invoice-merger",
    icon: FileStack,
  },
  {
    name: "Unit Economics",
    nameRu: "Юнит-экономика",
    href: "/dashboard/unit-economics",
    icon: Calculator,
  },
  {
    name: "Pre-orders",
    nameRu: "Предзаказы",
    href: "/dashboard/pre-orders",
    icon: Package,
  },
  {
    name: "WhatsApp",
    nameRu: "WhatsApp",
    href: "/dashboard/whatsapp",
    icon: MessageSquare,
  },
  {
    name: "Integrations",
    nameRu: "Интеграции",
    href: "/dashboard/integrations",
    icon: Plug,
  },
  {
    name: "AI Accountant",
    nameRu: "AI-Бухгалтер",
    href: "/dashboard/ai-accountant",
    icon: Receipt,
  },
  {
    name: "AI Lawyer",
    nameRu: "AI-Юрист",
    href: "/dashboard/ai-lawyer",
    icon: Scale,
  },
]

const bottomNavigation = [
  {
    name: "Profile",
    nameRu: "Профиль",
    href: "/dashboard/profile",
    icon: User,
  },
  {
    name: "Settings",
    nameRu: "Настройки",
    href: "/dashboard/settings",
    icon: Settings,
  },
]

export function Sidebar() {
  const pathname = usePathname()
  const { sidebarOpen, setSidebarOpen, locale } = useStore()

  return (
    <>
      {/* Mobile overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/20 backdrop-blur-sm lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={cn(
          "fixed inset-y-0 left-0 z-50 w-64 glass-sidebar transform transition-transform duration-300 ease-in-out lg:translate-x-0 lg:static lg:z-auto",
          sidebarOpen ? "translate-x-0" : "-translate-x-full"
        )}
      >
        <div className="flex h-full flex-col">
          {/* Header */}
          <div className="flex h-16 items-center justify-between px-4 border-b border-sidebar-border">
            <Link href="/dashboard" className="flex items-center gap-2">
              <div className="h-8 w-8 rounded-lg bg-primary flex items-center justify-center">
                <span className="text-primary-foreground font-bold text-sm">D</span>
              </div>
              <span className="font-semibold text-lg">Demper</span>
            </Link>
            <Button
              variant="ghost"
              size="icon"
              className="lg:hidden touch-target"
              onClick={() => setSidebarOpen(false)}
            >
              <X className="h-5 w-5" />
            </Button>
          </div>

          {/* Navigation */}
          <ScrollArea className="flex-1 py-4">
            <nav className="space-y-1 px-3">
              {navigation.map((item) => {
                const isActive = pathname === item.href
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    onClick={() => setSidebarOpen(false)}
                    className={cn(
                      "flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors touch-target",
                      isActive
                        ? "bg-sidebar-accent text-sidebar-accent-foreground"
                        : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
                    )}
                  >
                    <item.icon className="h-5 w-5 shrink-0" />
                    <span>{locale === 'ru' ? item.nameRu : item.name}</span>
                  </Link>
                )
              })}
            </nav>
          </ScrollArea>

          {/* Bottom navigation */}
          <div className="border-t border-sidebar-border p-3 space-y-1">
            {bottomNavigation.map((item) => {
              const isActive = pathname === item.href
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setSidebarOpen(false)}
                  className={cn(
                    "flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors touch-target",
                    isActive
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
                      : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
                  )}
                >
                  <item.icon className="h-5 w-5 shrink-0" />
                  <span>{locale === 'ru' ? item.nameRu : item.name}</span>
                </Link>
              )
            })}
          </div>
        </div>
      </aside>
    </>
  )
}
