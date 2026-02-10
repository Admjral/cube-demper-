"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { useStore } from "@/store/use-store"
import { useAuth } from "@/hooks/use-auth"
import { useFeatures } from "@/hooks/api/use-features"
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
  Scale,
  User,
  Settings,
  X,
  FileStack,
  Shield,
  Sparkles,
  CreditCard,
  Lock,
  Headphones,
} from "lucide-react"

// Pages accessible on free plan
const FREE_PLAN_ALLOWED = [
  '/dashboard',
  '/dashboard/billing',
  '/dashboard/profile',
  '/dashboard/settings',
  '/dashboard/integrations',
  '/dashboard/support',
]

const navigation = [
  {
    name: "Dashboard",
    nameRu: "Главная",
    nameKz: "Басты бет",
    href: "/dashboard",
    icon: LayoutDashboard,
  },
  {
    name: "Niche Search",
    nameRu: "Поиск ниш",
    nameKz: "Тауша іздеу",
    href: "/dashboard/niche-search",
    icon: Sparkles,
  },
  {
    name: "Price Bot",
    nameRu: "Демпинг цен",
    nameKz: "Баға боты",
    href: "/dashboard/price-bot",
    icon: Bot,
  },
  {
    name: "Sales",
    nameRu: "Продажи",
    nameKz: "Сатылымдар",
    href: "/dashboard/sales",
    icon: BarChart3,
  },
  {
    name: "Invoice Merger",
    nameRu: "Склейка накладных",
    nameKz: "Жүкқұжаттар",
    href: "/dashboard/invoice-merger",
    icon: FileStack,
  },
  {
    name: "Unit Economics",
    nameRu: "Юнит-экономика",
    nameKz: "Бірлік экономикасы",
    href: "/dashboard/unit-economics",
    icon: Calculator,
  },
  {
    name: "Pre-orders",
    nameRu: "Предзаказы",
    nameKz: "Алдын ала тапсырыс",
    href: "/dashboard/pre-orders",
    icon: Package,
  },
  {
    name: "WhatsApp",
    nameRu: "WhatsApp",
    nameKz: "WhatsApp",
    href: "/dashboard/whatsapp",
    icon: MessageSquare,
  },
  {
    name: "Integrations",
    nameRu: "Интеграции",
    nameKz: "Интеграциялар",
    href: "/dashboard/integrations",
    icon: Plug,
  },
  {
    name: "AI Lawyer",
    nameRu: "AI-Юрист",
    nameKz: "AI Заңгер",
    href: "/dashboard/ai-lawyer",
    icon: Scale,
  },
]

const bottomNavigation = [
  {
    name: "Support",
    nameRu: "Поддержка",
    nameKz: "Қолдау",
    href: "/dashboard/support",
    icon: Headphones,
  },
  {
    name: "Billing",
    nameRu: "Тарифы",
    nameKz: "Тарифтер",
    href: "/dashboard/billing",
    icon: CreditCard,
  },
  {
    name: "Profile",
    nameRu: "Профиль",
    nameKz: "Профиль",
    href: "/dashboard/profile",
    icon: User,
  },
  {
    name: "Settings",
    nameRu: "Настройки",
    nameKz: "Баптаулар",
    href: "/dashboard/settings",
    icon: Settings,
  },
]

export function Sidebar() {
  const pathname = usePathname()
  const { sidebarOpen, setSidebarOpen, locale } = useStore()
  const { user } = useAuth()
  const { data: features } = useFeatures()
  const isAdmin = user?.role === 'admin'
  const isFreePlan = features && (features.plan_code === 'free' || (!features.plan_code && features.features?.length === 0))

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
                const isLocked = isFreePlan && !FREE_PLAN_ALLOWED.includes(item.href)
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    onClick={() => setSidebarOpen(false)}
                    className={cn(
                      "flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors touch-target",
                      isActive
                        ? "bg-sidebar-accent text-sidebar-accent-foreground"
                        : isLocked
                          ? "text-sidebar-foreground/40"
                          : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
                    )}
                  >
                    <item.icon className="h-5 w-5 shrink-0" />
                    <span className="flex-1">{locale === 'ru' ? item.nameRu : locale === 'kz' ? item.nameKz : item.name}</span>
                    {isLocked && <Lock className="h-3.5 w-3.5 shrink-0" />}
                  </Link>
                )
              })}
            </nav>
          </ScrollArea>

          {/* Bottom navigation */}
          <div className="border-t border-sidebar-border p-3 space-y-1">
            {/* Admin Panel button - only for admins */}
            {isAdmin && (
              <Link
                href="/admin"
                onClick={() => setSidebarOpen(false)}
                className={cn(
                  "flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors touch-target",
                  pathname.startsWith('/admin')
                    ? "bg-primary text-primary-foreground"
                    : "text-primary hover:bg-primary/10"
                )}
              >
                <Shield className="h-5 w-5 shrink-0" />
                <span>{locale === 'ru' ? 'Админ-панель' : locale === 'kz' ? 'Әкімші тақтасы' : 'Admin Panel'}</span>
              </Link>
            )}
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
                  <span>{locale === 'ru' ? item.nameRu : locale === 'kz' ? item.nameKz : item.name}</span>
                </Link>
              )
            })}
          </div>
        </div>
      </aside>
    </>
  )
}
