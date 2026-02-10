"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { useStore } from "@/store/use-store"
import {
  LayoutDashboard,
  Bot,
  BarChart3,
  MessageSquare,
  Headphones,
} from "lucide-react"

const mobileNavigation = [
  {
    name: "Home",
    nameRu: "Главная",
    nameKz: "Басты",
    href: "/dashboard",
    icon: LayoutDashboard,
  },
  {
    name: "Sales",
    nameRu: "Продажи",
    nameKz: "Сату",
    href: "/dashboard/sales",
    icon: BarChart3,
  },
  {
    name: "Bot",
    nameRu: "Бот",
    nameKz: "Бот",
    href: "/dashboard/price-bot",
    icon: Bot,
  },
  {
    name: "Chat",
    nameRu: "Чат",
    nameKz: "Чат",
    href: "/dashboard/whatsapp",
    icon: MessageSquare,
  },
  {
    name: "Support",
    nameRu: "Поддержка",
    nameKz: "Қолдау",
    href: "/dashboard/support",
    icon: Headphones,
  },
]

export function BottomNav() {
  const pathname = usePathname()
  const { locale } = useStore()

  return (
    <nav className="fixed bottom-0 left-0 right-0 z-40 glass-bottom-nav lg:hidden safe-bottom">
      <div className="flex items-center justify-around h-16 px-2">
        {mobileNavigation.map((item) => {
          const isActive = pathname === item.href ||
            (item.href !== "/dashboard" && pathname.startsWith(item.href))

          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex flex-col items-center justify-center gap-1 px-3 py-2 rounded-xl transition-colors touch-target min-w-[64px]",
                isActive
                  ? "text-primary"
                  : "text-muted-foreground"
              )}
            >
              <item.icon className={cn(
                "h-5 w-5 transition-transform",
                isActive && "scale-110"
              )} />
              <span className="text-xs font-medium">
                {locale === 'ru' ? item.nameRu : locale === 'kz' ? item.nameKz : item.name}
              </span>
            </Link>
          )
        })}
      </div>
    </nav>
  )
}
