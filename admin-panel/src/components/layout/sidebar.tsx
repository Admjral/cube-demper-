"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { LayoutDashboard, Users, Store, UsersRound, LogOut } from "lucide-react"
import { authClient } from "@/lib/auth"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"

const navigation = [
  {
    name: "Статистика",
    href: "/dashboard",
    icon: LayoutDashboard,
  },
  {
    name: "Пользователи",
    href: "/dashboard/users",
    icon: Users,
  },
  {
    name: "Магазины",
    href: "/dashboard/stores",
    icon: Store,
  },
  {
    name: "Партнеры",
    href: "/dashboard/partners",
    icon: UsersRound,
  },
]

export function Sidebar() {
  const pathname = usePathname()
  const router = useRouter()

  const handleLogout = async () => {
    await authClient.signOut()
    router.push("/login")
    router.refresh()
  }

  return (
    <div className="glass-sidebar w-64 border-r min-h-screen p-4 flex flex-col">
      <div className="mb-8">
        <h1 className="text-xl font-bold">Admin Panel</h1>
        <p className="text-sm text-muted-foreground">Cube Demper</p>
      </div>

      <nav className="flex-1 space-y-1">
        {navigation.map((item) => {
          const isActive = pathname === item.href
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-lg transition-colors",
                isActive
                  ? "bg-primary text-primary-foreground"
                  : "hover:bg-accent hover:text-accent-foreground"
              )}
            >
              <item.icon className="h-5 w-5" />
              <span>{item.name}</span>
            </Link>
          )
        })}
      </nav>

      <Button
        variant="ghost"
        className="w-full justify-start"
        onClick={handleLogout}
      >
        <LogOut className="h-5 w-5 mr-3" />
        Выйти
      </Button>
    </div>
  )
}
