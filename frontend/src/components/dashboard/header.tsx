"use client"

import { useStore } from "@/store/use-store"
import { Button } from "@/components/ui/button"
import { ThemeToggle } from "@/components/shared/theme-toggle"
import { LanguageSwitcher } from "@/components/shared/language-switcher"
import { StoreSelector } from "@/components/shared/store-selector"
import { Menu, Bell } from "lucide-react"
import { Badge } from "@/components/ui/badge"

export function Header() {
  const { setSidebarOpen } = useStore()

  return (
    <header className="sticky top-0 z-30 glass-header safe-top">
      <div className="flex h-16 items-center justify-between px-4 gap-4">
        {/* Left side */}
        <div className="flex items-center gap-2">
          {/* Mobile menu button */}
          <Button
            variant="ghost"
            size="icon"
            className="lg:hidden touch-target"
            onClick={() => setSidebarOpen(true)}
          >
            <Menu className="h-5 w-5" />
            <span className="sr-only">Open menu</span>
          </Button>

          {/* Store selector */}
          <StoreSelector />
        </div>

        {/* Right side */}
        <div className="flex items-center gap-1">
          {/* Notifications */}
          <Button variant="ghost" size="icon" className="touch-target relative">
            <Bell className="h-5 w-5" />
            <Badge
              className="absolute -top-1 -right-1 h-5 w-5 p-0 flex items-center justify-center text-xs"
              variant="destructive"
            >
              3
            </Badge>
            <span className="sr-only">Notifications</span>
          </Button>

          {/* Language switcher */}
          <LanguageSwitcher />

          {/* Theme toggle */}
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}
