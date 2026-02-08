"use client"

import { authClient } from "@/lib/auth"

export function Header() {
  const user = authClient.getUser()

  return (
    <div className="glass-header h-16 border-b px-6 flex items-center justify-between">
      <h2 className="text-lg font-semibold">Админ-панель</h2>
      <div className="flex items-center gap-4">
        <span className="text-sm text-muted-foreground">
          {user?.email}
        </span>
      </div>
    </div>
  )
}
