"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { supportAuthClient } from "@/lib/auth"

export default function Home() {
  const router = useRouter()

  useEffect(() => {
    if (supportAuthClient.isAuthenticated()) {
      router.replace("/dashboard")
    } else {
      router.replace("/login")
    }
  }, [router])

  return (
    <div className="flex h-screen items-center justify-center">
      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
    </div>
  )
}
