"use client"

import { useStore } from "@/store/use-store"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Languages } from "lucide-react"

const languages = [
  { code: 'ru' as const, label: 'Русский', flag: '🇷🇺' },
  { code: 'kz' as const, label: 'Қазақша', flag: '🇰🇿' },
]

export function LanguageSwitcher() {
  const { locale, setLocale } = useStore()
  const currentLang = languages.find((l) => l.code === locale) || languages[0]

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon" className="touch-target">
          <Languages className="h-5 w-5" />
          <span className="sr-only">Change language</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="glass-card">
        {languages.map((lang) => (
          <DropdownMenuItem
            key={lang.code}
            onClick={() => setLocale(lang.code)}
            className={`touch-target ${locale === lang.code ? 'bg-accent' : ''}`}
          >
            <span className="mr-2">{lang.flag}</span>
            {lang.label}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
