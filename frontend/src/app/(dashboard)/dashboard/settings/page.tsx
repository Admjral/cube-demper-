"use client"

import { useStore } from "@/store/use-store"
import { useT } from "@/lib/i18n"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Separator } from "@/components/ui/separator"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Settings,
  Bell,
  Moon,
  Sun,
  Languages,
  Store,
  Smartphone,
  Mail,
  MessageSquare,
} from "lucide-react"
import { useTheme } from "next-themes"

export default function SettingsPage() {
  const { locale, setLocale, stores, selectedStore, setSelectedStore } = useStore()
  const { theme, setTheme } = useTheme()
  const t = useT()

  return (
    <div className="space-y-6 max-w-2xl">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold">
          {t("settings.title")}
        </h1>
        <p className="text-muted-foreground">
          {t("settings.subtitle")}
        </p>
      </div>

      {/* Appearance */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Moon className="h-5 w-5" />
            {t("settings.appearance")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Theme */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              {theme === "dark" ? (
                <Moon className="h-5 w-5 text-muted-foreground" />
              ) : (
                <Sun className="h-5 w-5 text-muted-foreground" />
              )}
              <div>
                <p className="font-medium">
                  {t("settings.theme")}
                </p>
                <p className="text-sm text-muted-foreground">
                  {t("settings.themeDesc")}
                </p>
              </div>
            </div>
            <Select value={theme} onValueChange={setTheme}>
              <SelectTrigger className="w-[140px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="light">
                  {t("settings.light")}
                </SelectItem>
                <SelectItem value="dark">
                  {t("settings.dark")}
                </SelectItem>
                <SelectItem value="system">
                  {t("settings.system")}
                </SelectItem>
              </SelectContent>
            </Select>
          </div>

          <Separator />

          {/* Language */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Languages className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="font-medium">
                  {t("settings.language")}
                </p>
                <p className="text-sm text-muted-foreground">
                  {t("settings.languageDesc")}
                </p>
              </div>
            </div>
            <Select value={locale} onValueChange={(v) => setLocale(v as 'ru' | 'kz')}>
              <SelectTrigger className="w-[140px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ru">Русский</SelectItem>
                <SelectItem value="kz">Қазақша</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Store */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Store className="h-5 w-5" />
            {t("settings.store")}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">
                {t("settings.currentStore")}
              </p>
              <p className="text-sm text-muted-foreground">
                {selectedStore?.name || t("settings.notSelected")}
              </p>
            </div>
            <Select
              value={selectedStore?.id}
              onValueChange={(id) => {
                const store = stores.find((s) => s.id === id)
                if (store) setSelectedStore(store)
              }}
            >
              <SelectTrigger className="w-[180px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {stores.map((store) => (
                  <SelectItem key={store.id} value={store.id}>
                    {store.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Notifications */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Bell className="h-5 w-5" />
            {t("settings.notifications")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Push notifications */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Smartphone className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="font-medium">
                  {t("settings.pushNotifications")}
                </p>
                <p className="text-sm text-muted-foreground">
                  {t("settings.browserNotifications")}
                </p>
              </div>
            </div>
            <Switch defaultChecked />
          </div>

          <Separator />

          {/* Email notifications */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Mail className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="font-medium">
                  {t("settings.emailNotifications")}
                </p>
                <p className="text-sm text-muted-foreground">
                  {t("settings.emailNotificationsDesc")}
                </p>
              </div>
            </div>
            <Switch defaultChecked />
          </div>

          <Separator />

          {/* Order notifications */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <MessageSquare className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="font-medium">
                  {t("settings.newOrders")}
                </p>
                <p className="text-sm text-muted-foreground">
                  {t("settings.newOrdersDesc")}
                </p>
              </div>
            </div>
            <Switch defaultChecked />
          </div>

          <Separator />

          {/* Price bot notifications */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Settings className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="font-medium">
                  {t("settings.priceChanges")}
                </p>
                <p className="text-sm text-muted-foreground">
                  {t("settings.priceChangesDesc")}
                </p>
              </div>
            </div>
            <Switch />
          </div>
        </CardContent>
      </Card>

      {/* Save button */}
      <Button className="w-full sm:w-auto">
        {t("settings.saveSettings")}
      </Button>
    </div>
  )
}
