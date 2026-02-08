"use client"

import { useStore } from "@/store/use-store"
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

  return (
    <div className="space-y-6 max-w-2xl">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold">
          {locale === "ru" ? "Настройки" : "Settings"}
        </h1>
        <p className="text-muted-foreground">
          {locale === "ru"
            ? "Настройки приложения и уведомлений"
            : "App and notification settings"}
        </p>
      </div>

      {/* Appearance */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Moon className="h-5 w-5" />
            {locale === "ru" ? "Внешний вид" : "Appearance"}
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
                  {locale === "ru" ? "Тема" : "Theme"}
                </p>
                <p className="text-sm text-muted-foreground">
                  {locale === "ru"
                    ? "Выберите светлую или тёмную тему"
                    : "Choose light or dark theme"}
                </p>
              </div>
            </div>
            <Select value={theme} onValueChange={setTheme}>
              <SelectTrigger className="w-[140px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="light">
                  {locale === "ru" ? "Светлая" : "Light"}
                </SelectItem>
                <SelectItem value="dark">
                  {locale === "ru" ? "Тёмная" : "Dark"}
                </SelectItem>
                <SelectItem value="system">
                  {locale === "ru" ? "Системная" : "System"}
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
                  {locale === "ru" ? "Язык" : "Language"}
                </p>
                <p className="text-sm text-muted-foreground">
                  {locale === "ru"
                    ? "Язык интерфейса"
                    : "Interface language"}
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
            {locale === "ru" ? "Магазин" : "Store"}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">
                {locale === "ru" ? "Текущий магазин" : "Current store"}
              </p>
              <p className="text-sm text-muted-foreground">
                {selectedStore?.name || (locale === "ru" ? "Не выбран" : "Not selected")}
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
            {locale === "ru" ? "Уведомления" : "Notifications"}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Push notifications */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Smartphone className="h-5 w-5 text-muted-foreground" />
              <div>
                <p className="font-medium">
                  {locale === "ru" ? "Push-уведомления" : "Push notifications"}
                </p>
                <p className="text-sm text-muted-foreground">
                  {locale === "ru"
                    ? "Уведомления в браузере"
                    : "Browser notifications"}
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
                  {locale === "ru" ? "Email-уведомления" : "Email notifications"}
                </p>
                <p className="text-sm text-muted-foreground">
                  {locale === "ru"
                    ? "Важные уведомления на почту"
                    : "Important notifications via email"}
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
                  {locale === "ru" ? "Новые заказы" : "New orders"}
                </p>
                <p className="text-sm text-muted-foreground">
                  {locale === "ru"
                    ? "Уведомления о новых заказах"
                    : "Notifications about new orders"}
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
                  {locale === "ru" ? "Изменения цен" : "Price changes"}
                </p>
                <p className="text-sm text-muted-foreground">
                  {locale === "ru"
                    ? "Уведомления от ценового бота"
                    : "Price bot notifications"}
                </p>
              </div>
            </div>
            <Switch />
          </div>
        </CardContent>
      </Card>

      {/* Save button */}
      <Button className="w-full sm:w-auto">
        {locale === "ru" ? "Сохранить настройки" : "Save settings"}
      </Button>
    </div>
  )
}
