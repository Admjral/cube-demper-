"use client"

import { useState } from "react"
import { useStore } from "@/store/use-store"
import { useAuth } from "@/hooks/use-auth"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Avatar } from "@/components/ui/avatar"
import { Skeleton } from "@/components/ui/skeleton"
import { Separator } from "@/components/ui/separator"
import {
  User,
  Mail,
  Phone,
  Building,
  Camera,
  Shield,
  LogOut,
} from "lucide-react"

export default function ProfilePage() {
  const { locale } = useStore()
  const { user, loading, signOut } = useAuth()
  const [isSaving, setIsSaving] = useState(false)

  // Extract user info
  const userEmail = user?.email || ""
  const userName = user?.full_name || user?.email?.split("@")[0] || ""
  const [firstName, lastName] = userName.split(" ")

  if (loading) {
    return (
      <div className="space-y-6 max-w-2xl">
        <div>
          <Skeleton className="h-8 w-32 mb-2" />
          <Skeleton className="h-4 w-48" />
        </div>
        <Card className="glass-card">
          <CardContent className="p-6">
            <div className="flex items-center gap-6">
              <Skeleton className="h-24 w-24 rounded-full" />
              <div className="space-y-2">
                <Skeleton className="h-6 w-32" />
                <Skeleton className="h-4 w-48" />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-2xl">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-semibold">
          {locale === "ru" ? "Профиль" : "Profile"}
        </h1>
        <p className="text-muted-foreground">
          {locale === "ru"
            ? "Управление данными аккаунта"
            : "Manage your account data"}
        </p>
      </div>

      {/* Avatar section */}
      <Card className="glass-card">
        <CardContent className="p-6">
          <div className="flex flex-col sm:flex-row items-center gap-6">
            <div className="relative">
              <Avatar className="h-24 w-24 bg-muted">
                <User className="h-12 w-12 text-muted-foreground m-auto" />
              </Avatar>
              <Button
                size="icon"
                variant="secondary"
                className="absolute -bottom-2 -right-2 h-8 w-8 rounded-full"
              >
                <Camera className="h-4 w-4" />
              </Button>
            </div>
            <div className="text-center sm:text-left">
              <h2 className="text-xl font-semibold">{userName || locale === "ru" ? "Пользователь" : "User"}</h2>
              <p className="text-muted-foreground">{userEmail}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Personal info */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <User className="h-5 w-5" />
            {locale === "ru" ? "Личная информация" : "Personal information"}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="firstName">
                {locale === "ru" ? "Имя" : "First name"}
              </Label>
              <Input id="firstName" defaultValue={firstName || ""} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="lastName">
                {locale === "ru" ? "Фамилия" : "Last name"}
              </Label>
              <Input id="lastName" defaultValue={lastName || ""} />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="email" className="flex items-center gap-2">
              <Mail className="h-4 w-4 text-muted-foreground" />
              Email
            </Label>
            <Input id="email" type="email" defaultValue={userEmail} disabled />
          </div>
          <div className="space-y-2">
            <Label htmlFor="phone" className="flex items-center gap-2">
              <Phone className="h-4 w-4 text-muted-foreground" />
              {locale === "ru" ? "Телефон" : "Phone"}
            </Label>
            <Input id="phone" placeholder={locale === "ru" ? "+7 XXX XXX XXXX" : "+7 XXX XXX XXXX"} />
          </div>
        </CardContent>
      </Card>

      {/* Business info */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Building className="h-5 w-5" />
            {locale === "ru" ? "Бизнес информация" : "Business information"}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="companyName">
              {locale === "ru" ? "Название компании" : "Company name"}
            </Label>
            <Input id="companyName" placeholder={locale === "ru" ? "ТОО Моя компания" : "My Company LLC"} />
          </div>
          <div className="grid sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="bin">
                {locale === "ru" ? "БИН/ИИН" : "BIN/IIN"}
              </Label>
              <Input id="bin" placeholder="123456789012" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="taxType">
                {locale === "ru" ? "Тип налогообложения" : "Tax type"}
              </Label>
              <Input
                id="taxType"
                placeholder={locale === "ru" ? "Упрощённый" : "Simplified"}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Security */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Shield className="h-5 w-5" />
            {locale === "ru" ? "Безопасность" : "Security"}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">
                {locale === "ru" ? "Пароль" : "Password"}
              </p>
              <p className="text-sm text-muted-foreground">
                {locale === "ru"
                  ? "Последнее изменение: 30 дней назад"
                  : "Last changed: 30 days ago"}
              </p>
            </div>
            <Button variant="outline">
              {locale === "ru" ? "Изменить" : "Change"}
            </Button>
          </div>
          <Separator />
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">
                {locale === "ru" ? "Двухфакторная аутентификация" : "Two-factor auth"}
              </p>
              <p className="text-sm text-muted-foreground">
                {locale === "ru" ? "Не включена" : "Not enabled"}
              </p>
            </div>
            <Button variant="outline">
              {locale === "ru" ? "Включить" : "Enable"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Actions */}
      <div className="flex flex-col sm:flex-row gap-4">
        <Button className="flex-1" disabled={isSaving}>
          {isSaving
            ? (locale === "ru" ? "Сохранение..." : "Saving...")
            : (locale === "ru" ? "Сохранить изменения" : "Save changes")}
        </Button>
        <Button
          variant="outline"
          className="flex-1 text-destructive"
          onClick={() => signOut()}
        >
          <LogOut className="h-4 w-4 mr-2" />
          {locale === "ru" ? "Выйти" : "Log out"}
        </Button>
      </div>
    </div>
  )
}
