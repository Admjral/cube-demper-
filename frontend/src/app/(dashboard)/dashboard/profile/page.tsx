"use client"

import { useState } from "react"
import { useT } from "@/lib/i18n"
import { useAuth } from "@/hooks/use-auth"
import { useFeatures } from "@/hooks/api/use-features"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Avatar } from "@/components/ui/avatar"
import { Skeleton } from "@/components/ui/skeleton"
import { Separator } from "@/components/ui/separator"
import { featureLabels } from "@/lib/features"
import { format } from "date-fns"
import { ru } from "date-fns/locale"
import {
  User,
  Mail,
  Phone,
  Building,
  Camera,
  Shield,
  LogOut,
  CreditCard,
  Sparkles,
  ArrowRight,
} from "lucide-react"
import Link from "next/link"

export default function ProfilePage() {
  const t = useT()
  const { user, loading, signOut } = useAuth()
  const { data: features } = useFeatures()
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
          {t("profile.title")}
        </h1>
        <p className="text-muted-foreground">
          {t("profile.subtitle")}
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
              <h2 className="text-xl font-semibold">{userName || t("profile.user")}</h2>
              <p className="text-muted-foreground">{userEmail}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Subscription info */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <CreditCard className="h-5 w-5" />
            {t("profile.subscription")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {features?.has_active_subscription ? (
            <>
              <div className="flex items-center justify-between">
                <div>
                  <p className="font-medium text-foreground">{features.plan_name || t("billing.plan")}</p>
                  {features.subscription_ends_at && (
                    <p className="text-sm text-muted-foreground">
                      {t("profile.activeUntil")} {format(new Date(features.subscription_ends_at), 'd MMMM yyyy', { locale: ru })}
                    </p>
                  )}
                  {features.is_trial && features.trial_ends_at && (
                    <p className="text-sm text-muted-foreground">
                      {t("profile.trialUntil")} {format(new Date(features.trial_ends_at), 'd MMMM yyyy', { locale: ru })}
                    </p>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  {features.is_trial && (
                    <Badge variant="outline">
                      <Sparkles className="h-3 w-3 mr-1" />
                      {t("profile.trial")}
                    </Badge>
                  )}
                  <Badge variant="default">{t("profile.activeSub")}</Badge>
                </div>
              </div>

              <Separator />

              <div>
                <p className="text-sm text-muted-foreground mb-2">{t("profile.limits")}</p>
                <div className="grid sm:grid-cols-2 gap-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">{t("profile.analytics")}</span>
                    <span className="font-medium">
                      {features.analytics_limit === -1 ? t("profile.unlimited") : `${features.analytics_limit} ${t("common.products")}`}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">{t("profile.demping")}</span>
                    <span className="font-medium">{features.demping_limit} {t("common.products")}</span>
                  </div>
                </div>
              </div>

              {(features.features || []).length > 0 && (
                <>
                  <Separator />
                  <div>
                    <p className="text-sm text-muted-foreground mb-2">{t("profile.availableFeatures")}</p>
                    <div className="flex flex-wrap gap-1.5">
                      {(features.features || []).map((feature) => (
                        <Badge key={feature} variant="secondary" className="text-xs">
                          {featureLabels[feature] || feature}
                        </Badge>
                      ))}
                    </div>
                  </div>
                </>
              )}

              <div className="pt-2">
                <Link href="/dashboard/billing">
                  <Button variant="outline" size="sm" className="w-full">
                    {t("profile.managePlan")} <ArrowRight className="h-4 w-4 ml-2" />
                  </Button>
                </Link>
              </div>
            </>
          ) : (
            <div className="text-center py-4">
              <p className="text-muted-foreground mb-3">{t("profile.noSubscription")}</p>
              <Link href="/dashboard/billing">
                <Button size="sm">
                  <CreditCard className="h-4 w-4 mr-2" />
                  {t("profile.activatePlan")}
                </Button>
              </Link>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Personal info */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <User className="h-5 w-5" />
            {t("profile.personalInfo")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="firstName">
                {t("profile.firstName")}
              </Label>
              <Input id="firstName" defaultValue={firstName || ""} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="lastName">
                {t("profile.lastName")}
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
              {t("profile.phone")}
            </Label>
            <Input id="phone" placeholder={t("profile.phonePlaceholder")} />
          </div>
        </CardContent>
      </Card>

      {/* Business info */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <Building className="h-5 w-5" />
            {t("profile.businessInfo")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="companyName">
              {t("profile.companyName")}
            </Label>
            <Input id="companyName" placeholder={t("profile.companyPlaceholder")} />
          </div>
          <div className="grid sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="bin">
                {t("profile.binIin")}
              </Label>
              <Input id="bin" placeholder="123456789012" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="taxType">
                {t("profile.taxType")}
              </Label>
              <Input
                id="taxType"
                placeholder={t("profile.simplified")}
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
            {t("profile.security")}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">
                {t("profile.passwordLabel")}
              </p>
              <p className="text-sm text-muted-foreground">
                {t("profile.passwordChanged")}
              </p>
            </div>
            <Button variant="outline">
              {t("profile.change")}
            </Button>
          </div>
          <Separator />
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">
                {t("profile.twoFactor")}
              </p>
              <p className="text-sm text-muted-foreground">
                {t("profile.notEnabled")}
              </p>
            </div>
            <Button variant="outline">
              {t("profile.enable")}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Actions */}
      <div className="flex flex-col sm:flex-row gap-4">
        <Button className="flex-1" disabled={isSaving}>
          {isSaving
            ? t("profile.saving")
            : t("profile.saveChanges")}
        </Button>
        <Button
          variant="outline"
          className="flex-1 text-destructive"
          onClick={() => signOut()}
        >
          <LogOut className="h-4 w-4 mr-2" />
          {t("profile.logout")}
        </Button>
      </div>
    </div>
  )
}
