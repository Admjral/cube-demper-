'use client'

import { Suspense, useState } from 'react'
import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Checkbox } from '@/components/ui/checkbox'
import { useAuth } from '@/hooks/use-auth'
import { useT } from '@/lib/i18n'
import { Loader2, Mail, Lock, User, Eye, EyeOff, Phone, Tag } from 'lucide-react'

export default function RegisterPage() {
  return (
    <Suspense>
      <RegisterForm />
    </Suspense>
  )
}

function RegisterForm() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const { signUp } = useAuth()
  const t = useT()

  const [name, setName] = useState('')
  const [email, setEmail] = useState('')
  const [phone, setPhone] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [refCode, setRefCode] = useState(searchParams.get('ref') || '')
  const [showPassword, setShowPassword] = useState(false)
  const [acceptTerms, setAcceptTerms] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    // Validate phone
    const phoneDigits = phone.replace(/\D/g, '')
    if (phoneDigits.length < 10 || phoneDigits.length > 15) {
      setError(t("auth.invalidPhone"))
      return
    }

    if (password !== confirmPassword) {
      setError(t("auth.passwordsMismatch"))
      return
    }

    if (password.length < 8) {
      setError(t("auth.passwordMin8"))
      return
    }

    if (!/\d/.test(password)) {
      setError(t("auth.passwordDigit"))
      return
    }

    if (!/[A-Z]/.test(password)) {
      setError(t("auth.passwordUpper"))
      return
    }

    if (!/[a-z]/.test(password)) {
      setError(t("auth.passwordLower"))
      return
    }

    if (!acceptTerms) {
      setError(t("auth.mustAcceptTerms"))
      return
    }

    setLoading(true)

    const { error } = await signUp(email, password, {
      full_name: name,
      phone: phoneDigits,
      ref_code: refCode.trim() || undefined,
    })

    if (error) {
      setError(error.message === 'User already registered'
        ? t("auth.emailExists")
        : error.message)
      setLoading(false)
      return
    }

    // Registration successful + auto-logged in + OTP sent
    // Redirect to phone verification
    router.push('/verify-phone')
  }

  return (
    <div className="glass-card p-8">
      <div className="text-center mb-6">
        <h2 className="text-2xl font-semibold text-foreground">{t("auth.registerTitle")}</h2>
        <p className="text-muted-foreground mt-1">
          {t("auth.registerSubtitle")}
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="space-y-2">
          <Label htmlFor="name">{t("auth.name")}</Label>
          <div className="relative">
            <User className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              id="name"
              type="text"
              placeholder={t("auth.yourName")}
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="pl-10"
              required
              disabled={loading}
            />
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="email">{t("auth.email")}</Label>
          <div className="relative">
            <Mail className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              id="email"
              type="email"
              placeholder="your@email.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="pl-10"
              required
              disabled={loading}
            />
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="phone">{t("auth.phoneWhatsApp")}</Label>
          <div className="relative">
            <Phone className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              id="phone"
              type="tel"
              placeholder="+7XXXXXXXXXX"
              value={phone}
              onChange={(e) => setPhone(e.target.value)}
              className="pl-10"
              required
              disabled={loading}
            />
          </div>
          <p className="text-xs text-muted-foreground">
            {t("auth.phoneWhatsAppDesc")}
          </p>
        </div>

        <div className="space-y-2">
          <Label htmlFor="password">{t("auth.password")}</Label>
          <div className="relative">
            <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              id="password"
              type={showPassword ? 'text' : 'password'}
              placeholder={t("auth.passwordHint")}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="pl-10 pr-10"
              required
              disabled={loading}
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              {showPassword ? (
                <EyeOff className="h-4 w-4" />
              ) : (
                <Eye className="h-4 w-4" />
              )}
            </button>
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="confirmPassword">{t("auth.confirmPassword")}</Label>
          <div className="relative">
            <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              id="confirmPassword"
              type={showPassword ? 'text' : 'password'}
              placeholder={t("auth.repeatPassword")}
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              className="pl-10"
              required
              disabled={loading}
            />
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="refCode">Промокод (необязательно)</Label>
          <div className="relative">
            <Tag className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              id="refCode"
              type="text"
              placeholder="Введите промокод"
              value={refCode}
              onChange={(e) => setRefCode(e.target.value)}
              className="pl-10"
              disabled={loading}
            />
          </div>
        </div>

        <div className="flex items-start space-x-2">
          <Checkbox
            id="terms"
            checked={acceptTerms}
            onCheckedChange={(checked) => setAcceptTerms(checked as boolean)}
            disabled={loading}
          />
          <label
            htmlFor="terms"
            className="text-sm text-muted-foreground leading-tight cursor-pointer"
          >
            {t("auth.acceptTerms").split(t("auth.termsOfService"))[0]}
            <Link href="/terms" className="text-primary hover:underline">
              {t("auth.termsOfService")}
            </Link>
            {t("auth.acceptTerms").split(t("auth.termsOfService"))[1].split(t("auth.privacyPolicy"))[0]}
            <Link href="/privacy" className="text-primary hover:underline">
              {t("auth.privacyPolicy")}
            </Link>
          </label>
        </div>

        {error && (
          <div className="p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
            {error}
          </div>
        )}

        <Button type="submit" className="w-full" disabled={loading}>
          {loading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              {t("auth.registering")}
            </>
          ) : (
            t("auth.createAccount")
          )}
        </Button>
      </form>

      <div className="mt-6 text-center text-sm text-muted-foreground">
        {t("auth.haveAccount")}{' '}
        <Link href="/login" className="text-primary hover:underline font-medium">
          {t("auth.signIn")}
        </Link>
      </div>
    </div>
  )
}
