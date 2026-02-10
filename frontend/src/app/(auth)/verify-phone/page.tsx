'use client'

import { useState, useEffect, useRef, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useAuth } from '@/hooks/use-auth'
import { useT } from '@/lib/i18n'
import { Loader2, Smartphone, RefreshCw } from 'lucide-react'

export default function VerifyPhonePage() {
  const router = useRouter()
  const { user, loading: authLoading, verifyOtp, sendOtp } = useAuth()
  const t = useT()

  const [digits, setDigits] = useState<string[]>(['', '', '', '', '', ''])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [resendCooldown, setResendCooldown] = useState(60)
  const [resending, setResending] = useState(false)
  const inputRefs = useRef<(HTMLInputElement | null)[]>([])

  // Redirect if not authenticated or already verified
  useEffect(() => {
    if (!authLoading) {
      if (!user) {
        router.push('/login')
      } else if (user.phone_verified) {
        router.push('/dashboard')
      }
    }
  }, [user, authLoading, router])

  // Countdown timer for resend
  useEffect(() => {
    if (resendCooldown <= 0) return
    const timer = setInterval(() => {
      setResendCooldown((prev) => prev - 1)
    }, 1000)
    return () => clearInterval(timer)
  }, [resendCooldown])

  const handleDigitChange = useCallback((index: number, value: string) => {
    if (value.length > 1) {
      // Handle paste
      const pastedDigits = value.replace(/\D/g, '').slice(0, 6).split('')
      const newDigits = [...digits]
      pastedDigits.forEach((d, i) => {
        if (index + i < 6) newDigits[index + i] = d
      })
      setDigits(newDigits)
      const nextIndex = Math.min(index + pastedDigits.length, 5)
      inputRefs.current[nextIndex]?.focus()
      return
    }

    if (value && !/^\d$/.test(value)) return

    const newDigits = [...digits]
    newDigits[index] = value
    setDigits(newDigits)
    setError(null)

    // Auto-focus next input
    if (value && index < 5) {
      inputRefs.current[index + 1]?.focus()
    }
  }, [digits])

  const handleKeyDown = useCallback((index: number, e: React.KeyboardEvent) => {
    if (e.key === 'Backspace' && !digits[index] && index > 0) {
      inputRefs.current[index - 1]?.focus()
    }
  }, [digits])

  // Auto-submit when all digits filled
  useEffect(() => {
    const code = digits.join('')
    if (code.length === 6 && digits.every(d => d !== '')) {
      handleVerify(code)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [digits])

  const handleVerify = async (code: string) => {
    if (loading) return
    setLoading(true)
    setError(null)

    const { error } = await verifyOtp(code)

    if (error) {
      setError(error.message)
      setLoading(false)
      // Clear digits on error
      setDigits(['', '', '', '', '', ''])
      inputRefs.current[0]?.focus()
      return
    }

    router.push('/dashboard')
  }

  const handleResend = async () => {
    if (resendCooldown > 0 || resending) return
    setResending(true)
    setError(null)

    const { error } = await sendOtp()

    if (error) {
      setError(error.message)
    } else {
      setResendCooldown(60)
    }

    setResending(false)
  }

  // Mask phone: 77001234567 -> +7 *** *** 45 67
  const maskedPhone = user?.phone
    ? `+${user.phone.slice(0, 1)} *** *** ${user.phone.slice(-4, -2)} ${user.phone.slice(-2)}`
    : ''

  if (authLoading) {
    return (
      <div className="glass-card p-8 flex justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="glass-card p-8">
      <div className="text-center mb-6">
        <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mx-auto mb-4">
          <Smartphone className="h-8 w-8 text-primary" />
        </div>
        <h2 className="text-2xl font-semibold text-foreground">{t("auth.verifyPhone")}</h2>
        <p className="text-muted-foreground mt-2">
          {t("auth.codeSentTo")}
        </p>
        {maskedPhone && (
          <p className="font-medium text-foreground mt-1">{maskedPhone}</p>
        )}
      </div>

      <div className="flex justify-center gap-2 mb-6">
        {digits.map((digit, index) => (
          <Input
            key={index}
            ref={(el) => { inputRefs.current[index] = el }}
            type="text"
            inputMode="numeric"
            maxLength={index === 0 ? 6 : 1}
            value={digit}
            onChange={(e) => handleDigitChange(index, e.target.value)}
            onKeyDown={(e) => handleKeyDown(index, e)}
            className="w-12 h-14 text-center text-xl font-semibold"
            disabled={loading}
            autoFocus={index === 0}
          />
        ))}
      </div>

      {error && (
        <div className="p-3 rounded-lg bg-destructive/10 text-destructive text-sm text-center mb-4">
          {error}
        </div>
      )}

      {loading && (
        <div className="flex justify-center mb-4">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
        </div>
      )}

      <div className="text-center">
        <Button
          variant="ghost"
          size="sm"
          onClick={handleResend}
          disabled={resendCooldown > 0 || resending}
          className="text-muted-foreground"
        >
          {resending ? (
            <Loader2 className="h-4 w-4 animate-spin mr-2" />
          ) : (
            <RefreshCw className="h-4 w-4 mr-2" />
          )}
          {resendCooldown > 0
            ? `${t("auth.resendCodeTimer")} (${resendCooldown}s)`
            : t("auth.resendCode")
          }
        </Button>
      </div>
    </div>
  )
}
