'use client'

import { useEffect, useState, useCallback } from 'react'
import { authClient, User } from '@/lib/auth'
import { useRouter } from 'next/navigation'

export interface AuthState {
  user: User | null
  loading: boolean
}

export function useAuth() {
  const router = useRouter()
  const [state, setState] = useState<AuthState>({
    user: null,
    loading: true,
  })

  useEffect(() => {
    // Get initial user from localStorage
    const { user } = authClient.getSession()
    setState({ user, loading: false })

    // Optionally verify token with backend
    if (user) {
      authClient.getCurrentUser().then(({ user: verifiedUser }) => {
        if (verifiedUser) {
          setState({ user: verifiedUser, loading: false })
        } else {
          setState({ user: null, loading: false })
        }
      })
    }
  }, [])

  const signIn = useCallback(async (email: string, password: string) => {
    const { data, error } = await authClient.signIn(email, password)
    if (data) {
      setState({ user: data.user, loading: false })
    }
    return { data, error }
  }, [])

  const signUp = useCallback(async (
    email: string,
    password: string,
    metadata?: { full_name?: string; phone?: string }
  ) => {
    const result = await authClient.signUp(email, password, metadata)
    if (result.data) {
      setState({ user: result.data.user, loading: false })
    }
    return result
  }, [])

  const signOut = useCallback(async () => {
    const result = await authClient.signOut()
    setState({ user: null, loading: false })
    router.push('/login')
    return result
  }, [router])

  const sendOtp = useCallback(async () => {
    return authClient.sendOtp()
  }, [])

  const verifyOtp = useCallback(async (code: string) => {
    const result = await authClient.verifyOtp(code)
    if (!result.error) {
      // Refresh user state
      const { user } = await authClient.getCurrentUser()
      if (user) setState({ user, loading: false })
    }
    return result
  }, [])

  const resetPassword = useCallback(async (email: string) => {
    return authClient.resetPassword(email)
  }, [])

  const updatePassword = useCallback(async (token: string, newPassword: string) => {
    return authClient.updatePassword(token, newPassword)
  }, [])

  return {
    ...state,
    signIn,
    signUp,
    signOut,
    sendOtp,
    verifyOtp,
    resetPassword,
    updatePassword,
  }
}
