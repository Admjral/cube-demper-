'use client'

import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useAuth } from '@/hooks/use-auth'

// Types for the new tariff system
export interface UserFeatures {
  plan_code: string | null
  plan_name: string | null
  features: string[]
  analytics_limit: number  // -1 = unlimited
  demping_limit: number
  has_active_subscription: boolean
  is_trial: boolean
  trial_ends_at: string | null
  subscription_ends_at: string | null
}

export interface Plan {
  id: string
  code: string
  name: string
  price: number  // in tenge
  analytics_limit: number
  demping_limit: number
  features: string[]
  trial_days: number
}

export interface Addon {
  id: string
  code: string
  name: string
  description: string
  price: number  // in tenge
  is_recurring: boolean
  stackable: boolean
  features: string[]
  extra_limits: Record<string, number> | null
}

// Query keys
export const featureKeys = {
  all: ['features'] as const,
  user: () => [...featureKeys.all, 'user'] as const,
  plans: () => [...featureKeys.all, 'plans'] as const,
  addons: () => [...featureKeys.all, 'addons'] as const,
}

/**
 * Get current user's features and limits based on subscription and add-ons.
 */
export function useFeatures() {
  const { user } = useAuth()

  return useQuery({
    queryKey: featureKeys.user(),
    queryFn: () => api.get<UserFeatures>('/billing/features'),
    enabled: !!user?.id,
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

/**
 * Check if user has access to a specific feature.
 */
export function useHasFeature(feature: string): boolean {
  const { data } = useFeatures()
  return data?.features?.includes(feature) ?? false
}

/**
 * Get the limit for a specific type (analytics or demping).
 * Returns -1 for unlimited.
 */
export function useFeatureLimit(limitType: 'analytics' | 'demping'): number {
  const { data } = useFeatures()
  if (!data) return 0
  return limitType === 'analytics' ? data.analytics_limit : data.demping_limit
}

/**
 * Get available plans from the new tariff system.
 */
export function usePlansV2() {
  return useQuery({
    queryKey: featureKeys.plans(),
    queryFn: () => api.get<Plan[]>('/billing/plans-v2'),
    staleTime: 10 * 60 * 1000, // 10 minutes
  })
}

/**
 * Get available add-ons.
 */
export function useAddons() {
  return useQuery({
    queryKey: featureKeys.addons(),
    queryFn: () => api.get<Addon[]>('/billing/addons'),
    staleTime: 10 * 60 * 1000, // 10 minutes
  })
}

// Feature upgrade info for showing messages
export const FEATURE_UPGRADE_INFO: Record<string, {
  plans: string[]
  addons: string[]
  message: string
}> = {
  preorder: {
    plans: ['Стандарт', 'Премиум'],
    addons: ['Предзаказ'],
    message: 'Доступно на тарифе Стандарт или Премиум, либо приобретите доп. услугу «Предзаказ»'
  },
  whatsapp_auto: {
    plans: ['Стандарт', 'Премиум'],
    addons: ['WhatsApp рассылка'],
    message: 'Доступно на тарифе Стандарт или Премиум, либо приобретите доп. услугу «WhatsApp рассылка»'
  },
  whatsapp_bulk: {
    plans: ['Премиум'],
    addons: ['WhatsApp рассылка'],
    message: 'Доступно на тарифе Премиум, либо приобретите доп. услугу «WhatsApp рассылка»'
  },
  ai_salesman: {
    plans: [],
    addons: ['ИИ продажник'],
    message: 'Приобретите доп. услугу «ИИ продажник»'
  },
}

/**
 * Get upgrade message for a feature.
 */
export function getFeatureUpgradeMessage(feature: string): string {
  return FEATURE_UPGRADE_INFO[feature]?.message ?? 'Функция недоступна для вашего тарифа'
}
