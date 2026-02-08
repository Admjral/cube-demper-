'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type {
  KaspiProduct,
  ProductListResponse,
  ProductUpdateRequest,
  PriceUpdateRequest,
  PriceHistory,
  DempingSettings,
  DempingSettingsUpdate,
  ProductDempingDetails,
  ProductDempingUpdate,
} from '@/types/api'

// Query keys
export const productKeys = {
  all: ['products'] as const,
  list: (storeId: string) => [...productKeys.all, 'list', storeId] as const,
  detail: (productId: string) => [...productKeys.all, 'detail', productId] as const,
  priceHistory: (productId: string) => [...productKeys.all, 'price-history', productId] as const,
}

export const dempingKeys = {
  all: ['demping'] as const,
  settings: (storeId: string) => [...dempingKeys.all, 'settings', storeId] as const,
}

// =============================================
// Products
// =============================================

interface ProductsQueryParams {
  skip?: number
  limit?: number
  is_active?: boolean
  bot_active?: boolean
  search?: string
}

// Get products for a store
export function useProducts(storeId: string | undefined, params?: ProductsQueryParams) {
  return useQuery({
    queryKey: [...productKeys.list(storeId || ''), params],
    queryFn: async () => {
      const queryParams = new URLSearchParams()
      if (params?.skip) queryParams.set('skip', params.skip.toString())
      if (params?.limit) queryParams.set('limit', params.limit.toString())
      if (params?.is_active !== undefined) queryParams.set('is_active', params.is_active.toString())
      if (params?.bot_active !== undefined) queryParams.set('bot_active', params.bot_active.toString())
      if (params?.search) queryParams.set('search', params.search)

      const queryString = queryParams.toString()
      const response = await api.get<ProductListResponse>(`/kaspi/stores/${storeId}/products${queryString ? `?${queryString}` : ''}`)
      // Return just the products array for easier consumption
      return response.products
    },
    enabled: !!storeId,
    staleTime: 30000,
  })
}

// Get single product
export function useProduct(productId: string | undefined) {
  return useQuery({
    queryKey: productKeys.detail(productId || ''),
    queryFn: () => api.get<KaspiProduct>(`/kaspi/products/${productId}`),
    enabled: !!productId,
  })
}

// Update product
export function useUpdateProduct() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ productId, data }: { productId: string; data: ProductUpdateRequest }) =>
      api.patch<KaspiProduct>(`/kaspi/products/${productId}`, data),
    onSuccess: (_, { productId }) => {
      queryClient.invalidateQueries({ queryKey: productKeys.detail(productId) })
      queryClient.invalidateQueries({ queryKey: productKeys.all })
    },
  })
}

// Update product price
export function useUpdateProductPrice() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ productId, data }: { productId: string; data: PriceUpdateRequest }) =>
      api.patch<KaspiProduct>(`/kaspi/products/${productId}/price`, data),
    onSuccess: (_, { productId }) => {
      queryClient.invalidateQueries({ queryKey: productKeys.detail(productId) })
      queryClient.invalidateQueries({ queryKey: productKeys.priceHistory(productId) })
      queryClient.invalidateQueries({ queryKey: productKeys.all })
    },
  })
}

// Get price history
export function usePriceHistory(productId: string | undefined, limit: number = 50) {
  return useQuery({
    queryKey: productKeys.priceHistory(productId || ''),
    queryFn: async () => {
      const response = await api.get<{ history: PriceHistory[] }>(`/kaspi/products/${productId}/price-history?limit=${limit}`)
      return response.history
    },
    enabled: !!productId,
  })
}

// Get product demping details
export function useProductDempingDetails(productId: string | undefined) {
  return useQuery({
    queryKey: [...productKeys.detail(productId || ''), 'demping-details'],
    queryFn: () => api.get<ProductDempingDetails>(`/kaspi/products/${productId}/demping-details`),
    enabled: !!productId,
  })
}

// Update product demping settings
export function useUpdateProductDemping() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ productId, data }: { productId: string; data: ProductDempingUpdate }) =>
      api.patch<KaspiProduct>(`/kaspi/products/${productId}`, data),
    onSuccess: (_, { productId }) => {
      queryClient.invalidateQueries({ queryKey: productKeys.detail(productId) })
      queryClient.invalidateQueries({ queryKey: [...productKeys.detail(productId), 'demping-details'] })
      queryClient.invalidateQueries({ queryKey: productKeys.all })
    },
  })
}

// Check demping for a product (manual trigger)
export interface CheckDempingResponse {
  status: string
  product_id: string
  product_name: string
  current_price: number
  min_price: number
  max_price: number | null
  price_step: number
  strategy: string
  bot_active: boolean
  message: string
  last_check_time: string | null
}

export function useCheckProductDemping() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (productId: string) =>
      api.post<CheckDempingResponse>(`/kaspi/products/${productId}/check-demping`),
    onSuccess: (_, productId) => {
      queryClient.invalidateQueries({ queryKey: productKeys.detail(productId) })
      queryClient.invalidateQueries({ queryKey: [...productKeys.detail(productId), 'demping-details'] })
    },
  })
}

// Run demping for a product (manual trigger with real Kaspi API call)
export interface RunDempingResponse {
  status: string
  message: string
  product_id: string
  old_price?: number
  new_price?: number
  current_price?: number
  target_price?: number
  min_competitor_price?: number
  strategy?: string
  offers?: Array<{
    merchant_id: string
    price: number
    is_ours: boolean
  }>
}

export function useRunProductDemping() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (productId: string) =>
      api.post<RunDempingResponse>(`/kaspi/products/${productId}/run-demping`),
    onSuccess: (_, productId) => {
      queryClient.invalidateQueries({ queryKey: productKeys.detail(productId) })
      queryClient.invalidateQueries({ queryKey: [...productKeys.detail(productId), 'demping-details'] })
      queryClient.invalidateQueries({ queryKey: productKeys.all })
    },
  })
}

// =============================================
// Demping Settings
// =============================================

// Get demping settings
export function useDempingSettings(storeId: string | undefined) {
  return useQuery({
    queryKey: dempingKeys.settings(storeId || ''),
    queryFn: () => api.get<DempingSettings>(`/kaspi/stores/${storeId}/demping`),
    enabled: !!storeId,
  })
}

// Update demping settings
export function useUpdateDempingSettings() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ storeId, data }: { storeId: string; data: DempingSettingsUpdate }) =>
      api.patch<DempingSettings>(`/kaspi/stores/${storeId}/demping`, data),
    onSuccess: (_, { storeId }) => {
      queryClient.invalidateQueries({ queryKey: dempingKeys.settings(storeId) })
    },
  })
}

// Sync products for a store
export interface SyncProductsResponse {
  status: string
  message: string
  store_id: string
}

export function useSyncProducts() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (storeId: string) =>
      api.post<SyncProductsResponse>(`/kaspi/stores/${storeId}/sync`),
    onSuccess: (_, storeId) => {
      // Invalidate products after a delay to allow sync to complete
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: productKeys.list(storeId) })
      }, 3000)
    },
  })
}

// Bulk update products (toggle demping for multiple products)
export interface BulkUpdateRequest {
  product_ids: string[]
  bot_active?: boolean
  price_change_percent?: number
  price_change_tiyns?: number
}

export interface BulkUpdateResponse {
  status: string
  updated_count: number
}

export function useBulkUpdateProducts() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (data: BulkUpdateRequest) =>
      api.post<BulkUpdateResponse>('/kaspi/products/bulk-update', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: productKeys.all })
    },
  })
}
