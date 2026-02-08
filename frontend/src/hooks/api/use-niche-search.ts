"use client"

import { useQuery } from "@tanstack/react-query"
import { api } from "@/lib/api"
import type {
  NicheSearchParams,
  NicheSearchResponse,
  NicheProduct,
  NicheStats,
  NicheCategory
} from "@/types/api"

// Query keys
export const nicheSearchKeys = {
  all: ['niche-search'] as const,
  search: (params: NicheSearchParams) => [...nicheSearchKeys.all, 'search', params] as const,
  categories: () => [...nicheSearchKeys.all, 'categories'] as const,
  stats: () => [...nicheSearchKeys.all, 'stats'] as const,
  product: (id: string) => [...nicheSearchKeys.all, 'product', id] as const,
}

// Hook for searching niches/products
export function useNicheSearch(params: NicheSearchParams, enabled = true) {
  return useQuery({
    queryKey: nicheSearchKeys.search(params),
    queryFn: async (): Promise<NicheSearchResponse> => {
      const searchParams = new URLSearchParams()

      if (params.category_id) searchParams.set('category_id', params.category_id)
      if (params.min_price) searchParams.set('min_price', params.min_price.toString())
      if (params.max_price) searchParams.set('max_price', params.max_price.toString())
      if (params.min_sales) searchParams.set('min_sales', params.min_sales.toString())
      if (params.max_revenue) searchParams.set('max_revenue', params.max_revenue.toString())
      if (params.min_revenue) searchParams.set('min_revenue', params.min_revenue.toString())
      // competition filter: max_sellers
      if (params.competition === 'low') searchParams.set('max_sellers', '5')
      else if (params.competition === 'medium') searchParams.set('max_sellers', '15')
      // sort_by mapping
      if (params.sort_by) {
        const sortMapping: Record<string, string> = {
          'sales': 'estimated_sales',
          'revenue': 'estimated_revenue',
          'reviews': 'reviews_count',
        }
        searchParams.set('sort_by', sortMapping[params.sort_by] || params.sort_by)
      }
      if (params.sort_order) searchParams.set('order', params.sort_order)
      // pagination: offset = (page - 1) * limit
      const limit = params.limit || 50
      const offset = ((params.page || 1) - 1) * limit
      searchParams.set('limit', limit.toString())
      searchParams.set('offset', offset.toString())

      const queryString = searchParams.toString()
      const url = `/niches/products${queryString ? `?${queryString}` : ''}`

      const response = await api.get<{products: any[], total: number, limit: number, offset: number}>(url)

      // Transform to match frontend types
      return {
        products: response.products.map(p => ({
          id: p.id,
          kaspi_product_id: p.kaspi_product_id,
          name: p.name,
          category_id: p.category_id,
          category_name: p.category_name || '',
          price: p.price,
          review_count: p.reviews_count,
          rating: p.rating,
          merchant_count: p.sellers_count,
          estimated_sales: p.estimated_sales,
          estimated_revenue: p.estimated_revenue,
          image_url: p.image_url,
          kaspi_url: p.kaspi_url,
        })),
        total: response.total,
        page: params.page || 1,
        limit: response.limit,
        has_more: (response.offset + response.products.length) < response.total,
        categories: [],
      }
    },
    enabled,
    staleTime: 60000, // 1 minute
  })
}

// Hook for getting niche categories
export function useNicheCategories() {
  return useQuery({
    queryKey: nicheSearchKeys.categories(),
    queryFn: async (): Promise<NicheCategory[]> => {
      const response = await api.get<{categories: any[], total: number}>('/niches/categories')
      return response.categories.map(c => ({
        id: c.id,
        name: c.name,
        products_count: c.total_products,
        avg_sales: 0,
        avg_revenue: c.total_revenue,
      }))
    },
    staleTime: 300000, // 5 minutes
  })
}

// Hook for getting niche stats
export function useNicheStats() {
  return useQuery({
    queryKey: nicheSearchKeys.stats(),
    queryFn: async (): Promise<NicheStats> => {
      return api.get<NicheStats>('/niches/stats')
    },
    staleTime: 300000, // 5 minutes
  })
}

// Hook for getting single product details
export function useNicheProduct(productId: string, enabled = true) {
  return useQuery({
    queryKey: nicheSearchKeys.product(productId),
    queryFn: async (): Promise<NicheProduct> => {
      return api.get<NicheProduct>(`/niches/products/${productId}`)
    },
    enabled: !!productId && enabled,
    staleTime: 60000,
  })
}
