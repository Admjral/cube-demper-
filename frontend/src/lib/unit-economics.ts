/**
 * Unit Economics API client
 */

import { authClient } from './auth'

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'https://cube-demper-backend-production.up.railway.app'

// Types
export interface CategoryCommission {
  category: string
  subcategory?: string
  commission_no_vat: number
  commission_with_vat: number
  has_variable_rates?: boolean
}

export interface DeliveryCost {
  delivery_type: string
  name: string
  cost: number
  margin: number
  margin_percent: number
  profit: number
}

export interface TaxRegime {
  name: string
  name_en: string
  rate: number
  description: string
}

export interface CalculationRequest {
  selling_price: number
  purchase_price: number
  category: string
  subcategory?: string
  weight_kg: number
  packaging_cost: number
  other_costs: number
  tax_regime: string
  use_vat: boolean
}

export interface CalculationResult {
  selling_price: number
  purchase_price: number
  category: string
  commission_rate: number
  commission_amount: number
  tax_regime: string
  tax_rate: number
  tax_amount: number
  packaging_cost: number
  other_costs: number
  delivery_scenarios: DeliveryCost[]
  best_scenario: string
  best_profit: number
  best_margin: number
}

export interface ProductParseResult {
  product_name?: string
  price?: number
  category?: string
  subcategory?: string
  image_url?: string
  kaspi_url: string
  success: boolean
  error?: string
}

export interface QuickCalculateResult {
  commission_rate: number
  commission: number
  tax_rate: number
  tax: number
  scenarios: {
    [key: string]: {
      delivery_cost: number
      profit: number
      margin: number
    }
  }
  best: {
    type: string
    profit: number
    margin: number
  }
}

// Saved Calculations Types
export interface SavedCalculation {
  id: string
  name: string
  kaspi_url?: string
  image_url?: string
  selling_price: number
  purchase_price: number
  category: string
  subcategory?: string
  weight_kg: number
  packaging_cost: number
  other_costs: number
  tax_regime: string
  use_vat: boolean
  commission_rate?: number
  commission_amount?: number
  tax_amount?: number
  best_scenario?: string
  best_profit?: number
  best_margin?: number
  notes?: string
  is_favorite: boolean
  created_at: string
  updated_at: string
}

export interface SaveCalculationRequest {
  name: string
  kaspi_url?: string
  image_url?: string
  selling_price: number
  purchase_price: number
  category: string
  subcategory?: string
  weight_kg: number
  packaging_cost: number
  other_costs: number
  tax_regime: string
  use_vat: boolean
  notes?: string
  is_favorite?: boolean
}

export interface SavedCalculationsList {
  items: SavedCalculation[]
  total: number
  page: number
  page_size: number
}

// Helper function for API calls
async function fetchAPI<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE}${endpoint}`
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    ...options?.headers,
  }

  // Add auth token if available
  const token = authClient.getToken()
  if (token) {
    (headers as Record<string, string>)['Authorization'] = `Bearer ${token}`
  }

  const response = await fetch(url, {
    ...options,
    headers,
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Unknown error' }))
    throw new Error(error.detail || `API Error: ${response.status}`)
  }

  return response.json()
}

// API functions

/**
 * Get all categories with their commission rates
 */
export async function getCategories(): Promise<CategoryCommission[]> {
  return fetchAPI<CategoryCommission[]>('/unit-economics/categories')
}

/**
 * Get commission rate for a specific category
 */
export async function getCommission(
  category: string,
  subcategory?: string,
  useVat: boolean = false
): Promise<CategoryCommission> {
  const params = new URLSearchParams({ category })
  if (subcategory) params.append('subcategory', subcategory)
  if (useVat) params.append('use_vat', 'true')

  return fetchAPI<CategoryCommission>(`/unit-economics/commission?${params}`)
}

/**
 * Get all delivery tariff structures
 */
export async function getDeliveryTariffs(): Promise<Record<string, { name: string; name_en: string; description: string }>> {
  return fetchAPI('/unit-economics/delivery-tariffs')
}

/**
 * Get all tax regimes
 */
export async function getTaxRegimes(): Promise<Record<string, TaxRegime>> {
  return fetchAPI('/unit-economics/tax-regimes')
}

/**
 * Calculate full unit economics
 */
export async function calculateUnitEconomics(request: CalculationRequest): Promise<CalculationResult> {
  return fetchAPI<CalculationResult>('/unit-economics/calculate', {
    method: 'POST',
    body: JSON.stringify(request),
  })
}

/**
 * Parse Kaspi product URL to extract info
 */
export async function parseKaspiUrl(url: string): Promise<ProductParseResult> {
  return fetchAPI<ProductParseResult>(`/unit-economics/parse-url?url=${encodeURIComponent(url)}`)
}

/**
 * Quick calculation for simple use cases
 */
export async function quickCalculate(params: {
  selling_price: number
  purchase_price: number
  category?: string
  weight_kg?: number
  tax_regime?: string
  use_vat?: boolean
}): Promise<QuickCalculateResult> {
  const queryParams = new URLSearchParams()
  queryParams.append('selling_price', params.selling_price.toString())
  queryParams.append('purchase_price', params.purchase_price.toString())
  if (params.category) queryParams.append('category', params.category)
  if (params.weight_kg) queryParams.append('weight_kg', params.weight_kg.toString())
  if (params.tax_regime) queryParams.append('tax_regime', params.tax_regime)
  if (params.use_vat) queryParams.append('use_vat', 'true')

  return fetchAPI<QuickCalculateResult>(`/unit-economics/quick-calculate?${queryParams}`)
}

// Predefined data for offline/quick access
export const TAX_REGIMES: Record<string, TaxRegime> = {
  ip_simplified: {
    name: 'ИП упрощёнка',
    name_en: 'Individual entrepreneur (simplified)',
    rate: 3.0,
    description: '3% от дохода',
  },
  ip_general: {
    name: 'ИП общеустановленный',
    name_en: 'Individual entrepreneur (general)',
    rate: 10.0,
    description: '10% ИПН',
  },
  too_simplified: {
    name: 'ТОО упрощёнка',
    name_en: 'LLC (simplified)',
    rate: 3.0,
    description: '3% от дохода',
  },
  too_general: {
    name: 'ТОО общеустановленный',
    name_en: 'LLC (general)',
    rate: 20.0,
    description: '20% КПН',
  },
  patent: {
    name: 'Патент',
    name_en: 'Patent',
    rate: 1.0,
    description: '1% (фикс. платёж)',
  },
  none: {
    name: 'Без налога',
    name_en: 'No tax',
    rate: 0.0,
    description: 'Не учитывать налог',
  },
}

export const DELIVERY_TYPES: Record<string, { name: string; name_en: string }> = {
  kaspi_city: { name: 'Kaspi Доставка (город)', name_en: 'Kaspi Delivery (city)' },
  kaspi_kz: { name: 'Kaspi Доставка (по KZ)', name_en: 'Kaspi Delivery (nationwide)' },
  kaspi_express: { name: 'Kaspi Express', name_en: 'Kaspi Express' },
  self_pickup: { name: 'Самовывоз', name_en: 'Self Pickup' },
}

export const CATEGORIES = [
  'Автотовары',
  'Аксессуары',
  'Аптека',
  'Бытовая техника',
  'Детские товары',
  'Досуг, книги',
  'Канцелярские товары',
  'Компьютеры',
  'Красота',
  'Мебель',
  'Обувь',
  'Одежда',
  'Подарки, сувениры',
  'Продукты питания',
  'Спорт, туризм',
  'Строительство, ремонт',
  'ТВ, Аудио, Видео',
  'Телефоны и гаджеты',
  'Товары для дома',
  'Украшения',
]

// =============================================================================
// SAVED CALCULATIONS API
// =============================================================================

/**
 * Create a new saved calculation
 */
export async function createSavedCalculation(data: SaveCalculationRequest): Promise<SavedCalculation> {
  return fetchAPI<SavedCalculation>('/unit-economics/saved', {
    method: 'POST',
    body: JSON.stringify(data),
  })
}

/**
 * Get list of saved calculations
 */
export async function getSavedCalculations(params?: {
  page?: number
  page_size?: number
  favorites_only?: boolean
  search?: string
}): Promise<SavedCalculationsList> {
  const queryParams = new URLSearchParams()
  if (params?.page) queryParams.append('page', params.page.toString())
  if (params?.page_size) queryParams.append('page_size', params.page_size.toString())
  if (params?.favorites_only) queryParams.append('favorites_only', 'true')
  if (params?.search) queryParams.append('search', params.search)

  const query = queryParams.toString()
  return fetchAPI<SavedCalculationsList>(`/unit-economics/saved${query ? `?${query}` : ''}`)
}

/**
 * Get a specific saved calculation
 */
export async function getSavedCalculation(id: string): Promise<SavedCalculation> {
  return fetchAPI<SavedCalculation>(`/unit-economics/saved/${id}`)
}

/**
 * Update a saved calculation
 */
export async function updateSavedCalculation(id: string, data: SaveCalculationRequest): Promise<SavedCalculation> {
  return fetchAPI<SavedCalculation>(`/unit-economics/saved/${id}`, {
    method: 'PUT',
    body: JSON.stringify(data),
  })
}

/**
 * Toggle favorite status
 */
export async function toggleFavorite(id: string): Promise<{ id: string; is_favorite: boolean }> {
  return fetchAPI<{ id: string; is_favorite: boolean }>(`/unit-economics/saved/${id}/favorite`, {
    method: 'PATCH',
  })
}

/**
 * Delete a saved calculation
 */
export async function deleteSavedCalculation(id: string): Promise<void> {
  await fetchAPI(`/unit-economics/saved/${id}`, {
    method: 'DELETE',
  })
}

/**
 * Export to CSV - returns download URL
 */
export function getExportCsvUrl(favoritesOnly: boolean = false): string {
  const params = new URLSearchParams()
  if (favoritesOnly) params.append('favorites_only', 'true')
  return `${API_BASE}/unit-economics/saved/export/csv?${params}`
}

/**
 * Export to Excel - returns download URL
 */
export function getExportExcelUrl(favoritesOnly: boolean = false): string {
  const params = new URLSearchParams()
  if (favoritesOnly) params.append('favorites_only', 'true')
  return `${API_BASE}/unit-economics/saved/export/excel?${params}`
}

/**
 * Download export file with auth
 */
export async function downloadExport(format: 'csv' | 'excel', favoritesOnly: boolean = false): Promise<void> {
  const token = authClient.getToken()
  const endpoint = format === 'csv' ? 'csv' : 'excel'
  const params = new URLSearchParams()
  if (favoritesOnly) params.append('favorites_only', 'true')

  const response = await fetch(`${API_BASE}/unit-economics/saved/export/${endpoint}?${params}`, {
    headers: {
      'Authorization': token ? `Bearer ${token}` : '',
    },
  })

  if (!response.ok) {
    throw new Error('Export failed')
  }

  // Get filename from header or generate
  const contentDisposition = response.headers.get('Content-Disposition')
  let filename = `unit_economics.${format === 'csv' ? 'csv' : 'xlsx'}`
  if (contentDisposition) {
    const match = contentDisposition.match(/filename=([^;]+)/)
    if (match) filename = match[1]
  }

  // Download file
  const blob = await response.blob()
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}
