// Kaspi Store types
export interface KaspiStore {
  id: string
  created_at: string
  updated_at: string
  user_id: string
  merchant_id: string
  name: string
  products_count: number
  last_sync: string | null
  is_active: boolean
}

export interface KaspiAuthRequest {
  email: string
  password: string
  merchant_id?: string
}

export interface KaspiAuthResponse {
  status: 'success' | 'sms_required'
  store_id?: string
  merchant_id?: string
  message?: string
}

export interface CreateStoreRequest {
  merchant_id: string
  name: string
}

export interface SyncStoreResponse {
  success: boolean
  products_count: number
  message: string
}

// User profile types
export interface UserProfile {
  id: string
  email: string
  full_name: string | null
  avatar_url: string | null
  role: 'user' | 'admin'
  created_at: string
  updated_at: string
}

// Subscription types
export interface Subscription {
  id: string
  user_id: string
  plan: string
  status: 'active' | 'canceled' | 'past_due' | 'trialing'
  current_period_start: string
  current_period_end: string
  created_at: string
}

export interface PricingPlan {
  id: string
  name: string
  price_monthly: number
  features: string[]
  limits: {
    products: number
    stores: number
  }
}

// AI Chat types
export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  created_at: string
}

export interface ChatRequest {
  user_id: string
  assistant_type: 'lawyer' | 'salesman'
  message: string
}

export interface ChatResponse {
  message: string
  chat_id: string
}

// Admin types
export interface AdminStats {
  total_users: number
  active_subscriptions: number
  blocked_users: number
  online_users: number
  total_products: number
  active_demping_products: number
  total_revenue_tiyns: number
  monthly_revenue: number
  new_connections: number
  demper_workers_status: {
    note: string
    expected_workers: number
    running_workers: number
  }
}

export interface AdminUser {
  id: string
  email: string
  full_name: string | null
  role: 'user' | 'admin' | 'support'
  is_blocked: boolean
  partner_id: string | null
  partner_name: string | null
  created_at: string
  updated_at: string
  subscription_plan: string | null
  subscription_status: string | null
  subscription_end_date: string | null
  stores_count: number
  products_count: number
}

export interface AdminUserListResponse {
  users: AdminUser[]
  total: number
  page: number
  page_size: number
}

export interface AdminStore {
  id: string
  user_id: string
  user_email: string
  user_name: string | null
  merchant_id: string
  name: string
  products_count: number
  is_active: boolean
  last_sync: string | null
  created_at: string
}

export interface AdminStoreListResponse {
  stores: AdminStore[]
  total: number
  page: number
  page_size: number
}

export interface AdminPayment {
  id: string
  user_id: string
  user_email: string
  amount: number
  status: 'pending' | 'completed' | 'failed' | 'refunded'
  plan: string
  created_at: string
}

export interface AdminPaymentListResponse {
  payments: AdminPayment[]
  total: number
  page: number
  page_size: number
}

// API Error
export interface ApiError {
  message: string
  status: number
}

// =============================================
// Products
// =============================================

export interface KaspiProduct {
  id: string
  store_id: string
  kaspi_product_id: string
  kaspi_sku: string | null
  external_kaspi_id: string | null
  name: string
  price: number // in tiyns
  min_profit: number
  bot_active: boolean
  last_check_time: string | null
  availabilities: Record<string, any> | null
  created_at: string
  updated_at: string
}

export interface ProductListResponse {
  products: KaspiProduct[]
  total: number
  page: number
  page_size: number
  has_more: boolean
}

export interface ProductUpdateRequest {
  price?: number
  min_profit?: number
  bot_active?: boolean
}

export interface PriceUpdateRequest {
  new_price: number
  reason?: 'manual' | 'demping' | 'sync'
}

export interface PriceHistory {
  id: string
  product_id: string
  old_price: number
  new_price: number
  competitor_price: number | null
  change_reason: string
  created_at: string
}

// =============================================
// Demping Settings
// =============================================

export interface DempingSettings {
  id: string
  store_id: string
  min_profit: number
  bot_active: boolean
  price_step: number
  min_margin_percent: number
  check_interval_minutes: number
  work_hours_start: string
  work_hours_end: string
  is_enabled: boolean
  excluded_merchant_ids: string[]
  last_check: string | null
  created_at: string
  updated_at: string
}

export interface DempingSettingsUpdate {
  min_profit?: number
  bot_active?: boolean
  price_step?: number
  min_margin_percent?: number
  check_interval_minutes?: number
  work_hours_start?: string
  work_hours_end?: string
  is_enabled?: boolean
  excluded_merchant_ids?: string[]
}

// =============================================
// Product Demping Details
// =============================================

export interface ProductDempingDetails {
  product_id: string
  product_name: string
  kaspi_sku: string | null
  current_price: number
  min_profit: number
  bot_active: boolean

  // Product-level settings (null = use global)
  max_price: number | null
  min_price: number | null
  price_step_override: number | null
  demping_strategy: 'standard' | 'always_first' | 'stay_top_n'
  strategy_params: { top_position?: number } | null

  // Global store settings (for display)
  store_price_step: number
  store_min_margin_percent: number
  store_work_hours_start: string
  store_work_hours_end: string

  // Statistics
  last_check_time: string | null
  price_changes_count: number
}

export interface ProductDempingUpdate {
  max_price?: number | null
  min_price?: number | null
  price_step_override?: number | null
  demping_strategy?: 'standard' | 'always_first' | 'stay_top_n'
  strategy_params?: { top_position?: number } | null
}

// =============================================
// Store Stats & Analytics
// =============================================

export interface StoreStats {
  store_id: string
  store_name: string
  products_count: number
  active_products_count: number
  demping_enabled_count: number
  today_orders: number
  today_revenue: number
  week_orders: number
  week_revenue: number
  month_orders: number
  month_revenue: number
  avg_order_value: number
  last_sync: string | null
}

export interface SalesAnalytics {
  store_id: string
  period: '7d' | '30d' | '90d'
  total_orders: number
  total_revenue: number
  total_items_sold: number
  avg_order_value: number
  daily_stats: {
    date: string
    orders: number
    revenue: number
    items: number
  }[]
}

export interface TopProduct {
  id: string
  kaspi_sku: string
  name: string
  current_price: number
  sales_count: number
  revenue: number
}

// =============================================
// City-based Pricing
// =============================================

export const KASPI_CITIES: Record<string, string> = {
  "750000000": "Алматы",
  "770000000": "Астана",
  "730000000": "Шымкент",
  "710000000": "Караганда",
  "790000000": "Актобе",
  "630000000": "Атырау",
  "610000000": "Актау",
  "510000000": "Костанай",
  "550000000": "Павлодар",
  "590000000": "Семей",
  "620000000": "Уральск",
  "470000000": "Тараз",
  "310000000": "Усть-Каменогорск",
  "350000000": "Кызылорда",
  "430000000": "Талдыкорган",
  "530000000": "Петропавловск",
  "570000000": "Экибастуз",
  "390000000": "Туркестан",
}

export interface CityInfo {
  city_id: string
  city_name: string
}

export interface ProductCityPrice {
  id: string
  product_id: string
  city_id: string
  city_name: string
  price: number | null
  min_price: number | null
  max_price: number | null
  bot_active: boolean
  last_check_time: string | null
  competitor_price: number | null
  our_position: number | null
  created_at: string
  updated_at: string
}

export interface ProductCityPriceCreate {
  city_id: string
  price?: number | null
  min_price?: number | null
  max_price?: number | null
  bot_active?: boolean
}

export interface ProductCityPriceUpdate {
  price?: number | null
  min_price?: number | null
  max_price?: number | null
  bot_active?: boolean
}

export interface ProductCityPricesRequest {
  apply_to_all_cities: boolean
  cities: ProductCityPriceCreate[]
}

export interface CityDempingResult {
  city_id: string
  city_name: string
  status: 'success' | 'no_change' | 'waiting' | 'error' | 'no_competitors' | 'no_data' | 'no_offers' | 'sync_failed'
  message: string
  old_price?: number
  new_price?: number
  competitor_price?: number
  our_position?: number
}

export interface MultiCityDempingResult {
  product_id: string
  product_name: string
  results: CityDempingResult[]
  total_cities: number
  successful_updates: number
}

// =============================================
// Niche Search (Поиск ниш)
// =============================================

export interface NicheSearchParams {
  category_id?: string
  min_price?: number
  max_price?: number
  min_sales?: number
  max_sales?: number
  min_revenue?: number
  max_revenue?: number
  min_reviews?: number
  max_reviews?: number
  competition?: 'low' | 'medium' | 'high'
  sort_by?: 'sales' | 'revenue' | 'reviews' | 'margin'
  sort_order?: 'asc' | 'desc'
  page?: number
  limit?: number
}

export interface NicheProduct {
  id: string
  kaspi_product_id: string
  name: string
  category_id: string
  category_name: string
  price: number
  review_count: number
  rating: number
  merchant_count: number
  estimated_sales: number
  estimated_revenue: number
  image_url?: string
  kaspi_url?: string
}

export interface NicheSearchResponse {
  products: NicheProduct[]
  total: number
  page: number
  limit: number
  has_more: boolean
  categories: NicheCategory[]
}

export interface NicheCategory {
  id: string
  name: string
  products_count: number
  avg_sales: number
  avg_revenue: number
}

export interface NicheStats {
  total_products: number
  total_categories: number
  avg_sales: number
  avg_revenue: number
  last_updated: string
}
