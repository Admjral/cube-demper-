// API Error
export interface ApiError {
  message: string
  status: number
}

// Admin Stats
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
    note?: string
    expected_workers?: number
    running_workers?: number
  }
}

// Admin User
export interface AdminUser {
  id: string
  email: string
  full_name: string | null
  role: 'user' | 'admin'
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

export interface UserListResponse {
  users: AdminUser[]
  total: number
  page: number
  page_size: number
}

export interface UserDetailsResponse {
  id: string
  email: string
  full_name: string | null
  role: string
  is_blocked: boolean
  partner_id: string | null
  partner_name: string | null
  created_at: string
  updated_at: string
  subscription: {
    id: string
    plan: string
    status: string
    products_limit: number
    current_period_start: string
    current_period_end: string
    created_at: string
    updated_at: string
  } | null
  stores: Array<{
    id: string
    merchant_id: string
    name: string
    products_count: number
    is_active: boolean
    last_sync: string | null
    created_at: string
  }>
  payments: Array<{
    id: string
    amount: number
    status: string
    plan: string | null
    created_at: string
  }>
}

// Partner
export interface Partner {
  id: string
  email: string
  full_name: string | null
  created_at: string
  updated_at: string
  referred_users_count: number
}

export interface PartnerListResponse {
  partners: Partner[]
  total: number
}

export interface PartnerStats {
  partner_id: string
  partner_email: string
  referred_users_count: number
  active_subscriptions_count: number
  total_revenue_tiyns: number
}

export interface PartnerCreateRequest {
  email: string
  password: string
  full_name?: string
}

// Store
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

export interface StoreListResponse {
  stores: AdminStore[]
  total: number
  page: number
  page_size: number
}

// Extend Subscription
export interface ExtendSubscriptionRequest {
  days: number
}
