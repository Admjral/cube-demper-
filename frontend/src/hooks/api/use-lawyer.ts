'use client'

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useAuth } from '@/hooks/use-auth'

// Types
export type LawyerLanguage = 'ru' | 'kk'

export type DocumentType = 
  | 'supply_contract'
  | 'sale_contract'
  | 'service_contract'
  | 'rent_contract'
  | 'employment_contract'
  | 'claim_to_supplier'
  | 'claim_to_buyer'
  | 'complaint_to_authority'

export type TaxType = 'simplified_ip' | 'standard_ip' | 'too_kpn' | 'vat' | 'social'

export type RiskLevel = 'critical' | 'high' | 'medium' | 'low'

export interface LawyerChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  created_at: string
}

export interface LawyerChatResponse {
  message: string
  language: LawyerLanguage
  sources: Array<{
    article_number: string
    title: string
    document_title: string
    source_url?: string
    similarity: number
  }>
  created_at: string
}

export interface GeneratedDocument {
  id: string
  document_type: DocumentType
  title: string
  content: string
  pdf_url?: string
  docx_url?: string
  created_at: string
}

export interface ContractRisk {
  level: RiskLevel
  title: string
  description: string
  clause?: string
  recommendation: string
}

export interface ContractAnalysis {
  summary: string
  key_conditions: string[]
  risks: ContractRisk[]
  recommendations: string[]
  overall_risk_level: RiskLevel
  language: LawyerLanguage
}

export interface TaxCalculation {
  name: string
  rate: number
  base: number
  amount: number
  description?: string
}

export interface TaxResult {
  tax_type: TaxType
  period: string
  revenue: number
  expenses: number
  taxable_income: number
  taxes: TaxCalculation[]
  total_tax: number
  net_income: number
}

export interface PenaltyResult {
  principal_amount: number
  days: number
  rate: number
  penalty_amount: number
  total_amount: number
  calculation_details: string
}

export interface FeeResult {
  fee_type: string
  fee_amount: number
  calculation_details: string
  legal_basis: string
}

export interface FAQ {
  question: string
  category: string
}

// Query keys
export const lawyerKeys = {
  all: ['lawyer'] as const,
  history: () => [...lawyerKeys.all, 'history'] as const,
  documents: () => [...lawyerKeys.all, 'documents'] as const,
  document: (id: string) => [...lawyerKeys.all, 'document', id] as const,
  language: () => [...lawyerKeys.all, 'language'] as const,
  faq: () => [...lawyerKeys.all, 'faq'] as const,
}

// Get current language
export function useLawyerLanguage() {
  const { user } = useAuth()

  return useQuery({
    queryKey: lawyerKeys.language(),
    queryFn: () => api.get<{ language: LawyerLanguage }>('/ai/lawyer/language'),
    enabled: !!user?.id,
  })
}

// Set language
export function useSetLawyerLanguage() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (language: LawyerLanguage) =>
      api.post<{ language: LawyerLanguage }>('/ai/lawyer/set-language', { language }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: lawyerKeys.language() })
    },
  })
}

// Get chat history
export function useLawyerChatHistory() {
  const { user } = useAuth()

  return useQuery({
    queryKey: lawyerKeys.history(),
    queryFn: () => api.get<{ messages: LawyerChatMessage[] }>('/ai/lawyer/chat/history'),
    enabled: !!user?.id,
    select: (data) => data.messages,
  })
}

// Send message to AI Lawyer
export function useLawyerChat() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (params: { 
      message: string
      language?: LawyerLanguage
      use_rag?: boolean
    }) => api.post<LawyerChatResponse>('/ai/lawyer/chat', {
      message: params.message,
      language: params.language || 'ru',
      include_history: true,
      use_rag: params.use_rag ?? true,
    }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: lawyerKeys.history() })
    },
  })
}

// Clear chat history
export function useClearLawyerChat() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: () => api.delete('/ai/lawyer/chat/history'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: lawyerKeys.history() })
    },
  })
}

// Submit feedback
export function useSubmitFeedback() {
  return useMutation({
    mutationFn: (params: { message_id: string; rating: -1 | 1; comment?: string }) =>
      api.post('/ai/lawyer/chat/feedback', params),
  })
}

// Generate document
export function useGenerateDocument() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (params: {
      document_type: DocumentType
      language?: LawyerLanguage
      data: Record<string, any>
    }) => api.post<GeneratedDocument>('/ai/lawyer/generate-document', {
      document_type: params.document_type,
      language: params.language || 'ru',
      data: params.data,
    }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: lawyerKeys.documents() })
    },
  })
}

// Get document history
export function useLawyerDocuments() {
  const { user } = useAuth()

  return useQuery({
    queryKey: lawyerKeys.documents(),
    queryFn: () => api.get<GeneratedDocument[]>('/ai/lawyer/documents'),
    enabled: !!user?.id,
  })
}

// Get single document
export function useLawyerDocument(id: string) {
  const { user } = useAuth()

  return useQuery({
    queryKey: lawyerKeys.document(id),
    queryFn: () => api.get<GeneratedDocument>(`/ai/lawyer/documents/${id}`),
    enabled: !!user?.id && !!id,
  })
}

// Analyze contract
export function useAnalyzeContract() {
  return useMutation({
    mutationFn: async (params: { file: File; language?: LawyerLanguage }) => {
      const formData = new FormData()
      formData.append('file', params.file)
      
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/ai/lawyer/analyze-contract?language=${params.language || 'ru'}`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('token')}`,
        },
        body: formData,
      })
      
      if (!response.ok) {
        throw new Error('Contract analysis failed')
      }
      
      return response.json() as Promise<ContractAnalysis>
    },
  })
}

// Calculate penalty
export function useCalculatePenalty() {
  return useMutation({
    mutationFn: (params: {
      principal_amount: number
      start_date: string
      end_date: string
      rate_type?: 'refinancing' | 'custom'
      custom_rate?: number
    }) => api.post<PenaltyResult>('/ai/lawyer/calculate-penalty', params),
  })
}

// Calculate tax
export function useCalculateTax() {
  return useMutation({
    mutationFn: (params: {
      tax_type: TaxType
      revenue: number
      expenses?: number
      period: string
      employee_salary?: number
    }) => api.post<TaxResult>('/ai/lawyer/calculate-tax', params),
  })
}

// Calculate fee
export function useCalculateFee() {
  return useMutation({
    mutationFn: (params: {
      fee_type: 'ip_registration' | 'too_registration' | 'court_fee_property' | 'court_fee_non_property' | 'license_fee'
      claim_amount?: number
    }) => api.post<FeeResult>('/ai/lawyer/calculate-fee', params),
  })
}

// Get FAQ
export function useLawyerFAQ(language: LawyerLanguage = 'ru') {
  return useQuery({
    queryKey: [...lawyerKeys.faq(), language],
    queryFn: () => api.get<{ questions: FAQ[]; language: LawyerLanguage }>(`/ai/lawyer/faq?language=${language}`),
    select: (data) => data.questions,
  })
}
