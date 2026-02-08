/**
 * Хуки для работы с накладными (Invoice Merger)
 */

import { useMutation } from '@tanstack/react-query'
import { authClient } from '@/lib/auth'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8010'

export type LayoutType = '4_on_1' | '6_on_1' | '8_on_1' | '9_on_1' | '16_on_1'

export interface LayoutInfo {
  value: LayoutType
  label: string
  description: string
  grid: string
}

export interface LayoutTypesResponse {
  layouts: LayoutInfo[]
}

/**
 * Получает список доступных типов сеток
 */
export async function getLayoutTypes(): Promise<LayoutTypesResponse> {
  const token = authClient.getToken()
  
  const response = await fetch(`${API_URL}/invoices/layout-types`, {
    method: 'GET',
    headers: {
      'Authorization': token ? `Bearer ${token}` : '',
    },
  })

  if (!response.ok) {
    throw new Error('Не удалось получить типы сеток')
  }

  return response.json()
}

/**
 * Отправляет ZIP-архив с накладными и получает объединённый PDF
 */
export async function processInvoices(
  file: File,
  layout: LayoutType
): Promise<Blob> {
  const token = authClient.getToken()
  
  const formData = new FormData()
  formData.append('file', file)

  const response = await fetch(
    `${API_URL}/invoices/process-invoices?layout=${layout}`,
    {
      method: 'POST',
      headers: {
        'Authorization': token ? `Bearer ${token}` : '',
      },
      body: formData,
    }
  )

  if (!response.ok) {
    const errorText = await response.text()
    let errorMessage = errorText
    try {
      const errorJson = JSON.parse(errorText)
      errorMessage = errorJson.detail || errorText
    } catch {
      // Keep errorText as is
    }
    throw new Error(errorMessage)
  }

  return response.blob()
}

/**
 * Хук для обработки накладных
 */
export function useProcessInvoices() {
  return useMutation({
    mutationFn: async ({
      file,
      layout,
    }: {
      file: File
      layout: LayoutType
    }) => {
      return processInvoices(file, layout)
    },
  })
}
