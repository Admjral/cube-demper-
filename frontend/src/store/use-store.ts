import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export interface Store {
  id: string
  name: string
  merchant_id?: string
  products_count?: number
  is_active?: boolean
  last_sync?: string | null
}

interface AppState {
  // Selected store
  selectedStore: Store | null
  setSelectedStore: (store: Store | null) => void

  // Available stores (loaded from API)
  stores: Store[]
  setStores: (stores: Store[]) => void

  // Initialize stores from API response
  initStoresFromApi: (apiStores: Store[]) => void

  // Sidebar state
  sidebarOpen: boolean
  setSidebarOpen: (open: boolean) => void
  toggleSidebar: () => void

  // Language
  locale: 'ru' | 'kz'
  setLocale: (locale: 'ru' | 'kz') => void
}

export const useStore = create<AppState>()(
  persist(
    (set, get) => ({
      // Selected store
      selectedStore: null,
      setSelectedStore: (store) => set({ selectedStore: store }),

      // Available stores
      stores: [],
      setStores: (stores) => set({ stores }),

      // Initialize stores from API and select first one if nothing selected
      initStoresFromApi: (apiStores) => {
        const currentStore = get().selectedStore
        const stores = apiStores.map(s => ({
          id: s.id,
          name: s.name,
          merchant_id: s.merchant_id,
          products_count: s.products_count,
          is_active: s.is_active,
          last_sync: s.last_sync,
        }))

        set({ stores })

        // If no store selected or selected store no longer exists, select first
        if (!currentStore || !stores.find(s => s.id === currentStore.id)) {
          if (stores.length > 0) {
            set({ selectedStore: stores[0] })
          } else {
            set({ selectedStore: null })
          }
        }
      },

      // Sidebar
      sidebarOpen: true,
      setSidebarOpen: (open) => set({ sidebarOpen: open }),
      toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),

      // Language
      locale: 'ru',
      setLocale: (locale) => set({ locale }),
    }),
    {
      name: 'demper-storage',
      partialize: (state) => ({
        selectedStore: state.selectedStore,
        sidebarOpen: state.sidebarOpen,
        locale: state.locale,
      }),
    }
  )
)
