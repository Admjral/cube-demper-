'use client'

import { useEffect, useState } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { formatPrice } from '@/lib/utils'
import { 
  Play, 
  Loader2, 
  MapPin, 
  CheckCircle, 
  AlertCircle, 
  Clock,
  Plus,
  Trash2,
  RefreshCw
} from 'lucide-react'
import { toast } from 'sonner'
import { api } from '@/lib/api'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import type { 
  ProductCityPrice, 
  ProductCityPricesRequest, 
  MultiCityDempingResult,
  CityInfo
} from '@/types/api'
import { KASPI_CITIES } from '@/types/api'

interface CityPricesDialogProps {
  productId: string | null
  productName?: string
  basePrice?: number
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function CityPricesDialog({ 
  productId, 
  productName, 
  basePrice = 0,
  open, 
  onOpenChange 
}: CityPricesDialogProps) {
  const queryClient = useQueryClient()
  
  const [applyToAll, setApplyToAll] = useState(true)
  const [selectedCities, setSelectedCities] = useState<Set<string>>(new Set())
  const [citySettings, setCitySettings] = useState<Record<string, {
    price: number | null
    min_price: number | null
    max_price: number | null
    bot_active: boolean
  }>>({})
  
  // Default settings for "apply to all" mode
  const [defaultMinPrice, setDefaultMinPrice] = useState<number | null>(null)
  const [defaultMaxPrice, setDefaultMaxPrice] = useState<number | null>(null)

  // Fetch existing city prices
  const { data: cityPrices, isLoading } = useQuery({
    queryKey: ['productCityPrices', productId],
    queryFn: async () => {
      if (!productId) return []
      return await api.get<ProductCityPrice[]>(`/kaspi/products/${productId}/city-prices`)
    },
    enabled: !!productId && open
  })

  // Set city prices mutation
  const setCityPricesMutation = useMutation({
    mutationFn: async (request: ProductCityPricesRequest) => {
      return await api.post<ProductCityPrice[]>(`/kaspi/products/${productId}/city-prices`, request)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['productCityPrices', productId] })
      toast.success('Настройки городов сохранены')
    },
    onError: () => {
      toast.error('Ошибка сохранения настроек')
    }
  })

  // Delete city price mutation
  const deleteCityPriceMutation = useMutation({
    mutationFn: async (cityId: string) => {
      await api.delete(`/kaspi/products/${productId}/city-prices/${cityId}`)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['productCityPrices', productId] })
      toast.success('Город удален')
    }
  })

  // Run city demping mutation
  const runCityDempingMutation = useMutation({
    mutationFn: async (cityIds?: string[]) => {
      return await api.post<MultiCityDempingResult>(`/kaspi/products/${productId}/run-city-demping`, 
        cityIds ? { city_ids: cityIds } : {}
      )
    },
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['productCityPrices', productId] })
      
      const successCount = result.successful_updates
      const totalCount = result.total_cities
      
      if (successCount > 0) {
        toast.success(
          <div className="space-y-1">
            <p className="font-medium">Демпинг выполнен!</p>
            <p className="text-sm text-muted-foreground">
              Обновлено {successCount} из {totalCount} городов
            </p>
          </div>
        )
      } else {
        toast.info(
          <div className="space-y-1">
            <p className="font-medium">Демпинг завершен</p>
            <p className="text-sm text-muted-foreground">
              Изменения не требуются
            </p>
          </div>
        )
      }
    },
    onError: () => {
      toast.error('Ошибка запуска демпинга')
    }
  })

  // Initialize from existing data
  useEffect(() => {
    if (cityPrices && cityPrices.length > 0) {
      const settings: typeof citySettings = {}
      const cities = new Set<string>()
      
      cityPrices.forEach(cp => {
        cities.add(cp.city_id)
        settings[cp.city_id] = {
          price: cp.price,
          min_price: cp.min_price,
          max_price: cp.max_price,
          bot_active: cp.bot_active
        }
      })
      
      setSelectedCities(cities)
      setCitySettings(settings)
      setApplyToAll(cityPrices.length === Object.keys(KASPI_CITIES).length)
    }
  }, [cityPrices])

  const handleSave = async () => {
    if (!productId) return

    const request: ProductCityPricesRequest = {
      apply_to_all_cities: applyToAll,
      cities: []
    }

    if (applyToAll) {
      // Single settings for all cities
      request.cities = [{
        city_id: Object.keys(KASPI_CITIES)[0], // Template
        price: basePrice,
        min_price: defaultMinPrice,
        max_price: defaultMaxPrice,
        bot_active: true
      }]
    } else {
      // Specific cities
      request.cities = Array.from(selectedCities).map(cityId => ({
        city_id: cityId,
        price: citySettings[cityId]?.price ?? basePrice,
        min_price: citySettings[cityId]?.min_price ?? null,
        max_price: citySettings[cityId]?.max_price ?? null,
        bot_active: citySettings[cityId]?.bot_active ?? true
      }))
    }

    await setCityPricesMutation.mutateAsync(request)
  }

  const toggleCity = (cityId: string) => {
    const newSelected = new Set(selectedCities)
    if (newSelected.has(cityId)) {
      newSelected.delete(cityId)
    } else {
      newSelected.add(cityId)
      // Initialize settings if not exists
      if (!citySettings[cityId]) {
        setCitySettings(prev => ({
          ...prev,
          [cityId]: {
            price: basePrice,
            min_price: null,
            max_price: null,
            bot_active: true
          }
        }))
      }
    }
    setSelectedCities(newSelected)
  }

  const updateCitySetting = (
    cityId: string, 
    field: 'min_price' | 'max_price' | 'bot_active', 
    value: number | null | boolean
  ) => {
    setCitySettings(prev => ({
      ...prev,
      [cityId]: {
        ...prev[cityId],
        [field]: value
      }
    }))
  }

  const getStatusBadge = (cityPrice: ProductCityPrice) => {
    if (!cityPrice.last_check_time) {
      return <Badge variant="secondary">Не проверено</Badge>
    }
    if (cityPrice.our_position === 1) {
      return <Badge className="bg-green-500">ТОП 1</Badge>
    }
    if (cityPrice.our_position && cityPrice.our_position <= 3) {
      return <Badge className="bg-yellow-500">ТОП {cityPrice.our_position}</Badge>
    }
    if (cityPrice.our_position) {
      return <Badge variant="outline">Позиция {cityPrice.our_position}</Badge>
    }
    return <Badge variant="secondary">Неизвестно</Badge>
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <MapPin className="h-5 w-5" />
            Настройка демпинга по городам
          </DialogTitle>
          <DialogDescription>
            {productName && `${productName} | `}
            Базовая цена: {formatPrice(basePrice)}
          </DialogDescription>
        </DialogHeader>

        {isLoading ? (
          <Skeleton className="h-96" />
        ) : (
          <Tabs defaultValue="settings">
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="settings">Настройки</TabsTrigger>
              <TabsTrigger value="status">Статус по городам</TabsTrigger>
            </TabsList>

            <TabsContent value="settings" className="space-y-6 mt-4">
              {/* Mode selector */}
              <Card>
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="font-medium">Режим применения</p>
                      <p className="text-sm text-muted-foreground">
                        {applyToAll 
                          ? 'Единые настройки для всех городов' 
                          : 'Индивидуальные настройки по городам'}
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className={`text-sm ${!applyToAll ? 'font-medium' : 'text-muted-foreground'}`}>
                        По городам
                      </span>
                      <Switch
                        checked={applyToAll}
                        onCheckedChange={setApplyToAll}
                      />
                      <span className={`text-sm ${applyToAll ? 'font-medium' : 'text-muted-foreground'}`}>
                        Все города
                      </span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {applyToAll ? (
                /* All cities mode */
                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">Настройки для всех городов</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                      <div>
                        <Label>Минимальная цена (₸)</Label>
                        <Input
                          type="number"
                          placeholder="Без ограничения"
                          value={defaultMinPrice ?? ''}
                          onChange={(e) => setDefaultMinPrice(e.target.value ? parseInt(e.target.value) : null)}
                          className="mt-1"
                        />
                      </div>
                      <div>
                        <Label>Максимальная цена (₸)</Label>
                        <Input
                          type="number"
                          placeholder="Без ограничения"
                          value={defaultMaxPrice ?? ''}
                          onChange={(e) => setDefaultMaxPrice(e.target.value ? parseInt(e.target.value) : null)}
                          className="mt-1"
                        />
                      </div>
                    </div>
                    <p className="text-sm text-muted-foreground">
                      Демпинг будет работать одинаково для всех {Object.keys(KASPI_CITIES).length} городов
                    </p>
                  </CardContent>
                </Card>
              ) : (
                /* Per-city mode */
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <Label>Выберите города</Label>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        setSelectedCities(new Set(Object.keys(KASPI_CITIES)))
                      }}
                    >
                      Выбрать все
                    </Button>
                  </div>

                  <div className="grid grid-cols-2 md:grid-cols-3 gap-2 max-h-48 overflow-y-auto p-2 border rounded-lg">
                    {Object.entries(KASPI_CITIES).map(([cityId, cityName]) => (
                      <div key={cityId} className="flex items-center space-x-2">
                        <Checkbox
                          id={cityId}
                          checked={selectedCities.has(cityId)}
                          onCheckedChange={() => toggleCity(cityId)}
                        />
                        <label
                          htmlFor={cityId}
                          className="text-sm cursor-pointer"
                        >
                          {cityName}
                        </label>
                      </div>
                    ))}
                  </div>

                  {selectedCities.size > 0 && (
                    <div className="space-y-3">
                      <Label>Настройки выбранных городов</Label>
                      {Array.from(selectedCities).map(cityId => (
                        <Card key={cityId} className="p-3">
                          <div className="flex items-center justify-between mb-2">
                            <span className="font-medium">{KASPI_CITIES[cityId]}</span>
                            <div className="flex items-center gap-2">
                              <Switch
                                checked={citySettings[cityId]?.bot_active ?? true}
                                onCheckedChange={(checked) => updateCitySetting(cityId, 'bot_active', checked)}
                              />
                              <span className="text-sm text-muted-foreground">
                                {citySettings[cityId]?.bot_active ? 'Активен' : 'Выключен'}
                              </span>
                            </div>
                          </div>
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                            <Input
                              type="number"
                              placeholder="Мин. цена"
                              value={citySettings[cityId]?.min_price ?? ''}
                              onChange={(e) => updateCitySetting(
                                cityId, 
                                'min_price', 
                                e.target.value ? parseInt(e.target.value) : null
                              )}
                            />
                            <Input
                              type="number"
                              placeholder="Макс. цена"
                              value={citySettings[cityId]?.max_price ?? ''}
                              onChange={(e) => updateCitySetting(
                                cityId, 
                                'max_price', 
                                e.target.value ? parseInt(e.target.value) : null
                              )}
                            />
                          </div>
                        </Card>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </TabsContent>

            <TabsContent value="status" className="mt-4">
              {cityPrices && cityPrices.length > 0 ? (
                <div className="space-y-4">
                  {/* Run demping button */}
                  <Card className="border-dashed">
                    <CardContent className="p-4">
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="font-medium">Запустить демпинг по всем городам</p>
                          <p className="text-sm text-muted-foreground">
                            Проверит цены конкурентов и обновит при необходимости
                          </p>
                        </div>
                        <Button
                          onClick={() => runCityDempingMutation.mutate(undefined)}
                          disabled={runCityDempingMutation.isPending}
                        >
                          {runCityDempingMutation.isPending ? (
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                          ) : (
                            <Play className="h-4 w-4 mr-2" />
                          )}
                          Запустить
                        </Button>
                      </div>
                    </CardContent>
                  </Card>

                  {/* City status list */}
                  <div className="space-y-2">
                    {cityPrices.map(cp => (
                      <Card key={cp.id} className="p-3">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-3">
                            <MapPin className="h-4 w-4 text-muted-foreground" />
                            <div>
                              <p className="font-medium">{cp.city_name}</p>
                              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                <span>Цена: {formatPrice(cp.price || 0)}</span>
                                {cp.competitor_price && (
                                  <span>| Конкурент: {formatPrice(cp.competitor_price)}</span>
                                )}
                              </div>
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            {getStatusBadge(cp)}
                            {!cp.bot_active && (
                              <Badge variant="outline" className="text-orange-500">
                                Выключен
                              </Badge>
                            )}
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => runCityDempingMutation.mutate([cp.city_id])}
                              disabled={runCityDempingMutation.isPending}
                            >
                              <RefreshCw className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => deleteCityPriceMutation.mutate(cp.city_id)}
                              disabled={deleteCityPriceMutation.isPending}
                            >
                              <Trash2 className="h-4 w-4 text-destructive" />
                            </Button>
                          </div>
                        </div>
                        {cp.last_check_time && (
                          <div className="flex items-center gap-1 mt-2 text-xs text-muted-foreground">
                            <Clock className="h-3 w-3" />
                            Последняя проверка: {new Date(cp.last_check_time).toLocaleString('ru')}
                          </div>
                        )}
                      </Card>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="text-center py-12 text-muted-foreground">
                  <MapPin className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>Города не настроены</p>
                  <p className="text-sm">Перейдите во вкладку "Настройки" чтобы добавить города</p>
                </div>
              )}
            </TabsContent>
          </Tabs>
        )}

        <div className="flex justify-end space-x-2 mt-6">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Закрыть
          </Button>
          <Button 
            onClick={handleSave} 
            disabled={setCityPricesMutation.isPending || (!applyToAll && selectedCities.size === 0)}
          >
            {setCityPricesMutation.isPending ? (
              <>
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                Сохранение...
              </>
            ) : (
              'Сохранить настройки'
            )}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
