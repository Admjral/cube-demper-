'use client'

import { useEffect, useState } from 'react'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useProductDempingDetails, useUpdateProductDemping, usePriceHistory, useCheckProductDemping, useRunProductDemping } from '@/hooks/api/use-products'
import { formatPrice } from '@/lib/utils'
import { PriceHistoryView } from './price-history-view'
import { CityPricesDialog } from './city-prices-dialog'
import { RefreshCw, Loader2, CheckCircle, AlertCircle, Play, MapPin } from 'lucide-react'
import { toast } from 'sonner'

interface ProductDempingDialogProps {
  productId: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function ProductDempingDialog({ productId, open, onOpenChange }: ProductDempingDialogProps) {
  const { data: details, isLoading } = useProductDempingDetails(productId || undefined)
  const { data: history } = usePriceHistory(productId || undefined)
  const updateDemping = useUpdateProductDemping()
  const checkDemping = useCheckProductDemping()
  const runDemping = useRunProductDemping()

  const [strategy, setStrategy] = useState<string>('standard')
  const [topPosition, setTopPosition] = useState(3)
  const [minPrice, setMinPrice] = useState<number | null>(null)
  const [maxPrice, setMaxPrice] = useState<number | null>(null)
  const [useGlobalStep, setUseGlobalStep] = useState(true)
  const [customStep, setCustomStep] = useState(100)
  const [showCityDialog, setShowCityDialog] = useState(false)

  // Load initial values from details
  useEffect(() => {
    if (details) {
      setStrategy(details.demping_strategy)
      setTopPosition(details.strategy_params?.top_position || 3)
      setMinPrice(details.min_price)
      setMaxPrice(details.max_price)
      setUseGlobalStep(!details.price_step_override)
      setCustomStep(details.price_step_override || details.store_price_step)
    }
  }, [details])

  const handleSave = async () => {
    if (!productId) return

    await updateDemping.mutateAsync({
      productId,
      data: {
        demping_strategy: strategy as 'standard' | 'always_first' | 'stay_top_n',
        strategy_params: strategy === 'stay_top_n' ? { top_position: topPosition } : null,
        min_price: minPrice,
        max_price: maxPrice,
        price_step_override: useGlobalStep ? null : customStep,
      }
    })

    toast.success('Настройки сохранены')
    onOpenChange(false)
  }

  const handleCheckDemping = async () => {
    if (!productId) return

    try {
      const result = await checkDemping.mutateAsync(productId)
      toast.success(
        <div className="space-y-1">
          <p className="font-medium">Проверка выполнена</p>
          <p className="text-sm text-muted-foreground">
            Текущая цена: {formatPrice(result.current_price)}
          </p>
          <p className="text-sm text-muted-foreground">
            Стратегия: {result.strategy === 'standard' ? 'Стандартная' :
                        result.strategy === 'always_first' ? 'Всегда первым' : 'Топ N'}
          </p>
        </div>
      )
    } catch (error) {
      toast.error('Ошибка проверки демпинга')
    }
  }

  const handleRunDemping = async () => {
    if (!productId) return

    try {
      const result = await runDemping.mutateAsync(productId)

      if (result.status === 'success') {
        toast.success(
          <div className="space-y-1">
            <p className="font-medium">Демпинг выполнен!</p>
            <p className="text-sm text-muted-foreground">
              {formatPrice(result.old_price)} → {formatPrice(result.new_price)}
            </p>
            {result.min_competitor_price && (
              <p className="text-sm text-muted-foreground">
                Мин. цена конкурента: {formatPrice(result.min_competitor_price)}
              </p>
            )}
          </div>
        )
      } else if (result.status === 'no_change') {
        toast.info(
          <div className="space-y-1">
            <p className="font-medium">Изменение не требуется</p>
            <p className="text-sm text-muted-foreground">{result.message}</p>
          </div>
        )
      } else if (result.status === 'error') {
        toast.error(result.message)
      } else {
        toast.info(result.message)
      }
    } catch (error) {
      toast.error('Ошибка запуска демпинга')
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Детальные настройки демпинга</DialogTitle>
          {details && (
            <p className="text-sm text-muted-foreground">
              {details.product_name} | SKU: {details.kaspi_sku || 'N/A'} |
              Текущая цена: {formatPrice(details.current_price)}
            </p>
          )}
        </DialogHeader>

        {isLoading ? (
          <Skeleton className="h-96" />
        ) : (
          <Tabs defaultValue="settings">
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="settings">Настройки</TabsTrigger>
              <TabsTrigger value="history">История цен</TabsTrigger>
            </TabsList>

            <TabsContent value="settings" className="space-y-6 mt-4">
              {/* Кнопки управления демпингом */}
              <Card className="border-dashed">
                <CardContent className="p-4">
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex-1">
                      <p className="font-medium">Ручное управление</p>
                      <p className="text-sm text-muted-foreground">
                        Запустить демпинг вручную или проверить настройки
                      </p>
                    </div>
                    <div className="flex gap-2">
                      <Button
                        variant="outline"
                        onClick={handleCheckDemping}
                        disabled={checkDemping.isPending || runDemping.isPending}
                      >
                        {checkDemping.isPending ? (
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                        ) : (
                          <RefreshCw className="h-4 w-4 mr-2" />
                        )}
                        Проверить
                      </Button>
                      <Button
                        onClick={handleRunDemping}
                        disabled={runDemping.isPending || checkDemping.isPending}
                      >
                        {runDemping.isPending ? (
                          <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                        ) : (
                          <Play className="h-4 w-4 mr-2" />
                        )}
                        Запустить демпинг
                      </Button>
                    </div>
                  </div>
                  {details?.bot_active ? (
                    <div className="flex items-center gap-2 mt-3 text-sm text-green-600">
                      <CheckCircle className="h-4 w-4" />
                      <span>Демпинг активен для этого товара</span>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 mt-3 text-sm text-orange-600">
                      <AlertCircle className="h-4 w-4" />
                      <span>Демпинг отключен для этого товара</span>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Демпинг по городам */}
              <Card className="border-primary/50 bg-primary/5">
                <CardContent className="p-4">
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex items-center gap-3">
                      <MapPin className="h-5 w-5 text-primary" />
                      <div>
                        <p className="font-medium">Демпинг по городам</p>
                        <p className="text-sm text-muted-foreground">
                          Настройте разные цены для разных городов Казахстана
                        </p>
                      </div>
                    </div>
                    <Button
                      variant="outline"
                      onClick={() => setShowCityDialog(true)}
                    >
                      <MapPin className="h-4 w-4 mr-2" />
                      Настроить города
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Стратегия демпинга */}
              <div className="space-y-2">
                <Label>Стратегия демпинга</Label>
                <Select value={strategy} onValueChange={setStrategy}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="standard">Стандартная</SelectItem>
                    <SelectItem value="always_first">Всегда быть первым</SelectItem>
                    <SelectItem value="stay_top_n">Держаться в топе</SelectItem>
                  </SelectContent>
                </Select>

                {strategy === 'stay_top_n' && (
                  <div className="ml-4 mt-2">
                    <Label className="text-sm">Позиция в топе</Label>
                    <Input
                      type="number"
                      min={1}
                      max={10}
                      value={topPosition}
                      onChange={(e) => setTopPosition(parseInt(e.target.value) || 3)}
                      className="mt-1"
                    />
                  </div>
                )}
              </div>

              {/* Ценовые ограничения */}
              <div className="space-y-2">
                <Label>Ценовые ограничения</Label>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label className="text-sm text-muted-foreground">Минимальная цена (₸)</Label>
                    <Input
                      type="number"
                      placeholder={`По умолчанию: ${formatPrice(details?.min_profit || 0)}`}
                      value={minPrice ?? ''}
                      onChange={(e) => setMinPrice(e.target.value ? parseInt(e.target.value) : null)}
                      className="mt-1"
                    />
                  </div>
                  <div>
                    <Label className="text-sm text-muted-foreground">Максимальная цена (₸)</Label>
                    <Input
                      type="number"
                      placeholder="Без ограничения"
                      value={maxPrice ?? ''}
                      onChange={(e) => setMaxPrice(e.target.value ? parseInt(e.target.value) : null)}
                      className="mt-1"
                    />
                  </div>
                </div>
              </div>

              {/* Шаг изменения цены */}
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <Label>Шаг изменения цены</Label>
                  <div className="flex items-center space-x-2">
                    <Switch
                      checked={useGlobalStep}
                      onCheckedChange={setUseGlobalStep}
                    />
                    <span className="text-sm text-muted-foreground">
                      Глобальный ({formatPrice(details?.store_price_step || 100)})
                    </span>
                  </div>
                </div>
                {!useGlobalStep && (
                  <Input
                    type="number"
                    value={customStep}
                    onChange={(e) => setCustomStep(parseInt(e.target.value) || 1)}
                    placeholder="Шаг в KZT"
                  />
                )}
              </div>

              {/* Глобальные настройки (info) */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Глобальные настройки магазина</CardTitle>
                </CardHeader>
                <CardContent className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <p className="text-muted-foreground">Часы работы</p>
                    <p className="font-medium">
                      {details?.store_work_hours_start} - {details?.store_work_hours_end}
                    </p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Мин. маржа</p>
                    <p className="font-medium">{details?.store_min_margin_percent}%</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Изменений за 7 дней</p>
                    <p className="font-medium">{details?.price_changes_count}</p>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="history" className="mt-4">
              <PriceHistoryView history={history || []} />
            </TabsContent>
          </Tabs>
        )}

        <div className="flex justify-end space-x-2 mt-6">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Отмена
          </Button>
          <Button onClick={handleSave} disabled={updateDemping.isPending}>
            {updateDemping.isPending ? 'Сохранение...' : 'Сохранить настройки'}
          </Button>
        </div>
      </DialogContent>

      {/* City Prices Dialog */}
      <CityPricesDialog
        productId={productId}
        productName={details?.product_name}
        basePrice={details?.current_price || 0}
        open={showCityDialog}
        onOpenChange={setShowCityDialog}
      />
    </Dialog>
  )
}
