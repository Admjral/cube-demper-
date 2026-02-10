"use client"

import { useState, useEffect } from "react"
import { useT } from "@/lib/i18n"
import { SubscriptionGate } from "@/components/shared/subscription-gate"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Separator } from "@/components/ui/separator"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Calculator,
  DollarSign,
  Percent,
  Truck,
  Package,
  RefreshCw,
  Link2,
  Search,
  Loader2,
  CheckCircle,
  XCircle,
  Scale,
  Building2,
  Info,
  ChevronDown,
  ChevronUp,
  Save,
  Star,
  Trash2,
  Download,
  FileSpreadsheet,
  FileText,
  BookMarked,
} from "lucide-react"
import {
  calculateUnitEconomics,
  parseKaspiUrl,
  TAX_REGIMES,
  DELIVERY_TYPES,
  CATEGORIES,
  type CalculationResult,
  type ProductParseResult,
  type SavedCalculation,
  getSavedCalculations,
  createSavedCalculation,
  deleteSavedCalculation,
  toggleFavorite,
  downloadExport,
} from "@/lib/unit-economics"

export default function UnitEconomicsPage() {
  const t = useT()
  const [activeTab, setActiveTab] = useState("calculator")
  const [parsingUrl, setParsingUrl] = useState(false)
  const [kaspiUrl, setKaspiUrl] = useState("")
  const [parsedProduct, setParsedProduct] = useState<ProductParseResult | null>(null)
  const [result, setResult] = useState<CalculationResult | null>(null)
  const [showAdvanced, setShowAdvanced] = useState(false)

  // Saved calculations state
  const [savedCalculations, setSavedCalculations] = useState<SavedCalculation[]>([])
  const [loadingSaved, setLoadingSaved] = useState(false)
  const [savingCalculation, setSavingCalculation] = useState(false)
  const [showSaveDialog, setShowSaveDialog] = useState(false)
  const [saveProductName, setSaveProductName] = useState("")
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [exporting, setExporting] = useState(false)

  const [values, setValues] = useState({
    sellingPrice: 0,
    purchasePrice: 0,
    category: "Автотовары",
    subcategory: "",
    weightKg: 1.0,
    packagingCost: 0,
    otherCosts: 0,
    taxRegime: "ip_simplified",
    useVat: false,
  })

  // Auto-calculate when values change
  useEffect(() => {
    const timer = setTimeout(() => {
      handleCalculate()
    }, 300)
    return () => clearTimeout(timer)
  }, [values])

  // Load saved calculations when switching to library tab
  useEffect(() => {
    if (activeTab === "library") {
      loadSavedCalculations()
    }
  }, [activeTab])

  const loadSavedCalculations = async () => {
    setLoadingSaved(true)
    try {
      const response = await getSavedCalculations({ page_size: 50 })
      setSavedCalculations(response.items)
    } catch (e) {
      console.error("Failed to load saved calculations:", e)
    } finally {
      setLoadingSaved(false)
    }
  }

  const handleCalculate = async () => {
    try {
      const response = await calculateUnitEconomics({
        selling_price: values.sellingPrice,
        purchase_price: values.purchasePrice,
        category: values.category,
        subcategory: values.subcategory || undefined,
        weight_kg: values.weightKg,
        packaging_cost: values.packagingCost,
        other_costs: values.otherCosts,
        tax_regime: values.taxRegime,
        use_vat: values.useVat,
      })
      setResult(response)
    } catch (e) {
      console.error("Calculation error:", e)
    }
  }

  const handleParseUrl = async () => {
    if (!kaspiUrl.trim()) return

    setParsingUrl(true)
    setParsedProduct(null)
    try {
      const response = await parseKaspiUrl(kaspiUrl)
      setParsedProduct(response)

      if (response.success) {
        // Auto-fill form with parsed data
        setValues((prev) => ({
          ...prev,
          sellingPrice: response.price || prev.sellingPrice,
          category: response.category || prev.category,
          subcategory: response.subcategory || "",
          weightKg: response.weight_kg || prev.weightKg,
        }))
        // Set product name for saving
        if (response.product_name) {
          setSaveProductName(response.product_name)
        }
      }
    } catch (e) {
      console.error("Parse error:", e)
      setParsedProduct({
        kaspi_url: kaspiUrl,
        success: false,
        error: "Ошибка парсинга URL",
      })
    } finally {
      setParsingUrl(false)
    }
  }

  const handleSaveCalculation = async () => {
    if (!saveProductName.trim() || !result) return

    setSavingCalculation(true)
    try {
      await createSavedCalculation({
        name: saveProductName,
        kaspi_url: kaspiUrl || undefined,
        image_url: parsedProduct?.image_url,
        selling_price: values.sellingPrice,
        purchase_price: values.purchasePrice,
        category: values.category,
        subcategory: values.subcategory || undefined,
        weight_kg: values.weightKg,
        packaging_cost: values.packagingCost,
        other_costs: values.otherCosts,
        tax_regime: values.taxRegime,
        use_vat: values.useVat,
      })
      setShowSaveDialog(false)
      setSaveProductName("")
      // Refresh library if on that tab
      if (activeTab === "library") {
        loadSavedCalculations()
      }
    } catch (e) {
      console.error("Save error:", e)
    } finally {
      setSavingCalculation(false)
    }
  }

  const handleToggleFavorite = async (id: string) => {
    try {
      const result = await toggleFavorite(id)
      setSavedCalculations(prev =>
        prev.map(calc =>
          calc.id === id ? { ...calc, is_favorite: result.is_favorite } : calc
        )
      )
    } catch (e) {
      console.error("Toggle favorite error:", e)
    }
  }

  const handleDeleteCalculation = async () => {
    if (!deleteId) return

    try {
      await deleteSavedCalculation(deleteId)
      setSavedCalculations(prev => prev.filter(calc => calc.id !== deleteId))
      setShowDeleteDialog(false)
      setDeleteId(null)
    } catch (e) {
      console.error("Delete error:", e)
    }
  }

  const handleExport = async (format: 'csv' | 'excel') => {
    setExporting(true)
    try {
      await downloadExport(format)
    } catch (e) {
      console.error("Export error:", e)
    } finally {
      setExporting(false)
    }
  }

  const loadSavedToCalculator = (calc: SavedCalculation) => {
    setValues({
      sellingPrice: calc.selling_price,
      purchasePrice: calc.purchase_price,
      category: calc.category,
      subcategory: calc.subcategory || "",
      weightKg: calc.weight_kg,
      packagingCost: calc.packaging_cost,
      otherCosts: calc.other_costs,
      taxRegime: calc.tax_regime,
      useVat: calc.use_vat,
    })
    setKaspiUrl(calc.kaspi_url || "")
    setSaveProductName(calc.name)
    setActiveTab("calculator")
  }

  const formatPrice = (price: number) => {
    return new Intl.NumberFormat("ru-RU").format(Math.round(price)) + " ₸"
  }

  const formatPercent = (value: number) => {
    return value.toFixed(1) + "%"
  }

  const getMarginColor = (margin: number) => {
    if (margin > 20) return "text-green-600"
    if (margin > 10) return "text-yellow-600"
    return "text-red-500"
  }

  const getMarginBg = (margin: number) => {
    if (margin > 20) return "bg-green-500/10 border-green-500/30"
    if (margin > 10) return "bg-yellow-500/10 border-yellow-500/30"
    return "bg-red-500/10 border-red-500/30"
  }

  return (
    <SubscriptionGate>
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">
            {t("unit.title")}
          </h1>
          <p className="text-muted-foreground">
            {t("unit.subtitle")}
          </p>
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="calculator" className="flex items-center gap-2">
            <Calculator className="h-4 w-4" />
            {t("unit.calculator")}
          </TabsTrigger>
          <TabsTrigger value="library" className="flex items-center gap-2">
            <BookMarked className="h-4 w-4" />
            {t("unit.library")}
            {savedCalculations.length > 0 && (
              <Badge variant="secondary" className="ml-1">
                {savedCalculations.length}
              </Badge>
            )}
          </TabsTrigger>
        </TabsList>

        {/* Calculator Tab */}
        <TabsContent value="calculator" className="space-y-6">
          {/* URL Parser */}
          <Card className="glass-card">
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2 text-base">
                <Link2 className="h-5 w-5" />
                {t("unit.quickImport")}
              </CardTitle>
              <CardDescription>
                {t("unit.quickImportDesc")}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex gap-2">
                <Input
                  placeholder="https://kaspi.kz/shop/p/..."
                  value={kaspiUrl}
                  onChange={(e) => setKaspiUrl(e.target.value)}
                  className="flex-1"
                />
                <Button onClick={handleParseUrl} disabled={parsingUrl || !kaspiUrl.trim()}>
                  {parsingUrl ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Search className="h-4 w-4" />
                  )}
                </Button>
              </div>

              {parsedProduct && (
                <div
                  className={`mt-3 p-3 rounded-lg border ${
                    parsedProduct.success
                      ? "bg-green-500/10 border-green-500/30"
                      : "bg-red-500/10 border-red-500/30"
                  }`}
                >
                  {parsedProduct.success ? (
                    <div className="flex items-start gap-2">
                      <CheckCircle className="h-4 w-4 text-green-600 mt-0.5" />
                      <div className="text-sm">
                        <p className="font-medium">{parsedProduct.product_name || "Товар найден"}</p>
                        {parsedProduct.price && (
                          <p className="text-muted-foreground">
                            {t("unit.priceLabel")} {formatPrice(parsedProduct.price)}
                          </p>
                        )}
                        {parsedProduct.category && (
                          <p className="text-muted-foreground">
                            {t("unit.categoryLabel")} {parsedProduct.category}
                          </p>
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="flex items-start gap-2">
                      <XCircle className="h-4 w-4 text-red-500 mt-0.5" />
                      <p className="text-sm">{parsedProduct.error || "Ошибка парсинга"}</p>
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>

          <div className="grid gap-6 lg:grid-cols-2">
            {/* Calculator Form */}
            <Card className="glass-card">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Calculator className="h-5 w-5" />
                  {t("unit.calcParams")}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Basic fields */}
                <div className="grid gap-4 sm:grid-cols-2">
                  {/* Selling price */}
                  <div className="space-y-2">
                    <Label htmlFor="sellingPrice" className="flex items-center gap-2">
                      <DollarSign className="h-4 w-4 text-muted-foreground" />
                      {t("unit.sellingPrice")}
                    </Label>
                    <Input
                      id="sellingPrice"
                      type="number"
                      value={values.sellingPrice}
                      onChange={(e) =>
                        setValues({ ...values, sellingPrice: Number(e.target.value) })
                      }
                    />
                  </div>

                  {/* Purchase price */}
                  <div className="space-y-2">
                    <Label htmlFor="purchasePrice" className="flex items-center gap-2">
                      <Package className="h-4 w-4 text-muted-foreground" />
                      {t("unit.purchasePrice")}
                    </Label>
                    <Input
                      id="purchasePrice"
                      type="number"
                      value={values.purchasePrice}
                      onChange={(e) =>
                        setValues({ ...values, purchasePrice: Number(e.target.value) })
                      }
                    />
                  </div>
                </div>

                {/* Category */}
                <div className="space-y-2">
                  <Label className="flex items-center gap-2">
                    <Percent className="h-4 w-4 text-muted-foreground" />
                    {t("unit.productCategory")}
                  </Label>
                  <Select
                    value={values.category}
                    onValueChange={(val) => setValues({ ...values, category: val })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {CATEGORIES.map((cat) => (
                        <SelectItem key={cat} value={cat}>
                          {cat}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {result && (
                    <p className="text-xs text-muted-foreground">
                      {t("unit.kaspiCommission")}{" "}
                      <span className="font-medium">
                        {formatPercent(result.commission_rate)}
                        {!values.useVat && ` + НДС = ${formatPercent(result.commission_effective_rate)}`}
                      </span>
                      {values.useVat ? ` ${t("unit.withVAT")}` : ` ${t("unit.withoutVAT")}`}
                    </p>
                  )}
                </div>

                {/* Tax regime */}
                <div className="space-y-2">
                  <Label className="flex items-center gap-2">
                    <Building2 className="h-4 w-4 text-muted-foreground" />
                    {t("unit.taxRegime")}
                  </Label>
                  <Select
                    value={values.taxRegime}
                    onValueChange={(val) => setValues({ ...values, taxRegime: val })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {Object.entries(TAX_REGIMES).map(([key, regime]) => (
                        <SelectItem key={key} value={key}>
                          {regime.name} ({regime.description})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Weight */}
                <div className="space-y-2">
                  <Label htmlFor="weight" className="flex items-center gap-2">
                    <Scale className="h-4 w-4 text-muted-foreground" />
                    {t("unit.productWeight")}
                  </Label>
                  <Input
                    id="weight"
                    type="number"
                    step="0.1"
                    min="0.1"
                    max="31"
                    value={values.weightKg}
                    onChange={(e) =>
                      setValues({ ...values, weightKg: Number(e.target.value) })
                    }
                  />
                </div>

                {/* Advanced toggle */}
                <Button
                  variant="ghost"
                  className="w-full justify-between"
                  onClick={() => setShowAdvanced(!showAdvanced)}
                >
                  <span>
                    {t("unit.additionalCosts")}
                  </span>
                  {showAdvanced ? (
                    <ChevronUp className="h-4 w-4" />
                  ) : (
                    <ChevronDown className="h-4 w-4" />
                  )}
                </Button>

                {showAdvanced && (
                  <div className="space-y-4 pt-2">
                    {/* Packaging */}
                    <div className="space-y-2">
                      <Label htmlFor="packagingCost">
                        {t("unit.packaging")}
                      </Label>
                      <Input
                        id="packagingCost"
                        type="number"
                        value={values.packagingCost}
                        onChange={(e) =>
                          setValues({ ...values, packagingCost: Number(e.target.value) })
                        }
                      />
                    </div>

                    {/* Other costs */}
                    <div className="space-y-2">
                      <Label htmlFor="otherCosts">
                        {t("unit.otherCosts")}
                      </Label>
                      <Input
                        id="otherCosts"
                        type="number"
                        value={values.otherCosts}
                        onChange={(e) =>
                          setValues({ ...values, otherCosts: Number(e.target.value) })
                        }
                      />
                    </div>

                    {/* VAT toggle */}
                    <div className="flex items-center justify-between">
                      <Label htmlFor="useVat" className="cursor-pointer">
                        {t("unit.commissionVAT")}
                      </Label>
                      <Switch
                        id="useVat"
                        checked={values.useVat}
                        onCheckedChange={(checked) =>
                          setValues({ ...values, useVat: checked })
                        }
                      />
                    </div>
                  </div>
                )}

                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    className="flex-1"
                    onClick={() =>
                      setValues({
                        sellingPrice: 0,
                        purchasePrice: 0,
                        category: "Автотовары",
                        subcategory: "",
                        weightKg: 1.0,
                        packagingCost: 0,
                        otherCosts: 0,
                        taxRegime: "ip_simplified",
                        useVat: false,
                      })
                    }
                  >
                    <RefreshCw className="h-4 w-4 mr-2" />
                    {t("unit.reset")}
                  </Button>
                  <Button
                    className="flex-1"
                    onClick={() => setShowSaveDialog(true)}
                    disabled={!result || values.sellingPrice === 0}
                  >
                    <Save className="h-4 w-4 mr-2" />
                    {t("unit.save")}
                  </Button>
                </div>
              </CardContent>
            </Card>

            {/* Results */}
            <div className="space-y-6">
              {/* Best scenario highlight */}
              {result && (
                <Card className={`glass-card border ${getMarginBg(result.best_margin)}`}>
                  <CardContent className="p-6">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground mb-1">
                        {t("unit.bestScenario")}:{" "}
                        <span className="font-medium">
                          {DELIVERY_TYPES[result.best_scenario]?.name || result.best_scenario}
                        </span>
                      </p>
                      <p className={`text-4xl font-bold ${getMarginColor(result.best_margin)}`}>
                        {formatPrice(result.best_profit)}
                      </p>
                      <p className="text-lg text-muted-foreground mt-1">
                        {t("unit.margin")}:{" "}
                        <span className={getMarginColor(result.best_margin)}>
                          {formatPercent(result.best_margin)}
                        </span>
                      </p>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Delivery scenarios comparison */}
              {result && (
                <Card className="glass-card">
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <Truck className="h-5 w-5" />
                      {t("unit.deliveryComparison")}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      {result.delivery_scenarios.map((scenario) => (
                        <div
                          key={scenario.delivery_type}
                          className={`p-3 rounded-lg border ${
                            scenario.delivery_type === result.best_scenario
                              ? "border-primary bg-primary/5"
                              : "border-border"
                          }`}
                        >
                          <div className="flex items-center justify-between">
                            <div>
                              <p className="font-medium text-sm">{scenario.name}</p>
                              <p className="text-xs text-muted-foreground">
                                {t("unit.delivery")}{" "}
                                {formatPrice(scenario.cost)}
                              </p>
                            </div>
                            <div className="text-right">
                              <p className={`font-bold ${getMarginColor(scenario.margin_percent)}`}>
                                {formatPrice(scenario.profit)}
                              </p>
                              <Badge
                                variant={scenario.margin_percent > 15 ? "default" : "secondary"}
                                className="text-xs"
                              >
                                {formatPercent(scenario.margin_percent)}
                              </Badge>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Cost breakdown */}
              {result && (
                <Card className="glass-card">
                  <CardHeader>
                    <CardTitle>
                      {t("unit.costBreakdown")}
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex justify-between items-center">
                      <span className="text-muted-foreground">
                        {t("unit.sellingPrice")}
                      </span>
                      <span className="font-semibold">{formatPrice(result.selling_price)}</span>
                    </div>
                    <Separator />
                    <div className="flex justify-between items-center">
                      <span className="text-muted-foreground">
                        {t("unit.purchasePrice")}
                      </span>
                      <span className="text-red-500">-{formatPrice(result.purchase_price)}</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-muted-foreground">
                        {t("unit.kaspiCommission")} (
                        {formatPercent(result.commission_effective_rate)})
                      </span>
                      <span className="text-red-500">-{formatPrice(result.commission_amount)}</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-muted-foreground">
                        Kaspi Pay ({formatPercent(result.kaspi_pay_rate)})
                      </span>
                      <span className="text-red-500">-{formatPrice(result.kaspi_pay_amount)}</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-muted-foreground">
                        {result.tax_regime} ({formatPercent(result.tax_rate)})
                      </span>
                      <span className="text-red-500">-{formatPrice(result.tax_amount)}</span>
                    </div>
                    {result.packaging_cost > 0 && (
                      <div className="flex justify-between items-center">
                        <span className="text-muted-foreground">
                          {t("unit.packaging")}
                        </span>
                        <span className="text-red-500">-{formatPrice(result.packaging_cost)}</span>
                      </div>
                    )}
                    {result.other_costs > 0 && (
                      <div className="flex justify-between items-center">
                        <span className="text-muted-foreground">
                          {t("unit.otherCosts")}
                        </span>
                        <span className="text-red-500">-{formatPrice(result.other_costs)}</span>
                      </div>
                    )}
                  </CardContent>
                </Card>
              )}

              {/* Tips */}
              <Card className="glass-card">
                <CardContent className="p-4">
                  <div className="flex items-start gap-3">
                    <Info className="h-5 w-5 text-muted-foreground mt-0.5 shrink-0" />
                    <div>
                      <p className="font-medium">
                        {t("unit.tip")}
                      </p>
                      <p className="text-sm text-muted-foreground">
                        {result && result.best_margin < 15
                          ? t("unit.tipLowMargin")
                          : t("unit.tipGoodMargin")}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        </TabsContent>

        {/* Library Tab */}
        <TabsContent value="library" className="space-y-6">
          {/* Export buttons */}
          <div className="flex justify-between items-center">
            <h2 className="text-lg font-semibold">
              {t("unit.savedCalcs")}
            </h2>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => handleExport('csv')}
                disabled={exporting || savedCalculations.length === 0}
              >
                {exporting ? (
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                ) : (
                  <FileText className="h-4 w-4 mr-2" />
                )}
                CSV
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => handleExport('excel')}
                disabled={exporting || savedCalculations.length === 0}
              >
                {exporting ? (
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                ) : (
                  <FileSpreadsheet className="h-4 w-4 mr-2" />
                )}
                Excel
              </Button>
            </div>
          </div>

          {loadingSaved ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : savedCalculations.length === 0 ? (
            <Card className="glass-card">
              <CardContent className="flex flex-col items-center justify-center py-12 text-center">
                <BookMarked className="h-12 w-12 text-muted-foreground mb-4" />
                <h3 className="text-lg font-medium mb-2">
                  {t("unit.noSavedCalcs")}
                </h3>
                <p className="text-muted-foreground mb-4">
                  {t("unit.noSavedCalcsDesc")}
                </p>
                <Button onClick={() => setActiveTab("calculator")}>
                  <Calculator className="h-4 w-4 mr-2" />
                  {t("unit.createCalc")}
                </Button>
              </CardContent>
            </Card>
          ) : (
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {savedCalculations.map((calc) => (
                <Card key={calc.id} className="glass-card hover:border-primary/50 transition-colors">
                  <CardHeader className="pb-2">
                    <div className="flex items-start justify-between">
                      <CardTitle className="text-base line-clamp-2">{calc.name}</CardTitle>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="shrink-0"
                        onClick={() => handleToggleFavorite(calc.id)}
                      >
                        <Star
                          className={`h-4 w-4 ${
                            calc.is_favorite ? "fill-yellow-500 text-yellow-500" : "text-muted-foreground"
                          }`}
                        />
                      </Button>
                    </div>
                    <CardDescription>{calc.category}</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="grid grid-cols-2 gap-2 text-sm">
                      <div>
                        <p className="text-muted-foreground">{t("unit.sale")}</p>
                        <p className="font-medium">{formatPrice(calc.selling_price)}</p>
                      </div>
                      <div>
                        <p className="text-muted-foreground">{t("unit.purchase")}</p>
                        <p className="font-medium">{formatPrice(calc.purchase_price)}</p>
                      </div>
                    </div>

                    {calc.best_profit !== undefined && (
                      <div className={`p-2 rounded-lg ${getMarginBg(calc.best_margin || 0)}`}>
                        <div className="flex justify-between items-center">
                          <span className="text-sm">{t("unit.profit")}</span>
                          <span className={`font-bold ${getMarginColor(calc.best_margin || 0)}`}>
                            {formatPrice(calc.best_profit)}
                          </span>
                        </div>
                        <div className="flex justify-between items-center text-sm">
                          <span className="text-muted-foreground">{t("unit.margin")}</span>
                          <span className={getMarginColor(calc.best_margin || 0)}>
                            {formatPercent(calc.best_margin || 0)}
                          </span>
                        </div>
                      </div>
                    )}

                    <div className="flex gap-2 pt-2">
                      <Button
                        variant="outline"
                        size="sm"
                        className="flex-1"
                        onClick={() => loadSavedToCalculator(calc)}
                      >
                        <Calculator className="h-4 w-4 mr-1" />
                        {t("common.open")}
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="text-red-500 hover:text-red-600 hover:bg-red-500/10"
                        onClick={() => {
                          setDeleteId(calc.id)
                          setShowDeleteDialog(true)
                        }}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </TabsContent>
      </Tabs>

      {/* Save Dialog */}
      <Dialog open={showSaveDialog} onOpenChange={setShowSaveDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {t("unit.saveCalc")}
            </DialogTitle>
            <DialogDescription>
              {t("unit.saveCalcDesc")}
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <Label htmlFor="productName">{t("unit.productName")}</Label>
            <Input
              id="productName"
              value={saveProductName}
              onChange={(e) => setSaveProductName(e.target.value)}
              placeholder={t("unit.productNamePlaceholder")}
              className="mt-2"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowSaveDialog(false)}>
              {t("common.cancel2")}
            </Button>
            <Button
              onClick={handleSaveCalculation}
              disabled={savingCalculation || !saveProductName.trim()}
            >
              {savingCalculation ? (
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
              ) : (
                <Save className="h-4 w-4 mr-2" />
              )}
              {t("unit.save")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {t("unit.deleteCalc")}
            </DialogTitle>
            <DialogDescription>
              {t("unit.deleteCalcDesc")}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDeleteDialog(false)}>
              {t("common.cancel2")}
            </Button>
            <Button variant="destructive" onClick={handleDeleteCalculation}>
              <Trash2 className="h-4 w-4 mr-2" />
              {t("common.delete")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
    </SubscriptionGate>
  )
}
