export interface PresetTemplate {
  id: string
  icon: string
  nameRu: string
  nameEn: string
  triggerEvent: string
  messageRu: string
  messageEn: string
}

export interface VariableChip {
  labelRu: string
  labelEn: string
  variable: string
}

export const PRESET_TEMPLATES: PresetTemplate[] = [
  {
    id: "order_approved",
    icon: "💳",
    nameRu: "Заказ оплачен",
    nameEn: "Order paid",
    triggerEvent: "order_approved",
    messageRu:
      "Здравствуйте, {customer_name}! 🎉\n\nВаш заказ #{order_code} на сумму {order_total} ₸ успешно оплачен.\n\nТовары: {items_list}\n\nСпасибо за покупку в {store_name}!",
    messageEn:
      "Hello, {customer_name}! 🎉\n\nYour order #{order_code} for {order_total} ₸ has been paid.\n\nItems: {items_list}\n\nThank you for shopping at {store_name}!",
  },
  {
    id: "order_accepted",
    icon: "✅",
    nameRu: "Заказ принят",
    nameEn: "Order accepted",
    triggerEvent: "order_accepted_by_merchant",
    messageRu:
      "Здравствуйте, {customer_name}!\n\nВаш заказ #{order_code} принят в обработку. Мы готовим его к отправке.\n\nС уважением, {store_name}",
    messageEn:
      "Hello, {customer_name}!\n\nYour order #{order_code} has been accepted. We are preparing it for shipment.\n\nBest regards, {store_name}",
  },
  {
    id: "order_shipped",
    icon: "📦",
    nameRu: "Заказ отправлен",
    nameEn: "Order shipped",
    triggerEvent: "order_shipped",
    messageRu:
      "Здравствуйте, {customer_name}! 📦\n\nВаш заказ #{order_code} отправлен!\nАдрес доставки: {delivery_address}\n\nОтслеживайте статус в приложении Kaspi.",
    messageEn:
      "Hello, {customer_name}! 📦\n\nYour order #{order_code} has been shipped!\nDelivery address: {delivery_address}\n\nTrack your order in the Kaspi app.",
  },
  {
    id: "order_delivered",
    icon: "🏠",
    nameRu: "Заказ доставлен",
    nameEn: "Order delivered",
    triggerEvent: "order_delivered",
    messageRu:
      "Здравствуйте, {customer_name}!\n\nВаш заказ #{order_code} доставлен! 🎁\n\nНадеемся, вам всё понравится. Будем рады вашему отзыву на Kaspi!\n\n{store_name}",
    messageEn:
      "Hello, {customer_name}!\n\nYour order #{order_code} has been delivered! 🎁\n\nWe hope you enjoy it. We'd love your review on Kaspi!\n\n{store_name}",
  },
  {
    id: "order_completed",
    icon: "🏁",
    nameRu: "Заказ завершён",
    nameEn: "Order completed",
    triggerEvent: "order_completed",
    messageRu:
      "Здравствуйте, {customer_name}!\n\nСпасибо за покупку в {store_name}! 🙏\n\nЕсли у вас есть вопросы по заказу #{order_code} — пишите, мы всегда на связи.\n\nДо новых покупок! 🛍️",
    messageEn:
      "Hello, {customer_name}!\n\nThank you for shopping at {store_name}! 🙏\n\nIf you have any questions about order #{order_code} — write to us anytime.\n\nSee you again! 🛍️",
  },
  {
    id: "review_request",
    icon: "⭐",
    nameRu: "Запрос отзыва",
    nameEn: "Review request",
    triggerEvent: "review_request",
    messageRu:
      "Здравствуйте, {customer_name}!\n\nНадеемся, вам понравился товар из заказа #{order_code}. ⭐\n\nБудем очень благодарны за отзыв на Kaspi — это помогает другим покупателям!\n\n{store_name}",
    messageEn:
      "Hello, {customer_name}!\n\nWe hope you enjoyed your purchase from order #{order_code}. ⭐\n\nWe'd really appreciate a review on Kaspi — it helps other buyers!\n\n{store_name}",
  },
  {
    id: "custom",
    icon: "✏️",
    nameRu: "Свой шаблон",
    nameEn: "Custom template",
    triggerEvent: "",
    messageRu: "",
    messageEn: "",
  },
]

export const VARIABLE_CHIPS: VariableChip[] = [
  { labelRu: "Имя клиента", labelEn: "Customer name", variable: "customer_name" },
  { labelRu: "Имя (кратко)", labelEn: "First name", variable: "customer_first_name" },
  { labelRu: "Код заказа", labelEn: "Order code", variable: "order_code" },
  { labelRu: "Сумма", labelEn: "Total", variable: "order_total" },
  { labelRu: "Товары", labelEn: "Items list", variable: "items_list" },
  { labelRu: "Кол-во товаров", labelEn: "Items count", variable: "items_count" },
  { labelRu: "Первый товар", labelEn: "First item", variable: "first_item" },
  { labelRu: "Адрес", labelEn: "Address", variable: "delivery_address" },
  { labelRu: "Город", labelEn: "City", variable: "delivery_city" },
  { labelRu: "Магазин", labelEn: "Store name", variable: "store_name" },
  { labelRu: "Промокод", labelEn: "Promo code", variable: "promo_code" },
]

export const SAMPLE_DATA: Record<string, string> = {
  customer_name: "Алия Сергеевна",
  customer_first_name: "Алия",
  order_code: "KSP-123456",
  order_total: "25 990",
  items_list: "iPhone 15 Case x1, AirPods Pro x1",
  items_count: "2",
  first_item: "iPhone 15 Case",
  delivery_address: "ул. Абая 150, кв. 42",
  delivery_city: "Алматы",
  store_name: "TechShop KZ",
  promo_code: "SALE10",
}
