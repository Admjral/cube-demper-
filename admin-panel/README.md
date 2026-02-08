# Admin Panel - Cube Demper

Админ-панель для управления системой Cube Demper.

## Возможности

- 📊 Общая статистика системы
- 👥 Управление пользователями (блокировка, продление подписки, детали)
- 🏪 Просмотр всех магазинов пользователей
- 🤝 Управление партнерами (создание, удаление, статистика)

## Установка

### Шаг 1: Установите Node.js (если не установлен)

**Вариант A: Через официальный сайт (рекомендуется)**
1. Скачайте Node.js LTS с https://nodejs.org/
2. Установите .pkg файл
3. Перезапустите терминал

**Вариант B: Через Homebrew**
```bash
# Установите Homebrew (если не установлен)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Установите Node.js
brew install node
```

**Проверка установки:**
```bash
node --version  # Должно показать версию, например v20.x.x
npm --version   # Должно показать версию npm
```

### Шаг 2: Установите зависимости проекта

```bash
cd "/Users/hasen/Desktop/cube-demper-full/admin panel"
npm install
```

### Шаг 3: Создайте файл .env.local

```bash
echo "NEXT_PUBLIC_API_URL=http://localhost:8010" > .env.local
```

Или создайте файл `.env.local` вручную с содержимым:
```
NEXT_PUBLIC_API_URL=http://localhost:8010
```

## Запуск

```bash
# Режим разработки
npm run dev

# Сборка для продакшена
npm run build

# Запуск продакшен версии
npm start
```

## Структура проекта

```
src/
├── app/
│   ├── dashboard/          # Страницы админ-панели
│   │   ├── page.tsx       # Статистика
│   │   ├── users/         # Пользователи
│   │   ├── stores/        # Магазины
│   │   └── partners/      # Партнеры
│   ├── login/             # Страница входа
│   └── layout.tsx         # Root layout
├── components/
│   ├── admin/             # Компоненты админ-панели
│   ├── layout/            # Layout компоненты (sidebar, header)
│   └── ui/                # UI компоненты (shadcn/ui)
├── hooks/
│   └── api/               # React Query hooks для API
├── lib/
│   ├── api.ts             # API client
│   └── auth.ts            # Аутентификация
└── types/
    └── admin.ts           # TypeScript типы
```

## Требования

- Node.js 18+
- Backend API должен быть запущен и доступен

## Аутентификация

Для входа в админ-панель требуется учетная запись с ролью `admin`. Обычные пользователи не могут получить доступ к админ-панели.
