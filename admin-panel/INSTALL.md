# Инструкция по установке

## Автоматическая установка

Запустите скрипт установки:

```bash
cd "/Users/hasen/Desktop/cube-demper-full/admin panel"
./install-node.sh
```

Скрипт автоматически:
1. Установит Homebrew (если не установлен)
2. Установит Node.js через Homebrew
3. Установит все зависимости проекта

## Ручная установка

### 1. Установите Node.js

**Вариант A: Через официальный сайт (рекомендуется)**
- Скачайте с https://nodejs.org/
- Выберите LTS версию
- Установите .pkg файл
- Перезапустите терминал

**Вариант B: Через Homebrew**
```bash
# Установите Homebrew (если не установлен)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Установите Node.js
brew install node
```

### 2. Установите зависимости проекта

```bash
cd "/Users/hasen/Desktop/cube-demper-full/admin panel"
npm install
```

### 3. Создайте файл .env.local

```bash
echo "NEXT_PUBLIC_API_URL=http://localhost:8010" > .env.local
```

### 4. Запустите проект

```bash
npm run dev
```

Проект будет доступен по адресу: http://localhost:3000
