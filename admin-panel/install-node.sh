#!/bin/bash

# Скрипт установки Node.js для macOS

echo "Проверка наличия Node.js..."

if command -v node &> /dev/null; then
    echo "Node.js уже установлен: $(node --version)"
    exit 0
fi

echo "Node.js не найден. Начинаю установку..."

# Проверка наличия Homebrew
if ! command -v brew &> /dev/null; then
    echo "Установка Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    
    # Добавление Homebrew в PATH для Apple Silicon
    if [ -f /opt/homebrew/bin/brew ]; then
        echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
        eval "$(/opt/homebrew/bin/brew shellenv)"
    fi
fi

echo "Установка Node.js через Homebrew..."
brew install node

echo "Проверка установки..."
if command -v node &> /dev/null; then
    echo "✅ Node.js успешно установлен: $(node --version)"
    echo "✅ npm установлен: $(npm --version)"
    
    echo ""
    echo "Установка зависимостей проекта..."
    cd "$(dirname "$0")"
    npm install
    
    echo ""
    echo "✅ Готово! Зависимости установлены."
else
    echo "❌ Ошибка установки Node.js"
    exit 1
fi
