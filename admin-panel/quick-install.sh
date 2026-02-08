#!/bin/bash

# Быстрая установка Node.js и зависимостей

echo "🚀 Быстрая установка для Admin Panel"
echo ""

# Проверка Node.js
if command -v node &> /dev/null; then
    echo "✅ Node.js уже установлен: $(node --version)"
    echo "✅ npm: $(npm --version)"
    echo ""
    echo "Установка зависимостей проекта..."
    npm install
    echo ""
    echo "✅ Готово! Запустите: npm run dev"
    exit 0
fi

echo "❌ Node.js не найден"
echo ""
echo "Для установки Node.js выполните одну из команд:"
echo ""
echo "1. Через Homebrew (требует пароль):"
echo "   /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
echo "   brew install node"
echo ""
echo "2. Или скачайте с официального сайта:"
echo "   https://nodejs.org/"
echo ""
echo "После установки Node.js запустите этот скрипт снова:"
echo "   ./quick-install.sh"
