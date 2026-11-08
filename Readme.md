# DataCollector

Платформа для потокового сбора, валидации и надежного сохранения рыночных данных (Market Data: сделки, котировки) фондового рынка (Московская биржа MOEX, СПБ Биржа SPB) в базу данных временных рядов TimescaleDB через брокер сообщений Apache Kafka.

---

## 1. Назначение системы

* **Количественные исследования:** расчет статистических характеристик, волатильности и рыночной микроструктуры.
* **Машинное обучение (Machine Learning):** формирование непрерывных датасетов для предиктивных моделей.
* **Бэктестинг (Backtesting):** воспроизведение исторических торгов с точностью до тика.
* **Эффективное хранение:** колоночное сжатие котировок в TimescaleDB (экономия до 90% дискового пространства).

---

## 2. Архитектура платформы

Система построена по мультимодульной сервисной модели с разделением контуров управления и консьюминга:

```
[ HTTP Клиент / Frontend ]
           | (REST API /api/v1/subscriptions)
           v
+-------------------------------------------------------------+
|               Control Plane (модуль control-plane)          |
|  - Реестр подписок в PostgreSQL (таблица subscriptions)     |
|  - Автоматический резолв тикеров в instrument_uid (T-Invest)|
|  - Неблокирующая трансляция сделок в Kafka (market.raw)     |
+-------------------------------------------------------------+
           | (Protobuf Trade, сжатие zstd, ключ EXCHANGE:...)
           v
+-------------------------------------------------------------+
|                     Apache Kafka Broker                     |
|  - Топик market.raw (сырые Protobuf-сообщения сделок)       |
+-------------------------------------------------------------+
           |
           v
+-------------------------------------------------------------+
|                Data Ingestor (модуль data-ingestor)         |
|  - Батчевый Kafka Listener (@KafkaListener batch = true)    |
|  - Десериализация Protobuf и парсинг ключа биржи            |
|  - Пакетная вставка (batchUpdate) в TimescaleDB             |
+-------------------------------------------------------------+
           |
           v
+-------------------------------------------------------------+
|                TimescaleDB (гипертаблица market_trades)     |
|  - Партиционирование по суткам (chunk_time_interval: 1 day) |
|  - Колоночное сжатие чанков старше 1 дня                    |
+-------------------------------------------------------------+
```

### Модули монорепозитория

* **`control-plane` (CP):** REST API управления подписками, интеграция с T-Invest gRPC SDK (`MarketDataStreamService`), резолв тикеров через справочник инструментов и публикация Protobuf-сообщений в топик `market.raw`.
* **`common` (CO):** общие контракты данных, мапперы и транзитивный экспорт скомпилированных Protobuf-схем T-Invest API для всех модулей проекта.
* **`data-ingestor` (DI):** легковесный воркер сохранения данных. Читает пачки из Kafka (`max-poll-records=1000`) и с минимальными накладными расходами выполняет пакетную запись в гипертаблицу `market_trades`.

---

## 3. Схема данных (TimescaleDB)

Сделки сохраняются в гипертаблицу `market_trades`:

| Колонка | Тип | Описание |
| :--- | :--- | :--- |
| `exchange` | `VARCHAR(16)` | Биржа (`MOEX`, `SPB`, `UNKNOWN`) |
| `symbol` | `VARCHAR(64)` | Идентификатор инструмента (`instrument_uid` или `figi`) |
| `ticker` | `VARCHAR(32)` | Биржевой тикер (`SBER`, `GAZP`, `AAPL`) |
| `price` | `NUMERIC(18, 4)` | Цена сделки |
| `quantity` | `BIGINT` | Количество лотов / штук |
| `trade_time` | `TIMESTAMP WITH TIME ZONE` | Время совершения сделки |

**Оптимизации хранения:**
* Индекс `(exchange, symbol, trade_time DESC)`.
* Автоматическое суточное партиционирование.
* Колоночное сжатие (TimescaleDB compression) для чанков старше 24 часов с сегментацией по `exchange, symbol`.

---

## 4. Технологический стек

* **Платформа:** Java 25
* **Фреймворк:** Spring Boot 4.1.1 (Data JDBC, Kafka)
* **Сериализация:** Google Protobuf 4, Jackson 3
* **Брокер сообщений:** Apache Kafka 3.9 (сжатие zstd, batching)
* **Хранилище данных:** TimescaleDB (PostgreSQL 16) + Flyway
* **Сборка и зависимости:** Gradle 9.4 (Version Catalog `gradle/libs.versions.toml`)
* **Контейнеризация:** Docker & Docker Compose

---

## 5. Быстрый старт

### Требования
* Установленный Docker и Docker Compose.
* Токен T-Invest API (при необходимости получения реальных рыночных данных).

### Запуск инфраструктуры и сервисов

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/vasyagladkih/DataCollector.git
   cd DataCollector
   ```

2. Запустите сборку и старт всех контейнеров:
   ```bash
   docker compose up -d --build
   ```

3. Доступные сервисы:
   * **Control Plane REST API:** `http://localhost:8081`
   * **Kafka UI:** `http://localhost:8080`
   * **TimescaleDB:** `localhost:5432` (БД: `datacollector`, пользователь: `postgres`, пароль: `postgres`)

### Примеры работы с API

* **Создание подписки на инструмент:**
  ```bash
  curl -X POST http://localhost:8081/api/v1/subscriptions \
    -H "Content-Type: application/json" \
    -d '{
      "exchange": "MOEX",
      "channel": "trades",
      "symbol": "SBER"
    }'
  ```

* **Просмотр активных подписок:**
  ```bash
  curl http://localhost:8081/api/v1/subscriptions
  ```

* **Просмотр сохраненных сделок в базе:**
  ```bash
  docker exec -it datacollector-postgres psql -U postgres -d datacollector -c \
    "SELECT exchange, ticker, price, quantity, trade_time FROM market_trades ORDER BY trade_time DESC LIMIT 10;"
  ```
