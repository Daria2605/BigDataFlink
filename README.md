# BigDataFlink

Лабораторная работа №3 по анализу больших данных: streaming processing с помощью Apache Flink.

Выполнила: Жгенти Дарья Никитична М8О-308Б-23

Pipeline:

```text
CSV → JSON → Kafka → Flink → PostgreSQL
```

## 1. Сборка Flink job

```bash
docker run --rm \
  -v "$PWD/flink-job:/app" \
  -v "$HOME/.m2:/root/.m2" \
  -w /app \
  maven:3.9-eclipse-temurin-11 \
  mvn -DskipTests package
```

## 2. Запуск инфраструктуры

```bash
docker compose up -d postgres kafka jobmanager taskmanager
```

## 3. Создание Kafka topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic sales_raw \
  --partitions 3 \
  --replication-factor 1
```

## 4. Проверка Kafka topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic sales_raw
```

## 5. Запуск Flink job

```bash
chmod +x scripts/submit_job.sh
./scripts/submit_job.sh
```

## 6. Проверка запущенной Flink job

```bash
docker compose exec jobmanager ./bin/flink list
```

Также Flink UI доступен в браузере:

```text
http://localhost:8081
```

## 7. Запуск producer

```bash
docker compose --profile producer up --no-deps producer
```

Если producer ещё не собран:

```bash
docker compose --profile producer up --build --no-deps producer
```

## 8. Проверка результата в PostgreSQL

```bash
docker compose exec -T postgres psql -U postgres -d petshop < scripts/check_result.sql
```

## 9. Ручная проверка PostgreSQL

```bash
docker compose exec postgres psql -U postgres -d petshop
```

Внутри `psql`:

```sql
SELECT COUNT(*) FROM star.fact_sales;

SELECT COUNT(*) FROM star.dim_customer;
SELECT COUNT(*) FROM star.dim_product;
SELECT COUNT(*) FROM star.dim_seller;
SELECT COUNT(*) FROM star.dim_store;
SELECT COUNT(*) FROM star.dim_supplier;

SELECT * FROM star.fact_sales LIMIT 5;
```

Выход из `psql`:

```sql
\q
```

## 10. Проверка логов

```bash
docker compose logs --tail=100 jobmanager
```

```bash
docker compose logs --tail=100 taskmanager
```

```bash
docker compose logs --tail=100 kafka
```

```bash
docker compose logs --tail=100 producer
```

## 11. Остановка контейнеров

```bash
docker compose down
```

## 12. Полная очистка контейнеров и данных

```bash
docker compose down -v
```
