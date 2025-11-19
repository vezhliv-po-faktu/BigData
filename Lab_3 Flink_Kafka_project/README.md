# Запуск
docker-compose up --build -d

# Запуск Flink Job
bash start.sh

## Остановка
docker-compose down

# Скрипт с анализом данных
analys.sql

# Скрипт по заполнению данных по модели снежинка
FlinkStreamingJob.java