# Используем официальный образ Go
FROM golang:latest

# Устанавливаем переменные окружения
ENV APP_DIR /go/src/app
ENV GO111MODULE=on

# Создаем директорию для приложения
RUN mkdir -p $APP_DIR

# Устанавливаем рабочую директорию
WORKDIR $APP_DIR

# Копируем go.mod и go.sum для эффективного кэширования зависимостей
COPY go.mod .
COPY go.sum .

# Загружаем зависимости
RUN go mod download

# Копируем все файлы проекта в рабочую директорию
COPY . .

# Собираем приложение
RUN go build -o main .


# Команда для запуска приложения
CMD ["./main"]