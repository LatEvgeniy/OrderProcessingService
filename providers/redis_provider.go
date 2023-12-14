package providers

import "github.com/redis/go-redis/v9"

var (
	address  = "redis:6379"
	password = ""
	db       = 0
)

func NewRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})

	return client
}
