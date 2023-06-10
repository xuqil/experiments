package bigkey

import (
	"context"
	"github.com/redis/go-redis/v9"
	"testing"
)

func TestDeleteBigHash(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "192.168.122.20:6379",
		Password: "redis123",
	})
	defer client.Close()

	if err := DeleteBigHash(context.Background(), client, "h1"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBigList(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "192.168.122.20:6379",
		Password: "redis123",
	})
	defer client.Close()

	if err := DeleteBigList(context.Background(), client, "list1"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBigSet(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "192.168.122.20:6379",
		Password: "redis123",
	})
	defer client.Close()

	if err := DeleteBigSet(context.Background(), client, "set1"); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteBigZSet(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "192.168.122.20:6379",
		Password: "redis123",
	})
	defer client.Close()

	if err := DeleteBigZSet(context.Background(), client, "zset1"); err != nil {
		t.Fatal(err)
	}
}

func DeleteBigHash(ctx context.Context, client *redis.Client, key string) error {
	var cursor uint64
	var count int64 = 100
	fields, cursor, err := client.HScan(ctx, key, cursor, "", count).Result()
	if err != nil {
		return err
	}
	client.HDel(ctx, key, fields...)
	for cursor != 0 {
		fields, cursor, err = client.HScan(ctx, key, cursor, "", count).Result()
		if err != nil {
			return err
		}
		if len(fields) != 0 {
			client.HDel(ctx, key, fields...)
		}
	}
	return client.Del(ctx, key).Err()
}

func DeleteBigList(ctx context.Context, client *redis.Client, key string) error {
	var counter int64
	var left int64 = 100
	lLen, err := client.LLen(ctx, key).Result()
	if err != nil {
		return err
	}
	for counter < lLen {
		// 每次从左侧截掉100个
		if err = client.LTrim(ctx, key, left, lLen).Err(); err != nil {
			return err
		}
		counter += left
	}
	return client.Del(ctx, key).Err()
}

func DeleteBigSet(ctx context.Context, client *redis.Client, key string) error {
	var cursor uint64
	var count int64 = 100
	members, cursor, err := client.SScan(ctx, key, cursor, "", count).Result()
	if err != nil {
		return err
	}
	if err = client.SRem(ctx, key, members).Err(); err != nil {
		return err
	}
	for cursor != 0 {
		members, cursor, err = client.SScan(ctx, key, cursor, "", count).Result()
		if err != nil {
			return err
		}
		if err = client.SRem(ctx, key, members).Err(); err != nil {
			return err
		}
	}
	return client.Del(ctx, key).Err()
}

func DeleteBigZSet(ctx context.Context, client *redis.Client, key string) error {
	var cursor uint64
	var count int64 = 100
	keys, cursor, err := client.ZScan(ctx, key, cursor, "", count).Result()
	if err != nil {
		return err
	}
	if err = client.ZRem(ctx, key, keys).Err(); err != nil {
		return err
	}
	for cursor != 0 {
		keys, cursor, err = client.ZScan(ctx, key, cursor, "", count).Result()
		if err != nil {
			return err
		}
		if err = client.ZRem(ctx, key, keys).Err(); err != nil {
			return err
		}
	}
	return client.Del(ctx, key).Err()
}
