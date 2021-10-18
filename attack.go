package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

type MessageSearchHint struct {
	Message string
	Chat_id int
}

func search(ctx context.Context, dbpool *pgxpool.Pool, prefix string, limit int) ([]MessageSearchHint, error) {

	const sql = `select messege, chat_id from messeges where messege like $1 order by chat_id asc limit $2;`

	patter := prefix + "%"

	rows, err := dbpool.Query(ctx, sql, patter, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query data: %w", err)
	}
	// Вызов Close нужен, чтобы вернуть соединение в пул
	defer rows.Close()

	//В слайс hints будут собраны все сторки, полученные из базы
	var hints []MessageSearchHint

	for rows.Next() {
		var hint MessageSearchHint

		err = rows.Scan(&hint.Message, &hint.Chat_id)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		hints = append(hints, hint)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to read response: %w", rows.Err())
	}

	return hints, nil

}

type AttackResults struct {
	Duration        time.Duration
	Threads         int
	QueriesPerfomed uint64
}

func attack(ctx context.Context, duration time.Duration, threads int, dbpool *pgxpool.Pool) AttackResults {
	var queries uint64

	attacker := func(stopAt time.Time) {
		for {
			_, err := search(ctx, dbpool, "Hello", 5)
			if err != nil {
				log.Fatal(err)
			}

			atomic.AddUint64(&queries, 1)

			if time.Now().After(stopAt) {
				return
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(threads)

	startAt := time.Now()
	stopAt := startAt.Add(duration)

	for i := 0; i < threads; i++ {
		go func() {
			attacker(stopAt)
			wg.Done()
		}()
	}
	wg.Wait()

	return AttackResults{
		Duration:        time.Now().Sub(startAt),
		Threads:         threads,
		QueriesPerfomed: queries,
	}
}

func main() {

	ctx := context.Background()

	url := "postgres://admin_my_messenger:admin@localhost:5432/my_messenger"

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		log.Fatal(err)
	}

	cfg.MaxConns = 8
	cfg.MinConns = 4

	dbpool, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer dbpool.Close()

	duration := time.Duration(10 * time.Second)
	threads := 1000
	fmt.Println("start attack")
	res := attack(ctx, duration, threads, dbpool)

	fmt.Println("duration: ", res.Duration)
	fmt.Println("threads: ", res.Threads)
	fmt.Println("queries: ", res.QueriesPerfomed)

	qps := res.QueriesPerfomed / uint64(res.Duration.Seconds())
	fmt.Println("QPS: ", qps)

}
