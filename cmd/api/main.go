package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/lumenliquid/backend/internal/config"
	"github.com/lumenliquid/backend/internal/db"
	"github.com/lumenliquid/backend/internal/log"
	"github.com/lumenliquid/backend/internal/pubsub"
	"github.com/lumenliquid/backend/internal/wsgateway"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	logger := log.Init(cfg.LogLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Connect to Redis
	rdb, err := pubsub.OpenRedis(ctx, cfg.RedisURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("redis open")
	}
	defer rdb.Close()

	// Connect to DB
	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("db open")
	}
	defer pool.Close()

	// Create trade cache and seed from DB
	cache := wsgateway.NewTradeCache(pool, rdb)
	if err := cache.LoadFromDB(ctx); err != nil {
		logger.Fatal().Err(err).Msg("load cache")
	}
	go cache.Start(ctx) // keep cache in sync via persistent events:global subscription

	// Create WebSocket hub
	hub := wsgateway.NewHub(cache, logger)
	go hub.Run(ctx)

	// Setup HTTP router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// CORS: comma-separated origins from CORS_ALLOWED_ORIGINS. Default "*" = all.
	origins := strings.Split(cfg.CORSAllowedOrigins, ",")
	for i := range origins {
		origins[i] = strings.TrimSpace(origins[i])
	}
	allowAll := len(origins) == 1 && origins[0] == "*"
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins: origins,
		AllowedMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Accept", "Authorization", "Content-Type"},
		// Credentials cannot be combined with wildcard "*" per CORS spec —
		// browsers reject Access-Control-Allow-Origin:* with credentials.
		AllowCredentials: !allowAll,
		MaxAge:           300,
	}))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// REST: Get trader's open trades
	r.Get("/v1/trades/{trader}", handleGetTrades(pool))

	// REST: Get trader's trading history
	r.Get("/api/v1/trading-history/{trader}", handleGetTradingHistory(pool))

	// WebSocket: Real-time updates for one trader
	r.Get("/ws/v1/trades/{trader}", handleTradeWebSocket(hub, logger))

	// WebSocket: Real-time updates for ALL trades
	r.Get("/ws/v1/trades", handleAllTradesWebSocket(hub, logger))

	// WebSocket: Real-time price feed from Binance
	r.Get("/ws/v1/prices", handlePricesWebSocket(hub, logger))

	// Start Binance price feed → broadcasts to "prices" channel
	if cfg.BinanceWSURL != "" && cfg.PairSymbolMap != "" {
		priceFeed := wsgateway.NewPriceFeed(hub, cfg.BinanceWSURL, cfg.PairSymbolMap, logger)
		go priceFeed.Run(ctx)
	}

	// Start server
	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info().Str("addr", cfg.HTTPAddr).Msg("API server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("server error")
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func handleGetTrades(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		trader := chi.URLParam(r, "trader")

		rows, err := pool.Query(r.Context(), `
			SELECT trader, pair_index, trade_index, is_long, leverage,
				   collateral, open_price, tp_price, sl_price, liq_price, opened_at
			FROM trades WHERE trader = $1
			ORDER BY pair_index, trade_index`,
			trader,
		)
		if err != nil {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var trades []map[string]any
		for rows.Next() {
			var t map[string]any = make(map[string]any)
			var isLong bool
			var lev int
			var pairIdx, tradeIdx int
			var collateral, openPrice, tpPrice, slPrice, liqPrice string
			var openedAt time.Time
			var traderAddr string

			if err := rows.Scan(&traderAddr, &pairIdx, &tradeIdx, &isLong, &lev,
				&collateral, &openPrice, &tpPrice, &slPrice, &liqPrice, &openedAt); err != nil {
				continue
			}

			t["trader"] = traderAddr
			t["pair_index"] = pairIdx
			t["trade_index"] = tradeIdx
			t["is_long"] = isLong
			t["leverage"] = lev
			t["collateral"] = collateral
			t["open_price"] = openPrice
			t["tp_price"] = tpPrice
			t["sl_price"] = slPrice
			t["liq_price"] = liqPrice
			t["opened_at"] = openedAt
			trades = append(trades, t)
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"trader":"%s","trades":`, trader)
		if len(trades) == 0 {
			w.Write([]byte("[]"))
		} else {
			// Simple JSON encoding
			data, _ := jsonMarshalTrades(trades)
			w.Write(data)
		}
		w.Write([]byte("}"))
	}
}

func handleGetTradingHistory(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		trader := chi.URLParam(r, "trader")

		// Parse pagination params: limit (default 20, max 100) and cursor.
		limit := 20
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 {
				limit = n
			}
		}
		if limit > 100 {
			limit = 100
		}

		// Build query: when a cursor is present, fetch rows strictly older
		// than it. Fetch limit+1 rows to detect whether more pages exist.
		var rows pgx.Rows
		var err error
		baseSelect := `
			SELECT pair_index, trade_index, is_long, leverage, collateral,
			       open_price, close_price, tp_price, sl_price,
			       realized_pnl, open_fee, close_fee, close_reason,
			       opened_at, opened_tx, closed_at, closed_tx
			FROM trade_history WHERE trader = $1`

		if cursor := r.URL.Query().Get("cursor"); cursor != "" {
			cursorTime, perr := time.Parse(time.RFC3339, cursor)
			if perr != nil {
				http.Error(w, "invalid cursor (want RFC3339 timestamp)", http.StatusBadRequest)
				return
			}
			rows, err = pool.Query(r.Context(),
				baseSelect+` AND closed_at < $2 ORDER BY closed_at DESC LIMIT $3`,
				trader, cursorTime, limit+1)
		} else {
			rows, err = pool.Query(r.Context(),
				baseSelect+` ORDER BY closed_at DESC LIMIT $2`,
				trader, limit+1)
		}
		if err != nil {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		history := []map[string]any{}
		var lastClosedAt time.Time
		for rows.Next() {
			var pairIdx, tradeIdx, leverage int
			var isLong bool
			var collateral, openPrice, tpPrice, slPrice string
			var closePrice, realizedPnl, openFee, closeFee *string
			var closeReason, openedTx, closedTx string
			var openedAt, closedAt time.Time

			if err := rows.Scan(&pairIdx, &tradeIdx, &isLong, &leverage, &collateral,
				&openPrice, &closePrice, &tpPrice, &slPrice,
				&realizedPnl, &openFee, &closeFee, &closeReason,
				&openedAt, &openedTx, &closedAt, &closedTx); err != nil {
				continue
			}
			lastClosedAt = closedAt
			history = append(history, map[string]any{
				"pair_index":   pairIdx,
				"trade_index":  tradeIdx,
				"is_long":      isLong,
				"leverage":     leverage,
				"collateral":   collateral,
				"open_price":   openPrice,
				"close_price":  closePrice,
				"tp_price":     tpPrice,
				"sl_price":     slPrice,
				"realized_pnl": realizedPnl,
				"open_fee":     openFee,
				"close_fee":    closeFee,
				"close_reason": closeReason,
				"opened_at":    openedAt.Format(time.RFC3339),
				"opened_tx":    openedTx,
				"closed_at":    closedAt.Format(time.RFC3339),
				"closed_tx":    closedTx,
			})
		}

		// Detect more pages: we fetched limit+1; if we got more than limit,
		// trim the extra and expose a next_cursor.
		hasMore := len(history) > limit
		var nextCursor any
		if hasMore {
			history = history[:limit]
			lastClosedAt = mustParseRFC3339(history[len(history)-1]["closed_at"].(string))
			nextCursor = lastClosedAt.Format(time.RFC3339)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"trader":      trader,
			"history":     history,
			"next_cursor": nextCursor,
			"has_more":    hasMore,
		})
	}
}

func mustParseRFC3339(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func handleTradeWebSocket(hub *wsgateway.Hub, logger zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		trader := chi.URLParam(r, "trader")

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: []string{"*"},
		})
		if err != nil {
			logger.Warn().Err(err).Msg("websocket accept error")
			return
		}

		remoteAddr := r.RemoteAddr
		client := wsgateway.NewClient(hub, conn, remoteAddr, logger)

		// Register and auto-subscribe to trader channel
		hub.Register(client)
		hub.Subscribe(client, "trader:"+trader)

		// Start pumps
		go client.WritePump(r.Context())
		client.ReadPump(r.Context())
	}
}

func handleAllTradesWebSocket(hub *wsgateway.Hub, logger zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: []string{"*"},
		})
		if err != nil {
			logger.Warn().Err(err).Msg("websocket accept error")
			return
		}

		remoteAddr := r.RemoteAddr
		client := wsgateway.NewClient(hub, conn, remoteAddr, logger)

		// Register and auto-subscribe to the global all-trades channel
		hub.Register(client)
		hub.Subscribe(client, "trades:all")

		// Start pumps
		go client.WritePump(r.Context())
		client.ReadPump(r.Context())
	}
}

func handlePricesWebSocket(hub *wsgateway.Hub, logger zerolog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: []string{"*"},
		})
		if err != nil {
			logger.Warn().Err(err).Msg("websocket accept error")
			return
		}

		remoteAddr := r.RemoteAddr
		client := wsgateway.NewClient(hub, conn, remoteAddr, logger)

		hub.Register(client)
		hub.Subscribe(client, "prices")

		go client.WritePump(r.Context())
		client.ReadPump(r.Context())
	}
}

func jsonMarshalTrades(trades []map[string]any) ([]byte, error) {
	// Simple manual JSON encoding to avoid import cycle
	var result []byte = []byte("[")
	for i, t := range trades {
		if i > 0 {
			result = append(result, ',')
		}
		result = append(result, fmt.Sprintf(
			`{"trader":"%s","pair_index":%d,"trade_index":%d,"is_long":%v,"leverage":%d,"collateral":"%s","open_price":"%s","tp_price":"%s","sl_price":"%s","liq_price":"%s","opened_at":"%s"}`,
			t["trader"], t["pair_index"], t["trade_index"], t["is_long"], t["leverage"],
			t["collateral"], t["open_price"], t["tp_price"], t["sl_price"], t["liq_price"], t["opened_at"].(time.Time).Format(time.RFC3339),
		)...)
	}
	result = append(result, ']')
	return result, nil
}
