package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sort"
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
	"github.com/lumenliquid/backend/internal/keeper"
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

	// REST: Get pair and group configuration
	r.Get("/v1/pairs", handleGetPairs(pool))
	r.Get("/v1/pairs/{pair_index}", handleGetPair(pool))
	r.Get("/v1/pair-groups", handleGetPairGroups(pool))
	r.Get("/v1/pair-groups/{group_index}", handleGetPairGroup(pool))
	r.Get("/api/v1/pairs", handleGetPairs(pool))
	r.Get("/api/v1/pairs/{pair_index}", handleGetPair(pool))
	r.Get("/api/v1/pair-groups", handleGetPairGroups(pool))
	r.Get("/api/v1/pair-groups/{group_index}", handleGetPairGroup(pool))

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

type pairGroupResponse struct {
	GroupIndex        int       `json:"group_index"`
	Name              string    `json:"name"`
	MaxCollateralUsdc string    `json:"max_collateral_usdc"`
	OpenFeeP          string    `json:"open_fee_p"`
	CloseFeeP         string    `json:"close_fee_p"`
	SyncedAt          time.Time `json:"synced_at"`
}

type pairResponse struct {
	PairIndex          int                `json:"pair_index"`
	Symbol             string             `json:"symbol"`
	ReflectorAssetType string             `json:"reflector_asset_type"`
	ReflectorAsset     string             `json:"reflector_asset"`
	GroupIndex         int                `json:"group_index"`
	SpreadP            string             `json:"spread_p"`
	MinLeverage        int                `json:"min_leverage"`
	MaxLeverage        int                `json:"max_leverage"`
	MinLevPosUsdc      string             `json:"min_lev_pos_usdc"`
	MaxOiUsdc          string             `json:"max_oi_usdc"`
	MaxNegPnlP         string             `json:"max_neg_pnl_p"`
	LiqThresholdP      int                `json:"liq_threshold_p"`
	MaxGainP           int                `json:"max_gain_p"`
	Disabled           bool               `json:"disabled"`
	OnePercentDepth    string             `json:"one_percent_depth"`
	SyncedAt           time.Time          `json:"synced_at"`
	Group              *pairGroupResponse `json:"group,omitempty"`
}

func handleGetPairs(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		groups, err := loadPairGroups(r.Context(), pool)
		if err != nil {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT pair_index, symbol, reflector_asset_type, reflector_asset,
			       group_index, spread_p, min_leverage, max_leverage,
			       min_lev_pos_usdc, max_oi_usdc, max_neg_pnl_p,
			       liq_threshold_p, max_gain_p, disabled, one_percent_depth, synced_at
			FROM pairs
			ORDER BY pair_index`)
		if err != nil {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		pairs := []pairResponse{}
		for rows.Next() {
			p, err := scanPair(rows)
			if err != nil {
				http.Error(w, "query failed", http.StatusInternalServerError)
				return
			}
			if group, ok := groups[p.GroupIndex]; ok {
				p.Group = &group
			}
			pairs = append(pairs, p)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		writeJSON(w, map[string]any{"pairs": pairs})
	}
}

func handleGetPair(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pairIndex, err := strconv.Atoi(chi.URLParam(r, "pair_index"))
		if err != nil || pairIndex < 0 {
			http.Error(w, "invalid pair_index", http.StatusBadRequest)
			return
		}

		var p pairResponse
		err = pool.QueryRow(r.Context(), `
			SELECT pair_index, symbol, reflector_asset_type, reflector_asset,
			       group_index, spread_p, min_leverage, max_leverage,
			       min_lev_pos_usdc, max_oi_usdc, max_neg_pnl_p,
			       liq_threshold_p, max_gain_p, disabled, one_percent_depth, synced_at
			FROM pairs
			WHERE pair_index = $1`,
			pairIndex,
		).Scan(
			&p.PairIndex, &p.Symbol, &p.ReflectorAssetType, &p.ReflectorAsset,
			&p.GroupIndex, &p.SpreadP, &p.MinLeverage, &p.MaxLeverage,
			&p.MinLevPosUsdc, &p.MaxOiUsdc, &p.MaxNegPnlP,
			&p.LiqThresholdP, &p.MaxGainP, &p.Disabled, &p.OnePercentDepth, &p.SyncedAt,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				http.Error(w, "pair not found", http.StatusNotFound)
				return
			}
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		group, err := loadPairGroup(r.Context(), pool, p.GroupIndex)
		if err == nil {
			p.Group = &group
		} else if err != pgx.ErrNoRows {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		writeJSON(w, map[string]any{"pair": p})
	}
}

func handleGetPairGroups(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		groupsByIndex, err := loadPairGroups(r.Context(), pool)
		if err != nil {
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		groups := []pairGroupResponse{}
		indexes := make([]int, 0, len(groupsByIndex))
		for idx := range groupsByIndex {
			indexes = append(indexes, idx)
		}
		sort.Ints(indexes)
		for _, idx := range indexes {
			groups = append(groups, groupsByIndex[idx])
		}

		writeJSON(w, map[string]any{"groups": groups})
	}
}

func handleGetPairGroup(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		groupIndex, err := strconv.Atoi(chi.URLParam(r, "group_index"))
		if err != nil || groupIndex < 0 {
			http.Error(w, "invalid group_index", http.StatusBadRequest)
			return
		}

		group, err := loadPairGroup(r.Context(), pool, groupIndex)
		if err != nil {
			if err == pgx.ErrNoRows {
				http.Error(w, "group not found", http.StatusNotFound)
				return
			}
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		writeJSON(w, map[string]any{"group": group})
	}
}

func handleGetTrades(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		trader := chi.URLParam(r, "trader")

		rows, err := pool.Query(r.Context(), `
			SELECT t.trader, t.pair_index, t.trade_index, t.is_long, t.leverage,
				   t.collateral, t.open_price, t.acc_rollover_open, t.acc_funding_open,
				   t.tp_price, t.sl_price, t.liq_price, COALESCE(p.liq_threshold_p, t.liq_threshold_p),
				   a.acc_rollover, a.acc_funding_long, a.acc_funding_short,
				   t.opened_at
			FROM trades t
			LEFT JOIN pairs p ON p.pair_index = t.pair_index
			LEFT JOIN pair_fee_accumulators a ON a.pair_index = t.pair_index
			WHERE t.trader = $1
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
			var lev uint32
			var pairIdx, tradeIdx int
			var collateral, openPrice, accRolloverOpen, accFundingOpen string
			var tpPrice, slPrice, liqPrice string
			var liqThresholdP uint32
			var accRolloverNow, accFundingLongNow, accFundingShortNow *string
			var openedAt time.Time
			var traderAddr string

			if err := rows.Scan(&traderAddr, &pairIdx, &tradeIdx, &isLong, &lev,
				&collateral, &openPrice, &accRolloverOpen, &accFundingOpen,
				&tpPrice, &slPrice, &liqPrice, &liqThresholdP,
				&accRolloverNow, &accFundingLongNow, &accFundingShortNow,
				&openedAt); err != nil {
				continue
			}

			t["trader"] = traderAddr
			t["pair_index"] = pairIdx
			t["trade_index"] = tradeIdx
			t["is_long"] = isLong
			t["leverage"] = lev
			t["collateral"] = collateral
			t["open_price"] = openPrice
			t["acc_rollover_open"] = accRolloverOpen
			t["acc_funding_open"] = accFundingOpen
			t["tp_price"] = tpPrice
			t["sl_price"] = slPrice
			t["liq_price"] = liqPrice
			t["current_liq_price"] = liqPrice
			t["rollover_fee"] = "0"
			t["funding_fee"] = "0"
			if accRolloverNow != nil && accFundingLongNow != nil && accFundingShortNow != nil {
				accFundingNow := *accFundingShortNow
				if isLong {
					accFundingNow = *accFundingLongNow
				}
				rolloverFee, fundingFee, currentLiq := calculateTradeTimeFeesAndLiq(
					openPrice, collateral, accRolloverOpen, accFundingOpen,
					*accRolloverNow, accFundingNow, isLong, lev, liqThresholdP,
				)
				if rolloverFee != nil {
					t["rollover_fee"] = rolloverFee.String()
				}
				if fundingFee != nil {
					t["funding_fee"] = fundingFee.String()
				}
				if currentLiq != nil {
					t["current_liq_price"] = currentLiq.String()
				}
			}
			t["opened_at"] = openedAt
			trades = append(trades, t)
		}

		writeJSON(w, map[string]any{"trader": trader, "trades": trades})
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
			       open_price, close_price, acc_rollover_open, acc_funding_open,
			       tp_price, sl_price,
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
			var collateral, openPrice, accRolloverOpen, accFundingOpen string
			var tpPrice, slPrice string
			var closePrice, realizedPnl, openFee, closeFee *string
			var closeReason, openedTx, closedTx string
			var openedAt, closedAt time.Time

			if err := rows.Scan(&pairIdx, &tradeIdx, &isLong, &leverage, &collateral,
				&openPrice, &closePrice, &accRolloverOpen, &accFundingOpen,
				&tpPrice, &slPrice,
				&realizedPnl, &openFee, &closeFee, &closeReason,
				&openedAt, &openedTx, &closedAt, &closedTx); err != nil {
				continue
			}
			lastClosedAt = closedAt
			history = append(history, map[string]any{
				"pair_index":        pairIdx,
				"trade_index":       tradeIdx,
				"is_long":           isLong,
				"leverage":          leverage,
				"collateral":        collateral,
				"open_price":        openPrice,
				"close_price":       closePrice,
				"acc_rollover_open": accRolloverOpen,
				"acc_funding_open":  accFundingOpen,
				"tp_price":          tpPrice,
				"sl_price":          slPrice,
				"realized_pnl":      realizedPnl,
				"open_fee":          openFee,
				"close_fee":         closeFee,
				"close_reason":      closeReason,
				"opened_at":         openedAt.Format(time.RFC3339),
				"opened_tx":         openedTx,
				"closed_at":         closedAt.Format(time.RFC3339),
				"closed_tx":         closedTx,
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

type pairScanner interface {
	Scan(dest ...any) error
}

func scanPair(row pairScanner) (pairResponse, error) {
	var p pairResponse
	err := row.Scan(
		&p.PairIndex, &p.Symbol, &p.ReflectorAssetType, &p.ReflectorAsset,
		&p.GroupIndex, &p.SpreadP, &p.MinLeverage, &p.MaxLeverage,
		&p.MinLevPosUsdc, &p.MaxOiUsdc, &p.MaxNegPnlP,
		&p.LiqThresholdP, &p.MaxGainP, &p.Disabled, &p.OnePercentDepth, &p.SyncedAt,
	)
	return p, err
}

func loadPairGroups(ctx context.Context, pool *pgxpool.Pool) (map[int]pairGroupResponse, error) {
	rows, err := pool.Query(ctx, `
		SELECT group_index, name, max_collateral_usdc, open_fee_p, close_fee_p, synced_at
		FROM pair_groups
		ORDER BY group_index`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	groups := make(map[int]pairGroupResponse)
	for rows.Next() {
		var group pairGroupResponse
		if err := rows.Scan(
			&group.GroupIndex, &group.Name, &group.MaxCollateralUsdc,
			&group.OpenFeeP, &group.CloseFeeP, &group.SyncedAt,
		); err != nil {
			return nil, err
		}
		groups[group.GroupIndex] = group
	}
	return groups, rows.Err()
}

func loadPairGroup(ctx context.Context, pool *pgxpool.Pool, groupIndex int) (pairGroupResponse, error) {
	var group pairGroupResponse
	err := pool.QueryRow(ctx, `
		SELECT group_index, name, max_collateral_usdc, open_fee_p, close_fee_p, synced_at
		FROM pair_groups
		WHERE group_index = $1`,
		groupIndex,
	).Scan(
		&group.GroupIndex, &group.Name, &group.MaxCollateralUsdc,
		&group.OpenFeeP, &group.CloseFeeP, &group.SyncedAt,
	)
	return group, err
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payload)
}

func mustParseRFC3339(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func calculateTradeTimeFeesAndLiq(
	openPrice, collateral, accRolloverOpen, accFundingOpen string,
	accRolloverNow, accFundingNow string,
	isLong bool,
	leverage uint32,
	liqThresholdP uint32,
) (*big.Int, *big.Int, *big.Int) {
	open := parseBig(openPrice)
	collat := parseBig(collateral)
	rollOpen := parseBig(accRolloverOpen)
	fundingOpen := parseBig(accFundingOpen)
	rollNow := parseBig(accRolloverNow)
	fundingNow := parseBig(accFundingNow)
	if open == nil || collat == nil || rollOpen == nil || fundingOpen == nil || rollNow == nil || fundingNow == nil {
		return nil, nil, nil
	}
	rolloverFee, err := keeper.RolloverFeeForTrade(rollOpen, rollNow, collat)
	if err != nil {
		return nil, nil, nil
	}
	fundingFee, err := keeper.FundingFeeForTrade(fundingOpen, fundingNow, collat, leverage)
	if err != nil {
		return rolloverFee, nil, nil
	}
	if liqThresholdP == 0 {
		liqThresholdP = 90
	}
	liq, err := keeper.LiquidationPrice(open, isLong, collat, leverage, rolloverFee, fundingFee, liqThresholdP)
	if err != nil {
		return rolloverFee, fundingFee, nil
	}
	return rolloverFee, fundingFee, liq
}

func parseBig(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil
	}
	return v
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
			`{"trader":"%s","pair_index":%d,"trade_index":%d,"is_long":%v,"leverage":%d,"collateral":"%s","open_price":"%s","acc_rollover_open":"%s","acc_funding_open":"%s","tp_price":"%s","sl_price":"%s","liq_price":"%s","opened_at":"%s"}`,
			t["trader"], t["pair_index"], t["trade_index"], t["is_long"], t["leverage"],
			t["collateral"], t["open_price"], t["acc_rollover_open"], t["acc_funding_open"],
			t["tp_price"], t["sl_price"], t["liq_price"], t["opened_at"].(time.Time).Format(time.RFC3339),
		)...)
	}
	result = append(result, ']')
	return result, nil
}
