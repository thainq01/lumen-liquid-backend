package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	SorobanRPCURL        string        `mapstructure:"soroban_rpc_url"`
	NetworkPassphrase    string        `mapstructure:"network_passphrase"`
	PMContractID         string        `mapstructure:"pm_contract_id"`
	VaultContractID      string        `mapstructure:"vault_contract_id"`
	RegistryContractID   string        `mapstructure:"registry_contract_id"`
	OracleContractID     string        `mapstructure:"oracle_contract_id"`
	DatabaseURL          string        `mapstructure:"database_url"`
	RedisURL             string        `mapstructure:"redis_url"`
	IndexerPollInterval  time.Duration `mapstructure:"indexer_poll_interval"`
	IndexerStartLedger   uint32        `mapstructure:"indexer_start_ledger"`
	LogLevel             string        `mapstructure:"log_level"`
	HTTPAddr             string        `mapstructure:"http_addr"`
	StellarSourceAccount string        `mapstructure:"stellar_source_account"`

	// Keeper
	KeeperSecret       string        `mapstructure:"keeper_secret"`
	KeeperPollInterval time.Duration `mapstructure:"keeper_poll_interval"`
	KeeperMaxRetries   int           `mapstructure:"keeper_max_retries"`
	BinanceWSURL       string        `mapstructure:"binance_ws_url"`
	PairSymbolMap      string        `mapstructure:"pair_symbol_map"`
}

func Load() (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetDefault("soroban_rpc_url", "https://soroban-testnet.stellar.org")
	v.SetDefault("network_passphrase", "Test SDF Network ; September 2015")
	v.SetDefault("database_url", "postgres://lumen:lumen@localhost:5432/lumenliquid?sslmode=disable")
	v.SetDefault("redis_url", "redis://localhost:6379/0")
	v.SetDefault("indexer_poll_interval", 2*time.Second)
	v.SetDefault("indexer_start_ledger", 0)
	v.SetDefault("log_level", "info")
	v.SetDefault("http_addr", ":8080")

	v.SetDefault("keeper_poll_interval", 3*time.Second)
	v.SetDefault("keeper_max_retries", 20)
	v.SetDefault("binance_ws_url", "wss://fstream.binance.com/market/ws")

	// Explicitly bind every env var. viper's AutomaticEnv does NOT surface keys
	// to Unmarshal unless they're known via a default, a config-file entry, or
	// an explicit BindEnv. Without this, keys that have no default (keeper_secret,
	// pm_contract_id, ...) are silently dropped when .env is absent — e.g. inside
	// a container where env comes from the process environment, not a .env file.
	for _, key := range []string{
		"soroban_rpc_url", "network_passphrase",
		"pm_contract_id", "vault_contract_id", "registry_contract_id", "oracle_contract_id",
		"database_url", "redis_url",
		"indexer_poll_interval", "indexer_start_ledger",
		"log_level", "http_addr", "stellar_source_account",
		"keeper_secret", "keeper_poll_interval", "keeper_max_retries",
		"binance_ws_url", "pair_symbol_map",
	} {
		_ = v.BindEnv(key)
	}

	v.SetConfigFile(".env")
	v.SetConfigType("env")
	_ = v.ReadInConfig()

	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return nil, err
	}
	return &c, nil
}
