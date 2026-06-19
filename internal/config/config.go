package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	SorobanRPCURL      string        `mapstructure:"soroban_rpc_url"`
	NetworkPassphrase  string        `mapstructure:"network_passphrase"`
	PMContractID       string        `mapstructure:"pm_contract_id"`
	VaultContractID    string        `mapstructure:"vault_contract_id"`
	RegistryContractID string        `mapstructure:"registry_contract_id"`
	OracleContractID   string        `mapstructure:"oracle_contract_id"`
	DatabaseURL        string        `mapstructure:"database_url"`
	RedisURL           string        `mapstructure:"redis_url"`
	IndexerPollInterval time.Duration `mapstructure:"indexer_poll_interval"`
	IndexerStartLedger uint32        `mapstructure:"indexer_start_ledger"`
	LogLevel           string        `mapstructure:"log_level"`
	HTTPAddr           string        `mapstructure:"http_addr"`
	StellarSourceAccount string      `mapstructure:"stellar_source_account"`
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

	v.SetConfigFile(".env")
	v.SetConfigType("env")
	_ = v.ReadInConfig()

	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return nil, err
	}
	return &c, nil
}
