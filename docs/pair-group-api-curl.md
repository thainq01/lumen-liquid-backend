# Pair And Group Config API

Production base URL:

```sh
BASE_URL="https://services.lumenliquid.xyz/api/v1"
```

All numeric amount fields are returned as raw integer strings:

- `*_usdc` uses `USDC_SCALE = 1e7`
- `*_p` uses `P_SCALE = 1e10`

## Get All Pairs

```sh
curl -sS "https://services.lumenliquid.xyz/api/v1/pairs"
```

Example response:

```json
{
  "pairs": [
    {
      "pair_index": 0,
      "symbol": "BTC/USD",
      "reflector_asset_type": "stellar",
      "reflector_asset": "C...",
      "group_index": 0,
      "spread_p": "10000000",
      "min_leverage": 1,
      "max_leverage": 10,
      "min_lev_pos_usdc": "100000000",
      "max_oi_usdc": "100000000000",
      "max_neg_pnl_p": "9000000000",
      "liq_threshold_p": 90,
      "max_gain_p": 900,
      "disabled": false,
      "one_percent_depth": "0",
      "synced_at": "2026-06-19T10:00:00Z",
      "group": {
        "group_index": 0,
        "name": "crypto",
        "max_collateral_usdc": "10000000000",
        "open_fee_p": "10000000",
        "close_fee_p": "10000000",
        "synced_at": "2026-06-19T10:00:00Z"
      }
    }
  ]
}
```

## Get One Pair

```sh
curl -sS "https://services.lumenliquid.xyz/api/v1/pairs/0"
```

Example response:

```json
{
  "pair": {
    "pair_index": 0,
    "symbol": "BTC/USD",
    "reflector_asset_type": "stellar",
    "reflector_asset": "C...",
    "group_index": 0,
    "spread_p": "10000000",
    "min_leverage": 1,
    "max_leverage": 10,
    "min_lev_pos_usdc": "100000000",
    "max_oi_usdc": "100000000000",
    "max_neg_pnl_p": "9000000000",
    "liq_threshold_p": 90,
    "max_gain_p": 900,
    "disabled": false,
    "one_percent_depth": "0",
    "synced_at": "2026-06-19T10:00:00Z",
    "group": {
      "group_index": 0,
      "name": "crypto",
      "max_collateral_usdc": "10000000000",
      "open_fee_p": "10000000",
      "close_fee_p": "10000000",
      "synced_at": "2026-06-19T10:00:00Z"
    }
  }
}
```

Not found response:

```text
HTTP/1.1 404 Not Found
pair not found
```

## Get All Pair Groups

```sh
curl -sS "https://services.lumenliquid.xyz/api/v1/pair-groups"
```

Example response:

```json
{
  "groups": [
    {
      "group_index": 0,
      "name": "crypto",
      "max_collateral_usdc": "10000000000",
      "open_fee_p": "10000000",
      "close_fee_p": "10000000",
      "synced_at": "2026-06-19T10:00:00Z"
    }
  ]
}
```

## Get One Pair Group

```sh
curl -sS "https://services.lumenliquid.xyz/api/v1/pair-groups/0"
```

Example response:

```json
{
  "group": {
    "group_index": 0,
    "name": "crypto",
    "max_collateral_usdc": "10000000000",
    "open_fee_p": "10000000",
    "close_fee_p": "10000000",
    "synced_at": "2026-06-19T10:00:00Z"
  }
}
```

Not found response:

```text
HTTP/1.1 404 Not Found
group not found
```

## Fields

Pair fields:

- `pair_index`: on-chain pair index.
- `symbol`: trading pair symbol.
- `reflector_asset_type`: `stellar` or `other`.
- `reflector_asset`: reflector asset address or symbol.
- `group_index`: linked group index.
- `spread_p`: pair spread, raw `P_SCALE` string.
- `min_leverage`: minimum leverage.
- `max_leverage`: maximum leverage.
- `min_lev_pos_usdc`: minimum leveraged position size, raw `USDC_SCALE` string.
- `max_oi_usdc`: maximum open interest, raw `USDC_SCALE` string.
- `max_neg_pnl_p`: maximum negative PnL percentage, raw `P_SCALE` string.
- `liq_threshold_p`: liquidation threshold percentage.
- `max_gain_p`: maximum gain percentage.
- `disabled`: whether trading is disabled for this pair.
- `one_percent_depth`: one percent depth, raw `USDC_SCALE` string.
- `synced_at`: last backend sync time.
- `group`: linked group object.

Group fields:

- `group_index`: on-chain group index.
- `name`: group name.
- `max_collateral_usdc`: maximum collateral per position/group config, raw `USDC_SCALE` string.
- `open_fee_p`: open fee, raw `P_SCALE` string.
- `close_fee_p`: close fee, raw `P_SCALE` string.
- `synced_at`: last backend sync time.
