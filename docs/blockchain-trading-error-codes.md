# Blockchain Trading Error Codes

This document is for frontend handling of failed Soroban simulations or contract
submissions for trading actions.

Primary user trading calls go through the `PositionManager` contract:

- `open_market_trade`
- `close_market_trade`
- `place_limit_order`
- `execute_limit_order`
- `cancel_limit_order`
- `update_limit_order`
- `update_tp_sl`
- `execute_tp_sl`
- `liquidate_trade`

Source enum:

```text
/Users/thainq/Desktop/stellar/lumenliquid-contracts/contracts/position-manager/src/errors.rs
```

## PositionManager Errors

| Code | Name | UI Message |
|------|------|------------|
| 1 | AlreadyInitialized | Contract is already initialized |
| 2 | NotInitialized | Trading contract is not initialized |
| 3 | NotAdmin | You are not allowed to perform this action |
| 4 | Paused | Trading is paused |
| 5 | PairDisabled | This market is currently disabled |
| 6 | LeverageIncorrect | Selected leverage is not allowed for this market |
| 7 | AboveMaxPos | Position exceeds max size |
| 8 | BelowMinPos | Position is below the minimum size |
| 9 | MaxTradesReached | Too many open trades on this pair |
| 10 | InvalidPriceProof | Price proof is invalid |
| 11 | PriceImpactTooHigh | Price impact is too high |
| 12 | WrongTp | Take-profit price is invalid |
| 13 | WrongSl | Stop-loss price is invalid |
| 14 | OiCapExceeded | Market open interest limit reached |
| 15 | GroupCollateralCapExceeded | Collateral limit reached for this market group |
| 16 | InsufficientSubscriptionReserve | Not enough subscription reserve |
| 17 | TradeNotFound | Trade was not found |
| 18 | UnauthorizedCallback | Unauthorized callback |
| 19 | SubNotFound | Subscription was not found |
| 20 | SubOrphaned | Subscription is no longer linked to a trade |
| 21 | PriceMismatch | Price condition is not met yet |
| 22 | OracleDeviationTooHigh | Oracle price moved too far from expected price |
| 23 | NotLiquidatable | Position is not liquidatable |
| 24 | InsufficientAccruedFees | Not enough accrued fees |
| 25 | MathFault | Calculation failed. Please try again |
| 26 | InvalidParam | Invalid trade parameters |
| 27 | OracleUnavailable | Market price is temporarily unavailable |
| 28 | LimitNotFound | Limit order was not found |
| 29 | SubscriptionNotConfigured | Subscription is not configured |

## Errors Returned By Current Trading Flows

Some enum values are defined for the contract but are not currently returned by
the present trading implementation. The frontend can still keep messages for all
codes above as a fallback-safe mapping.

### Open Market Trade

Contract call:

```text
open_market_trade(trader, pair_index, is_long, collateral, leverage, tp_price, sl_price)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 4 | Paused | Trading is paused |
| 5 | PairDisabled | Pair exists but is disabled |
| 6 | LeverageIncorrect | Leverage outside pair min/max |
| 9 | MaxTradesReached | No free trade slot for trader and pair |
| 25 | MathFault | Fee or settlement math failed |
| 26 | InvalidParam | Collateral is `<= 0`, leverage is `0`, or fee consumes all collateral |
| 27 | OracleUnavailable | Reflector price is missing |

### Close Market Trade

Contract call:

```text
close_market_trade(trader, pair_index, trade_index)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 17 | TradeNotFound | Trade index does not exist for trader and pair |
| 25 | MathFault | PnL, fee, or settlement math failed |
| 27 | OracleUnavailable | Reflector price is missing |

### Place Limit Order

Contract call:

```text
place_limit_order(trader, pair_index, is_long, collateral, leverage, limit_price, tp_price, sl_price)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 4 | Paused | Trading is paused |
| 5 | PairDisabled | Pair exists but is disabled |
| 6 | LeverageIncorrect | Leverage outside pair min/max |
| 9 | MaxTradesReached | No free limit-order slot for trader and pair |
| 26 | InvalidParam | Collateral is `<= 0`, leverage is `0`, or limit price is `<= 0` |

### Execute Limit Order

Usually called by keeper, but useful for frontend decoding.

Contract call:

```text
execute_limit_order(trader, pair_index, limit_index)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 4 | Paused | Trading is paused |
| 9 | MaxTradesReached | No free trade slot for the resulting position |
| 21 | PriceMismatch | Current price has not reached the limit price |
| 25 | MathFault | Open fee math failed |
| 27 | OracleUnavailable | Reflector price is missing |
| 28 | LimitNotFound | Limit order index does not exist |

### Cancel Limit Order

Contract call:

```text
cancel_limit_order(trader, pair_index, limit_index)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 4 | Paused | Trading is paused |
| 28 | LimitNotFound | Limit order index does not exist |

### Update Limit Order

Contract call:

```text
update_limit_order(trader, pair_index, limit_index, limit_price, tp_price, sl_price)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 4 | Paused | Trading is paused |
| 26 | InvalidParam | Limit price is `<= 0` |
| 28 | LimitNotFound | Limit order index does not exist |

### Set TP/SL

Contract call:

```text
update_tp_sl(trader, pair_index, trade_index, tp_price, sl_price)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 4 | Paused | Trading is paused |
| 17 | TradeNotFound | Trade index does not exist for trader and pair |

Note: `WrongTp` and `WrongSl` are defined in the enum, but the current
`update_tp_sl` implementation does not validate TP/SL direction and does not
return those errors yet.

### Execute TP/SL

Usually called by keeper, but useful for frontend decoding.

Contract call:

```text
execute_tp_sl(keeper, trader, pair_index, trade_index)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 4 | Paused | Trading is paused |
| 17 | TradeNotFound | Trade index does not exist for trader and pair |
| 21 | PriceMismatch | Current price has not reached TP or SL |
| 25 | MathFault | Close fee or settlement math failed |
| 27 | OracleUnavailable | Reflector price is missing |

### Liquidate Trade

Usually called by keeper, but useful for frontend decoding.

Contract call:

```text
liquidate_trade(trader, pair_index, trade_index)
```

Current direct `PositionManager` errors:

| Code | Name | Common Cause |
|------|------|--------------|
| 17 | TradeNotFound | Trade index does not exist for trader and pair |
| 23 | NotLiquidatable | Position has not reached liquidation price |
| 25 | MathFault | Liquidation price math failed |
| 27 | OracleUnavailable | Reflector price is missing |

## Cross-Contract Errors That May Surface

Trading calls read pair/group config from `PairRegistry` and settle funds through
`Vault`. Depending on the client SDK and simulation result, nested contract
errors may appear under the nested contract's enum instead of
`PositionManagerError`.

### PairRegistry Errors

Source enum:

```text
/Users/thainq/Desktop/stellar/lumenliquid-contracts/contracts/pair-registry/src/errors.rs
```

Most relevant to frontend trading calls:

| Code | Name | UI Message |
|------|------|------------|
| 2 | NotInitialized | Pair registry is not initialized |
| 5 | PairNotFound | Market was not found |
| 7 | GroupNotFound | Market group was not found |
| 9 | InvalidParam | Invalid pair registry parameter |
| 10 | MathFault | Pair registry calculation failed |
| 11 | StaleLedger | Ledger state is stale. Please retry |

Full PairRegistry enum:

| Code | Name |
|------|------|
| 1 | AlreadyInitialized |
| 2 | NotInitialized |
| 3 | NotAdmin |
| 4 | NotPositionManager |
| 5 | PairNotFound |
| 6 | PairAlreadyExists |
| 7 | GroupNotFound |
| 8 | GroupAlreadyExists |
| 9 | InvalidParam |
| 10 | MathFault |
| 11 | StaleLedger |

### Vault Errors

Source enum:

```text
/Users/thainq/Desktop/stellar/lumenliquid-contracts/contracts/vault/src/errors.rs
```

Most relevant to frontend trading settlement:

| Code | Name | UI Message |
|------|------|------------|
| 2 | NotInitialized | Vault is not initialized |
| 4 | NotPositionManager | Vault rejected the trading contract |
| 7 | InsufficientAssets | Vault has insufficient liquidity |
| 9 | Paused | Vault is paused |
| 10 | InvalidParam | Invalid vault parameter |
| 11 | MathFault | Vault calculation failed |

Full Vault enum:

| Code | Name |
|------|------|
| 1 | AlreadyInitialized |
| 2 | NotInitialized |
| 3 | NotAdmin |
| 4 | NotPositionManager |
| 5 | WithdrawLocked |
| 6 | InsufficientShares |
| 7 | InsufficientAssets |
| 8 | InsufficientAllowance |
| 9 | Paused |
| 10 | InvalidParam |
| 11 | MathFault |

## Recommended Frontend Fallback

Use the enum name when available. If the client only exposes a numeric contract
error code, map it by contract address:

- PositionManager contract error code `4` means `Paused`.
- PairRegistry contract error code `4` means `NotPositionManager`.
- Vault contract error code `4` means `NotPositionManager`.

If the contract address or enum namespace is unknown, show:

```text
Transaction failed. Please check your inputs and try again.
```
