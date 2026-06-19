// Package soroban — scval decoding helpers.
//
// Soroban's RPC returns event topics + values as base64-encoded XDR ScVal.
// stellar/go/xdr exposes the typed structures. This file walks those into
// idiomatic Go: int128 becomes *big.Int, ScSymbol/ScString become string,
// ScAddress becomes the strkey form (G…/C…), ScVec becomes []any, ScMap
// becomes map[string]any (key stringified via Native).
package soroban

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// DecodeScValB64 parses a base64 ScVal payload (as returned by getEvents) into xdr.ScVal.
func DecodeScValB64(b64 string) (xdr.ScVal, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("base64: %w", err)
	}
	var v xdr.ScVal
	if err := v.UnmarshalBinary(raw); err != nil {
		return xdr.ScVal{}, fmt.Errorf("xdr unmarshal: %w", err)
	}
	return v, nil
}

// Native walks an xdr.ScVal into idiomatic Go.
//
// Returned types:
//   - ScvBool         → bool
//   - ScvVoid         → nil
//   - ScvU32/I32      → uint32 / int32
//   - ScvU64/I64      → uint64 / int64
//   - ScvU128/I128    → *big.Int (signed for I128, magnitude for U128)
//   - ScvU256/I256    → *big.Int
//   - ScvBytes        → []byte
//   - ScvString       → string
//   - ScvSymbol       → string
//   - ScvAddress      → string (strkey: G… for accounts, C… for contracts)
//   - ScvVec          → []any
//   - ScvMap          → map[string]any
//   - ScvError, ScvLedgerKey…, ScvContractInstance → opaque, returned as the raw xdr value
func Native(v xdr.ScVal) (any, error) {
	switch v.Type {
	case xdr.ScValTypeScvBool:
		return *v.B, nil
	case xdr.ScValTypeScvVoid:
		return nil, nil
	case xdr.ScValTypeScvU32:
		return uint32(*v.U32), nil
	case xdr.ScValTypeScvI32:
		return int32(*v.I32), nil
	case xdr.ScValTypeScvU64:
		return uint64(*v.U64), nil
	case xdr.ScValTypeScvI64:
		return int64(*v.I64), nil
	case xdr.ScValTypeScvU128:
		return u128ToBig(*v.U128), nil
	case xdr.ScValTypeScvI128:
		return i128ToBig(*v.I128), nil
	case xdr.ScValTypeScvU256:
		return u256ToBig(*v.U256), nil
	case xdr.ScValTypeScvI256:
		return i256ToBig(*v.I256), nil
	case xdr.ScValTypeScvBytes:
		return []byte(*v.Bytes), nil
	case xdr.ScValTypeScvString:
		return string(*v.Str), nil
	case xdr.ScValTypeScvSymbol:
		return string(*v.Sym), nil
	case xdr.ScValTypeScvAddress:
		return addressToStrkey(*v.Address)
	case xdr.ScValTypeScvVec:
		if v.Vec == nil || *v.Vec == nil {
			return []any{}, nil
		}
		out := make([]any, 0, len(**v.Vec))
		for _, e := range **v.Vec {
			n, err := Native(e)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		return out, nil
	case xdr.ScValTypeScvMap:
		if v.Map == nil || *v.Map == nil {
			return map[string]any{}, nil
		}
		out := make(map[string]any, len(**v.Map))
		for _, kv := range **v.Map {
			kn, err := Native(kv.Key)
			if err != nil {
				return nil, err
			}
			vn, err := Native(kv.Val)
			if err != nil {
				return nil, err
			}
			out[fmt.Sprintf("%v", kn)] = vn
		}
		return out, nil
	}
	// Unhandled but harmless variants — surface raw for diagnostics.
	return v, nil
}

// ── int helpers ────────────────────────────────────────────

func u128ToBig(p xdr.UInt128Parts) *big.Int {
	hi := new(big.Int).SetUint64(uint64(p.Hi))
	lo := new(big.Int).SetUint64(uint64(p.Lo))
	z := new(big.Int).Lsh(hi, 64)
	return z.Or(z, lo)
}

func i128ToBig(p xdr.Int128Parts) *big.Int {
	// Hi is signed, Lo unsigned. Reconstruct: (Hi << 64) | Lo, with sign from Hi.
	hi := big.NewInt(int64(p.Hi))
	lo := new(big.Int).SetUint64(uint64(p.Lo))
	z := new(big.Int).Lsh(hi, 64)
	return z.Or(z, lo)
}

func u256ToBig(p xdr.UInt256Parts) *big.Int {
	z := new(big.Int)
	z.SetUint64(uint64(p.HiHi))
	z.Lsh(z, 64).Or(z, new(big.Int).SetUint64(uint64(p.HiLo)))
	z.Lsh(z, 64).Or(z, new(big.Int).SetUint64(uint64(p.LoHi)))
	z.Lsh(z, 64).Or(z, new(big.Int).SetUint64(uint64(p.LoLo)))
	return z
}

func i256ToBig(p xdr.Int256Parts) *big.Int {
	hi := big.NewInt(int64(p.HiHi))
	z := new(big.Int).Lsh(hi, 64)
	z.Or(z, new(big.Int).SetUint64(uint64(p.HiLo)))
	z.Lsh(z, 64).Or(z, new(big.Int).SetUint64(uint64(p.LoHi)))
	z.Lsh(z, 64).Or(z, new(big.Int).SetUint64(uint64(p.LoLo)))
	return z
}

// ── address ────────────────────────────────────────────────

func addressToStrkey(a xdr.ScAddress) (string, error) {
	switch a.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		raw := a.AccountId.Ed25519
		if raw == nil {
			return "", errors.New("nil ed25519 in account address")
		}
			return strkey.Encode(strkey.VersionByteAccountID, raw[:])
	case xdr.ScAddressTypeScAddressTypeContract:
		c := *a.ContractId
		return strkey.Encode(strkey.VersionByteContract, c[:])
	}
	return "", fmt.Errorf("unknown address type %v", a.Type)
}
