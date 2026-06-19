package keeper

import (
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

type TxBuilder struct {
	kp                *keypair.Full
	networkPassphrase string
	pmContractID      string

	mu  sync.Mutex
	seq int64
}

func NewTxBuilder(secret, networkPassphrase, pmContractID string, initialSeq int64) (*TxBuilder, error) {
	kp, err := keypair.ParseFull(secret)
	if err != nil {
		return nil, fmt.Errorf("parse keeper secret: %w", err)
	}
	return &TxBuilder{
		kp:                kp,
		networkPassphrase: networkPassphrase,
		pmContractID:      pmContractID,
		seq:               initialSeq,
	}, nil
}

func (b *TxBuilder) Address() string {
	return b.kp.Address()
}

func (b *TxBuilder) ResetSequence(seq int64) {
	b.mu.Lock()
	b.seq = seq
	b.mu.Unlock()
}

func (b *TxBuilder) BuildLiquidateTx(trader string, pairIndex, tradeIndex int) (string, error) {
	args := []xdr.ScVal{
		mustAccountAddressScVal(trader),
		mustU32ScVal(uint32(pairIndex)),
		mustU32ScVal(uint32(tradeIndex)),
	}
	return b.buildInvokeTx("liquidate_trade", args)
}

func (b *TxBuilder) BuildExecuteTpSlTx(trader string, pairIndex, tradeIndex int) (string, error) {
	args := []xdr.ScVal{
		mustAccountAddressScVal(b.kp.Address()),
		mustAccountAddressScVal(trader),
		mustU32ScVal(uint32(pairIndex)),
		mustU32ScVal(uint32(tradeIndex)),
	}
	return b.buildInvokeTx("execute_tp_sl", args)
}

func (b *TxBuilder) buildInvokeTx(funcName string, args []xdr.ScVal) (string, error) {
	contractAddr, err := contractScAddress(b.pmContractID)
	if err != nil {
		return "", fmt.Errorf("contract address: %w", err)
	}

	fnName := xdr.ScSymbol(funcName)
	hostFn := xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		InvokeContract: &xdr.InvokeContractArgs{
			ContractAddress: contractAddr,
			FunctionName:    fnName,
			Args:            args,
		},
	}

	op := &txnbuild.InvokeHostFunction{
		HostFunction:  hostFn,
		Auth:          []xdr.SorobanAuthorizationEntry{},
		SourceAccount: b.kp.Address(),
	}

	// Use current seq without incrementing — simulation doesn't validate seq.
	// The real seq is set in BuildFinalTx after simulation passes.
	b.mu.Lock()
	seq := b.seq + 1
	b.mu.Unlock()

	account := txnbuild.SimpleAccount{
		AccountID: b.kp.Address(),
		Sequence:  seq,
	}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              100000,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
	})
	if err != nil {
		return "", fmt.Errorf("build tx: %w", err)
	}

	tx, err = tx.Sign(b.networkPassphrase, b.kp)
	if err != nil {
		return "", fmt.Errorf("sign tx: %w", err)
	}

	xdrBytes, err := tx.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshal tx: %w", err)
	}
	return base64.StdEncoding.EncodeToString(xdrBytes), nil
}

// IncrementSeq should be called ONLY after a successful send (status=PENDING).
func (b *TxBuilder) IncrementSeq() {
	b.mu.Lock()
	b.seq++
	b.mu.Unlock()
}

func contractScAddress(contractID string) (xdr.ScAddress, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return xdr.ScAddress{}, err
	}
	var contractHash xdr.Hash
	copy(contractHash[:], raw)
	contractIdType := xdr.ContractId(contractHash)
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractIdType,
	}, nil
}

func accountScAddress(accountID string) (xdr.ScAddress, error) {
	var aid xdr.AccountId
	if err := aid.SetAddress(accountID); err != nil {
		return xdr.ScAddress{}, err
	}
	return xdr.ScAddress{
		Type:      xdr.ScAddressTypeScAddressTypeAccount,
		AccountId: &aid,
	}, nil
}

func mustAccountAddressScVal(accountID string) xdr.ScVal {
	addr, _ := accountScAddress(accountID)
	return xdr.ScVal{
		Type:    xdr.ScValTypeScvAddress,
		Address: &addr,
	}
}

func mustU32ScVal(v uint32) xdr.ScVal {
	val := xdr.Uint32(v)
	return xdr.ScVal{
		Type: xdr.ScValTypeScvU32,
		U32:  &val,
	}
}
