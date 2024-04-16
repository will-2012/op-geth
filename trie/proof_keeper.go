package trie

import "github.com/ethereum/go-ethereum/common"

type ProofKeeper interface {
	IsProposeProofQuery(address common.Address, storageKeys []string, blockID uint64) bool
	QueryProposeProof(blockID uint64) (*common.AccountResult, error)
}
