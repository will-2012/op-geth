package pathdb

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	l2ToL1MessagePasser   = "0x4200000000000000000000000000000000000016"
	maxKeeperMetaNumber   = 1000
	backoffIntervalSecond = 1
	maxRetryNumber        = 10
	gcIntervalSecond      = 1
)

var (
	l2ToL1MessagePasserAddr = common.HexToAddress(l2ToL1MessagePasser)
)

type proofData struct {
	proofID       uint64               `json:"proofID"`
	blockID       uint64               `json:"blockID"`
	stateRoot     common.Hash          `json:"stateRoot"`
	withDrawProof common.AccountResult `json:"withDrawProof"`
}

type keeperMeta struct { // is ued to keep proofid is continus and convert block to proofid for query
	// enable           bool
	proposedInterval uint64
	contractAddress  common.Address
	blockID          uint64
	proofID          uint64
}

func (meta *keeperMeta) isMetaChanged(oldMeta *keeperMeta) bool {
	// return oldMeta.enable != meta.enable || oldMeta.proposedInterval != meta.proposedInterval || oldMeta.contractAddress != meta.contractAddress
	return oldMeta.proposedInterval != meta.proposedInterval || oldMeta.contractAddress != meta.contractAddress
}

type withDrawProofKeeperOptions struct {
	enable           bool
	proposedInterval uint64
	contractAddress  common.Address
}

// It is used in buffer-list mode
type withDrawProofKeeper struct {
	opts                 *withDrawProofKeeperOptions
	keeperMetaDB         ethdb.Database // ensure the proofs which are held in proofDataDb is continuous.
	proofDataDB          *rawdb.Freezer
	selfRpc              *rpc.Client
	newKeeperMeta        *keeperMeta
	needUpdateKeeperMeta bool
	nextProofID          uint64

	// for event loop
	UpdateKeeperMetaCh     chan struct{}
	WaitUpdateKeeperMetaCh chan struct{}
	UpdateProofDataCh      chan struct{}
	WaitUpdateProofDataCh  chan struct{}
	QueryProofCh           chan struct{}
	WaitQueryProofCh       chan struct{}
}

func newWithDrawProofKeeper(keeperMetaDB ethdb.Database, opts *withDrawProofKeeperOptions) *withDrawProofKeeper {
	keeper := &withDrawProofKeeper{
		keeperMetaDB: keeperMetaDB,
	}
	keeper.newKeeperMeta = &keeperMeta{
		// enable:           opts.enable,
		proposedInterval: opts.proposedInterval,
		contractAddress:  opts.contractAddress,
	}

	LatestKeeperMeta := keeper.getLatestKeeperMeta()

	keeper.needUpdateKeeperMeta = LatestKeeperMeta.isMetaChanged(keeper.newKeeperMeta)

	keeper.UpdateKeeperMetaCh = make(chan struct{})
	keeper.WaitUpdateKeeperMetaCh = make(chan struct{})
	keeper.UpdateProofDataCh = make(chan struct{})
	keeper.WaitUpdateProofDataCh = make(chan struct{})
	keeper.QueryProofCh = make(chan struct{})
	keeper.WaitQueryProofCh = make(chan struct{})

	go keeper.eventLoop()

	return keeper
}

func (keeper *withDrawProofKeeper) getProofIDByBlockID(blockID uint64) uint64 {
	return 0
}

func (keeper *withDrawProofKeeper) getLatestKeeperMeta() *keeperMeta {
	// scan
	// truncate keeper meta
	return nil
}

// todo: event loop
func (keeper *withDrawProofKeeper) eventLoop() {
	gcTicker := time.NewTicker(time.Second * gcIntervalSecond)
	defer gcTicker.Stop()
	for {
		select {
		case <-gcTicker.C:
			// todo
			// truncate meta and data
		case <-keeper.UpdateKeeperMetaCh:
			// todo: get proof id
			// maybe truncate meta and data

			// add meta
			keeper.WaitUpdateKeeperMetaCh <- struct{}{}
		case <-keeper.UpdateProofDataCh:
			// maybe truncate meta and data
			client := ethclient.NewClient(keeper.selfRpc)
			client.GetProof(nil, common.MaxAddress, nil, "")
			// todo
			// add data
			keeper.WaitUpdateProofDataCh <- struct{}{}
		case <-keeper.QueryProofCh:
			// query meta to get proofid
			// query data by proofid
			// some check
			keeper.WaitQueryProofCh <- struct{}{}
		}
	}
}

// for store, is a sync interface
func (keeper *withDrawProofKeeper) keepWithDrawProofIfNeeded(blockID uint64) {
	// 1.
	//if keeper.needUpdateKeeperMeta && blockID%keeper.opts.proposedInterval == 0 {
	//	keeper.newKeeperMeta.blockID = blockID
	//	keeper.UpdateKeeperMetaCh <- struct{}{}
	//	<-keeper.WaitUpdateKeeperMetaCh
	//	keeper.needUpdateKeeperMeta = false
	//}

	// 2.truncate for gc
	// todo

	if !keeper.opts.enable {
		return
	}

	if blockID%keeper.opts.proposedInterval != 0 {
		return
	}

	if keeper.needUpdateKeeperMeta {
		keeper.newKeeperMeta.blockID = blockID
		keeper.UpdateKeeperMetaCh <- struct{}{}
		<-keeper.WaitUpdateKeeperMetaCh
		keeper.needUpdateKeeperMeta = false
	}

}

// for query
func (keeper *withDrawProofKeeper) isWithDrawProofQuery() bool {
	if !keeper.opts.enable {
		return false
	}
	return true
}

// for query
func (keeper *withDrawProofKeeper) queryWithDrawProof() {
	// getProofIDByBlockID
	keeper.QueryProofCh <- struct{}{}
	<-keeper.WaitQueryProofCh
}

// for reorg
func (keeper *withDrawProofKeeper) revert() {
	// action in event loop
	// nil
}
