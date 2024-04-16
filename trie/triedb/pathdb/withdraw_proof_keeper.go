package pathdb

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
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
	proofID       uint64                `json:"proofID"`
	blockID       uint64                `json:"blockID"`
	withDrawProof *common.AccountResult `json:"withDrawProof"`
}

type keeperMeta struct { // is ued to keep proofid is continus and convert block to proofid for query
	proposedInterval uint64
	contractAddress  common.Address
	blockID          uint64
	proofID          uint64
}

type withDrawProofKeeperOptions struct {
	enable           bool
	proposedInterval uint64
	contractAddress  common.Address
}

// It is used in buffer-list mode
type withDrawProofKeeper struct {
	opts                     *withDrawProofKeeperOptions
	keeperMetaDB             ethdb.Database // ensure the proofs which are held in proofDataDb is continuous.
	proofDataDB              *rawdb.ResettableFreezer
	selfRpc                  *rpc.Client
	needUpdateKeeperMetaOnce bool // only update once on startup.

	// for event loop
	UpdateProofDataCh     chan uint64
	WaitUpdateProofDataCh chan struct{}
	QueryProofCh          chan struct{}
	WaitQueryProofCh      chan struct{}
}

func newWithDrawProofKeeper(keeperMetaDB ethdb.Database, opts *withDrawProofKeeperOptions) (*withDrawProofKeeper, error) {
	var (
		err        error
		ancientDir string
		keeper     *withDrawProofKeeper
	)

	if ancientDir, err = keeperMetaDB.AncientDatadir(); err != nil {
		return nil, err
	}
	keeper = &withDrawProofKeeper{
		keeperMetaDB:             keeperMetaDB,
		needUpdateKeeperMetaOnce: true,
	}
	if keeper.proofDataDB, err = rawdb.NewProofFreezer(ancientDir, false); err != nil {
		return nil, err
	}

	keeper.UpdateProofDataCh = make(chan uint64)
	keeper.WaitUpdateProofDataCh = make(chan struct{})
	keeper.QueryProofCh = make(chan struct{})
	keeper.WaitQueryProofCh = make(chan struct{})

	go keeper.eventLoop()

	return keeper, nil
}

func (keeper *withDrawProofKeeper) getProofIDByBlockID(blockID uint64) uint64 {
	return 0
}

func (keeper *withDrawProofKeeper) getLatestKeeperMeta() *keeperMeta {
	// scan
	// truncate keeper meta
	return nil
}

func (keeper *withDrawProofKeeper) getKeeperMetaList() []keeperMeta {
	var (
		metaList []keeperMeta
		err      error
		iter     ethdb.Iterator
	)
	iter = keeper.keeperMetaDB.NewIterator(rawdb.ProofKeeperMetaPrefix, nil)
	defer iter.Release()

	for iter.Next() {
		if len(iter.Key()) == len(rawdb.ProofKeeperMetaPrefix)+32 {
			m := keeperMeta{}
			if err = json.Unmarshal(iter.Value(), &m); err != nil {
				continue
			}
			metaList = append(metaList, m)
		}
	}
	return metaList
}

func (keeper *withDrawProofKeeper) truncateKeeperMetaHeadIfNeeded(blockID uint64) bool {
	var (
		err        error
		iter       ethdb.Iterator
		batch      ethdb.Batch
		isTruncate bool
	)
	iter = keeper.keeperMetaDB.NewIterator(rawdb.ProofKeeperMetaPrefix, nil)
	defer iter.Release()

	batch = keeper.keeperMetaDB.NewBatch()

	for iter.Next() {
		if len(iter.Key()) == len(rawdb.ProofKeeperMetaPrefix)+32 {
			m := keeperMeta{}
			if err = json.Unmarshal(iter.Value(), &m); err != nil {
				continue
			}
			if m.blockID >= blockID {
				isTruncate = true
				rawdb.DeleteKeeperMeta(batch, m.blockID)
			}
		}
	}
	err = batch.Write()
	if err != nil {
		log.Crit("Failed to truncate keeper meta head", "err", err)
	}
	return isTruncate
}

func (keeper *withDrawProofKeeper) truncateKeeperMetaTail(blockID uint64) {
	//return nil
}

func (keeper *withDrawProofKeeper) truncateProofDataHeadIfNeeded(blockID uint64) {
	latestProofData := keeper.queryLatestProofData()
	if latestProofData == nil {
		return
	}
	if blockID > latestProofData.blockID {
		return
	}

	truncateProofID := uint64(0)
	proofID := latestProofData.proofID
	for proofID > 0 {
		proof := keeper.queryProofData(proofID)
		if proof == nil {
			keeper.proofDataDB.Reset()
			return
		}
		if proof.blockID < blockID {
			truncateProofID = proof.proofID
		}
	}
	rawdb.TruncateProofHead(keeper.proofDataDB, truncateProofID)
}

func (keeper *withDrawProofKeeper) truncateProofDataTail(proofID uint64) {
	//return nil
}

func (keeper *withDrawProofKeeper) addKeeperMeta(m *keeperMeta) {
	meta, err := json.Marshal(*m)
	if err != nil {
		log.Crit("Failed to marshal keeper meta", "err", err)
	}
	rawdb.WriteKeeperMeta(keeper.keeperMetaDB, m.blockID, meta)
}

func (keeper *withDrawProofKeeper) addProofData(p *proofData) {
	proof, err := json.Marshal(*p)
	if err != nil {
		log.Crit("Failed to marshal proof data", "err", err)
	}
	rawdb.WriteProof(keeper.proofDataDB, p.proofID, proof)
}

func (keeper *withDrawProofKeeper) queryLatestProofData() *proofData {
	latestProofData := rawdb.ReadLatestProof(keeper.proofDataDB)
	if latestProofData == nil {
		return nil
	}
	var data proofData
	err := json.Unmarshal(latestProofData, &data)
	if err != nil {
		log.Crit("Failed to unmarshal proof data", "err", err)
	}
	return &data
}

func (keeper *withDrawProofKeeper) queryProofData(proofID uint64) *proofData {
	proof := rawdb.ReadProof(keeper.proofDataDB, proofID)
	if proof == nil {
		return nil
	}
	var data proofData
	err := json.Unmarshal(proof, &data)
	if err != nil {
		log.Crit("Failed to unmarshal proof data", "err", err)
	}
	return &data
}

// seekto

// todo: event loop
func (keeper *withDrawProofKeeper) eventLoop() {
	gcTicker := time.NewTicker(time.Second * gcIntervalSecond)
	defer gcTicker.Stop()
	for {
		select {
		// todo: stop
		case <-gcTicker.C:
			// todo
			// truncate meta and data
		case blockID := <-keeper.UpdateProofDataCh:
			proofID := uint64(1)
			metaList := keeper.getKeeperMetaList()
			isTruncateKeeperMeta := false
			//var err error

			// In rare cases such as abnormal restart or reorg, truncate may occur.
			if len(metaList) == 0 {
				keeper.proofDataDB.Reset() // truncate proof data
			} else {
				isTruncateKeeperMeta = keeper.truncateKeeperMetaHeadIfNeeded(blockID)
				metaList = keeper.getKeeperMetaList()
				if len(metaList) == 0 {
					keeper.proofDataDB.Reset() // truncate proof data
				} else {
					keeper.truncateProofDataHeadIfNeeded(blockID)
					latestProofData := keeper.queryLatestProofData()
					if latestProofData != nil {
						proofID = latestProofData.proofID + 1
					}
				}
			}

			// add keeper meta
			if keeper.needUpdateKeeperMetaOnce || isTruncateKeeperMeta {
				keeper.addKeeperMeta(&keeperMeta{
					proposedInterval: keeper.opts.proposedInterval,
					contractAddress:  keeper.opts.contractAddress,
					blockID:          blockID,
					proofID:          proofID,
				})
			}
			keeper.needUpdateKeeperMetaOnce = false

			// add proof data
			client := ethclient.NewClient(keeper.selfRpc)
			rawPoof, _ := client.GetProof(context.Background(), l2ToL1MessagePasserAddr, nil, strconv.FormatUint(blockID, 16))
			withDrawProof := &proofData{
				proofID:       proofID,
				blockID:       blockID,
				withDrawProof: rawPoof,
			}
			keeper.addProofData(withDrawProof)

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
	if !keeper.opts.enable {
		return
	}

	if blockID%keeper.opts.proposedInterval != 0 {
		return
	}

	keeper.UpdateProofDataCh <- blockID
	<-keeper.WaitUpdateProofDataCh
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
