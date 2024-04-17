package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

func WriteProof(db ethdb.AncientWriter, id uint64, proof []byte) {
	db.ModifyAncients(func(op ethdb.AncientWriteOp) error {
		op.AppendRaw(proposeProofTable, id-1, proof)
		return nil
	})
}

// ReadLatestProof is used to read the latest proof
func ReadLatestProof(f *ResettableFreezer) []byte {
	proofTable := f.freezer.tables[proposeProofTable]
	if proofTable == nil {
		return nil
	}
	blob, err := f.Ancient(proposeProofTable, proofTable.items.Load())
	if err != nil {
		return nil
	}
	return blob
}

func ReadProof(f *ResettableFreezer, proofID uint64) []byte {
	proofTable := f.freezer.tables[proposeProofTable]
	if proofTable == nil {
		return nil
	}
	blob, err := f.Ancient(proposeProofTable, proofID)
	if err != nil {
		return nil
	}
	return blob
}

func TruncateProofHead(f *ResettableFreezer, proofID uint64) {
	f.freezer.TruncateHead(proofID)
}

// WriteKeeperMeta xx
func WriteKeeperMeta(db ethdb.KeyValueWriter, blockID uint64, meta []byte) {
	key := proofKeeperMetaKey(blockID)
	if err := db.Put(key, meta); err != nil {
		log.Crit("Failed to store keeper meta", "err", err)
	}
}

// DeleteKeeperMeta xx
func DeleteKeeperMeta(db ethdb.KeyValueWriter, blockID uint64) {
	if err := db.Delete(proofKeeperMetaKey(blockID)); err != nil {
		log.Crit("Failed to delete keeper meta", "err", err)
	}
}
