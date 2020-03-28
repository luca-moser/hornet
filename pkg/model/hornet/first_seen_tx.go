package hornet

import (
	"encoding/binary"

	"github.com/iotaledger/iota.go/trinary"

	"github.com/iotaledger/hive.go/objectstorage"

	"github.com/gohornet/hornet/pkg/model/milestone"
)

type FirstSeenTx struct {
	objectstorage.StorableObjectFlags
	FirstSeenLatestMilestoneIndex milestone.Index
	TxHash                        []byte
}

func (t *FirstSeenTx) GetFirstSeenLatestMilestoneIndex() milestone.Index {
	return t.FirstSeenLatestMilestoneIndex
}

func (t *FirstSeenTx) GetTransactionHash() trinary.Hash {
	return trinary.MustBytesToTrytes(t.TxHash, 81)
}

// ObjectStorage interface

func (t *FirstSeenTx) Update(_ objectstorage.StorableObject) {
	panic("FirstSeenTx should never be updated")
}

func (t *FirstSeenTx) ObjectStorageKey() []byte {
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, uint32(t.FirstSeenLatestMilestoneIndex))
	return append(key, t.TxHash...)
}

func (t *FirstSeenTx) ObjectStorageValue() (data []byte) {
	return nil
}

func (t *FirstSeenTx) UnmarshalObjectStorageValue(_ []byte) (err error, consumedBytes int) {
	return nil, 0
}