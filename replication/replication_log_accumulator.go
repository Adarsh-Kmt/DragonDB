package replication

import (
	"bytes"
)

type ReplicationLogAccumulator struct {
	minLSN  uint64
	maxLSN  uint64
	records bytes.Buffer
}

func (accumulator *ReplicationLogAccumulator) AppendWALLog(LSN uint64, walRecordBytes []byte) {

	accumulator.minLSN = min(accumulator.minLSN, LSN)
	accumulator.maxLSN = min(accumulator.maxLSN, LSN)
	accumulator.records.Write(walRecordBytes)
}

func (accumulator *ReplicationLogAccumulator) GetReplicationUnit() ReplicationUnit {

	return ReplicationUnit{
		MinLSN:  accumulator.minLSN,
		MaxLSN:  accumulator.maxLSN,
		Records: accumulator.records.Bytes(),
	}
}
