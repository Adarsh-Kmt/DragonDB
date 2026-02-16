package pagecodec

import "encoding/binary"

// add currBPlusTreeId
type MetaData struct {
	CurrBPlusTreeId       uint64
	RootPages             map[uint64]uint64
	MaxAllocatedPageId    uint64
	DeallocatedPageIdList []uint64
	FirstLeafNodePages    map[uint64]uint64
}

type MetaDataCodec struct {
}

func DefaultMetaDataCodec() MetaDataCodec {
	return MetaDataCodec{}
}

// encodeMetaDataPage encodes the list of deallocated page IDs and max allocated page ID into a byte slice
// so it can be written to disk. This ensures persistence of the free list across restarts.
func (codec MetaDataCodec) EncodeMetaDataPage(metadata *MetaData) []byte {

	data := make([]byte, 4096)

	pointer := 0

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], metadata.CurrBPlusTreeId)
	pointer += 8

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(len(metadata.RootPages)))
	pointer += 8

	for BPlusTreeId, rootPage := range metadata.RootPages {
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], BPlusTreeId)
		pointer += 8
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], rootPage)
		pointer += 8
	}

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], metadata.MaxAllocatedPageId)
	pointer += 8

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(len(metadata.DeallocatedPageIdList)))
	pointer += 8

	for _, pageId := range metadata.DeallocatedPageIdList {
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], pageId)
		pointer += 8
	}

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(len(metadata.FirstLeafNodePages)))
	pointer += 8
	for BPlusTreeId, firstLeafNodePage := range metadata.FirstLeafNodePages {
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], BPlusTreeId)
		pointer += 8
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], firstLeafNodePage)
		pointer += 8
	}

	return data
}

// decodeMetaDataPage decodes the byte slice from disk into the in-memory
// list of deallocated page IDs. This restores the free list after a database restart.
func (codec MetaDataCodec) DecodeMetaDataPage(data []byte) *MetaData {

	pointer := 0

	currBPlusTreeId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	BPlusTreeRootPagesLength := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	BPlusTreeRootPages := make(map[uint64]uint64, 0)

	for range int(BPlusTreeRootPagesLength) {
		BPlusTreeId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8
		rootPage := binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8

		BPlusTreeRootPages[BPlusTreeId] = rootPage
	}

	maxAllocatedPageId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	deallocatedPageListSize := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	deallocatedPageIdList := make([]uint64, deallocatedPageListSize)

	for i := range int(deallocatedPageListSize) {
		deallocatedPageIdList[i] = binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8
	}

	FirstLeafNodePages := make(map[uint64]uint64, 0)

	FirstLeafNodePagesLength := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	for range int(FirstLeafNodePagesLength) {
		BPlusTreeId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8
		rootPage := binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8

		FirstLeafNodePages[BPlusTreeId] = rootPage
	}
	return &MetaData{
		CurrBPlusTreeId:       currBPlusTreeId,
		RootPages:             BPlusTreeRootPages,
		MaxAllocatedPageId:    maxAllocatedPageId,
		DeallocatedPageIdList: deallocatedPageIdList,
		FirstLeafNodePages:    FirstLeafNodePages,
	}
}
