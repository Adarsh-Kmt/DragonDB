package page_codec

import "encoding/binary"

// add currBTreeId
type MetaData struct {
	CurrBTreeId           uint64
	BTreeRootPages        map[uint64]uint64
	DeallocatedPageIdList []uint64
	MaxAllocatedPageId    uint64
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

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], metadata.CurrBTreeId)
	pointer += 8

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(len(metadata.BTreeRootPages)))
	pointer += 8

	for BTreeId, rootPage := range metadata.BTreeRootPages {
		binary.LittleEndian.PutUint64(data[pointer:pointer+8], BTreeId)
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

	return data
}

// decodeMetaDataPage decodes the byte slice from disk into the in-memory
// list of deallocated page IDs. This restores the free list after a database restart.
func (codec MetaDataCodec) DecodeMetaDataPage(data []byte) *MetaData {

	pointer := 0

	currBTreeId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	BTreeRootPagesLength := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += 8

	BTreeRootPages := make(map[uint64]uint64, 0)

	for range int(BTreeRootPagesLength) {
		BTreeId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8
		rootPage := binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8

		BTreeRootPages[BTreeId] = rootPage
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

	return &MetaData{
		CurrBTreeId:           currBTreeId,
		BTreeRootPages:        BTreeRootPages,
		MaxAllocatedPageId:    maxAllocatedPageId,
		DeallocatedPageIdList: deallocatedPageIdList,
	}
}
