package page_codec

import "encoding/binary"

type MetaData struct {
	RootNodePageId        uint64
	DeallocatedPageIdList []uint64
	MaxAllocatedPageId    uint64
}

type MetaDataCodec struct {
	rootNodePageIdOffset        int
	maxAllocatedPageIdOffset    int
	deallocatedPageIdListOffset int
}

func DefaultMetaDataCodec() MetaDataCodec {
	return MetaDataCodec{
		rootNodePageIdOffset:        0,
		maxAllocatedPageIdOffset:    8,
		deallocatedPageIdListOffset: 16,
	}
}

// encodeMetaDataPage encodes the list of deallocated page IDs and max allocated page ID into a byte slice
// so it can be written to disk. This ensures persistence of the free list across restarts.
func (codec MetaDataCodec) EncodeMetaDataPage(metadata *MetaData) []byte {

	data := make([]byte, 4096)

	pointer := 0

	binary.LittleEndian.PutUint64(data[pointer:pointer+8], metadata.RootNodePageId)
	pointer += codec.rootNodePageIdOffset + 8
	binary.LittleEndian.PutUint64(data[pointer:pointer+8], metadata.MaxAllocatedPageId)
	pointer += codec.maxAllocatedPageIdOffset + 8
	binary.LittleEndian.PutUint64(data[pointer:pointer+8], uint64(len(metadata.DeallocatedPageIdList)))
	pointer += codec.deallocatedPageIdListOffset + 8

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

	rootNodePageId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += codec.rootNodePageIdOffset + 8

	maxAllocatedPageId := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += codec.maxAllocatedPageIdOffset + 8

	deallocatedPageListSize := binary.LittleEndian.Uint64(data[pointer : pointer+8])
	pointer += codec.deallocatedPageIdListOffset + 8

	deallocatedPageIdList := make([]uint64, deallocatedPageListSize)

	for i := range int(deallocatedPageListSize) {
		deallocatedPageIdList[i] = binary.LittleEndian.Uint64(data[pointer : pointer+8])
		pointer += 8
	}

	return &MetaData{
		RootNodePageId:        rootNodePageId,
		MaxAllocatedPageId:    maxAllocatedPageId,
		DeallocatedPageIdList: deallocatedPageIdList,
	}
}
