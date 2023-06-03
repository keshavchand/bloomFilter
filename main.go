package main

import (
	"fmt"
	"hash"

	"github.com/google/uuid"
	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	filter      []bool
	hasher      []hash.Hash32
	hasherCount int32
	sizeCount   int32
}

func NewBloomFilter(size int32, hasherCount int32) *BloomFilter {
	var hasher []hash.Hash32
	for i := int32(0); i < hasherCount; i++ {
		hasher = append(hasher, murmur3.New32WithSeed(uint32(i)))
	}
	return &BloomFilter{
		filter:      make([]bool, size*hasherCount),
		hasher:      hasher,
		hasherCount: hasherCount,
		sizeCount:   size,
	}
}

func (b *BloomFilter) Add(key string) {
	size := b.sizeCount
	for idx, hasher := range b.hasher {

		hasher.Write([]byte(key))
		h := hasher.Sum32()
		hasher.Reset()

		i := (uint32(idx) * uint32(size)) + uint32(h)%uint32(size)
		b.filter[i] = true
	}
}

func (b *BloomFilter) Exists(key string) bool {
	size := b.sizeCount
	for idx, hasher := range b.hasher {

		hasher.Write([]byte(key))
		h := hasher.Sum32()
		hasher.Reset()

		i := (uint32(idx) * uint32(size)) + uint32(h)%uint32(size)
		if !b.filter[i] {
			return false
		}
	}
	return true
}

func (b *BloomFilter) Reset() {
	b.filter = make([]bool, len(b.filter))
}

func randStringGen() string {
	return uuid.New().String()
}

func testBFwithSize(size int32) {
	bloom := NewBloomFilter(size, 5)
	dataset := []string{}
	datasetSize := 1000
	for i := 0; i < datasetSize; i++ {
		dataset = append(dataset, randStringGen())
	}

	for pos := 0; pos < datasetSize/2; pos++ {
		for i := 0; i < pos; i++ {
			bloom.Add(dataset[i])
		}

		falseCounter := 0
		for i := pos; i < len(dataset); i++ {
			if bloom.Exists(dataset[i]) {
				falseCounter++
			}
		}
		fmt.Println(pos, ": ", float64(falseCounter)/float64(len(dataset)-pos))
		bloom.Reset()
	}
}

func main() {
	testBFwithSize(1000)
}
