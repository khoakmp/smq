package bfpool

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"
)

type bsPool struct {
	pools               [24]sync.Pool
	getFromPoolcounters [24]uint32
	putToPoolCounters   [24]uint32
	allocateCounters    [24]uint32
	getCount            uint32
	putCount            uint32
}

var bspool bsPool

func GetByteSlice(sz uint32) []byte {
	if sz == 0 {
		return nil
	}
	return bspool.Get(sz)
}

func PutByteSlice(buf []byte) {
	if len(buf) == 0 {
		return
	}
	bspool.Put(buf)
}
func PrintBsPoolStats() {
	bspool.PrintCounters()
}

func (b *bsPool) PrintCounters() {
	for i := range 24 {
		a, g, p := atomic.LoadUint32(&b.allocateCounters[i]), atomic.LoadUint32(&b.getFromPoolcounters[i]),
			atomic.LoadUint32(&b.putToPoolCounters[i])
		if a == 0 && g == 0 && p == 0 {
			continue
		}

		fmt.Printf("BufferCap[%d]: alloc=%d, get=%d,put=%d\n", 1<<i, a,
			g, p)
	}
}

func (b *bsPool) Get(sz uint32) []byte {
	atomic.AddUint32(&b.getCount, 1)
	if sz <= 0 {
		return nil
	}
	idx := bits.Len32(sz)
	if (1 << (idx - 1)) == sz {
		idx--
	}
	if idx >= 24 {

		return make([]byte, sz)
	}
	//fmt.Println("Need byte slice size:", sz, "idx:", idx)

	ptr := b.pools[idx].Get()

	if ptr == nil {
		//atomic.AddUint32(&p.allocatedCount, 1)
		//atomic.AddUint32(&b.allocateCounters[idx], 1)
		//fmt.Printf("create new byte slice with sz: %d, cap: %d\n", sz, (1 << idx))
		return make([]byte, sz, (1 << idx))
	}
	pt := ptr.(unsafe.Pointer)
	//atomic.AddUint32(&b.getFromPoolcounters[idx], 1)
	return unsafe.Slice((*byte)(pt), (1 << idx))[:sz]
}

func (b *bsPool) Put(buf []byte) {
	//atomic.AddUint32(&b.putCount, 1)
	c := cap(buf)
	if c > (1 << 23) {
		return
	}
	idx := bits.Len32(uint32(c))
	if (1 << (idx - 1)) == c {
		idx--
	} else {
		return
	}

	//atomic.AddUint32(&b.putToPoolCounters[idx], 1)
	//fmt.Println("Put buf to pool idx:", idx)
	ptr := unsafe.Pointer(&buf[0])
	b.pools[idx].Put(ptr)
}
