package ingest

// intKeyOrder reproduces the iteration order of scouter.util.IntKeyMap (the
// ObjectMap.objectTable that the AgentManager daemon walks): capacity 101,
// load factor 0.75f, head insertion into the bucket of the mixed hash, rehash
// to 2n+1 walking the old buckets from the top, enumeration from the highest
// bucket down. Only keys are kept; it is bounded by the live object count.
type intKeyOrder struct {
	table     [][]int32
	count     int
	threshold int
}

var intKeyLoadFactor float32 = 0.75 // a variable: Java multiplies in float at run time

func newIntKeyOrder() *intKeyOrder {
	return &intKeyOrder{table: make([][]int32, 101), threshold: int(float32(101) * intKeyLoadFactor)}
}

// javaIntKeyHash is IntKeyMap.hash.
func javaIntKeyHash(k int32) int {
	h := uint32(k)
	h ^= (h >> 20) ^ (h >> 12)
	h = h ^ (h >> 7) ^ (h >> 4)
	return int(h & 0x7fffffff)
}

func (m *intKeyOrder) index(k int32) int { return javaIntKeyHash(k) % len(m.table) }

func (m *intKeyOrder) contains(k int32) bool {
	for _, x := range m.table[m.index(k)] {
		if x == k {
			return true
		}
	}
	return false
}

// put is IntKeyMap.put: an existing key keeps its position.
func (m *intKeyOrder) put(k int32) {
	if m.contains(k) {
		return
	}
	if m.count >= m.threshold {
		nc := len(m.table)*2 + 1
		nt := make([][]int32, nc)
		for i := len(m.table) - 1; i >= 0; i-- {
			for _, x := range m.table[i] {
				at := javaIntKeyHash(x) % nc
				nt[at] = append([]int32{x}, nt[at]...)
			}
		}
		m.table, m.threshold = nt, int(float32(nc)*intKeyLoadFactor)
	}
	at := m.index(k)
	m.table[at] = append([]int32{k}, m.table[at]...)
	m.count++
}

func (m *intKeyOrder) remove(k int32) {
	at := m.index(k)
	for i, x := range m.table[at] {
		if x == k {
			m.table[at] = append(m.table[at][:i:i], m.table[at][i+1:]...)
			m.count--
			return
		}
	}
}

// keys returns the IntKeyMap enumeration order.
func (m *intKeyOrder) keys() []int32 {
	out := make([]int32, 0, m.count)
	for i := len(m.table) - 1; i >= 0; i-- {
		out = append(out, m.table[i]...)
	}
	return out
}
