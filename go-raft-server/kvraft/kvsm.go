package kvraft

type KVStateMachine interface {
	Get(key string) (value string, version int, err Err)
	Put(key string, value string, version int) Err
}

type KeyValue struct {
	Value   string
	Version int
}

type MemoryKV struct {
	KV map[string]KeyValue
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{KV: make(map[string]KeyValue)}
}

func (memoryKV *MemoryKV) Get(key string) (string, int, Err) {
	if kv, ok := memoryKV.KV[key]; ok {
		return kv.Value, kv.Version, Ok
	}
	return "", 0, ErrNoKey
}

func (memoryKV *MemoryKV) Put(key string, value string, version int) Err {
	if kv, ok := memoryKV.KV[key]; ok {
		if kv.Version+1 == version {
			memoryKV.KV[key] = KeyValue{value, version}
			return Ok
		}
		return ErrVersion
	}
	if version == 1 {
		memoryKV.KV[key] = KeyValue{value, version}
		return Ok
	}

	return ErrVersion
}
