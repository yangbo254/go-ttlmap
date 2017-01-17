package ttlmap

// Options for initializing a new Map.
type Options struct {
	InitialCapacity int
	OnWillExpire    func(key string, item *Item)
	OnWillEvict     func(key string, item *Item)
}

type KeyExistMode int

const (
	KeyExistDontCare KeyExistMode = 0
	KeyExistNotYet   KeyExistMode = 1
	KeyExistAlready  KeyExistMode = 2
)

// Options for setting items on a Map.
type SetOptions struct {
	KeyExist KeyExistMode
}

func (opts *SetOptions) keyExist() KeyExistMode {
	if opts == nil {
		return KeyExistDontCare
	}
	return opts.KeyExist
}
