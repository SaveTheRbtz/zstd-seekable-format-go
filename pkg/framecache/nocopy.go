package framecache

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
