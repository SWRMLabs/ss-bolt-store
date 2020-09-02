module github.com/StreamSpace/ss-bolt-store

go 1.14

require (
	github.com/StreamSpace/ss-store v0.0.0-20200901070416-1a3426ec649b
	github.com/boltdb/bolt v1.3.1
	github.com/google/uuid v1.1.2
	github.com/ipfs/go-log/v2 v2.1.1
)

replace github.com/StreamSpace/ss-store => ../ss-store
