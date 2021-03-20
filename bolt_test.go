package boltHandler

import (
	"os"
	"testing"

	testsuite "github.com/SWRMLabs/ss-store/testsuite"
	logger "github.com/ipfs/go-log/v2"
)

func TestStoreSuite(t *testing.T) {
	const testStorePath = "/tmp/testStore"
	if _, e := os.Stat(testStorePath); e == nil {
		_ = os.RemoveAll(testStorePath)
	}

	if e := os.Mkdir(testStorePath, 0775); e != nil {
		panic("Failed to initialize test directory")
	}
	logger.SetLogLevel("*", "Debug")
	bltHndlr, err := NewBoltStore(&BoltConfig{
		Root:   testStorePath,
		DbName: "ssBolt",
		Bucket: "ss-curator",
	})
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	testsuite.RunTestsuite(t, bltHndlr, testsuite.Advanced)
}
