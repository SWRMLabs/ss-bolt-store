package boltHandler

import (
	"os"
	"testing"

	testsuite "github.com/StreamSpace/store-test-suite"
	logger "github.com/ipfs/go-log/v2"
)

var bltCnfg BoltConfig

const testStorePath = "/tmp/testStore"

func TestMain(m *testing.M) {
	if _, e := os.Stat(testStorePath); e == nil {
		_ = os.RemoveAll(testStorePath)
	}

	if e := os.Mkdir(testStorePath, 0775); e != nil {
		panic("Failed to initialize test directory")
	}
	logger.SetLogLevel("*", "Debug")
	bltCnfg = BoltConfig{
		Root:   testStorePath,
		DbName: "ssBolt",
		Bucket: "ss-curator",
	}
	code := m.Run()
	os.Exit(code)
}

func TestBoltHandler(t *testing.T) {
	handler := bltCnfg.Handler()

	if handler != "boltdb" {
		t.Fatalf("Handler returned %s", handler)
	}
}

func TestNewBoltStoreInvalUpdatedAtConfig(t *testing.T) {
	// Test sending incomplete Bolt config
	_bltCnfg2 := BoltConfig{
		Bucket: "ss-curator",
	}
	_, err := NewBoltStore(&_bltCnfg2)
	if err == nil {
		t.Fatalf("Bolt store init should fail")
	}
}

func TestSuite(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()
	testsuite.TestNilStorage(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestCreation(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestRead(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestDelete(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestUpdate(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestSortNaturalLIST(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestSortCreatedAscLIST(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestSortCreatedDscLIST(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestSortUpdatedAscLIST(t, testsuite.Tester{Store: bltHndlr})
	testsuite.TestSortUpdatedDscLIST(t, testsuite.Tester{Store: bltHndlr})
}

func TestNewBoltStore(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()
}
