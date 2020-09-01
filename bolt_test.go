package boltHandler

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
	logger "github.com/ipfs/go-log/v2"
)

type successStruct struct {
	Namespace string
	Id        string
	CreatedAt int64
	UpdatedAt int64
}

type streamspaceFactory struct {}

func (f streamspaceFactory) Factory() store.SerializedItem {
	return &successStruct{
		Namespace: "StreamSpace",
	}
}

func (t *successStruct) GetNamespace() string { return t.Namespace }

func (t *successStruct) GetId() string { return t.Id }

func (t *successStruct) Marshal() ([]byte, error) { return json.Marshal(t) }

func (t *successStruct) Unmarshal(val []byte) error { return json.Unmarshal(val, t) }

func (t *successStruct) SetCreated(unixTime int64) { t.CreatedAt = unixTime }

func (t *successStruct) SetUpdated(unixTime int64) { t.UpdatedAt = unixTime }

func (t *successStruct) GetCreated() int64 { return t.CreatedAt }

func (t *successStruct) GetUpdated() int64 { return t.UpdatedAt }

var bltCnfg BoltConfig

var bltHndlr *ssBoltHandler

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

func TestNewBoltStore(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()
}

func TestNewBoltCreation(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)

	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}

	err = bltHndlr.Create(&d)

	if err != nil {
		t.Fatalf(err.Error())
	}

	defer bltHndlr.Close()
}

func TestNewBoltRead(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()

	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}
	err = bltHndlr.Read(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestNewBoltUpdate(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}
	err = bltHndlr.Create(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = bltHndlr.Update(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestNewBoltDelete(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()

	d := successStruct{
		Namespace: "StreamSpace",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}
	err = bltHndlr.Create(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = bltHndlr.Update(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = bltHndlr.Delete(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestSortNaturalLIST(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()
	// Create some dummies with StreamSpace namespace
	for i := 0; i < 5; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
			Id:        uuid.New().String(),
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		}
		err = bltHndlr.Create(&d)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	//Create some dummies with Other namespace
	for i := 0; i < 5; i++ {
		d := successStruct{
			Namespace: "Other",
			Id:        uuid.New().String(),
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		}

		err = bltHndlr.Create(&d)

		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	var sort store.Sort = 0
	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	ds, err := bltHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

func TestSortCreatedAscLIST(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()

	var sort store.Sort = 1
	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	ds, err := bltHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}
func TestSortCreatedDscLIST(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()

	var sort store.Sort = 2
	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	ds, err := bltHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}
func TestSortUpdatedAscLIST(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()

	var sort store.Sort = 3
	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	ds, err := bltHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}

func TestSortUpdatedDscLIST(t *testing.T) {
	bltHndlr, err := NewBoltStore(&bltCnfg)
	if err != nil {
		t.Fatalf("Bolt store init failed")
	}
	defer bltHndlr.Close()

	var sort store.Sort = 4
	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	ds, err := bltHndlr.List(&streamspaceFactory{}, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for i := 0; i < len(ds); i++ {
		if ds[i].GetNamespace() != "StreamSpace" {
			t.Fatalf("Namespace of the %vth element in list dosn't match", i)
		}
	}
}
