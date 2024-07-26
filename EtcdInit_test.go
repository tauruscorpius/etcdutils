package Etc

import (
	"github.com/tauruscorpius/appcommon/Log"
	"github.com/tauruscorpius/etcdutils/etcdclient"
	"os"
	"testing"
	"time"
)

var etcdNodes = os.Getenv("ETCD_NODES")

func init() {
	err := EtcdInit(etcdNodes)
	if err != nil {
		Log.Fatalf("Failed to initialize etcd: %v", err)
	}
}

func TestEtcdInit(t *testing.T) {
}

func TestEtcdCreateSyncLocker(t *testing.T) {
	lockerName := "/locker_test"
	if !EtcdCreateSyncLocker(lockerName) {
		t.Error("create sync locker failed")
	}
	EtcdLock()
	time.Sleep(1 * time.Second)
	EtcdUnlock()

}

func TestEtcdCreateLeaseLocker(t *testing.T) {
	lockerName := "/lease_test"
	if !EtcdCreateLeaseLocker(lockerName, "lockervalue", 2) {
		t.Error("create lease locker failed")
	}
	EtcdLock()
	time.Sleep(5 * time.Second)
	EtcdUnlock()

}

func TestDoWatch(t *testing.T) {
	prefix := "/watch_test"
	WatchRegister(prefix, func(action int, key, value string) bool {
		Log.Criticalf("action : %d key : %s value : %s", action, key, value)
		return true
	})
	DoWatch()
	Log.Criticalf("test watch, writing")
	go func() {
		etcdclient.GetImpl().Put("/watch_test/t1", "value1")
		etcdclient.GetImpl().Put("/watch_test/t1", "value2")
		etcdclient.GetImpl().Del("/watch_test/t1")
	}()
	time.Sleep(2 * time.Second)

}
