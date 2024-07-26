package etcdclient

import (
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/tauruscorpius/appcommon/ExitHandler"
	"github.com/tauruscorpius/appcommon/Log"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	defaultEtcdLeaseTTL = 5
	debugEtcdLeaseTTL   = 60 * 60
)

type EtcdLocker struct {
	// Client
	client *clientv3.Client

	// distribute locker - type
	distLockerType string
	lockerName     string

	// distribute locker - sync.Locker
	sess   *concurrency.Session
	locker sync.Locker

	// distribute locker - lease
	lease       clientv3.Lease
	leaseId     clientv3.LeaseID
	leaseValue  string
	leaseCancel func()
}

var etcdLocker EtcdLocker

func GetLocker() *EtcdLocker {
	return &etcdLocker
}

func (t *EtcdLocker) Init(client *clientv3.Client) error {
	t.client = client
	if t.client == nil {
		return errors.New("invalid etcd client")
	}
	return nil
}

func (t *EtcdLocker) CreateSyncLocker(lockerName string) bool {
	t.distLockerType = "SyncLocker"
	t.lockerName = lockerName + ".sync"
	sess, err := concurrency.NewSession(t.client)
	if err != nil {
		Log.Errorf("error : new session error : {}\n", err)
		return false
	}
	t.sess = sess
	locker := concurrency.NewLocker(sess, t.lockerName)
	t.locker = locker
	return true
}

func (t *EtcdLocker) CreateLeaseLocker(lockerName, lockerValue string, ttl int64) bool {
	t.distLockerType = "LeaseLocker"
	t.leaseValue = lockerValue
	t.lockerName = lockerName + ".lease"
	t.lease = clientv3.NewLease(t.client)
	var leaseGrantResp *clientv3.LeaseGrantResponse
	var err error

	if ttl != debugEtcdLeaseTTL {
		ttl = defaultEtcdLeaseTTL
	}
	Log.Criticalf("Etcd Lease TTL : %d\n", ttl)
	if leaseGrantResp, err = t.lease.Grant(context.TODO(), ttl); err != nil {
		Log.Errorf("error : new grant error : %v\n", err)
		return false
	}
	t.leaseId = leaseGrantResp.ID
	ctx, cancelFunc := context.WithCancel(context.TODO())
	t.leaseCancel = cancelFunc
	ExitHandler.GetExitFuncChain().Add(func() bool {
		t.unlockLeaseLocker()
		return true
	})
	if keepRespChan, err := t.lease.KeepAlive(ctx, t.leaseId); err != nil {
		Log.Errorf("error : new lease keepalive error : %v\n", err)
		return false
	} else {
		// lease answer
		go func() {
		L1:
			for {
				select {
				case <-ctx.Done():
					Log.Criticalf("lease Id Do Revoke : %+v\n", t.leaseId)
					t.lease.Revoke(context.TODO(), t.leaseId)
					Log.Criticalf("lease Id Revoked : %+v\n", t.leaseId)
					break L1
				case keepResp := <-keepRespChan:
					if keepResp == nil {
						Log.Errorf("error : lease Id Expired : %+v, send self SIGTERM\n", t.leaseId)
						_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
						break L1
					} else {
						Log.Tracef("lease Id auto lease answer : %+v => %+v\n", t.leaseId, keepResp)
					}
				}
			}
			// make sure that process exit
			time.Sleep(time.Second * 5)
			os.Exit(1)
		}()
	}
	return true
}

func (t *EtcdLocker) Lock() error {
	switch t.distLockerType {
	case "SyncLocker":
		t.lockSyncLocker()
	case "LeaseLocker":
		t.lockLeaseLocker()
	}
	return nil
}

func (t *EtcdLocker) lockSyncLocker() error {
	Log.Criticalf("etcd lock[SyncLocker] start, waiting...\n")
	t.locker.Lock()
	Log.Criticalf("etcd lock[SyncLocker] succeed.\n")
	return nil
}

func (t *EtcdLocker) lockLeaseLocker() error {
	Log.Criticalf("etcd lock[LeaseLocker] start, waiting...\n")
	var errFinal error = nil
	if t.leaseValue == "" {
		t.leaseValue = t.lockerName + ".value"
	}
	tryTimes := (uint64)(0)
	for {
		// locker in etcd just means a key
		kv := clientv3.NewKV(t.client)

		txn := kv.Txn(context.TODO())

		// if not exist, create it, otherwise locker campaign failed
		txn.If(clientv3.Compare(clientv3.CreateRevision(t.lockerName), "=", 0)).
			Then(clientv3.OpPut(t.lockerName, t.leaseValue, clientv3.WithLease(t.leaseId))).
			Else(clientv3.OpGet(t.lockerName))

		// transaction commit
		if txnResp, err := txn.Commit(); err != nil {
			errFinal = err
			break
		} else {
			if txnResp.Succeeded {
				break
			}
			tryTimes++
			if tryTimes%60 == 1 {
				Log.Criticalf("etcd lock %+v occupied, [%d] times tried, try later ....\n",
					string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value), tryTimes)
			}
		}
		time.Sleep(time.Second)
	}
	Log.Criticalf("etcd lock[LeaseLocker] final err : %v.\n", errFinal)
	return errFinal
}

func (t *EtcdLocker) Unlock() {
	switch t.distLockerType {
	case "SyncLocker":
		t.unlockSyncLocker()
	case "LeaseLocker":
		t.unlockLeaseLocker()
	}
}

func (t *EtcdLocker) unlockSyncLocker() {
	Log.Criticalf("etcd unlock[syncLocker] start, waiting....\n")
	t.locker.Unlock()
	Log.Criticalf("etcd unlock[syncLocker] succeed.\n")
}

func (t *EtcdLocker) unlockLeaseLocker() {
	Log.Criticalf("etcd unlock[LeaseLocker] start, waiting....\n")
	t.leaseCancel()
	Log.Criticalf("etcd unlock[leaseLocker] succeed.\n")
}
