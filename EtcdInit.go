package Etc

import (
	"errors"
	"github.com/tauruscorpius/appcommon/Consts"
	"github.com/tauruscorpius/appcommon/ExitHandler"
	"github.com/tauruscorpius/appcommon/Log"
	"github.com/tauruscorpius/etcdutils/etcdaction"
	"github.com/tauruscorpius/etcdutils/etcdclient"
	"time"
)

const (
	servicePingPath = "/etc/service/ping"
)

func EtcdInit(etcdNodes string) error {
	err := etcdclient.GetClient().Init(etcdNodes)
	if err != nil {
		Log.Criticalf("etcd init failed : err[%v]\n", err)
	}
	err = etcdclient.GetImpl().Init(etcdclient.GetClient().Client())
	if err != nil {
		Log.Criticalf("init etcd impl failed : err[%v]\n", err)
		return err
	}
	err = etcdclient.GetLocker().Init(etcdclient.GetClient().Client())
	if err != nil {
		Log.Criticalf("init etcd locker failed : err[%v]\n", err)
		return err
	}
	err = etcdPing()
	if err != nil {
		Log.Errorf("etcd ping failed: err:%v\n", err)
	}
	return err
}

func EtcdCreateSyncLocker(lockerName string) bool {
	return etcdclient.GetLocker().CreateSyncLocker(lockerName)
}

func EtcdCreateLeaseLocker(lockerName string, lockerValue string, ttl int64) bool {
	return etcdclient.GetLocker().CreateLeaseLocker(lockerName, lockerValue, ttl)
}

func EtcdLock() bool {
	if err := etcdclient.GetLocker().Lock(); err != nil {
		Log.Errorf("etcd locker failed: err:%v\n", err)
		return false
	}
	return true
}

func AddEtcdExitUnlock() {
	ExitHandler.GetExitFuncChain().Add(func() bool {
		EtcdUnlock()
		return true
	})
}

func EtcdUnlock() {
	etcdclient.GetLocker().Unlock()
}

func etcdPing() error {
	c0 := make(chan struct{})
	go func() {
		etcdclient.GetImpl().Put(servicePingPath, time.Now().Format(Consts.MyDF))
		c0 <- struct{}{}
	}()
	select {
	// timeout
	case <-time.After(time.Second):
		return errors.New("ping time timeout")
	case <-c0:
		return nil
	}
}

func WatchRegister(prefix string, cb func(int, string, string) bool) {
	etcdaction.RegisterRpcNodeUpdate(prefix, cb)
}

func DoWatch() {
	cb := func(a int, k, v string) bool {
		Log.Debugf("action : %d key : %s value : %s\n", a, k, v)
		if a == 0 { // put
			etcdaction.PutAction(k, v)
		} else if a == 1 { // delete
			etcdaction.DeleteAction(k, v)
		}
		return true
	}

	for _, v := range etcdaction.GetWatchPath() {
		go etcdclient.GetImpl().DoWatch(v, cb)
	}
}
