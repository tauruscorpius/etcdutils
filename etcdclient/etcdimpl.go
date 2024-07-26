package etcdclient

import (
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/tauruscorpius/appcommon/Log"
)

type EtcdImpl struct {
	// Client
	client *clientv3.Client
	// KV
	kv clientv3.KV
}

var etcdImpl EtcdImpl

func GetImpl() *EtcdImpl {
	return &etcdImpl
}

func (t *EtcdImpl) Init(client *clientv3.Client) error {
	t.client = client
	if t.client == nil {
		return errors.New("invalid etcd client")
	}
	t.kv = clientv3.NewKV(t.client)
	return nil
}

func (t *EtcdImpl) Put(path, val string) error {
	{
		_, err := t.kv.Put(context.TODO(), path, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *EtcdImpl) Del(path string) error {
	{
		_, err := t.kv.Delete(context.TODO(), path, clientv3.WithPrefix())
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *EtcdImpl) Get(path string) (int64, *map[string]string, error) {
	{
		var map1 map[string]string
		map1 = make(map[string]string)

		getRsp, err := t.kv.Get(context.TODO(), path, clientv3.WithPrefix())
		if err != nil {
			return 0, nil, err
		}

		for _, ev := range getRsp.Kvs {
			map1[string(ev.Key)] = string(ev.Value)
		}

		return getRsp.Header.Revision, &map1, nil
	}
}

func (t *EtcdImpl) DoWatch(path string, cb func(a int, k, v string) bool) {
	for {
		Log.Criticalf("Do watch path : [%s], match method prefix\n", path)
		rch := t.client.Watch(context.Background(), path, clientv3.WithPrefix() /*, clientv3.WithRev(revision)*/)
		for wresp := range rch {
			for _, ev := range wresp.Events {
				Log.Criticalf("%s %s : %s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				cb(int(ev.Type), string(ev.Kv.Key), string(ev.Kv.Value))
			}
		}
	}

}
