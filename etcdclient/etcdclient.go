package etcdclient

import (
	"errors"
	_ "fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/tauruscorpius/appcommon/Log"
	"strings"
	"time"
)

type EtcdClient struct {
	// etcdHosts
	etcdHosts string

	// Config
	config *clientv3.Config

	// Client
	client *clientv3.Client
}

var etcdClient EtcdClient

func GetClient() *EtcdClient {
	return &etcdClient
}

func (t *EtcdClient) Client() *clientv3.Client {
	return t.client
}

func (t *EtcdClient) EtcdHosts() string {
	return t.etcdHosts
}

func (t *EtcdClient) Init(etcdNodes string) error {
	t.etcdHosts = etcdNodes
	if t.etcdHosts == "" {
		return errors.New("miss etcd url")
	}
	urls := strings.Split(etcdNodes, ",")
	Log.Criticalf("Using ETCD nodes : %v\n", urls)
	return t.init(urls)
}

func (t *EtcdClient) init(urls []string) error {
	t.config = &clientv3.Config{Endpoints: urls, DialTimeout: 2 * time.Second}
	cli, err := clientv3.New(*t.config)
	if err != nil {
		return err
	}
	t.client = cli
	return nil
}
