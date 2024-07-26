package etcdaction

import (
	"github.com/tauruscorpius/appcommon/Log"
	"strings"
)

type registerWatchAction struct {
	Prefix string
	Action func(a int, k, v string) bool
}

var actionTable []registerWatchAction
var watchPaths []string

func RegisterRpcNodeUpdate(prefix string, cb func(int, string, string) bool) {
	actionTable = append(actionTable, registerWatchAction{Prefix: prefix, Action: cb})
	watchPaths = append(watchPaths, prefix)
}

func GetWatchPath() []string {
	return watchPaths
}

func PutAction(k, v string) {
	for _, item := range actionTable {
		if strings.HasPrefix(k, item.Prefix) {
			if !item.Action(0, k, v) {
				Log.Criticalf("Etcd Put Action Path[%s] Value[%s] Rpc Node Update\n", k, v)
			}
		}
	}
}

func DeleteAction(k, v string) {
	for _, item := range actionTable {
		if strings.HasPrefix(k, item.Prefix) {
			if !item.Action(1, k, v) {
				Log.Criticalf("Etcd Del Action Path[%s] Value[%s] Rpc Node Update\n", k, v)
			}
		}
	}
}
