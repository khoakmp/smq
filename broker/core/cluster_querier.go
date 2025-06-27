package core

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
)

type ClusterQuerier interface {
	QueryGroupsOfTopic(topicName string) []string
}

type ClusterQuerierV1 struct {
	monitorHttpClient *http.Client
	clientMaker       sync.Once
	monitorHttpAddrs  atomic.Value
}

// addr host:port
func NewClusterQuerierV1(addrs []string) *ClusterQuerierV1 {
	q := &ClusterQuerierV1{
		monitorHttpClient: nil,
	}
	q.monitorHttpAddrs.Store(addrs)
	return q
}
func (b *ClusterQuerierV1) SetMonitorHttpAddrs(addrs []string) {
	b.monitorHttpAddrs.Swap(addrs)
	//TODO: add logs
}

func (b *ClusterQuerierV1) QueryGroupsOfTopic(topicName string) []string {
	// this client keeps a connection pool to multi-monitors

	b.clientMaker.Do(func() {
		// TODO: create custom http.Client
		b.monitorHttpClient = http.DefaultClient
	})

	var tries int = 0
	var addrs []string = b.monitorHttpAddrs.Load().([]string)
	var n = len(addrs)

	if n == 0 {
		return nil
	}
	var idx int = rand.Int() % n
query:
	tries++
	// TODO: build url
	url := fmt.Sprintf("http://%s/query_groups?topic=%s", addrs[idx], topicName)
	idx = (idx + 1) % n
	resp, err := b.monitorHttpClient.Get(url)

	if err != nil {
		if tries < n {
			goto query
		}
		return nil
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		if tries < n {
			goto query
		}
		return nil
	}
	var groups Groups
	err = json.Unmarshal(buf, &groups)
	if err != nil {
		if tries >= n {
			goto query
		}
	}
	return groups.GroupNames
}
