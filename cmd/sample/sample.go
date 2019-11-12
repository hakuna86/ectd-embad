package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/etcd-io/etcd/embed"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const testClusterTkn = "token-andus"

func newEmbedURLs(port int) ([]url.URL, error) {
	var urls []url.URL
	u, err := url.Parse(fmt.Sprintf("http://localhost:%d", port))
	if err != nil {
		return nil, err
	}
	urls = append(urls, *u)
	return urls, nil
}

var (
	cURLs   []url.URL
	pURLs   []url.URL
	endCh   = make(chan struct{})
	startCh = make(chan struct{})
	keys    = []string{"foo", "bar", "hakuna"}
	cluster = "node0=http://localhost:3021,node1=http://localhost:3022,node2=http://localhost:3023"
)

func etedServe(id int) {
	cfg := embed.NewConfig()
	cfg.Name = fmt.Sprintf("node%d", id)
	cfg.InitialClusterToken = testClusterTkn
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.LCUrls, cfg.ACUrls = cURLs, cURLs
	cfg.LPUrls, cfg.APUrls = pURLs, pURLs
	cfg.InitialCluster = cluster
	cfg.Dir = filepath.Join(os.TempDir(), fmt.Sprint(time.Now().Nanosecond()))

	srv, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		os.RemoveAll(cfg.Dir)
		srv.Close()
	}()

	cli := v3client.New(srv.Server)
	defer cli.Close()

	for {
		select {
		case <-time.NewTicker(2 * time.Second).C:
			_, err = cli.Put(context.Background(), keys[id], time.Now().String())
			if err != nil {
				if err != nil {
					switch err {
					case context.Canceled:
						log.Fatalf("ctx is canceled by another routine: %v", err)
					case context.DeadlineExceeded:
						log.Fatalf("ctx is attached with a deadline is exceeded: %v", err)
					case rpctypes.ErrEmptyKey:
						log.Fatalf("client-side error: %v", err)
					default:
						log.Fatalf("bad cluster endpoints, which are not etcd servers: %v", err)
					}
				}
			}

			for _, k := range keys {
				var gresp *clientv3.GetResponse
				gresp, err = cli.Get(context.Background(), k)
				if gresp.Count > 0 {
					for _, v := range gresp.Kvs {
						fmt.Println("gresp :", k, "===>", v.String())
					}
				}
				fmt.Println("gresp :", k, "===>", gresp.Count)
			}
		}
	}
}

func main() {
	id := flag.Int("id", 0, "node key")
	cPort := flag.Int("client", 3000, "client port")
	pPort := flag.Int("peer", 3001, "peer port")
	flag.Parse()

	cURLs, _ = newEmbedURLs(*cPort)
	pURLs, _ = newEmbedURLs(*pPort)
	etedServe(*id)
	<-endCh
	fmt.Println("main was dead")
}
