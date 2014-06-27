package main

import (
	"flag"
	"github.com/fzzy/radix/redis"
	"github.com/golang/groupcache"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// errorHandler, big surprise, reduces the 'iferr' forms all over the place. :-D
// Log the error err and fatally exit the application.
func errorHandler(err error) {
	if err != nil {
		log.Fatal("error:", err)
	}
}

// CLI Flags
var (
	me        = flag.String("me", "http://localhost:1025", "Address for the groupcache endpoint")
	peerAddrs = flag.String("peer_addrs", "", "Comma-separated list of peers for groupcache")
	limit     = flag.Int("limit", 128, "Maximum size of keys (MB)")
)

func main() {
	// Parse CLI Flags
	flag.Parse()

	// Not the most flexible but allows one to run within Docker with links
	redisAddr := os.Getenv("REDIS_PORT_6379_TCP")
	if len(redisAddr) == 0 {
		log.Fatal("environment variable REDIS_PORT_6379_TCP not set, use a docker link of name: redis with port 6379 exposed")
	}
	parsed, err := url.Parse(redisAddr)
	errorHandler(err)

	// We might consider enabling a non-Redis mode, groupcache->groupcache->redis.
	log.Println("connecting to url:", parsed)
	client, err := redis.DialTimeout("tcp", parsed.Host, time.Duration(10)*time.Second)
	errorHandler(err)
	defer client.Close()

	// The hypercache is real. What comes after hyper?
	hypercache := groupcache.NewGroup("hypercache", int64(*limit)<<20, groupcache.GetterFunc(
		func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
			rv, _ := client.Cmd("get", key).Str()
			// BUG(fujin): Write protobuf schema for adunit. Encode to Proto. use dest.SetProto().
			dest.SetString(rv)
			return err
		}))

	// New pool
	peers := groupcache.NewHTTPPool(*me)
	// Parse the peer addresses
	addrs := strings.Split(*peerAddrs, ",")
	// Add 'em if ya got 'em.
	peers.Set(addrToURL(addrs)...)

	// Fire in the hole
	log.Println("warming cache")
	t0 := time.Now()
	keys, err := client.Cmd("keys", "*").List()
	for _, key := range keys {
		var value string
		if err = hypercache.Get(nil, key, groupcache.StringSink(&value)); err != nil {
			log.Fatal(err)
		}
	}
	t1 := time.Now()
	log.Println("warmed cache in:", t1.Sub(t0))

	// Parse the host out of the 'me' string
	addr, err := url.Parse(*me)
	errorHandler(err)

	log.Fatal(http.ListenAndServe(addr.Host, peers))
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}
