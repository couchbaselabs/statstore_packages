package sharded_counter

import (
	"fmt"
	"math/rand"
	"time"

	"appengine"
	"appengine/datastore"
	"appengine/memcache"
)

type counterConfig struct {
	Shards int
}

type shard struct {
	Name  string
	Count int
}

const (
	defaultShards = 20
	configKind    = "GeneralCounterShardConfig"
	shardKind     = "GeneralCounterShard"
)

func memcacheKey(name string) string {
	return shardKind + ":" + name
}

// Count retrieves the value of the named counter.
func Count(c appengine.Context, name string) (int, error) {
	total := 0
	mkey := memcacheKey(name)
	if _, err := memcache.JSON.Get(c, mkey, &total); err == nil {
		return total, nil
	}
	q := datastore.NewQuery(shardKind).Filter("Name =", name)
	for t := q.Run(c); ; {
		var s shard
		_, err := t.Next(&s)
		if err == datastore.Done {
			break
		}
		if err != nil {
			return total, err
		}
		total += s.Count
	}
	memcache.JSON.Set(c, &memcache.Item{
		Key:        mkey,
		Object:     &total,
		Expiration: time.Minute,
	})
	return total, nil
}

type shardMemo struct {
}

var cfgMemo shardMemo

func (s *shardMemo) getOrCreate(c appengine.Context, name string) (counterConfig, error) {
	return counterConfig{defaultShards}, nil
}

func (s *shardMemo) forget(name string) {
}

// Increment increments the named counter by one.
func Increment(c appengine.Context, name string) error {
	return IncrementBy(c, name, 1)
}

// IncrementBy increments the named counter by a specified amount.
func IncrementBy(c appengine.Context, name string, by int) error {
	// Get counter config.
	cfg, err := cfgMemo.getOrCreate(c, name)
	if err != nil {
		return err
	}
	err = datastore.RunInTransaction(c, func(c appengine.Context) error {
		shardName := fmt.Sprintf("shard%d", rand.Intn(cfg.Shards))
		key := datastore.NewKey(c, shardKind, shardName, 0, nil)
		var s shard
		err := datastore.Get(c, key, &s)
		// A missing entity and a present entity will both work.
		if err != nil && err != datastore.ErrNoSuchEntity {
			return err
		}
		s.Count += by
		_, err = datastore.Put(c, key, &s)
		return err
	}, nil)
	if err == nil {
		memcache.Increment(c, memcacheKey(name), int64(by), 0)
	}
	return err
}

// IncreaseShards increases the number of shards for the named counter to n.
// It will never decrease the number of shards.
func IncreaseShards(c appengine.Context, name string, n int) error {
	defer cfgMemo.forget(name)

	ckey := datastore.NewKey(c, configKind, name, 0, nil)
	return datastore.RunInTransaction(c, func(c appengine.Context) error {
		var cfg counterConfig
		mod := false
		err := datastore.Get(c, ckey, &cfg)
		if err == datastore.ErrNoSuchEntity {
			cfg.Shards = defaultShards
			mod = true
		} else if err != nil {
			return err
		}
		if cfg.Shards < n {
			cfg.Shards = n
			mod = true
		}
		if mod {
			_, err = datastore.Put(c, ckey, &cfg)
		}
		return err
	}, nil)
}
