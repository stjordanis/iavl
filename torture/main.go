package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof" // nolint: gosec
	"runtime"

	"github.com/tendermint/iavl"
	db "github.com/tendermint/tm-db"
)

var (
	r = rand.New(rand.NewSource(49872768940))
)

func main() {
	go func() {
		err := http.ListenAndServe("localhost:6060", nil)
		fmt.Printf("HTTP server exited: %v\n", err)
	}()
	err := runPruning()
	if err != nil {
		log.Fatal(err)
	}
}

// pruning=everything
func runPruning() error {
	levelDB, err := db.NewGoLevelDB("pruning", "")
	if err != nil {
		return err
	}
	memDB := db.NewMemDB()
	tree, err := iavl.NewMutableTreeWithOpts(levelDB, memDB, 0, &iavl.Options{
		// These settings correspond to PruneEverything in the SDK.
		KeepEvery:  1,
		KeepRecent: 0,
		Sync:       true,
	})
	if err != nil {
		return err
	}

	version, err := tree.LoadVersion(0)
	if err != nil {
		return err
	}
	fmt.Printf("Loaded version %v\n", version)

	for {
		for i := 0; i < 4096; i++ {
			key := make([]byte, 16)
			value := make([]byte, 16)
			r.Read(key)
			r.Read(value)
			// if we get an update, set again
			for tree.Set(key, value) {
				r.Read(key)
			}
		}

		// Save the next version
		_, version, err = tree.SaveVersion()
		if err != nil {
			return err
		}
		fmt.Printf("Saved version %v\n", version)

		// Delete the previous version
		if version > 1 {
			err = tree.DeleteVersion(version - 1)
			if err != nil {
				return err
			}
			fmt.Printf("Deleted version %v\n", version-1)
		}
	}
}

// memory leak tests
// nolint
func runMemory() error {
	levelDB, err := db.NewGoLevelDB("leveldb", "torture")
	if err != nil {
		return err
	}
	memDB := db.NewMemDB()
	tree, err := iavl.NewMutableTreeWithOpts(levelDB, memDB, 0, &iavl.Options{
		KeepEvery:  10,
		KeepRecent: 10,
		Sync:       false,
	})
	if err != nil {
		return err
	}

	const (
		keySize   = 16
		valueSize = 16

		versions    = 100000 // number of versions to generate
		versionOps  = 4096   // number of operations (create/update/delete) per version
		updateRatio = 0.0    // ratio of updates out of all operations
		deleteRatio = 0.0    // ratio of deletes out of all operations
	)

	memStats := runtime.MemStats{}

	keys := make([][]byte, 0, versionOps)
	for i := 0; i < versions; i++ {
		for j := 0; j < versionOps; j++ {
			key := make([]byte, keySize)
			value := make([]byte, valueSize)

			// The performance of this is likely to be terrible, but that's fine for small tests
			switch {
			case len(keys) >= versionOps && r.Float64() <= deleteRatio:
				index := r.Intn(len(keys))
				key = keys[index]
				keys = append(keys[:index], keys[index+1:]...)
				_, removed := tree.Remove(key)
				if !removed {
					return fmt.Errorf("remove failed for key %x", key)
				}

			case len(keys) >= versionOps && r.Float64() <= updateRatio:
				key = keys[r.Intn(len(keys))]
				r.Read(value)
				updated := tree.Set(key, value)
				if !updated {
					return fmt.Errorf("update failed for key %x", key)
				}

			default:
				r.Read(key)
				r.Read(value)
				// if we get an update, set again
				for tree.Set(key, value) {
					r.Read(key)
				}
				keys = append(keys, key)
			}
		}
		_, version, err := tree.SaveVersion()
		if err != nil {
			return err
		}

		/*if version > 10 {
			err = tree.DeleteVersion(version - 10)
			if err != nil {
				return err
			}
		}*/

		runtime.GC()
		runtime.ReadMemStats(&memStats)
		stats := memDB.Stats()

		fmt.Printf("Saved version %v with %v nodes (heap=%vMB memDB=%v)\n",
			version, tree.Size(), memStats.HeapAlloc/1000/1000, stats["database.size"])
	}
	//require.EqualValues(t, versions, tree.Version())
	//require.GreaterOrEqual(t, tree.Size(), int64(math.Trunc(versions*versionOps*(1-updateRatio-deleteRatio))/2))
	return nil
}
