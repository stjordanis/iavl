package iavl

import (
	"encoding/base64"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/tendermint/tm-db"
)

// Randomized test that runs all sorts of random operations, mirroring them in a known-good
// map, and verifying the state of the tree.
func TestRandomOperations(t *testing.T) {
	const (
		randSeed  = 49872768940 // for deterministic tests
		keySize   = 16          // before base64-encoding
		valueSize = 16          // before base64-encoding

		versions = 32 // number of final versions to generate
		//reloadChance = 0.2 // chance of tree reload after version save (discards recent versions).
		//cacheChance  = 0.2  // chance of enabling caching
		//cacheSizeMax = 4096 // maximum size of cache (will be random from 1)

		versionOps  = 64  // number of operations (create/update/delete) per version
		updateRatio = 0.4 // ratio of updates out of all operations
		deleteRatio = 0.2 // ratio of deletes out of all operations
	)

	r := rand.New(rand.NewSource(randSeed))

	// loadTree loads the last persisted version of a tree with random pruning settings.
	loadTree := func(levelDB db.DB) (tree *MutableTree, version int64, options *Options) {
		var err error
		options = &Options{
			KeepRecent: 5,
			KeepEvery:  10,
		}
		tree, err = NewMutableTreeWithOpts(levelDB, db.NewMemDB(), 0, options)
		require.NoError(t, err)
		version, err = tree.Load()
		require.NoError(t, err)
		return
	}

	// generates random keys and values
	randString := func(size int) string {
		buf := make([]byte, size)
		r.Read(buf)
		return base64.StdEncoding.EncodeToString(buf)
	}

	// Use the same on-disk database for the entire run.
	tempdir, err := ioutil.TempDir("", "iavl")
	require.NoError(t, err)
	defer os.RemoveAll(tempdir)

	levelDB, err := db.NewGoLevelDB("leveldb", tempdir)
	require.NoError(t, err)

	tree, version, options := loadTree(levelDB)

	// Set up a mirror of the current IAVL state, as well as the history of saved mirrors
	// on disk and in memory.
	mirror := make(map[string]string, versionOps)
	mirrorKeys := make([]string, 0, versionOps)
	diskMirrors := make(map[int64]map[string]string)
	memMirrors := make(map[int64]map[string]string)

	for version < versions {
		for i := 0; i < versionOps; i++ {
			switch {
			case len(mirror) > 0 && r.Float64() <= deleteRatio:
				index := r.Intn(len(mirrorKeys))
				key := mirrorKeys[index]
				mirrorKeys = append(mirrorKeys[:index], mirrorKeys[index+1:]...)
				_, removed := tree.Remove([]byte(key))
				require.True(t, removed)
				delete(mirror, key)

			case len(mirror) > 0 && r.Float64() <= updateRatio:
				key := mirrorKeys[r.Intn(len(mirrorKeys))]
				value := randString(valueSize)
				updated := tree.Set([]byte(key), []byte(value))
				require.True(t, updated)
				mirror[key] = value

			default:
				key := randString(keySize)
				value := randString(valueSize)
				for tree.Has([]byte(key)) {
					key = randString(keySize)
				}
				updated := tree.Set([]byte(key), []byte(value))
				require.False(t, updated)
				mirror[key] = value
				mirrorKeys = append(mirrorKeys, key)
			}
		}
		_, version, err = tree.SaveVersion()
		require.NoError(t, err)

		t.Logf("Saved tree at version %v with %v keys and %v versions",
			version, tree.Size(), len(tree.AvailableVersions()))

		// Verify that the version matches the mirror.
		assertMirror(t, tree, mirror, 0)

		// Save the mirror of this version as a disk and/or recent mirror, as appropriate,
		// and remove expired memory/recent mirror.
		if version%options.KeepEvery == 0 {
			diskMirrors[version] = copyMirror(mirror)
		}
		if options.KeepRecent > 0 {
			memMirrors[version] = copyMirror(mirror)
			delete(memMirrors, version-options.KeepRecent)
		}

		// Verify all historical versions.
		assertVersions(t, tree, diskMirrors, memMirrors)

		for diskVersion, diskMirror := range diskMirrors {
			assertMirror(t, tree, diskMirror, diskVersion)
		}

		for memVersion, memMirror := range memMirrors {
			assertMirror(t, tree, memMirror, memVersion)
		}

		// Log progress
	}
}

// Checks that a mirror, optionally for a given version, matches the tree contents.
func assertMirror(t *testing.T, tree *MutableTree, mirror map[string]string, version int64) {
	var err error
	itree := tree.ImmutableTree
	if version > 0 {
		itree, err = tree.GetImmutable(version)
		require.NoError(t, err)
	}
	iterated := 0
	itree.Iterate(func(key, value []byte) bool {
		require.Equal(t, string(value), mirror[string(key)], "Invalid value for key %q", key)
		iterated++
		return false
	})
	require.EqualValues(t, len(mirror), itree.Size())
	require.EqualValues(t, len(mirror), iterated)
}

// Checks that all versions in the tree are present in the mirrors, and vice-versa.
func assertVersions(t *testing.T, tree *MutableTree, mirrors ...map[int64]map[string]string) {
	mirrorVersionsMap := make(map[int]bool)
	for _, m := range mirrors {
		for version := range m {
			mirrorVersionsMap[int(version)] = true
		}
	}
	mirrorVersions := make([]int, 0, len(mirrorVersionsMap))
	for version := range mirrorVersionsMap {
		mirrorVersions = append(mirrorVersions, version)
	}
	sort.Ints(mirrorVersions)
	require.Equal(t, mirrorVersions, tree.AvailableVersions())
}

func copyMirror(mirror map[string]string) map[string]string {
	c := make(map[string]string, len(mirror))
	for k, v := range mirror {
		c[k] = v
	}
	return c
}
