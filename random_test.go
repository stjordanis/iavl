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
// FIXME This should test LoadVersionForOverwriting as well.
func TestRandomOperations(t *testing.T) {
	const (
		randSeed  = 49872768940 // for deterministic tests
		keySize   = 16          // before base64-encoding
		valueSize = 16          // before base64-encoding

		versions      = 64   // number of final versions to generate
		reloadChance  = 0.1  // chance of tree reload after save (discards non-persisted versions).
		deleteChance  = 0.1  // chance of random version deletion after save.
		flushChance   = 0.1  // chance of random version being flushed with FlushVersion() after save.
		syncChance    = 0.3  // chance of enabling sync writes on tree load
		maxKeepEvery  = 10   // max KeepEvery (random 0-max on load)
		maxKeepRecent = 10   // max KeepRecent (random 0-max on load, if 0 then KeepEvery=1)
		cacheChance   = 0.4  // chance of enabling caching
		cacheSizeMax  = 4096 // maximum size of cache (will be random from 1)

		versionOps  = 64  // number of operations (create/update/delete) per version
		updateRatio = 0.4 // ratio of updates out of all operations
		deleteRatio = 0.2 // ratio of deletes out of all operations
	)

	r := rand.New(rand.NewSource(randSeed))

	// loadTree loads the last persisted version of a tree with random pruning settings.
	loadTree := func(levelDB db.DB) (tree *MutableTree, version int64, options *Options) {
		var err error
		keepRecent := r.Int63n(maxKeepRecent + 1)
		keepEvery := r.Int63n(maxKeepEvery + 1)
		if keepRecent == 0 {
			keepEvery = 1
		}
		options = &Options{
			KeepRecent: keepRecent,
			KeepEvery:  keepEvery,
			Sync:       r.Float64() < syncChance,
		}
		cacheSize := 0
		if r.Float64() < cacheChance {
			cacheSize = int(r.Int63n(cacheSizeMax + 1))
		}
		tree, err = NewMutableTreeWithOpts(levelDB, db.NewMemDB(), cacheSize, options)
		require.NoError(t, err)
		version, err = tree.Load()
		require.NoError(t, err)
		t.Logf("Loaded last persisted version %v", version)
		t.Logf("KeepRecent=%v KeepEvery=%v Sync=%v cacheSize=%v",
			options.KeepRecent, options.KeepEvery, options.Sync, cacheSize)
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
			case len(mirror) > 0 && r.Float64() < deleteRatio:
				index := r.Intn(len(mirrorKeys))
				key := mirrorKeys[index]
				mirrorKeys = append(mirrorKeys[:index], mirrorKeys[index+1:]...)
				_, removed := tree.Remove([]byte(key))
				require.True(t, removed)
				delete(mirror, key)

			case len(mirror) > 0 && r.Float64() < updateRatio:
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

		// Flush a random version if requested.
		if r.Float64() < flushChance {
			versions := getMirrorVersions(diskMirrors, memMirrors)
			flushVersion := int64(versions[r.Intn(len(versions)-1)])
			t.Logf("Flushing version %v", flushVersion)
			err = tree.FlushVersion(int64(flushVersion))
			require.NoError(t, err)
			if m, ok := memMirrors[flushVersion]; ok {
				diskMirrors[flushVersion] = copyMirror(m)
				delete(memMirrors, flushVersion)
			}
		}

		// Delete a random version if requested, but never the latest version.
		if r.Float64() < deleteChance {
			versions := getMirrorVersions(diskMirrors, memMirrors)
			if len(versions) > 2 {
				deleteVersion := int64(versions[r.Intn(len(versions)-1)])
				t.Logf("Deleting version %v", deleteVersion)
				err = tree.DeleteVersion(int64(deleteVersion))
				require.NoError(t, err)
				delete(diskMirrors, deleteVersion)
				delete(memMirrors, deleteVersion)
			}
		}

		// Reload tree from last persisted version if requested, checking that it matches the
		// latest disk mirror version and discarding memory mirrors.
		if r.Float64() < reloadChance {
			tree, version, options = loadTree(levelDB)
			assertMaxVersion(t, tree, version, diskMirrors)
			memMirrors = make(map[int64]map[string]string, options.KeepRecent)
			mirror = copyMirror(diskMirrors[version])
			mirrorKeys = getMirrorKeys(mirror)
		}

		// Verify all historical versions.
		assertVersions(t, tree, diskMirrors, memMirrors)

		for diskVersion, diskMirror := range diskMirrors {
			assertMirror(t, tree, diskMirror, diskVersion)
		}

		for memVersion, memMirror := range memMirrors {
			assertMirror(t, tree, memMirror, memVersion)
		}
	}
}

// Checks that a version is the maximum mirrored version.
func assertMaxVersion(t *testing.T, tree *MutableTree, version int64, mirrors map[int64]map[string]string) {
	max := int64(0)
	for v := range mirrors {
		if v > max {
			max = v
		}
	}
	require.Equal(t, max, version)
}

// Checks that a mirror, optionally for a given version, matches the tree contents.
func assertMirror(t *testing.T, tree *MutableTree, mirror map[string]string, version int64) {
	var err error
	itree := tree.ImmutableTree
	if version > 0 {
		itree, err = tree.GetImmutable(version)
		require.NoError(t, err, "loading version %v", version)
	}
	// We check both ways: first check that iterated keys match the mirror, then iterate over the
	// mirror and check with get. This is to exercise both the iteration and Get() code paths.
	iterated := 0
	itree.Iterate(func(key, value []byte) bool {
		require.Equal(t, string(value), mirror[string(key)], "Invalid value for key %q", key)
		iterated++
		return false
	})
	require.EqualValues(t, len(mirror), itree.Size())
	require.EqualValues(t, len(mirror), iterated)
	for key, value := range mirror {
		_, actual := itree.Get([]byte(key))
		require.Equal(t, value, string(actual))
	}
}

// Checks that all versions in the tree are present in the mirrors, and vice-versa.
func assertVersions(t *testing.T, tree *MutableTree, mirrors ...map[int64]map[string]string) {
	require.Equal(t, getMirrorVersions(mirrors...), tree.AvailableVersions())
}

// copyMirror copies a mirror map.
func copyMirror(mirror map[string]string) map[string]string {
	c := make(map[string]string, len(mirror))
	for k, v := range mirror {
		c[k] = v
	}
	return c
}

// getMirrorKeys returns the keys of a mirror, unsorted.
func getMirrorKeys(mirror map[string]string) []string {
	keys := make([]string, 0, len(mirror))
	for key := range mirror {
		keys = append(keys, key)
	}
	return keys
}

// getMirrorVersions returns the versions of the given mirrors, sorted. Returns []int to
// match tree.AvailableVersions().
func getMirrorVersions(mirrors ...map[int64]map[string]string) []int {
	versionMap := make(map[int]bool)
	for _, m := range mirrors {
		for version := range m {
			versionMap[int(version)] = true
		}
	}
	versions := make([]int, 0, len(versionMap))
	for version := range versionMap {
		versions = append(versions, version)
	}
	sort.Ints(versions)
	return versions
}
