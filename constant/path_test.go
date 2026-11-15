package constant

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestPath(t *testing.T) {
	assert.False(t, (&path{}).IsSafePath("/usr/share/metacubexd/"))
	assert.True(t, (&path{
		safePaths: []string{"/usr/share/metacubexd"},
	}).IsSafePath("/usr/share/metacubexd/"))

	assert.False(t, (&path{}).IsSafePath("../metacubexd/"))
	assert.True(t, (&path{
		homeDir:   "/usr/share/mihomo",
		safePaths: []string{"/usr/share/metacubexd"},
	}).IsSafePath("../metacubexd/"))
	assert.False(t, (&path{
		homeDir:   "/usr/share/mihomo",
		safePaths: []string{"/usr/share/ycad"},
	}).IsSafePath("../metacubexd/"))

	assert.False(t, (&path{}).IsSafePath("/opt/mykeys/key1.key"))
	assert.True(t, (&path{
		safePaths: []string{"/opt/mykeys"},
	}).IsSafePath("/opt/mykeys/key1.key"))
	assert.True(t, (&path{
		safePaths: []string{"/opt/mykeys/"},
	}).IsSafePath("/opt/mykeys/key1.key"))
	assert.True(t, (&path{
		safePaths: []string{"/opt/mykeys/key1.key"},
	}).IsSafePath("/opt/mykeys/key1.key"))

	assert.True(t, (&path{}).IsSafePath("key1.key"))
	assert.True(t, (&path{}).IsSafePath("./key1.key"))
	assert.True(t, (&path{}).IsSafePath("./mykey/key1.key"))
	assert.True(t, (&path{}).IsSafePath("./mykey/../key1.key"))
	assert.False(t, (&path{}).IsSafePath("./mykey/../../key1.key"))

}

func TestMMDBKeepsGeoIPName(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"GeoIP.dat", "geoip.metadb"} {
		assert.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o644))
	}
	defer func(name string) { GeoipName = name }(GeoipName)

	p := &path{homeDir: dir}
	p.GeoIP()
	p.MMDB()
	assert.Equal(t, "GeoIP.dat", GeoipName)
}
