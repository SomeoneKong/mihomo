package outbound

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	C "github.com/metacubex/mihomo/constant"

	"github.com/stretchr/testify/require"
)

type fakeLoginable struct {
	instance string
	stateDir string
}

func (f *fakeLoginable) LoginInstance() string { return f.instance }

func (f *fakeLoginable) LoginConfig() C.LoginConfig { return C.LoginConfig{StateDir: f.stateDir} }

func (f *fakeLoginable) LoginStatus(context.Context) C.LoginStatus {
	return C.LoginStatus{State: C.LoginNotStarted}
}

func (f *fakeLoginable) Login(context.Context, string, string) (C.LoginStatus, error) {
	return C.LoginStatus{}, nil
}

func (f *fakeLoginable) Logout(context.Context, string) (C.LoginStatus, error) {
	return C.LoginStatus{}, nil
}

func useLoginTestHome(t *testing.T) string {
	t.Helper()
	previous := C.Path.HomeDir()
	home := t.TempDir()
	C.SetHomeDir(home)
	t.Cleanup(func() { C.SetHomeDir(previous) })
	return home
}

func nodeOn(name, dir string) LoginNode {
	return LoginNode{Name: name, Loginable: &fakeLoginable{instance: name + "-1", stateDir: dir}}
}

func matchedNames(m StateDirMatch) []string {
	var names []string
	for _, node := range m.Nodes {
		names = append(names, node.Name)
	}
	return names
}

func TestMatchStateDirResolvesAsAConfigWould(t *testing.T) {
	home := useLoginTestHome(t)
	nodes := []LoginNode{
		nodeOn("default", filepath.Join(home, "tailscale")),
		nodeOn("a", filepath.Join(home, "a")),
		nodeOn("also-a", filepath.Join(home, "a")),
	}

	m := MatchStateDir("", nodes)
	require.Equal(t, filepath.Join(home, "tailscale"), m.Resolved)
	require.Equal(t, []string{"default"}, matchedNames(m), "no state-dir is the default one")

	require.Equal(t, []string{"a", "also-a"}, matchedNames(MatchStateDir("./a", nodes)), "every node on the dir")
	require.Equal(t, []string{"a", "also-a"}, matchedNames(MatchStateDir(filepath.Join(home, "a"), nodes)))
	require.Empty(t, MatchStateDir("b", nodes).Nodes)

	_, err := os.Stat(filepath.Join(home, "a"))
	require.True(t, os.IsNotExist(err), "matching creates no dir")
}

func TestMatchStateDirRejectsAnUnsafePath(t *testing.T) {
	home := useLoginTestHome(t)
	m := MatchStateDir(filepath.Join(filepath.Dir(home), "elsewhere"), []LoginNode{nodeOn("a", filepath.Join(home, "a"))})
	require.True(t, m.Invalid)
	require.Empty(t, m.Nodes)
}

func TestMatchStateDirFindsAnotherSpellingOfAnExistingDir(t *testing.T) {
	home := useLoginTestHome(t)
	target := filepath.Join(home, "state")
	require.NoError(t, os.Mkdir(target, 0o700))
	alias := filepath.Join(home, "alias")
	if err := os.Symlink(target, alias); err != nil {
		if runtime.GOOS == "windows" {
			t.Skip("symbolic links need a privilege on Windows:", err)
		}
		t.Fatal(err)
	}
	require.Equal(t, []string{"a"}, matchedNames(MatchStateDir("alias", []LoginNode{nodeOn("a", target)})))
}

func TestMatchStateDirKeepsCaseOfDirsOnACaseSensitiveFileSystem(t *testing.T) {
	home := useLoginTestHome(t)
	upper := filepath.Join(home, "State")
	require.NoError(t, os.Mkdir(upper, 0o700))
	if err := os.Mkdir(filepath.Join(home, "state"), 0o700); err != nil {
		t.Skip("the file system ignores case")
	}
	require.Empty(t, MatchStateDir("state", []LoginNode{nodeOn("a", upper)}).Nodes,
		"two dirs that differ in case are two identities")
}
