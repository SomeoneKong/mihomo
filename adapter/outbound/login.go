package outbound

import (
	"os"
	"path/filepath"

	C "github.com/metacubex/mihomo/constant"
)

// LoginableOf returns the outbound an adapter wraps if a user can log it in.
func LoginableOf(adapter C.ProxyAdapter) (C.Loginable, bool) {
	l, ok := unwrap(adapter).(C.Loginable)
	return l, ok
}

// LoginNode is an outbound of the running config that a user can log in.
type LoginNode struct {
	Name      string
	Loginable C.Loginable
}

type LoginNodeRef struct {
	Name     string `json:"name"`
	Instance string `json:"instance"`
}

// StateDirMatch tells which nodes of the running config share the device identity a
// state-dir written in another config would have.
type StateDirMatch struct {
	StateDir string         `json:"stateDir"`
	Resolved string         `json:"resolved,omitempty"`
	Invalid  bool           `json:"invalid,omitempty"`
	Nodes    []LoginNodeRef `json:"nodes,omitempty"`
}

// MatchStateDir resolves a state-dir as the config of a tailscale outbound would, and
// finds the nodes on the same dir, as the handover of state dirs tells them apart: by the
// cleaned path, or once both exist, by the file system. It neither creates the dir nor
// starts a node.
func MatchStateDir(raw string, nodes []LoginNode) StateDirMatch {
	match := StateDirMatch{StateDir: raw}
	dir := raw
	if dir == "" {
		dir = "tailscale"
	}
	dir = C.Path.Resolve(dir)
	if !C.Path.IsSafePath(dir) {
		match.Invalid = true
		return match
	}
	key := filepath.Clean(dir)
	match.Resolved = key
	info, _ := os.Stat(key)
	for _, node := range nodes {
		other := node.Loginable.LoginConfig().StateDir
		if other == "" {
			continue
		}
		same := filepath.Clean(other) == key
		if !same && info != nil {
			if otherInfo, err := os.Stat(other); err == nil {
				same = os.SameFile(info, otherInfo)
			}
		}
		if same {
			match.Nodes = append(match.Nodes, LoginNodeRef{Name: node.Name, Instance: node.Loginable.LoginInstance()})
		}
	}
	return match
}
