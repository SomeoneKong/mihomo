package socks

import (
	"net"
	"net/netip"
	"sync"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/component/auth"
	C "github.com/metacubex/mihomo/constant"
	authStore "github.com/metacubex/mihomo/listener/auth"
)

// associationKey identifies the clients of a SOCKS5 UDP relay: the port the
// control connection was accepted on, which is also the port of the relay, and
// the IP of the client. The datagrams of an association come from another
// source port than its control connection, so the client port is not part of
// the key, and clients behind one IP share their associations.
type associationKey struct {
	port uint16
	ip   netip.Addr
}

var (
	associationMu sync.RWMutex
	// associations holds the users of the open UDP ASSOCIATE control
	// connections of each key, the most recent last.
	associations = make(map[associationKey][]string)
)

func addrPortOf(addr net.Addr) netip.AddrPort {
	m := C.Metadata{}
	if err := m.SetRemoteAddr(addr); err != nil {
		return netip.AddrPort{}
	}
	return m.AddrPort()
}

func associationKeyOf(localAddr, remoteAddr net.Addr) (associationKey, bool) {
	local, remote := addrPortOf(localAddr), addrPortOf(remoteAddr)
	if local.Port() == 0 || !remote.Addr().IsValid() {
		return associationKey{}, false
	}
	return associationKey{port: local.Port(), ip: remote.Addr()}, true
}

func addAssociation(key associationKey, user string) {
	associationMu.Lock()
	defer associationMu.Unlock()
	associations[key] = append(associations[key], user)
}

func removeAssociation(key associationKey, user string) {
	associationMu.Lock()
	defer associationMu.Unlock()
	users := associations[key]
	for i := len(users) - 1; i >= 0; i-- {
		if users[i] == user {
			users = append(users[:i], users[i+1:]...)
			break
		}
	}
	if len(users) == 0 {
		delete(associations, key)
	} else {
		associations[key] = users
	}
}

func lookupAssociation(key associationKey) (string, bool) {
	associationMu.RLock()
	defer associationMu.RUnlock()
	users := associations[key]
	if len(users) == 0 {
		return "", false
	}
	return users[len(users)-1], true
}

// admitUDP applies the admission of the TCP listener to a datagram from
// remoteAddr to the relay at localAddr. When the listener requires
// authentication, the client must hold an association, i.e. an open UDP
// ASSOCIATE control connection, on this port. It returns the user of the
// client's association, if any.
func admitUDP(store auth.AuthStore, isDefault bool, localAddr, remoteAddr net.Addr) (user string, ok bool) {
	if isDefault || store == authStore.Default { // only apply on default listener
		if !inbound.IsRemoteAddrDisAllowed(remoteAddr) {
			return "", false
		}
		if inbound.SkipAuthRemoteAddr(remoteAddr) {
			store = authStore.Nil
		}
	}
	var associated bool
	if key, valid := associationKeyOf(localAddr, remoteAddr); valid {
		user, associated = lookupAssociation(key)
	}
	if !associated && store.Authenticator() != nil {
		return "", false
	}
	return user, true
}
