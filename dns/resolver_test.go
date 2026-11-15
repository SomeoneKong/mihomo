//go:build !(android && cmfa)

package dns

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/trie"

	D "github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

type resetCountClient struct{ resets int }

func (c *resetCountClient) ExchangeContext(context.Context, *D.Msg) (*D.Msg, error) {
	return nil, nil
}

func (c *resetCountClient) Address() string { return "reset-count" }

func (c *resetCountClient) ResetConnection() { c.resets++ }

func TestResetConnectionReachesPolicyAndSystemClients(t *testing.T) {
	trieClient, matcherClient, cachedSystemClient := &resetCountClient{}, &resetCountClient{}, &resetCountClient{}
	policyTrie := trie.New[[]dnsClient]()
	require.NoError(t, policyTrie.Insert("+.example.com", []dnsClient{trieClient}))
	system := newSystemClient()
	system.dnsClients["192.0.2.1"] = &systemDnsClient{dnsClient: cachedSystemClient}
	system.lastFlush = time.Now()

	r := &Resolver{
		main: []dnsClient{system},
		policy: []dnsPolicy{
			domainTriePolicy{policyTrie},
			domainMatcherPolicy{dnsClients: []dnsClient{matcherClient}},
		},
	}
	r.ResetConnection()

	require.NotZero(t, trieClient.resets)
	require.NotZero(t, matcherClient.resets)
	require.NotZero(t, cachedSystemClient.resets)
	require.True(t, system.lastFlush.IsZero(), "the system nameservers are read again on the next query")
}

func TestSystemClientSkipsBlacklistedCachedNameserver(t *testing.T) {
	system := newSystemClient()
	system.dnsClients["192.0.2.1"] = &systemDnsClient{dnsClient: &resetCountClient{}}
	system.dnsClients["192.0.2.2"] = &systemDnsClient{dnsClient: &resetCountClient{}}
	system.lastFlush = time.Now()

	resolver.AddSystemDnsBlacklist("192.0.2.2")
	defer resolver.RemoveSystemDnsBlacklist("192.0.2.2")

	dnsClients, err := system.getDnsClients()
	require.NoError(t, err)
	require.Len(t, dnsClients, 1)
	require.Same(t, system.dnsClients["192.0.2.1"].dnsClient, dnsClients[0])
}
