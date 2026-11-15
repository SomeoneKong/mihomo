//go:build !(android && cmfa)

package dns

import (
	"context"
	"testing"
	"time"

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
