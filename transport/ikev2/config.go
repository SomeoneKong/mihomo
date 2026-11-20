package ikev2

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Defaults follow the FortiClient IKEv2 profile: AES-CBC with HMAC-SHA2-256 or HMAC-SHA1 and DH group 20,
// PFS with the same group, 86400s IKE SA and 43200s CHILD SA lifetimes.
var (
	DefaultIKEProposals = []string{"aes256-sha256-ecp384", "aes128-sha1-ecp384"}
	DefaultESPProposals = []string{"aes256-sha256-ecp384", "aes128-sha1-ecp384"}
)

const (
	DefaultPort        = 500
	DefaultIKELifetime = 86400 * time.Second
	DefaultESPLifetime = 43200 * time.Second
	DefaultDPDInterval = 30 * time.Second
)

// Config describes an IKEv2 remote access connection: the gateway authenticates with a pre-shared key,
// the client with EAP-MSCHAPv2 when Username is set and with the pre-shared key otherwise.
type Config struct {
	PSK      string
	Username string
	Password string
	// LocalID is sent as IDi: an IP address, an RFC 822 address (contains '@') or an FQDN.
	// When empty the local IPv4 address of the UDP flow is used.
	LocalID string
	// Port is the IKE port. Port 500 floats to 4500 after IKE_SA_INIT; any other port carries
	// IKE and ESP together with the non-ESP marker.
	Port         uint16
	IKEProposals []string
	ESPProposals []string
	IKELifetime  time.Duration
	ESPLifetime  time.Duration
	DPDInterval  time.Duration

	ikeSuites []*suite
	espSuites []*suite
}

// Prepare applies defaults and validates the configuration.
func (c *Config) Prepare() error {
	if c.PSK == "" {
		return errors.New("ikev2: psk is required")
	}
	if (c.Username == "") != (c.Password == "") {
		return errors.New("ikev2: username and password must be set together")
	}
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if c.IKELifetime == 0 {
		c.IKELifetime = DefaultIKELifetime
	}
	if c.ESPLifetime == 0 {
		c.ESPLifetime = DefaultESPLifetime
	}
	if c.DPDInterval == 0 {
		c.DPDInterval = DefaultDPDInterval
	}
	if c.IKELifetime < 0 || c.ESPLifetime < 0 || c.DPDInterval < 0 {
		return errors.New("ikev2: lifetimes and dpd interval must be positive")
	}
	if len(c.IKEProposals) == 0 {
		c.IKEProposals = DefaultIKEProposals
	}
	if len(c.ESPProposals) == 0 {
		c.ESPProposals = DefaultESPProposals
	}
	c.ikeSuites = c.ikeSuites[:0]
	for _, p := range c.IKEProposals {
		s, err := parseSuite(p)
		if err != nil {
			return err
		}
		if s.dh == nil {
			return fmt.Errorf("ikev2: ike proposal %q needs a DH group", p)
		}
		c.ikeSuites = append(c.ikeSuites, s)
	}
	c.espSuites = c.espSuites[:0]
	for _, p := range c.ESPProposals {
		s, err := parseSuite(p)
		if err != nil {
			return err
		}
		c.espSuites = append(c.espSuites, s)
	}
	if len(c.ikeSuites) > 255 || len(c.espSuites) > 255 {
		return errors.New("ikev2: too many proposals")
	}
	return nil
}

// suite is one proposal: AES-CBC, an HMAC integrity algorithm (also the PRF of an IKE SA) and a DH group.
// For ESP the DH group is the PFS group used when rekeying and may be nil.
type suite struct {
	encrKeyLen int
	integ      *hashAlg
	dh         *dhGroup
}

func (s *suite) String() string {
	name := fmt.Sprintf("aes%d-%s", s.encrKeyLen*8, s.integ.name)
	if s.dh != nil {
		name += "-" + s.dh.name
	}
	return name
}

// parseSuite parses a strongSwan style proposal such as "aes256-sha256-ecp384".
func parseSuite(text string) (*suite, error) {
	s := &suite{}
	for _, token := range strings.Split(strings.ToLower(strings.TrimSpace(text)), "-") {
		switch {
		case token == "aes128" || token == "aes192" || token == "aes256":
			if s.encrKeyLen != 0 {
				return nil, fmt.Errorf("ikev2: proposal %q has more than one cipher", text)
			}
			s.encrKeyLen = map[string]int{"aes128": 16, "aes192": 24, "aes256": 32}[token]
		case hashByName(token) != nil:
			if s.integ != nil {
				return nil, fmt.Errorf("ikev2: proposal %q has more than one hash", text)
			}
			s.integ = hashByName(token)
		case dhGroupByName(token) != nil:
			if s.dh != nil {
				return nil, fmt.Errorf("ikev2: proposal %q has more than one DH group", text)
			}
			s.dh = dhGroupByName(token)
		default:
			return nil, fmt.Errorf("ikev2: unsupported algorithm %q in proposal %q", token, text)
		}
	}
	if s.encrKeyLen == 0 || s.integ == nil {
		return nil, fmt.Errorf("ikev2: proposal %q needs a cipher and a hash", text)
	}
	return s, nil
}

func hashByName(name string) *hashAlg {
	for _, h := range hashAlgs {
		if h.name == name {
			return h
		}
	}
	return nil
}

func dhGroupByName(name string) *dhGroup {
	for _, g := range dhGroups {
		if g.name == name {
			return g
		}
	}
	return nil
}

// transforms lists the transforms of the suite for an IKE or an ESP proposal. withDH controls whether
// the DH group is offered; the first CHILD SA, negotiated inside IKE_AUTH, carries no key exchange.
func (s *suite) transforms(ike, withDH bool) []transform {
	t := []transform{{typ: transformENCR, id: encrAESCBC, keyLen: uint16(s.encrKeyLen * 8)}}
	if ike {
		t = append(t, transform{typ: transformPRF, id: s.integ.prfID})
	}
	t = append(t, transform{typ: transformINTEG, id: s.integ.integID})
	if withDH && s.dh != nil {
		t = append(t, transform{typ: transformDH, id: s.dh.id})
	}
	if !ike {
		t = append(t, transform{typ: transformESN, id: 0})
	}
	return t
}

// matches reports whether the responder's choice is exactly this suite's transforms.
func (s *suite) matches(chosen []transform, ike, withDH bool) bool {
	want := s.transforms(ike, withDH)
	if len(want) != len(chosen) {
		return false
	}
	for _, w := range want {
		found := false
		for _, c := range chosen {
			if c == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
