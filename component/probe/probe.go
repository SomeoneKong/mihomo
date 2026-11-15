// Package probe decides when the on-demand fallback groups test their members, runs the tests,
// keeps what they found, and records every decision with its inputs.
package probe

import (
	"context"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// Strategy constants. They are recorded in the params context of every instance, so the
// records tell which values a decision was taken under.
const (
	UseWindow          = 10 * time.Minute // a group is in use this long after real traffic
	SilentAfter        = 10 * time.Second // a connection that sent and never received counts as silent after this
	UDPSilentCount     = 3                // silent UDP associations within UDPSilentWindow make a signal
	UDPSilentWindow    = 60 * time.Second
	ReuseWindow        = 60 * time.Second // a success stands for maintenance needs this long
	BackoffFirst       = 15 * time.Second
	BackoffMax         = 30 * time.Minute
	PoolSize           = 2
	RefillRange        = 6
	BatchSize          = 4
	PoolFresh          = 30 * time.Minute
	PoolFreshSwitched  = 10 * time.Minute // while the group switched within SwitchedWindow
	SwitchedWindow     = time.Hour
	PoolJitter         = 0.2
	CooldownFirst      = 60 * time.Second
	CooldownMax        = 5 * time.Minute
	CooldownReset      = time.Hour
	RepairWindow       = 10 * time.Minute
	VerifySlots        = 4
	SearchSlots        = 4
	MaintainSlots      = 8
	EventCapacity      = 50000
	SignalsPerMinute   = 60
	SchemaVersion      = 1
	MinCensusInterval  = 24 * time.Hour
	DefaultTestTimeout = 5 * time.Second
)

// Constants of the fast tiers, the deviations from the effective order and the quality of the
// members.
const (
	FastDial          = 1500 * time.Millisecond // a real dial not done by then calls on a standby
	FastProbe         = 2500 * time.Millisecond // a test not done by then is compared, at most half its timeout
	SlowerRatio       = 2                       // slower means taking at least this many times as long
	CertainWindow     = 10 * time.Minute        // a new connection that received data keeps a route certain this long
	HedgeAttempts     = 3                       // members a group tries in one dial
	HedgeInFlight     = 2                       // of them at the same time
	DialBudget        = 4                       // attempts in flight across the groups of one dial
	PairWindow        = time.Second             // tests compared started at most this far apart
	AttributionDelay  = 10 * time.Minute        // a failure counts in the memory after this
	LocalFailures     = 3                       // failures of distinct members within LocalWindow that check the network
	LocalWindow       = 30 * time.Second
	LocalTargets      = 3
	LocalCheckTimeout = 3 * time.Second
	DownInterval      = 15 * time.Second // while the network seems down: one test and one check this often
	PoorInterval      = 60 * time.Second
	OutageNodes       = 2 // nodes of a pool failing within OutageWindow suggest an outage of the pool
	OutageWindow      = 10 * time.Minute
	PersistentAfter   = time.Hour
	LongOutageAfter   = 6 * time.Hour
	BackoffMaxLong    = 2 * time.Hour
	RecentWindow      = 8 * time.Hour
	LongWindow        = 72 * time.Hour
	YieldRecent       = 3
	YieldLong         = 6
	ReturnRecent      = 2
	ReturnLong        = 4
	DegradeWeight     = 0.5
	DegradeMerge      = 10 * time.Minute
	TrialLength       = 30 * time.Minute
	ReadmitFirst      = time.Hour
	ReadmitMax        = 8 * time.Hour
	RepeatWindow      = time.Hour // a second failure within it puts the member on trial
	TrustedStandbys   = 2
	TrustedStandbysAt = 3 // when the preferred member had an anomaly lately or is on trial
	PoolMax           = 4
	RefillMax         = 10
	DisplayVersion    = 1
	SummaryDelay      = 2 * time.Minute // a minute of connections is summed up this long after it ends
)

// FastProbeFor is the fast tier of tests under a timeout.
func FastProbeFor(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		timeout = DefaultTestTimeout
	}
	if half := timeout / 2; half < FastProbe {
		return half
	}
	return FastProbe
}

// Condition is what a test checks: the URL, the expected status and the time allowed.
// Groups with the same condition share what tests of a proxy found.
type Condition struct {
	URL      string        `json:"url"`
	Expected string        `json:"expectedStatus"`
	Timeout  time.Duration `json:"-"`
}

// Key identifies a piece of evidence: a proxy object under a condition. A proxy rebuilt from
// a changed config is another object and starts unknown.
type Key struct {
	Target C.Proxy
	Cond   Condition
}

// State is what the evidence says of a proxy.
type State uint8

const (
	Unknown State = iota
	Alive
	Dead
)

func (s State) String() string {
	switch s {
	case Alive:
		return "alive"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

// MarshalText implements encoding.TextMarshaler.
func (s State) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// Evidence is what the latest test of a proxy under a condition found.
type Evidence struct {
	State State
	// Started is when the test that decided State started; tests that started earlier do not
	// replace it.
	Started  time.Time
	Exec     uint64 // the execution of that test
	Failures int    // consecutive failed tests
	// NextAllowed is the earliest time an automatic test of a dead proxy may run again.
	NextAllowed time.Time
	// DeadSince is when the test that found a dead proxy dead first started.
	DeadSince time.Time
	Seq       uint64 // the event of the result that decided State
	// Version orders the changes of all evidence of this process: a later change has a
	// greater version.
	Version uint64
	// Delegated is set on the evidence of a group that judges its own availability: it is
	// what the group published, never a test through it. Terminal is set when the group
	// ends at a direct outbound it does not test.
	Delegated bool
	Terminal  bool
}

// Outage is how long a dead proxy has been dead, the time the local network did not seem
// normal included: the coordinator leaves that out (DeadFor).
func (ev *Evidence) Outage(now time.Time) time.Duration {
	if ev.State != Dead || ev.DeadSince.IsZero() {
		return 0
	}
	return now.Sub(ev.DeadSince)
}

// Class is how urgent a need is. Each class has slots of its own.
type Class uint8

const (
	ClassVerify   Class = iota // verifying the current member, or the member a switch landed on
	ClassSearch                // searching after a failure
	ClassMaintain              // keeping the standbys, recovering preferred members, census
	classCount
)

// Sources of needs.
const (
	SourceVerify   = "verify"
	SourceContinue = "continue"
	SourceSearch   = "search"
	SourcePool     = "pool"
	SourceRecover  = "recover"
	SourceCensus   = "census"
	SourceManual   = "manual"
	SourceStandby  = "standby" // a standby tested alongside the verification of the preferred member
)

// Need is a test a group asks for. A group submits all its needs at once; the set replaces
// the needs it submitted before.
type Need struct {
	Target C.Proxy
	Cond   Condition
	Class  Class
	// Rank orders needs within the maintenance class: lower runs first.
	Rank   int
	Source string
	// NotBefore, when set, asks for a test that started after it; a test that started
	// earlier, however fresh, does not do. Needs without it may reuse a recent success.
	NotBefore time.Time
	Search    uint64 // the search the need belongs to
	Batch     uint64
	// RepairUntil is set on the recovery of a member only a dependency repair asks for, to
	// when the repair window ends: while the local network seems down, a recovery the window
	// ran out on before it got its turn keeps one chance to run.
	RepairUntil time.Time
	// Snapshot is the state of the group when it asked, recorded with the need.
	Snapshot any
	// Config and Exclusions are the context versions the group derived the need under.
	Config, Exclusions uint64
}

func (n *Need) key() Key {
	return Key{Target: n.Target, Cond: n.Cond}
}

// Request is a need an execution served.
type Request struct {
	Group  string `json:"group"`
	Source string `json:"source"`
	Need   uint64 `json:"need"`
}

// Completion tells a group that a test of a proxy it refers to ended.
type Completion struct {
	Exec    uint64
	Seq     uint64 // of the probe event
	Key     Key
	Started time.Time
	Result  C.ProbeResult
	// Adopted is false when the test was canceled, or started before the test the evidence
	// already holds; such a result changes nothing.
	Adopted bool
	Before  Evidence
	After   Evidence
	// Requests are the needs of groups the execution served; API names the API of a manual
	// test, which serves no need.
	Requests []Request
	API      string
	// Policy is set when a group judging its own availability became unavailable by a pin,
	// an exclusion or a change of its members: no test found it dead.
	Policy bool
	// Episode is the failure the test ended, when it found a dead proxy alive again.
	Episode *Episode
	// Pools are the traffic pools the test went through, as its dialer route tells;
	// UnknownPath is set when the route is not known.
	Pools       []string
	UnknownPath bool
}

// Episode is a time a proxy was dead, from the test that found it dead to the one that found
// it alive again.
type Episode struct {
	Since    time.Time
	Until    time.Time
	Duration time.Duration // the time the local network seemed down or poor left out
}

// Persistent reports whether the proxy was dead long enough to count as down.
func (e *Episode) Persistent() bool {
	return e != nil && e.Duration >= PersistentAfter
}

// Notice is something the coordinator tells groups: a Completion, or a change of the
// exclusions.
type Notice interface {
	notice()
}

func (Completion) notice() {}

// ExclusionsChanged tells groups that the exclusions changed, with the table of that change:
// a group evaluates against it where the notice stands among the others it gets.
type ExclusionsChanged struct {
	Table Table
}

func (ExclusionsChanged) notice() {}

// Running tells the groups referring to a target that a test of it is still running at the
// fast tier of tests.
type Running struct {
	Exec     uint64
	Key      Key
	Started  time.Time
	Requests []Request
}

func (Running) notice() {}

// Outcomes of attributing a failure.
const (
	OutcomeMember    = "member"      // counted for the member
	OutcomeLocal     = "local"       // the local network seemed down or poor
	OutcomePool      = "pool"        // an outage of a traffic pool, counted for the pool
	OutcomeUnknown   = "unknownPath" // the route of the test is not known
	OutcomeUndecided = "undecided"   // what it depends on is gone
	OutcomeExpired   = "expired"     // too old to count when decided
)

// Attributed tells the groups referring to a target how a failure a test found was
// attributed, once it is.
type Attributed struct {
	Exec    uint64
	Key     Key
	Started time.Time
	Outcome string
	Pool    string
}

func (Attributed) notice() {}

// NetworkState is what the checks of the local network found.
type NetworkState uint8

const (
	NetworkNormal NetworkState = iota
	NetworkDown                // no first hop could be reached
	NetworkPoor                // some could not, or slowly
)

func (s NetworkState) String() string {
	switch s {
	case NetworkDown:
		return "down"
	case NetworkPoor:
		return "poor"
	default:
		return "normal"
	}
}

// MarshalText implements encoding.TextMarshaler.
func (s NetworkState) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// NetworkChanged tells the groups that the state of the local network changed.
type NetworkChanged struct {
	State NetworkState
	Since time.Time
}

func (NetworkChanged) notice() {}

// Outage states of a traffic pool.
const (
	OutageSuspected   = "suspected"
	OutageEstablished = "established"
	OutageDisproved   = "disproved"
	// OutageUndecided ends an outage that cannot be concluded: its nodes are gone, or the
	// local network seemed down when it began. It counts for neither the pool nor the nodes.
	OutageUndecided = "undecided"
)

// OutageChanged tells the groups that an outage of a traffic pool was suspected or concluded.
type OutageChanged struct {
	Pool  string
	Cond  Condition
	State string
	// First is when the first failure of it started, Since when it was suspected, End when it
	// was concluded.
	First, Since, End time.Time
}

func (OutageChanged) notice() {}

// Group is an on-demand group as the coordinator sees it.
type Group interface {
	Name() string
	// References reports whether the group reads the evidence of target under cond.
	References(target C.Proxy, cond Condition) bool
	// Deliver queues a notice for the group. It is called with the coordinator locked, so it
	// must only queue the notice.
	Deliver(Notice)
}

type executionKey struct{}

// withExecution marks ctx as the context of the execution id.
func withExecution(ctx context.Context, id uint64) context.Context {
	return context.WithValue(C.WithProbe(ctx), executionKey{}, id)
}

// ExecutionOf returns the execution a probe dialing under ctx belongs to, 0 when unknown.
func ExecutionOf(ctx context.Context) uint64 {
	id, _ := ctx.Value(executionKey{}).(uint64)
	return id
}
