package outboundgroup

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
)

// binding ties a connection an on-demand group handed out to the group, so that the data the
// connection receives renews the use of the group, and what it observes reaches the group of
// the same name after a reload. A binding is released exactly once, when its connection
// closes.
type binding struct {
	member C.Proxy
	cond   probe.Condition
	probe  bool
	// generation is of the group that dialed the connection; the binding keeps no reference
	// to it, so that the groups it moved through can be released. The name of the group and
	// the coordinator record what is observed once the binding ended.
	generation uint64
	group      string
	coord      *probe.Coordinator
	target     atomic.Pointer[onDemand]
}

// Renew implements C.ActivityBinding.
func (b *binding) Renew() {
	if od := b.target.Load(); od != nil {
		od.noteUseAt("data", od.now().UnixNano(), b)
	}
}

// bindings registers the live bindings of each group, to move them to the group of the same
// name when a new config is activated.
var bindings = struct {
	sync.Mutex
	byGroup map[*onDemand]map[*binding]struct{}
}{byGroup: map[*onDemand]map[*binding]struct{}{}}

// bind ties a connection dialed through member to the group, or, when the group was retired
// while the dial ran, to the group that replaced it; the binding ends at once when there is
// none or the member left it.
func (od *onDemand) bind(member C.Proxy, isProbe bool) *binding {
	b := &binding{member: member, cond: od.cond, probe: isProbe, generation: od.generation, group: od.name(), coord: od.coord}
	bindings.Lock()
	defer bindings.Unlock()
	target := od
	for target != nil && target.retired {
		target = target.successor
	}
	if target == nil {
		return b
	}
	if _, ok := (*target.memberSet.Load())[member]; !ok {
		return b
	}
	b.target.Store(target)
	set := bindings.byGroup[target]
	if set == nil {
		set = map[*binding]struct{}{}
		bindings.byGroup[target] = set
	}
	set[b] = struct{}{}
	return b
}

func (b *binding) release() {
	bindings.Lock()
	if od := b.target.Load(); od != nil {
		if set := bindings.byGroup[od]; set != nil {
			delete(set, b)
			if len(set) == 0 {
				delete(bindings.byGroup, od)
			}
		}
	}
	b.target.Store(nil)
	bindings.Unlock()
}

// migrateBindings moves the bindings of a retired group to the group replacing it, when their
// member still belongs to it; the others end.
func migrateBindings(from, to *onDemand) {
	bindings.Lock()
	defer bindings.Unlock()
	from.retired = true
	from.successor = to
	set := bindings.byGroup[from]
	delete(bindings.byGroup, from)
	if len(set) == 0 {
		return
	}
	var members map[C.Proxy]struct{}
	if to != nil {
		members = *to.memberSet.Load()
	}
	for b := range set {
		if _, ok := members[b.member]; !ok || to == nil {
			b.target.Store(nil)
			continue
		}
		b.target.Store(to)
		dest := bindings.byGroup[to]
		if dest == nil {
			dest = map[*binding]struct{}{}
			bindings.byGroup[to] = dest
		}
		dest[b] = struct{}{}
	}
}

// dropBindings ends the bindings of od whose member left it.
func (od *onDemand) dropBindings() {
	members := *od.memberSet.Load()
	bindings.Lock()
	defer bindings.Unlock()
	for b := range bindings.byGroup[od] {
		if _, ok := members[b.member]; !ok {
			b.target.Store(nil)
			delete(bindings.byGroup[od], b)
		}
	}
}

// observation watches a connection until its first data and its end.
type observation struct {
	binding   *binding
	dialed    time.Time
	handshake bool
	udp       bool
	hedged    bool // carried for a member the fast tier called on
	sent      atomic.Bool
	received  atomic.Bool
	failed    atomic.Bool
	closed    atomic.Bool
}

func (od *onDemand) observe(member C.Proxy, dialed time.Time, isProbe bool) *observation {
	return &observation{binding: od.bind(member, isProbe), dialed: dialed}
}

func (o *observation) target() *onDemand {
	return o.binding.target.Load()
}

func (o *observation) signal(strong bool, class string, err error) {
	od := o.target()
	now := time.Now()
	note := signalNote{
		member: o.binding.member, cond: o.binding.cond, network: "tcp", strong: strong, class: class,
		at: now, dialed: o.dialed, age: now.Sub(o.dialed), sent: o.sent.Load(),
		probe: o.binding.probe, beforeReload: od == nil || od.generation != o.binding.generation,
		hedged: o.hedged,
	}
	if o.udp {
		note.network = "udp"
	}
	if err != nil {
		note.err = shortError(err)
	}
	if od == nil {
		// The member left the group, or the group is gone: the failure tells nothing of what
		// is in use now.
		b := o.binding
		member := map[string]any{"name": b.member.Name(), "object": b.coord.KnownObjectID(b.member)}
		b.coord.Records().Signal(b.group, "stale", signalFields(note, member))
		return
	}
	od.post(note)
}

func (o *observation) gotData() {
	if o.received.Load() || !o.received.CompareAndSwap(false, true) {
		return
	}
	if od := o.target(); od != nil {
		od.post(successNote{member: o.binding.member, cond: o.binding.cond, dialed: o.dialed, probe: o.binding.probe})
	}
}

func (o *observation) readFailed(err error) {
	if o.received.Load() || !o.failed.CompareAndSwap(false, true) {
		return
	}
	class := "read"
	if errors.Is(err, io.EOF) {
		class = "eof"
	}
	o.signal(true, class, err)
}

func (o *observation) wrote(err error) {
	if o.sent.Load() {
		return
	}
	if err == nil {
		o.sent.Store(true)
		return
	}
	if o.handshake && !o.received.Load() && o.failed.CompareAndSwap(false, true) {
		o.signal(true, "handshake", err)
	}
}

func (o *observation) close() {
	if !o.closed.CompareAndSwap(false, true) {
		return
	}
	defer o.binding.release()
	// failed is taken as for a failed read, so that the connection ends in one observation
	if o.received.Load() || !o.sent.Load() || !o.failed.CompareAndSwap(false, true) {
		return
	}
	age := time.Since(o.dialed)
	if age >= probe.SilentAfter {
		o.signal(false, "silent", nil)
		return
	}
	if o.udp {
		if od := o.target(); od != nil {
			od.post(udpSilentNote{member: o.binding.member, cond: o.binding.cond, at: time.Now(), dialed: o.dialed, probe: o.binding.probe, beforeReload: od.generation != o.binding.generation})
		}
	}
}

func (o *observation) packetRead(got bool, err error) {
	switch {
	case got:
		o.gotData()
	case err != nil && !o.closed.Load() && !errors.Is(err, net.ErrClosed):
		o.readFailed(err)
	}
}

// ActivityBinding implements C.ActivityBinder for both kinds of observed connections.
func (o *observation) activityBinding() C.ActivityBinding {
	if o.binding.probe {
		return nil
	}
	return o.binding
}

type observedConn struct {
	C.Conn
	obs *observation
}

func newObservedConn(c C.Conn, obs *observation) C.Conn {
	obs.handshake = N.NeedHandshake(c)
	return &observedConn{Conn: c, obs: obs}
}

func (c *observedConn) Read(b []byte) (int, error) {
	if c.obs.received.Load() {
		return c.Conn.Read(b)
	}
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.obs.gotData()
	} else if err != nil {
		c.obs.readFailed(err)
	}
	return n, err
}

func (c *observedConn) ReadBuffer(buffer *buf.Buffer) error {
	if c.obs.received.Load() {
		return c.Conn.ReadBuffer(buffer)
	}
	err := c.Conn.ReadBuffer(buffer)
	if buffer.Len() > 0 {
		c.obs.gotData()
	} else if err != nil {
		c.obs.readFailed(err)
	}
	return err
}

func (c *observedConn) Write(b []byte) (int, error) {
	if c.obs.sent.Load() {
		return c.Conn.Write(b)
	}
	n, err := c.Conn.Write(b)
	c.obs.wrote(err)
	return n, err
}

func (c *observedConn) WriteBuffer(buffer *buf.Buffer) error {
	if c.obs.sent.Load() {
		return c.Conn.WriteBuffer(buffer)
	}
	err := c.Conn.WriteBuffer(buffer)
	c.obs.wrote(err)
	return err
}

func (c *observedConn) Close() error {
	c.obs.close()
	return c.Conn.Close()
}

func (c *observedConn) Upstream() any {
	return c.Conn
}

func (c *observedConn) ReaderReplaceable() bool {
	return c.obs.received.Load()
}

func (c *observedConn) ReaderPossiblyReplaceable() bool {
	return !c.obs.received.Load()
}

func (c *observedConn) WriterReplaceable() bool {
	return c.obs.sent.Load()
}

func (c *observedConn) WriterPossiblyReplaceable() bool {
	return !c.obs.sent.Load()
}

func (c *observedConn) ActivityBinding() C.ActivityBinding {
	return c.obs.activityBinding()
}

type observedPacketConn struct {
	C.PacketConn
	obs *observation
}

func newObservedPacketConn(pc C.PacketConn, obs *observation) C.PacketConn {
	obs.udp = true
	return &observedPacketConn{PacketConn: pc, obs: obs}
}

func (c *observedPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if !c.obs.received.Load() {
		// an empty datagram is a datagram too
		c.obs.packetRead(err == nil || n > 0, err)
	}
	return n, addr, err
}

func (c *observedPacketConn) WaitReadFrom() (data []byte, put func(), addr net.Addr, err error) {
	data, put, addr, err = c.PacketConn.WaitReadFrom()
	if !c.obs.received.Load() {
		c.obs.packetRead(err == nil || len(data) > 0, err)
	}
	return
}

func (c *observedPacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(b, addr)
	if !c.obs.sent.Load() && err == nil {
		c.obs.sent.Store(true)
	}
	return n, err
}

func (c *observedPacketConn) Close() error {
	c.obs.close()
	return c.PacketConn.Close()
}

func (c *observedPacketConn) Upstream() any {
	return c.PacketConn
}

func (c *observedPacketConn) ActivityBinding() C.ActivityBinding {
	return c.obs.activityBinding()
}

type udpSilentEntry struct {
	at, dialed time.Time
}

type udpSilentNote struct {
	member       C.Proxy
	cond         probe.Condition
	at, dialed   time.Time
	probe        bool
	beforeReload bool
}

// onUDPSilent counts the short UDP associations that sent and never received: enough of them
// within a while make one weak signal.
func (od *onDemand) onUDPSilent(n udpSilentNote, now time.Time) {
	if n.cond != od.cond || !od.isMember(n.member) {
		return
	}
	if !n.probe {
		if sum := od.summaryOf(n.member, n.dialed); sum != nil {
			sum.failed["silent"]++
		}
	}
	ms := od.member(n.member)
	ms.udpSilent = append(ms.udpSilent, udpSilentEntry{at: n.at, dialed: n.dialed})
	od.pruneUDPSilent(ms, now)
	if len(ms.udpSilent) < probe.UDPSilentCount {
		return
	}
	// one signal for them all, as late as the latest association was dialed
	var dialed time.Time
	for _, entry := range ms.udpSilent {
		dialed = maxTime(dialed, entry.dialed)
	}
	count := len(ms.udpSilent)
	ms.udpSilent = nil
	od.onSignal(signalNote{
		member: n.member, cond: n.cond, network: "udp", class: "silent", at: n.at, dialed: dialed,
		age: n.at.Sub(dialed), sent: true, probe: n.probe, beforeReload: n.beforeReload, count: count,
	}, now)
}

// pruneUDPSilent forgets the silent associations that are too old to count, and those dialed
// before a connection that received data.
func (od *onDemand) pruneUDPSilent(ms *memberState, now time.Time) {
	kept := ms.udpSilent[:0]
	for _, entry := range ms.udpSilent {
		if now.Sub(entry.at) < probe.UDPSilentWindow && !entry.dialed.Before(ms.lastSuccessDial) {
			kept = append(kept, entry)
		}
	}
	ms.udpSilent = kept
}
