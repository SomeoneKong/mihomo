//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/metacubex/mihomo/common/contextutils"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/tailscale/client/local"
	"github.com/metacubex/tailscale/health"
	"github.com/metacubex/tailscale/ipn"
	"github.com/metacubex/tailscale/ipn/ipnstate"
)

const (
	tailscaleLoginKeyTimeout  = 90 * time.Second
	tailscaleLoginLinkTimeout = 60 * time.Second
	tailscaleLogoutTimeout    = 30 * time.Second
	tailscaleLoginPoll        = 250 * time.Millisecond
)

var (
	errLoginSuperseded = &C.LoginError{Code: C.LoginSuperseded, Message: "a newer login of the node took over"}
	errLoginReplaced   = &C.LoginError{Code: C.LoginReplaced, Message: "the node was replaced by a new config"}
	errLoginTimeout    = &C.LoginError{Code: C.LoginTimeout, Message: "no result yet; check the status of the node later"}
	errLoginLoggedIn   = &C.LoginError{Code: C.LoginLoggedIn, Message: "the node is logged in; log it out first"}

	// tells the outbounds of this process apart from those of an earlier one
	tailscaleLoginNonce = func() string {
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		return hex.EncodeToString(b)
	}()
)

// tailscaleLogin is what an outbound keeps for the logins of its device at run time.
type tailscaleLogin struct {
	opOnce sync.Once
	op     chan struct{} // held while a login or logout changes the backend

	mutex          sync.Mutex
	cancelRound    context.CancelCauseFunc // of the login or logout in progress
	cancelExitNode context.CancelFunc      // of the wait to set the exit node after a login
	unpublished    bool
	takenOverBy    string   // the outbound that took the state dir over and closed this one
	exitNodeError  string   // of setting the exit node after the latest login, redacted
	keys           []string // every auth key given, to redact until the outbound is gone
}

func (l *tailscaleLogin) lockOp(ctx context.Context) error {
	l.opOnce.Do(func() { l.op = make(chan struct{}, 1) })
	select {
	case l.op <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *tailscaleLogin) unlockOp() {
	<-l.op
}

// remember keeps an auth key to redact, with the key a wrapped one carries, which is what the
// client sends to register.
func (l *tailscaleLogin) remember(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	l.mutex.Lock()
	defer l.mutex.Unlock()
	keys := []string{key}
	if base, _, wrapped := strings.Cut(key, "--TL"); wrapped && base != "" {
		keys = append(keys, base)
	}
next:
	for _, key := range keys {
		for _, k := range l.keys {
			if k == key {
				continue next
			}
		}
		l.keys = append(l.keys, key)
	}
}

// rememberConfigKey keeps the auth key the server starts with: the one of the config, or as
// tsnet falls back to, of the environment.
func (l *tailscaleLogin) rememberConfigKey(key string) {
	if key == "" {
		key = os.Getenv("TS_AUTHKEY")
	}
	if key == "" {
		key = os.Getenv("TS_AUTH_KEY")
	}
	l.remember(key)
}

// redact hides the auth keys the outbound was given. The client that registered with one
// keeps it and retries after a failure, and the backend logs the errors it records later,
// so a key is hidden for as long as the outbound lives. Every place a key occurs is found in
// the original text before any is replaced, so that a key that contains or overlaps another
// is hidden whole.
func (l *tailscaleLogin) redact(s string) string {
	l.mutex.Lock()
	keys := l.keys
	l.mutex.Unlock()

	var hidden []bool
	for _, key := range keys {
		for from := 0; ; {
			i := strings.Index(s[from:], key)
			if i < 0 {
				break
			}
			if hidden == nil {
				hidden = make([]bool, len(s))
			}
			for j := from + i; j < from+i+len(key); j++ {
				hidden[j] = true
			}
			from += i + 1
		}
	}
	if hidden == nil {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if !hidden[i] {
			b.WriteByte(s[i])
			i++
			continue
		}
		b.WriteString("***")
		for i < len(s) && hidden[i] {
			i++
		}
	}
	return b.String()
}

func (l *tailscaleLogin) takenOver(by string) {
	l.mutex.Lock()
	l.takenOverBy = by
	l.mutex.Unlock()
}

func (l *tailscaleLogin) setExitNodeError(s string) {
	l.mutex.Lock()
	l.exitNodeError = s
	l.mutex.Unlock()
}

// stopExitNodeWait ends the wait of the last login to set the exit node.
func (l *tailscaleLogin) stopExitNodeWait() {
	l.mutex.Lock()
	cancel := l.cancelExitNode
	l.cancelExitNode = nil
	l.mutex.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (t *Tailscale) LoginInstance() string {
	return fmt.Sprintf("%s-%d", tailscaleLoginNonce, t.serial)
}

func (t *Tailscale) LoginConfig() C.LoginConfig {
	return C.LoginConfig{
		ControlURL:    t.option.ControlURL,
		StateDir:      t.option.StateDir,
		Ephemeral:     t.option.Ephemeral,
		ConfigAuthKey: t.option.AuthKey != "",
	}
}

func (t *Tailscale) LoginStatus(ctx context.Context) C.LoginStatus {
	if t.closed() {
		t.login.mutex.Lock()
		takenOverBy := t.login.takenOverBy
		t.login.mutex.Unlock()
		if takenOverBy != "" {
			return C.LoginStatus{State: C.LoginFailed, Error: fmt.Sprintf("the state-dir was taken over by %s", takenOverBy)}
		}
		return C.LoginStatus{State: C.LoginFailed, Error: errTailscaleClosed.Error()}
	}
	if !t.startClaimed.Load() {
		return C.LoginStatus{State: C.LoginNotStarted}
	}
	select {
	case <-t.startDone:
	default:
		return C.LoginStatus{State: C.LoginStarting}
	}
	if t.startErr != nil {
		return C.LoginStatus{State: C.LoginFailed, Error: t.login.redact(t.startErr.Error())}
	}
	select {
	case <-t.backendInitCh:
		if t.backendInitErr != nil {
			return C.LoginStatus{State: C.LoginFailed, Error: t.login.redact(t.backendInitErr.Error())}
		}
	default:
		return C.LoginStatus{State: C.LoginStarting}
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	status, err := t.backendStatus(ctx)
	if err != nil {
		return C.LoginStatus{State: C.LoginStarting, Error: t.login.redact(err.Error())}
	}
	return status
}

// backendStatus tells the state of the device, with the error of the login the backend
// tries now, which it keeps retrying after a request ends, or else of setting the exit node.
func (t *Tailscale) backendStatus(ctx context.Context) (C.LoginStatus, error) {
	lc, err := t.server.LocalClient()
	if err != nil {
		return C.LoginStatus{}, err
	}
	st, err := lc.StatusWithoutPeers(ctx)
	if err != nil {
		return C.LoginStatus{}, err
	}
	status := tailscaleLoginStatus(st)
	status.Error = t.login.redact(t.loginHealthError())
	if status.Error == "" {
		t.login.mutex.Lock()
		status.Error = t.login.exitNodeError
		t.login.mutex.Unlock()
	}
	if status.State == C.LoginNeedsLogin {
		status.AuthURL = st.AuthURL
	}
	return status, nil
}

func tailscaleLoginStatus(st *ipnstate.Status) C.LoginStatus {
	status := C.LoginStatus{Identity: st.HaveNodeKey}
	switch st.BackendState {
	case ipn.NeedsLogin.String():
		status.State = C.LoginNeedsLogin
	case ipn.NeedsMachineAuth.String():
		status.State = C.LoginNeedsApproval
	case ipn.Running.String():
		status.State = C.LoginRunning
	case ipn.Stopped.String():
		status.State = C.LoginStopped
	default:
		status.State = C.LoginStarting
	}
	if self := st.Self; self != nil && st.HaveNodeKey {
		status.Device = self.HostName
		status.KeyExpiry = self.KeyExpiry
		if user, ok := st.User[self.UserID]; ok {
			status.User = user.LoginName
		}
	}
	if st.HaveNodeKey {
		for _, ip := range st.TailscaleIPs {
			status.Addresses = append(status.Addresses, ip.String())
		}
		if st.CurrentTailnet != nil {
			status.Tailnet = st.CurrentTailnet.Name
		}
	}
	return status
}

// beginLoginRound starts a login or logout, which a newer one of the same outbound takes
// over from, and which ends once the outbound is replaced or closed, or after timeout.
func (t *Tailscale) beginLoginRound(ctx context.Context, instance string, timeout time.Duration) (context.Context, func(), error) {
	if instance != t.LoginInstance() {
		return nil, nil, errLoginReplaced
	}
	ctx, cancel := context.WithCancelCause(ctx)
	t.login.mutex.Lock()
	if t.login.unpublished || t.closed() {
		t.login.mutex.Unlock()
		cancel(nil)
		return nil, nil, errLoginReplaced
	}
	if t.login.cancelRound != nil {
		t.login.cancelRound(errLoginSuperseded)
	}
	t.login.cancelRound = cancel
	t.login.mutex.Unlock()

	timer := time.AfterFunc(timeout, func() { cancel(errLoginTimeout) })
	stop := contextutils.AfterFunc(t.ctx, func() { cancel(errLoginReplaced) })
	return ctx, func() {
		timer.Stop()
		stop()
		cancel(nil)
	}, nil
}

func (t *Tailscale) unpublished() {
	t.login.mutex.Lock()
	defer t.login.mutex.Unlock()
	t.login.unpublished = true
	if t.login.cancelRound != nil {
		t.login.cancelRound(errLoginReplaced)
	}
}

// loginError tells why a login or logout under ctx ended with err.
func (t *Tailscale) loginError(ctx context.Context, err error) error {
	var loginErr *C.LoginError
	if errors.As(err, &loginErr) {
		return loginErr
	}
	if cause := context.Cause(ctx); cause != nil {
		if errors.As(cause, &loginErr) {
			return loginErr
		}
	}
	return &C.LoginError{Code: C.LoginOtherError, Message: t.login.redact(err.Error())}
}

// runLoginOp runs op of a login or logout under ctx, holding the op lock of the outbound
// until op ends even if ctx ends first. op waits on ctx, but changes the backend without it,
// as the local API carries a change through whether or not the request is still there; so a
// change is never cut short and two never overlap. The caller gets the result of op, or why
// ctx ended.
func (t *Tailscale) runLoginOp(ctx context.Context, op func() (C.LoginStatus, error)) (C.LoginStatus, error) {
	if err := t.login.lockOp(ctx); err != nil {
		return C.LoginStatus{}, t.loginError(ctx, err)
	}
	type result struct {
		status C.LoginStatus
		err    error
	}
	done := make(chan result, 1)
	go func() {
		defer t.login.unlockOp()
		status, err := op()
		if err == nil && ctx.Err() != nil {
			err = t.loginError(ctx, ctx.Err())
		}
		done <- result{status, err}
	}()
	select {
	case r := <-done:
		return r.status, r.err
	case <-ctx.Done():
		return C.LoginStatus{}, t.loginError(ctx, ctx.Err())
	}
}

func (t *Tailscale) startForLogin(ctx context.Context) error {
	if err := t.ensureStarted(ctx); err != nil {
		if context.Cause(ctx) != nil {
			return t.loginError(ctx, err)
		}
		if errors.Is(err, errTailscaleClosed) {
			return errLoginReplaced
		}
		return &C.LoginError{Code: C.LoginStartFailed, Message: t.login.redact(err.Error())}
	}
	return nil
}

// loginPrefs are the prefs the server starts with, as tsnet sets them on start. A logout
// replaces them with the defaults, so a login sets them again.
func (t *Tailscale) loginPrefs() *ipn.Prefs {
	prefs := ipn.NewPrefs()
	prefs.Hostname = t.option.Hostname
	if prefs.Hostname == "" {
		prefs.Hostname = "tsnet"
		if exe, err := os.Executable(); err == nil {
			prefs.Hostname = strings.TrimSuffix(strings.ToLower(filepath.Base(exe)), ".exe")
		}
	}
	prefs.WantRunning = true
	prefs.ControlURL = t.option.ControlURL
	if prefs.ControlURL == "" {
		prefs.ControlURL = os.Getenv("TS_CONTROL_URL")
	}
	return prefs
}

func (t *Tailscale) Login(ctx context.Context, instance, authKey string) (C.LoginStatus, error) {
	authKey = strings.TrimSpace(authKey)
	if authKey != "" {
		t.login.remember(authKey)
	}
	timeout := tailscaleLoginLinkTimeout
	if authKey != "" {
		timeout = tailscaleLoginKeyTimeout
	}
	ctx, done, err := t.beginLoginRound(ctx, instance, timeout)
	if err != nil {
		return C.LoginStatus{}, err
	}
	defer done()

	return t.runLoginOp(ctx, func() (C.LoginStatus, error) {
		if err := t.startForLogin(ctx); err != nil {
			return C.LoginStatus{}, err
		}
		lc, err := t.server.LocalClient()
		if err != nil {
			return C.LoginStatus{}, t.loginError(ctx, err)
		}
		current, err := t.backendStatus(ctx)
		if err != nil {
			return C.LoginStatus{}, t.loginError(ctx, err)
		}
		if current.Identity && authKey != "" {
			return current, errLoginLoggedIn
		}
		if current.State == C.LoginRunning {
			return current, nil
		}
		if err := t.restartLogin(lc, authKey); err != nil {
			return C.LoginStatus{}, t.loginError(ctx, err)
		}
		return t.waitLogin(ctx, authKey != "")
	})
}

// restartLogin starts a new round of the login from a clean state: the control client of
// the last round stops and the login error it left is cleared before a new client starts,
// so whatever error shows up afterwards is this round's. A new client also drops the auth
// key the last round or the config gave.
func (t *Tailscale) restartLogin(lc *local.Client, authKey string) error {
	ctx := context.Background()
	if err := lc.DisconnectControl(ctx); err != nil {
		return err
	}
	if authKey != "" {
		// the last client may have registered the device just before it stopped
		st, err := lc.StatusWithoutPeers(ctx)
		if err != nil {
			return err
		}
		if st.HaveNodeKey {
			if err := lc.Start(ctx, ipn.Options{}); err != nil {
				return err
			}
			return errLoginLoggedIn
		}
	}
	t.login.stopExitNodeWait()
	t.login.setExitNodeError("")
	tracker := t.server.Sys().HealthTracker.Get()
	tracker.SetAuthRoutineInError(nil)
	if !forgetLastNoiseDial(tracker) {
		log.Warnln("[Tailscale](%s) cannot clear the time control was last dialed; a login right after another may only try port 443", t.Name())
	}
	if err := lc.Start(ctx, ipn.Options{AuthKey: authKey, UpdatePrefs: t.loginPrefs()}); err != nil {
		return err
	}
	if mp, err := buildTailscaleMaskedPrefs(t.option); err != nil {
		return err
	} else if mp != nil {
		if _, err := lc.EditPrefs(ctx, mp); err != nil {
			return err
		}
	}
	t.setExitNodeOnRun(lc)
	st, err := lc.StatusWithoutPeers(ctx)
	if err != nil {
		return err
	}
	if st.BackendState == ipn.NeedsLogin.String() {
		return lc.StartLoginInteractive(ctx)
	}
	return nil
}

// forgetLastNoiseDial makes the next dial of the control server not count as a retry. A
// control client that dials less than two minutes after the last dial takes that one for a
// failure and tries only port 443; a login starts a new client right after the one before
// dialed, which with a control server served over HTTP on another port fails every time.
// The time is kept in an unexported field of the tracker, cleared here under its lock; false
// when the tracker no longer has such a field.
func forgetLastNoiseDial(tracker *health.Tracker) bool {
	v := reflect.ValueOf(tracker).Elem()
	mu, last := v.FieldByName("mu"), v.FieldByName("lastNoiseDial")
	if !mu.IsValid() || mu.Type() != reflect.TypeOf((*sync.Mutex)(nil)).Elem() ||
		!last.IsValid() || last.Type() != reflect.TypeOf(time.Time{}) {
		return false
	}
	lock := (*sync.Mutex)(unsafe.Pointer(mu.UnsafeAddr()))
	lock.Lock()
	*(*time.Time)(unsafe.Pointer(last.UnsafeAddr())) = time.Time{}
	lock.Unlock()
	return true
}

// setExitNodeOnRun sets the exit node of the config once the node runs after a login, which
// may finish long after the request, as through a link or an approval. The start of the
// outbound sets it only for the first run. The wait gives way to a newer login or logout,
// and ends with the outbound.
func (t *Tailscale) setExitNodeOnRun(lc *local.Client) {
	if !tailscaleExitNodeNeedsStatus(t.option) {
		return
	}
	ctx, cancel := context.WithCancel(t.ctx)
	t.login.mutex.Lock()
	t.login.cancelExitNode = cancel
	t.login.mutex.Unlock()
	watcher, err := lc.WatchIPNBus(ctx, ipn.NotifyInitialState)
	if err != nil {
		cancel()
		log.Warnln("[Tailscale](%s) watch for the node to run after a login: %v", t.Name(), err)
		return
	}
	go func() {
		defer cancel()
		defer watcher.Close()
		for {
			n, err := watcher.Next()
			if err != nil {
				return
			}
			if n.State != nil && *n.State == ipn.Running {
				break
			}
		}
		if err := t.applyExitNodePrefs(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			message := t.login.redact(fmt.Sprintf("set exit node: %v", err))
			t.login.setExitNodeError(message)
			log.Warnln("[Tailscale](%s) %s", t.Name(), message)
		}
	}()
}

// waitLogin waits for the result of the round: with an auth key, the node running, waiting
// for approval, or failing; without one, a URL to log in with.
func (t *Tailscale) waitLogin(ctx context.Context, withKey bool) (C.LoginStatus, error) {
	ticker := time.NewTicker(tailscaleLoginPoll)
	defer ticker.Stop()
	for {
		status, err := t.backendStatus(ctx)
		if err == nil {
			if loginErr := t.loginHealthError(); loginErr != "" {
				return status, &C.LoginError{Code: C.LoginRejected, Message: t.login.redact(loginErr)}
			}
			switch {
			case status.State == C.LoginRunning, status.State == C.LoginNeedsApproval:
				return status, nil
			case !withKey && status.AuthURL != "":
				return status, nil
			}
		}
		select {
		case <-ctx.Done():
			return status, t.loginError(ctx, ctx.Err())
		case <-ticker.C:
		}
	}
}

// loginHealthError returns the error of the login the backend tries now.
func (t *Tailscale) loginHealthError() string {
	state := t.server.Sys().HealthTracker.Get().CurrentState()
	if unhealthy, ok := state.Warnings[health.LoginStateWarnable.Code]; ok {
		return unhealthy.Args[health.ArgError]
	}
	return ""
}

func (t *Tailscale) Logout(ctx context.Context, instance string) (C.LoginStatus, error) {
	ctx, done, err := t.beginLoginRound(ctx, instance, tailscaleLogoutTimeout)
	if err != nil {
		return C.LoginStatus{}, err
	}
	defer done()

	return t.runLoginOp(ctx, func() (C.LoginStatus, error) {
		if err := t.startForLogin(ctx); err != nil {
			return C.LoginStatus{}, err
		}
		lc, err := t.server.LocalClient()
		if err != nil {
			return C.LoginStatus{}, t.loginError(ctx, err)
		}
		current, err := t.backendStatus(ctx)
		if err != nil {
			return C.LoginStatus{}, t.loginError(ctx, err)
		}
		if !current.Identity {
			return current, nil
		}
		t.login.stopExitNodeWait()
		if err := lc.Logout(context.Background()); err != nil {
			return C.LoginStatus{}, t.loginError(ctx, err)
		}
		t.login.setExitNodeError("")
		status, err := t.backendStatus(ctx)
		if err != nil {
			return C.LoginStatus{}, t.loginError(ctx, err)
		}
		return status, nil
	})
}
