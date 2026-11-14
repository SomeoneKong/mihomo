//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/tailscale/health"
	"github.com/stretchr/testify/require"
)

func loginErrorCode(t *testing.T, err error) C.LoginErrorCode {
	t.Helper()
	var loginErr *C.LoginError
	require.True(t, errors.As(err, &loginErr), "not a login error: %v", err)
	return loginErr.Code
}

func TestTailscaleLoginStatusDoesNotStartTheOutbound(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	require.Equal(t, C.LoginNotStarted, ts.LoginStatus(context.Background()).State)
	require.False(t, ts.startClaimed.Load())
	_, err := os.Stat(ts.option.StateDir)
	require.True(t, os.IsNotExist(err), "the state dir is not created")
}

func TestTailscaleLoginConfigTellsOfAnAuthKeyWithoutIt(t *testing.T) {
	useTestHome(t)
	ts, err := NewTailscale(TailscaleOption{Name: "ts", StateDir: "state", AuthKey: "tskey-auth-secret", ControlURL: "http://hs.example", Ephemeral: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ts.Close() })

	config := ts.LoginConfig()
	require.True(t, config.ConfigAuthKey)
	require.True(t, config.Ephemeral)
	require.Equal(t, "http://hs.example", config.ControlURL)
	require.Equal(t, ts.option.StateDir, config.StateDir)
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "tskey-auth-secret")
}

func TestTailscaleLoginOfAnotherInstanceIsRefused(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	_, err := ts.Login(context.Background(), "someone-else", "tskey-auth-1")
	require.Equal(t, C.LoginReplaced, loginErrorCode(t, err))
	_, err = ts.Logout(context.Background(), "someone-else")
	require.Equal(t, C.LoginReplaced, loginErrorCode(t, err))
	require.False(t, ts.startClaimed.Load(), "a refused login starts nothing")
}

func TestTailscaleInstancesDiffer(t *testing.T) {
	a := newTestTailscale(t, "ts", "a")
	b := newTestTailscale(t, "ts", "b")
	require.NotEqual(t, a.LoginInstance(), b.LoginInstance())
	require.Equal(t, a.LoginInstance(), a.LoginInstance())
}

func TestTailscaleUnpublishedOutboundRefusesLogins(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	Unpublished(ts)
	_, err := ts.Login(context.Background(), ts.LoginInstance(), "")
	require.Equal(t, C.LoginReplaced, loginErrorCode(t, err))
	require.False(t, ts.startClaimed.Load())
}

func TestTailscaleLoginRoundGivesWay(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	first, doneFirst, err := ts.beginLoginRound(context.Background(), ts.LoginInstance(), time.Minute)
	require.NoError(t, err)
	defer doneFirst()
	second, doneSecond, err := ts.beginLoginRound(context.Background(), ts.LoginInstance(), time.Minute)
	require.NoError(t, err)
	defer doneSecond()

	<-first.Done()
	require.Equal(t, C.LoginSuperseded, loginErrorCode(t, context.Cause(first)), "a newer round takes over")
	require.NoError(t, second.Err())

	ts.unpublished()
	<-second.Done()
	require.Equal(t, C.LoginReplaced, loginErrorCode(t, context.Cause(second)), "a new config replaces the node")
}

func TestTailscaleLoginRoundEndsWhenTheOutboundCloses(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	ctx, done, err := ts.beginLoginRound(context.Background(), ts.LoginInstance(), time.Minute)
	require.NoError(t, err)
	defer done()
	require.NoError(t, ts.Close())
	<-ctx.Done()
	require.Equal(t, C.LoginReplaced, loginErrorCode(t, context.Cause(ctx)))
}

func TestTailscaleLoginRoundTimesOut(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	ctx, done, err := ts.beginLoginRound(context.Background(), ts.LoginInstance(), 20*time.Millisecond)
	require.NoError(t, err)
	defer done()
	<-ctx.Done()
	require.Equal(t, C.LoginTimeout, loginErrorCode(t, context.Cause(ctx)))
}

func TestTailscaleTakenOverOutboundShowsWhoTookItOver(t *testing.T) {
	older := newTestTailscale(t, "older", "state")
	newer := newTestTailscale(t, "newer", "state")
	require.NoError(t, older.takeOverState())
	require.NoError(t, newer.takeOverState())

	status := older.LoginStatus(context.Background())
	require.Equal(t, C.LoginFailed, status.State)
	require.Contains(t, status.Error, "newer")
}

func TestTailscaleLoginRedactsEveryKeyGiven(t *testing.T) {
	var l tailscaleLogin
	l.remember("tskey-auth-first")
	l.remember("tskey-auth-second")
	l.remember("tskey-auth-first")
	got := l.redact("register: tskey-auth-first was refused, then tskey-auth-second")
	require.Equal(t, "register: *** was refused, then ***", got)
	require.Len(t, l.keys, 2)
	require.False(t, strings.Contains(got, "tskey"))
}

func TestTailscaleLoginRedactsKeysThatContainOrOverlapAnother(t *testing.T) {
	cases := []struct {
		keys []string
		text string
		want string
	}{
		{[]string{"tskey-auth-", "tskey-auth-secret"}, "register: tskey-auth-secret", "register: ***"},
		{[]string{"tskey-auth-secret", "tskey-auth-"}, "register: tskey-auth-secret", "register: ***"},
		{[]string{"abcdef", "defghi"}, "x abcdefghi y", "x *** y"},
		{[]string{"aa"}, "aaa b aa", "*** b ***"},
	}
	for _, c := range cases {
		var l tailscaleLogin
		for _, key := range c.keys {
			l.remember(key)
		}
		require.Equal(t, c.want, l.redact(c.text), c.keys)
	}
}

func TestTailscaleLoginRedactsTheKeysTheServerStartsWith(t *testing.T) {
	useTestHome(t)
	ts, err := NewTailscale(TailscaleOption{Name: "ts", StateDir: "state", AuthKey: " tskey-auth-config "})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ts.Close() })
	require.Equal(t, "register: ***", ts.login.redact("register: tskey-auth-config"))

	t.Setenv("TS_AUTHKEY", "tskey-auth-environment")
	fromEnv := newTestTailscale(t, "env", "env")
	require.Equal(t, "register: ***", fromEnv.login.redact("register: tskey-auth-environment"))
}

func TestTailscaleLoginRedactsTheKeyAWrappedKeyCarries(t *testing.T) {
	var l tailscaleLogin
	l.remember("tskey-auth-base--TLsignature-private")
	require.Equal(t, "register: ***", l.redact("register: tskey-auth-base"))
	require.Equal(t, "got ***", l.redact("got tskey-auth-base--TLsignature-private"))
}

func TestTailscaleLoginOpHoldsTheLockUntilItEnds(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	release := make(chan struct{})
	ended := make(chan struct{})
	started := time.Now()
	_, err := ts.runLoginOp(ctx, func() (C.LoginStatus, error) {
		defer close(ended)
		<-release
		return C.LoginStatus{State: C.LoginRunning}, nil
	})
	require.Error(t, err, "the caller gets why its context ended")
	require.Less(t, time.Since(started), time.Second)

	next, cancelNext := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelNext()
	require.Error(t, ts.login.lockOp(next), "no other op starts while the change goes on")

	close(release)
	<-ended
	locked, cancelLocked := context.WithTimeout(context.Background(), time.Second)
	defer cancelLocked()
	require.NoError(t, ts.login.lockOp(locked), "the lock is released once the change ends")
	ts.login.unlockOp()
}

func TestTailscaleCloseWaitsForAChangeOfTheBackendToEnd(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	opDone := make(chan struct{})
	go func() {
		defer close(opDone)
		_, _ = ts.runLoginOp(ctx, func() (C.LoginStatus, error) {
			<-release
			return C.LoginStatus{}, nil
		})
	}()
	// the op holds the lock once another can no longer take it
	require.Eventually(t, func() bool {
		probe, cancelProbe := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancelProbe()
		if ts.login.lockOp(probe) != nil {
			return true
		}
		ts.login.unlockOp()
		return false
	}, time.Second, time.Millisecond)
	cancel()
	<-opDone

	closed := make(chan struct{})
	go func() {
		_ = ts.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("closed while a change of the backend went on")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("not closed once the change ended")
	}
}

func TestTailscaleLoginOpThatEndsAfterItsRoundIsNoSuccess(t *testing.T) {
	ts := newTestTailscale(t, "ts", "state")
	ctx, done, err := ts.beginLoginRound(context.Background(), ts.LoginInstance(), time.Minute)
	require.NoError(t, err)
	defer done()
	_, err = ts.runLoginOp(ctx, func() (C.LoginStatus, error) {
		ts.unpublished()
		return C.LoginStatus{State: C.LoginRunning}, nil
	})
	require.Equal(t, C.LoginReplaced, loginErrorCode(t, err))
}

func TestTailscaleLoginForgetsTheLastDialOfControl(t *testing.T) {
	tracker := new(health.Tracker)
	require.False(t, tracker.LastNoiseDialWasRecent())
	require.True(t, tracker.LastNoiseDialWasRecent(), "a dial right after another counts as a retry")
	require.True(t, forgetLastNoiseDial(tracker), "the tracker keeps the time where it is cleared")
	require.False(t, tracker.LastNoiseDialWasRecent(), "the next dial tries the port of the control URL")
}

func TestTailscaleLoginPrefsSetWhatTsnetSetsOnStart(t *testing.T) {
	useTestHome(t)
	ts, err := NewTailscale(TailscaleOption{Name: "ts", StateDir: "state", Hostname: "laptop", ControlURL: "http://hs.example"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ts.Close() })
	prefs := ts.loginPrefs()
	require.Equal(t, "laptop", prefs.Hostname)
	require.Equal(t, "http://hs.example", prefs.ControlURL)
	require.True(t, prefs.WantRunning)

	unnamed := newTestTailscale(t, "unnamed", "other")
	require.NotEmpty(t, unnamed.loginPrefs().Hostname, "the name of the program, as tsnet uses")
}
