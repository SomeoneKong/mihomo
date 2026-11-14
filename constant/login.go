package constant

import (
	"context"
	"time"
)

// LoginState is where an outbound that joins a network as a device of an account is
// with its login.
type LoginState string

const (
	LoginNotStarted    LoginState = "not_started"
	LoginStarting      LoginState = "starting"
	LoginNeedsLogin    LoginState = "needs_login"
	LoginNeedsApproval LoginState = "needs_approval" // waits for the admin of the control server
	LoginRunning       LoginState = "running"
	LoginStopped       LoginState = "stopped"
	LoginFailed        LoginState = "failed"
)

// LoginStatus is the login of an outbound as it is now.
type LoginStatus struct {
	State     LoginState `json:"state"`
	Identity  bool       `json:"identity,omitempty"` // the device has an identity, whatever the state
	Device    string     `json:"device,omitempty"`
	User      string     `json:"user,omitempty"`
	Tailnet   string     `json:"tailnet,omitempty"`
	Addresses []string   `json:"addresses,omitempty"`
	KeyExpiry *time.Time `json:"keyExpiry,omitempty"`
	AuthURL   string     `json:"authUrl,omitempty"`
	Error     string     `json:"error,omitempty"`
}

// LoginConfig is what the config of an outbound says about its login, without secrets.
type LoginConfig struct {
	ControlURL    string `json:"controlUrl,omitempty"`
	StateDir      string `json:"stateDir,omitempty"`
	Ephemeral     bool   `json:"ephemeral,omitempty"`
	ConfigAuthKey bool   `json:"configAuthKey,omitempty"` // the config has an auth key
}

type LoginErrorCode string

const (
	LoginInvalidRequest LoginErrorCode = "invalid_request"
	LoginNotFound       LoginErrorCode = "not_found"
	LoginLoggedIn       LoginErrorCode = "logged_in"
	LoginStartFailed    LoginErrorCode = "start_failed"
	LoginRejected       LoginErrorCode = "rejected"
	LoginSuperseded     LoginErrorCode = "superseded"
	LoginReplaced       LoginErrorCode = "replaced"
	LoginTimeout        LoginErrorCode = "timeout"
	LoginOtherError     LoginErrorCode = "failed"
)

type LoginError struct {
	Code    LoginErrorCode `json:"code"`
	Message string         `json:"message"`
}

func (e *LoginError) Error() string {
	return string(e.Code) + ": " + e.Message
}

// Loginable is an outbound that joins a network as a device of an account, which a user
// logs in at run time.
type Loginable interface {
	// LoginInstance tells this outbound apart from another one of the same name, such as
	// the one a new config replaces it with.
	LoginInstance() string
	LoginConfig() LoginConfig
	// LoginStatus does not start the outbound.
	LoginStatus(ctx context.Context) LoginStatus
	// Login logs the device in with the auth key, or with an empty one gets a URL to log
	// it in with in a browser. It fails with a LoginError.
	Login(ctx context.Context, instance, authKey string) (LoginStatus, error)
	// Logout fails with a LoginError.
	Logout(ctx context.Context, instance string) (LoginStatus, error)
}
