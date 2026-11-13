package route

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

// slowBody holds its content back until released, as a request still being sent.
type slowBody struct {
	reading chan struct{}
	release chan struct{}
	content io.Reader
}

func (b *slowBody) Read(p []byte) (int, error) {
	select {
	case <-b.reading:
	default:
		close(b.reading)
	}
	<-b.release
	return b.content.Read(p)
}

func (b *slowBody) Close() error { return nil }

// orderRecorder notes when the update answers, which it does before letting the next
// one through.
type orderRecorder struct {
	*httptest.ResponseRecorder
	name  string
	mu    *sync.Mutex
	order *[]string
}

func (r *orderRecorder) WriteHeader(code int) {
	r.mu.Lock()
	*r.order = append(*r.order, r.name)
	r.mu.Unlock()
	r.ResponseRecorder.WriteHeader(code)
}

func TestConfigUpdatesGoInTheOrderTheyCome(t *testing.T) {
	// payloads that fail to parse: only the order of the updates matters here
	body := &slowBody{reading: make(chan struct{}), release: make(chan struct{}), content: strings.NewReader(`{"payload":"proxies: ["}`)}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(body.release) }) }
	t.Cleanup(release)
	first := httptest.NewRequest(http.MethodPut, "/configs", nil)
	first.Body = body
	second := httptest.NewRequest(http.MethodPut, "/configs", strings.NewReader(`{"payload":"proxies: ["}`))

	var (
		mu    sync.Mutex
		order []string
		wg    sync.WaitGroup
	)
	answered := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), order...)
	}
	update := func(name string, r *http.Request) {
		defer wg.Done()
		updateConfigs(&orderRecorder{httptest.NewRecorder(), name, &mu, &order}, r)
	}
	wg.Add(2)
	go update("first", first)
	<-body.reading
	go update("second", second)
	time.Sleep(100 * time.Millisecond)
	if got := answered(); len(got) != 0 {
		t.Fatalf("%v went through while the first update was still being received", got)
	}
	release()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("an update never went through")
	}
	if got := answered(); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("the updates went through in the order %v", got)
	}
}

func TestStalledConfigUpdateDoesNotHoldOthersOff(t *testing.T) {
	timeout := configBodyTimeout
	configBodyTimeout = 200 * time.Millisecond
	t.Cleanup(func() { configBodyTimeout = timeout })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			patchConfigs(w, r)
		} else {
			updateConfigs(w, r)
		}
	}))
	t.Cleanup(server.Close)
	address := strings.TrimPrefix(server.URL, "http://")

	payload := `{"payload":"proxies: ["}`
	for method, partial := range map[string]string{
		http.MethodPut:   payload,          // a complete JSON object
		http.MethodPatch: `{"mode":"rule"`, // an object not yet closed
	} {
		// the rest of the body announced never comes
		stalled, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = stalled.Close() })
		if _, err := io.WriteString(stalled, method+" / HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n"+partial); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond) // queued first

		next, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = next.Close() })
		if _, err := io.WriteString(next, "PUT / HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: "+strconv.Itoa(len(payload))+"\r\n\r\n"+payload); err != nil {
			t.Fatal(err)
		}
		_ = next.SetReadDeadline(time.Now().Add(5 * time.Second))
		status, err := bufio.NewReader(next).ReadString('\n')
		if err != nil {
			t.Fatalf("the update after a stalled %s never went through: %v", method, err)
		}
		if !strings.Contains(status, "400") {
			t.Fatalf("unexpected answer %q after a stalled %s", status, method)
		}
	}
}
