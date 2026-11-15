package probe

import "fmt"

// Text is a text of the records shown by a GUI, in Chinese and English.
type Text map[string]string

// T returns a text in Chinese and English.
func T(zh, en string) Text {
	return Text{"zh": zh, "en": en}
}

// Tf formats a text in Chinese and English with the same arguments.
func Tf(zh, en string, args ...any) Text {
	return Text{"zh": fmt.Sprintf(zh, args...), "en": fmt.Sprintf(en, args...)}
}

// Display levels.
const (
	LevelOK    = "ok"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
	LevelMuted = "muted"
)

// DisplayMember names a member an event shown on a timeline involves.
type DisplayMember struct {
	Name     string `json:"name"`
	Provider string `json:"provider"`
	Object   uint64 `json:"object"`
}

// Display is what a GUI shows of an event on its timeline. It is for showing only: the other
// fields of the event are what it is analyzed by.
func Display(category, level string, groups []string, members []DisplayMember, text Text) map[string]any {
	scope := map[string]any{"kind": "global"}
	if groups != nil {
		scope = map[string]any{"kind": "groups", "groups": groups}
	}
	if members == nil {
		members = []DisplayMember{}
	}
	display := map[string]any{
		"v": DisplayVersion, "category": category, "level": level, "scope": scope, "members": members, "text": text,
	}
	if name, ok := categoryNames[category]; ok {
		display["categoryText"] = name
	}
	return display
}

// categoryNames name the categories of the events a GUI shows.
var categoryNames = map[string]Text{
	"selection": T("选路", "routing"),
	"switch":    T("切换", "switch"),
	"pin":       T("固定", "pin"),
	"quality":   T("质量", "quality"),
	"search":    T("故障寻找", "search"),
	"manual":    T("手动测试", "manual test"),
	"network":   T("本机网络", "local network"),
	"outage":    T("流量池故障", "pool outage"),
}

func networkDisplay(state NetworkState, reason string) map[string]any {
	switch state {
	case NetworkDown:
		return Display("network", LevelError, nil, nil, T(
			"疑似本机断网：自动探测限流，各成员的失效不计入质量记忆",
			"The local network seems down: automatic tests are throttled and failures do not count against members"))
	case NetworkPoor:
		return Display("network", LevelWarn, nil, nil, T(
			"本机网络不佳：探测照常，各成员的失效不计入质量记忆",
			"The local network is poor: tests go on, failures do not count against members"))
	}
	return Display("network", LevelOK, nil, nil, Tf(
		"本机网络恢复正常（%s）", "The local network is normal again (%s)", reason))
}

func outageDisplay(o *outage, groups []string) map[string]any {
	if groups == nil {
		groups = []string{}
	}
	switch o.state {
	case OutageSuspected:
		return Display("outage", LevelWarn, groups, nil, Tf(
			"疑似流量池 %s 整体故障：%d 个节点相继失效，只轮流重测其中一个",
			"Traffic pool %s seems out: %d nodes failed one after another, one of them is tested again at a time",
			o.key.pool, len(o.dead)))
	case OutageEstablished:
		return Display("outage", LevelInfo, groups, nil, Tf(
			"流量池 %s 的整体故障已恢复，计入该流量池一次",
			"Traffic pool %s is back from its outage, counted once against the pool", o.key.pool))
	case OutageUndecided:
		return Display("outage", LevelInfo, groups, nil, Tf(
			"流量池 %s 的疑似整体故障无法判定（节点已移除或本机网络异常），不计入任何一方",
			"The suspected outage of traffic pool %s cannot be decided (its nodes are gone, or the local network was out): it counts for none", o.key.pool))
	}
	return Display("outage", LevelInfo, groups, nil, Tf(
		"流量池 %s 并非整体故障：池中另一节点仍存活，失效计入各节点",
		"Traffic pool %s was not out: another node of it is alive, the failures count against the nodes", o.key.pool))
}
