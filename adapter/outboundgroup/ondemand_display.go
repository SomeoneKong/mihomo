package outboundgroup

import (
	"fmt"
	"strings"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/exp/slices"
)

// label names a member with its place in the group as the config lists it, which tells apart
// members of like names.
func (od *onDemand) label(p C.Proxy) string {
	if p == nil {
		return "-"
	}
	if i := slices.Index(od.members, p); i >= 0 {
		return fmt.Sprintf("#%d %s", i+1, p.Name())
	}
	return p.Name()
}

func (od *onDemand) labelNamed(name string) string {
	if p := od.memberNamed(name); p != nil {
		return od.label(p)
	}
	return name
}

func (od *onDemand) labels(list []C.Proxy) probe.Text {
	if len(list) == 0 {
		return probe.T("无", "none")
	}
	names := make([]string, 0, len(list))
	for _, p := range list {
		names = append(names, od.label(p))
	}
	return probe.T(strings.Join(names, "、"), strings.Join(names, ", "))
}

// displayMember names a member on a timeline.
func (od *onDemand) displayMember(p C.Proxy) probe.DisplayMember {
	if p == nil {
		return probe.DisplayMember{}
	}
	return probe.DisplayMember{Name: p.Name(), Provider: p.ProxyInfo().ProviderName, Object: od.coord.KnownObjectID(p)}
}

func (od *onDemand) displayMembers(list ...C.Proxy) []probe.DisplayMember {
	members := make([]probe.DisplayMember, 0, len(list))
	for _, p := range list {
		if p != nil {
			members = append(members, od.displayMember(p))
		}
	}
	return members
}

func (od *onDemand) display(category, level string, text probe.Text, members ...C.Proxy) map[string]any {
	return probe.Display(category, level, []string{od.name()}, od.displayMembers(members...), text)
}

// causeText tells why the selection changed.
func causeText(cause selectionCause) probe.Text {
	switch cause.Reason {
	case causeVerify:
		return probe.T("验证判定失效", "verification found it dead")
	case causeContinue:
		return probe.T("切换后的验证判定失效", "the verification after a switch found it dead")
	case causeSearch:
		return probe.T("故障寻找判定失效", "the search found it dead")
	case causeExternal:
		return probe.T("其它来源的探测判定失效", "another test found it dead")
	case causeRecover:
		return probe.T("排在前面的成员恢复可用", "a member before it came back")
	case causeExclusion:
		return probe.T("排除表变化", "the exclusions changed")
	case causePin:
		return probe.T("固定的成员变化", "the pinned member changed")
	case causeReload:
		return probe.T("重载配置", "the config was reloaded")
	case causeTopology:
		return probe.T("成员变化", "the members changed")
	case causeSuspect:
		return probe.T("当前选择暂缓使用：一次拨号中明显落后于热备", "the member in use is set aside: it fell behind a standby in a dial")
	case causeSuspectEnd:
		return probe.T("暂缓结束", "the member in use is no longer set aside")
	case causeDemoted:
		return probe.T("成员反复异常而让位", "a member yields after repeated anomalies")
	case causeTrial:
		return probe.T("成员进入试用", "a member is on trial")
	case causeRestored:
		return probe.T("成员通过试用", "a member passed its trial")
	case causeMemoryCleared:
		return probe.T("策略记忆被清除", "policy memory was cleared")
	case causeDelegate:
		return probe.T("子组的路径都不可用", "none of the paths of the group is available")
	case causePolicy:
		return probe.T("子组因固定、排除或成员变化不可用", "the group is pinned off, excluded inside or lost its members")
	}
	return probe.T("探测结果", "a test result")
}

func (od *onDemand) selectionDisplay(from, to C.Proxy, cause selectionCause) map[string]any {
	why := causeText(cause)
	level := probe.LevelInfo
	switch cause.Reason {
	case causeVerify, causeContinue, causeSearch, causeExternal, causeSuspect, causeDemoted, causeDelegate:
		level = probe.LevelWarn
	}
	if from == nil {
		return od.display("selection", level, probe.Text{
			"zh": "选路为 " + od.label(to) + "：" + why["zh"],
			"en": "Routing set to " + od.label(to) + ": " + why["en"],
		}, to)
	}
	return od.display("selection", level, probe.Text{
		"zh": "选路从 " + od.label(from) + " 换到 " + od.label(to) + "：" + why["zh"],
		"en": "Routing moved from " + od.label(from) + " to " + od.label(to) + ": " + why["en"],
	}, from, to)
}

func (od *onDemand) switchDisplay(from, to dialedMember, cause selectionCause) map[string]any {
	why := causeText(cause)
	return probe.Display("switch", probe.LevelInfo, []string{od.name()}, []probe.DisplayMember{
		{Name: from.name, Provider: from.provider, Object: from.object}, {Name: to.name, Provider: to.provider, Object: to.object},
	}, probe.Text{
		"zh": "真实拨号从 " + od.labelNamed(from.name) + " 换到 " + od.labelNamed(to.name) + "：" + why["zh"],
		"en": "Dialing moved from " + od.labelNamed(from.name) + " to " + od.labelNamed(to.name) + ": " + why["en"],
	})
}

func (od *onDemand) pinDisplay(from, to, source string) map[string]any {
	by := probe.T("用户操作", "by the user")
	if source == "restore" {
		by = probe.T("恢复配置保存的选择", "restored from the config")
	}
	members := od.displayMembers(od.memberNamed(from), od.memberNamed(to))
	var text probe.Text
	switch {
	case to == "":
		text = probe.Tf("取消固定 %s", "Unpinned %s", od.labelNamed(from))
	case from == "":
		text = probe.Tf("固定 %s", "Pinned %s", od.labelNamed(to))
	default:
		text = probe.Tf("固定从 %s 改为 %s", "Pin moved from %s to %s", od.labelNamed(from), od.labelNamed(to))
	}
	text = probe.Text{"zh": text["zh"] + "（" + by["zh"] + "）", "en": text["en"] + " (" + by["en"] + ")"}
	return probe.Display("pin", probe.LevelInfo, []string{od.name()}, members, text)
}

// memberNamed is the member of a name, nil for none.
func (od *onDemand) memberNamed(name string) C.Proxy {
	if name == "" {
		return nil
	}
	for _, p := range od.members {
		if p.Name() == name {
			return p
		}
	}
	return nil
}

func (od *onDemand) standingDisplay(p C.Proxy, q *memberQuality, counts probe.Counts, reason string) map[string]any {
	if reason == "cleared" {
		return od.display("quality", probe.LevelInfo, probe.Tf("%s 恢复正常：策略记忆被清除", "%s is back in normal standing: policy memory was cleared", od.label(p)), p)
	}
	switch q.standing {
	case standingYield:
		if reason == "trialFailed" {
			return od.display("quality", probe.LevelWarn, probe.Tf(
				"%s 试用失败，让位，%s 后可再试用", "%s failed its trial and yields, it may try again at %s",
				od.label(p), q.readmitAt.Local().Format("01-02 15:04")), p)
		}
		return od.display("quality", probe.LevelWarn, probe.Tf(
			"%s 异常过多而让位：8 小时内 %s 次、3 天内 %s 次",
			"%s yields after repeated anomalies: %s in 8 hours, %s in 3 days",
			od.label(p), times(counts.Recent), times(counts.Long)), p)
	case standingTrial:
		why := trialReasonText(reason)
		return od.display("quality", probe.LevelInfo, probe.Text{
			"zh": od.label(p) + " 进入试用（" + why["zh"] + "）：回到原位，拨号时同时拨热备",
			"en": od.label(p) + " is on trial (" + why["en"] + "): back in place, dialed alongside a standby",
		}, p)
	}
	return od.display("quality", probe.LevelOK, probe.Tf("%s 通过试用，恢复正常", "%s passed its trial", od.label(p)), p)
}

func trialReasonText(reason string) probe.Text {
	switch reason {
	case trialPersistent:
		return probe.T("从持续故障中恢复", "back from a persistent failure")
	case trialOutage:
		return probe.T("从流量池整体故障中恢复", "back from an outage of its pool")
	case trialRepeat:
		return probe.T("1 小时内第二次失效", "a second failure within an hour")
	}
	return probe.T("计次回落", "its counts fell")
}

func (od *onDemand) searchDisplay(start bool, member C.Proxy, result string) map[string]any {
	if start {
		return od.display("search", probe.LevelWarn, probe.Tf("%s 不可用，开始故障寻找", "%s is dead, searching", od.label(member)), member)
	}
	if result == "exhausted" {
		return od.display("search", probe.LevelWarn, probe.Tf("故障寻找结束：没有找到可用成员，当前 %s", "The search ended without a working member, now on %s", od.label(member)), member)
	}
	return od.display("search", probe.LevelInfo, probe.Tf("故障寻找结束：找到可用成员 %s", "The search ended on %s, a working member", od.label(member)), member)
}
