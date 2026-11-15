package outboundgroup

import (
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
)

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

func nameOf(p C.Proxy) string {
	if p == nil {
		return "-"
	}
	return p.Name()
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
		return probe.T("前面的成员恢复", "a preferred member came back")
	case causeExclusion:
		return probe.T("排除表变化", "the exclusions changed")
	case causePin:
		return probe.T("固定的成员变化", "the pinned member changed")
	case causeReload:
		return probe.T("重载配置", "the config was reloaded")
	case causeTopology:
		return probe.T("成员变化", "the members changed")
	case causeSuspect:
		return probe.T("首选成员暂缓：它落后于后备", "the preferred member is set aside: it fell behind a standby")
	case causeSuspectEnd:
		return probe.T("暂缓结束", "the preferred member is no longer set aside")
	case causeDemoted:
		return probe.T("成员反复异常而让位", "a member yields after repeated anomalies")
	case causeTrial:
		return probe.T("成员进入试用", "a member is on trial")
	case causeRestored:
		return probe.T("成员通过试用", "a member passed its trial")
	}
	return probe.T("探测结果", "a test result")
}

func (od *onDemand) selectionDisplay(from, to C.Proxy, cause selectionCause) map[string]any {
	why := causeText(cause)
	level := probe.LevelInfo
	switch cause.Reason {
	case causeVerify, causeContinue, causeSearch, causeExternal, causeSuspect, causeDemoted:
		level = probe.LevelWarn
	}
	return od.display("selection", level, probe.Text{
		"zh": "选路从 " + nameOf(from) + " 换到 " + nameOf(to) + "：" + why["zh"],
		"en": "Routing moved from " + nameOf(from) + " to " + nameOf(to) + ": " + why["en"],
	}, from, to)
}

func (od *onDemand) switchDisplay(from, to dialedMember, cause selectionCause) map[string]any {
	why := causeText(cause)
	return probe.Display("switch", probe.LevelInfo, []string{od.name()}, []probe.DisplayMember{
		{Name: from.name, Provider: from.provider, Object: from.object}, {Name: to.name, Provider: to.provider, Object: to.object},
	}, probe.Text{
		"zh": "真实拨号从 " + from.name + " 换到 " + to.name + "：" + why["zh"],
		"en": "Dialing moved from " + from.name + " to " + to.name + ": " + why["en"],
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
		text = probe.Tf("取消固定 %s", "Unpinned %s", from)
	case from == "":
		text = probe.Tf("固定 %s", "Pinned %s", to)
	default:
		text = probe.Tf("固定从 %s 改为 %s", "Pin moved from %s to %s", from, to)
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
	switch q.standing {
	case standingYield:
		if reason == "trialFailed" {
			return od.display("quality", probe.LevelWarn, probe.Tf(
				"%s 试用失败，让位，%s 后可再试用", "%s failed its trial and yields, it may try again at %s",
				p.Name(), formatTime(q.readmitAt)), p)
		}
		return od.display("quality", probe.LevelWarn, probe.Tf(
			"%s 反复异常而让位：近期计次 %.1f（8 小时）、长期计次 %.1f（3 天）",
			"%s yields after repeated anomalies: %.1f lately (8 hours), %.1f in 3 days",
			p.Name(), counts.Recent, counts.Long), p)
	case standingTrial:
		why := trialReasonText(reason)
		return od.display("quality", probe.LevelInfo, probe.Text{
			"zh": p.Name() + " 进入试用（" + why["zh"] + "）：回到原位，拨号时同时拨后备",
			"en": p.Name() + " is on trial (" + why["en"] + "): back in place, dialed alongside a standby",
		}, p)
	}
	return od.display("quality", probe.LevelOK, probe.Tf("%s 通过试用，恢复正常", "%s passed its trial", p.Name()), p)
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
		return od.display("search", probe.LevelWarn, probe.Tf("%s 失效，开始故障寻找", "%s is dead, searching", nameOf(member)), member)
	}
	return od.display("search", probe.LevelInfo, probe.Tf("故障寻找结束（%s），当前 %s", "The search ended (%s), now on %s", result, nameOf(member)), member)
}
