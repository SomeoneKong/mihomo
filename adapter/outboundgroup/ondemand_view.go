package outboundgroup

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"

	"golang.org/x/exp/slices"
)

// The view of the strategy is made of a few generic elements whose texts the core writes; a
// GUI renders them without knowing what they mean.

type text = probe.Text

func badge(id, level string, t text) map[string]any {
	return map[string]any{"type": "badge", "id": id, "level": level, "text": t}
}

func metric(id, level string, label, t text, value any) map[string]any {
	e := map[string]any{"type": "metric", "id": id, "level": level, "label": label, "text": t}
	if value != nil {
		e["value"] = value
	}
	return e
}

// badgeTip is a badge with what it means, shown on hover.
func badgeTip(id, level string, t, tip text) map[string]any {
	e := badge(id, level, t)
	e["tip"] = tip
	return e
}

// role is the role of a member, with what it means.
func role(id, level string, t, tip text) map[string]any {
	return map[string]any{"id": id, "text": t, "level": level, "tip": tip}
}

func detail(label, t text, level string) map[string]any {
	e := map[string]any{"type": "detail", "label": label, "text": t}
	if level != "" {
		e["level"] = level
	}
	return e
}

func moment(id string, label text, at time.Time) map[string]any {
	return map[string]any{"type": "time", "id": id, "label": label, "at": formatTime(at)}
}

func section(title text, items ...map[string]any) map[string]any {
	list := make([]any, 0, len(items))
	for _, item := range items {
		if item != nil {
			list = append(list, item)
		}
	}
	return map[string]any{"title": title, "items": list}
}

func ago(now, t time.Time) text {
	d := now.Sub(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return probe.Tf("%d 秒前", "%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return probe.Tf("%d 分钟前", "%dm ago", int(d.Minutes()))
	}
	return probe.Tf("%.1f 小时前", "%.1fh ago", d.Hours())
}

// View is the view of the strategy of the on-demand groups of the config in use, for a GUI.
func View() map[string]any {
	lifecycle.Lock()
	groups := make([]*onDemand, 0, len(lifecycle.groups))
	for _, od := range lifecycle.groups {
		groups = append(groups, od)
	}
	lifecycle.Unlock()
	sort.Slice(groups, func(i, j int) bool { return groups[i].name() < groups[j].name() })
	coord := probe.Default
	page := coord.Records().Events(0, 0)
	now := coord.Now()
	var banners []any
	switch state, since := coord.Network(); state {
	case probe.NetworkDown:
		banners = append(banners, badge("network", probe.LevelError, probe.Tf(
			"疑似本机断网（%s 起）：自动探测限流，失效不计入质量记忆，探测成功或首跳检查正常即结束",
			"The local network seems down (since %s): tests are throttled and failures do not count; a successful test or check ends it",
			since.Local().Format("15:04:05"))))
	case probe.NetworkPoor:
		banners = append(banners, badge("network", probe.LevelWarn, probe.Tf(
			"本机网络不佳（%s 起）：探测照常，失效不计入质量记忆，每分钟复查",
			"The local network is poor (since %s): tests go on, failures do not count; checked every minute",
			since.Local().Format("15:04:05"))))
	}
	for _, o := range coord.Outages() {
		banners = append(banners, badge("outage:"+o.Pool, probe.LevelWarn, probe.Tf(
			"疑似流量池 %s 整体故障（%s 起）：只轮流重测其中一个已失效节点；池中其它节点存活即反证，失效节点恢复即成立",
			"Traffic pool %s seems out (since %s): its dead nodes are tested again one at a time; another node alive disproves it, a dead one back establishes it",
			o.Pool, o.First.Local().Format("15:04:05"))))
	}
	if banners == nil {
		banners = []any{}
	}
	list := make([]any, 0, len(groups))
	for _, od := range groups {
		list = append(list, od.strategyView(now))
	}
	return map[string]any{
		"view":        1,
		"instance":    page.Instance,
		"generatedAt": formatTime(now),
		"banners":     banners,
		"groups":      list,
	}
}

func (od *onDemand) strategyView(now time.Time) map[string]any {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	refill, pool := od.refillRange()
	_, trustedCount, target, limited := od.dynamicPool(refill, now)
	var headline []any
	if od.inUseLocked(now) {
		headline = append(headline, badgeTip("use", probe.LevelOK, probe.T("在用", "in use"), probe.T(
			"10 分钟内有真实连接经过本组：维护热备，按连接的失败信号验证成员",
			"real connections went through the group within 10 minutes: standbys are kept tested, failures are verified")))
	} else {
		headline = append(headline, badgeTip("use", probe.LevelMuted, probe.T("空闲", "idle"), probe.T(
			"10 分钟内没有真实连接经过本组：不做维护探测，有连接时再开始",
			"no real connection within 10 minutes: nothing is tested until one comes")))
	}
	if od.network != probe.NetworkNormal {
		headline = append(headline, badgeTip("network", probe.LevelWarn, networkText(od.network), probe.T(
			"本机网络异常期间，各成员的失效不计入质量记忆", "while the local network is out, failures do not count against members")))
	}
	if s := od.suspended; s != nil {
		headline = append(headline, badgeTip("suspended", probe.LevelWarn, probe.Tf(
			"%s 暂缓使用，新连接先走 %s", "%s set aside, new dials start from %s", od.label(s.member), od.label(od.route)), suspendTip))
	}
	if od.search != nil {
		headline = append(headline, badgeTip("search", probe.LevelWarn, probe.T("故障寻找中", "searching"), probe.T(
			"当前选择不可用，正在成批确认后面的成员", "the member in use is dead, the members after it are confirmed in batches")))
	}
	standbyLevel := probe.LevelOK
	if trustedCount < target {
		standbyLevel = probe.LevelWarn
	}
	standbys := probe.Tf("%d/%d", "%d/%d", trustedCount, target)
	if unconfirmed := len(pool) - trustedCount; unconfirmed > 0 {
		standbys = probe.Tf("%d/%d，另有 %d 个未确认", "%d/%d, %d more unconfirmed", trustedCount, target, unconfirmed)
	}
	headline = append(headline, metric("standbys", standbyLevel, probe.T("可用热备", "standbys"), standbys, trustedCount))

	routing := []map[string]any{
		detail(probe.T("当前选择", "in use"), probe.T(od.label(od.current), od.label(od.current)), ""),
	}
	if od.route != od.current {
		why := probe.T("当前选择暂缓使用", "the member in use is set aside")
		if s := od.suspended; s != nil && s.reason == "slowTest" {
			why = probe.Tf("当前选择的验证已超过快速档，热备 %s 只用了 %d ms", "the verification of the member in use ran past the fast tier, standby %s took %d ms", od.label(s.standby), s.standbyElapsed.Milliseconds())
		} else if s != nil {
			why = probe.T("当前选择在一次拨号中明显落后于热备，等待验证结果", "the member in use fell behind a standby in a dial, waiting for its verification")
		}
		routing = append(routing, detail(probe.T("新连接实际走", "new dials go to"), probe.Text{
			"zh": od.label(od.route) + "（" + why["zh"] + "）", "en": od.label(od.route) + " (" + why["en"] + ")",
		}, probe.LevelWarn))
	}
	if d := od.carried.Load(); d != nil {
		name := od.labelNamed(d.name)
		carried := probe.T(name, name)
		if d.hedged {
			carried = probe.Tf("%s（快速档改拨：当前选择没有及时连上）", "%s (by the fast tier: the member in use was late)", name)
		}
		routing = append(routing, detail(probe.T("最近一次连接", "last connection"), carried, ""))
	}
	if od.pinned != "" {
		routing = append(routing, detail(probe.T("手动固定", "pinned"), probe.Tf(
			"%s（固定优先，不受让位与试用影响）", "%s (pinned first, whatever its standing)", od.labelNamed(od.pinned)), probe.LevelInfo))
	}
	if !od.lastSwitch.IsZero() {
		routing = append(routing, moment("lastSwitch", probe.T("最近切换", "last switch"), od.lastSwitch))
	}
	routing = append(routing, detail(probe.T("选择原因", "why"), causeText(od.cause), ""))

	var poolItems []map[string]any
	poolItems = append(poolItems, metric("trusted", standbyLevel, probe.T("已确认可用", "confirmed"),
		probe.Tf("%d / 需要 %d", "%d / %d needed", trustedCount, target), trustedCount))
	if limited {
		// why the standbys of the pool are not trusted, as trusted() decides
		var confirming, obliged, yielding, trying, anomalies int
		for _, p := range pool {
			if od.trusted(p, now) {
				continue
			}
			ev := od.view[p]
			switch {
			case ev.State == probe.Unknown || (ev.State == probe.Alive && !od.fresh(p, now)):
				confirming++
			case od.standingOf(p) == standingYield:
				yielding++
			case od.standingOf(p) == standingTrial:
				trying++
			case od.states[p] != nil && len(od.states[p].signals) > 0:
				obliged++
			default:
				anomalies++
			}
		}
		var zh, en []string
		if confirming > 0 {
			zh, en = append(zh, fmt.Sprintf("%d 个仍在确认", confirming)), append(en, fmt.Sprintf("%d still being confirmed", confirming))
		}
		if yielding > 0 {
			zh, en = append(zh, fmt.Sprintf("%d 个让位", yielding)), append(en, fmt.Sprintf("%d yielding", yielding))
		}
		if trying > 0 {
			zh, en = append(zh, fmt.Sprintf("%d 个在试用", trying)), append(en, fmt.Sprintf("%d on trial", trying))
		}
		if obliged > 0 {
			zh, en = append(zh, fmt.Sprintf("%d 个有待验证的失败信号", obliged)), append(en, fmt.Sprintf("%d with failure signals to verify", obliged))
		}
		if anomalies > 0 {
			zh, en = append(zh, fmt.Sprintf("%d 个近期有异常（异常过期后恢复）", anomalies)), append(en, fmt.Sprintf("%d with anomalies lately (until these expire)", anomalies))
		}
		level := probe.LevelWarn
		if confirming > 0 {
			level = probe.LevelInfo
		}
		poolItems = append(poolItems, detail(probe.T("热备不足", "short of standbys"), probe.Text{
			"zh": fmt.Sprintf("热备已达上限 %d 个，已确认的仍不足：", probe.PoolMax) + strings.Join(zh, "，"),
			"en": fmt.Sprintf("at the limit of %d standbys and still short of confirmed ones: ", probe.PoolMax) + strings.Join(en, ", "),
		}, level))
	}
	poolItems = append(poolItems,
		detail(probe.T("热备成员", "standbys"), od.labels(pool), ""),
		detail(probe.T("说明", "about"), probe.T(
			"热备是排在当前选择之后、预先测过的成员，当前选择不可用时直接接替；最近测过且没有异常的算已确认，已确认的不够时会多维护几个",
			"standbys come after the member in use and are tested ahead, to take over at once; those tested lately without anomalies are confirmed, and more are kept while too few are"),
			probe.LevelMuted),
	)
	sections := []any{
		section(probe.T("选路", "routing"), routing...),
		section(probe.T("热备", "standbys"), poolItems...),
	}
	if s := od.search; s != nil {
		sections = append(sections, section(probe.T("故障寻找", "search"),
			moment("t0", probe.T("开始", "since"), s.t0),
			detail(probe.T("本批确认", "this batch"), od.labels(s.members), ""),
		))
	}
	if now.Before(od.repairUntil) {
		sections = append(sections, section(probe.T("依赖修复", "repair"),
			moment("repairUntil", probe.T("持续到", "until"), od.repairUntil),
			detail(probe.T("说明", "about"), probe.T(
				"其它组的探测经过本组：在此之前，即使空闲也像在用时一样重测不可用的成员",
				"tests of other groups go through this group: until then it tests its dead members again as if in use"),
				probe.LevelMuted)))
	}

	members := make([]any, 0, len(od.order))
	for position, p := range od.order {
		members = append(members, od.memberView(p, position, pool, refill, now))
	}
	return map[string]any{
		"name":     od.name(),
		"headline": headline,
		"sections": sections,
		"members":  members,
	}
}

var suspendTip = probe.T(
	"当前选择在一次拨号中明显落后于热备；验证结果出来前，新连接先走热备",
	"the member in use fell behind a standby in a dial; until its verification ends, new dials start from the standby")

func (od *onDemand) memberView(p C.Proxy, position int, pool, refill []C.Proxy, now time.Time) map[string]any {
	entry := map[string]any{
		"name":     p.Name(),
		"index":    slices.Index(od.members, p),
		"provider": p.ProxyInfo().ProviderName,
		"object":   od.coord.KnownObjectID(p),
		"position": position,
	}
	ev := od.view[p]
	if od.unprobed(p) {
		entry["role"] = role("builtin", probe.LevelMuted, probe.T("内置", "built-in"), probe.T(
			"DIRECT、REJECT 等内置出站，不做探测", "a built-in outbound such as DIRECT or REJECT, never tested"))
		entry["headline"] = []any{}
		if ev.Delegated {
			entry["role"] = role("builtin", probe.LevelMuted, probe.T("直连兜底", "direct last resort"), probe.T(
				"由子组判定：它当前落到不测试的直连兜底，始终可用", "judged by the group itself: it ends at a direct outbound it does not test, always there"))
			entry["headline"] = []any{od.delegatedBadge(p)}
		}
		entry["sections"] = []any{}
		return entry
	}
	q := od.quality[p]
	ms := od.states[p]
	counts := od.countsOf(p, now)
	entry["role"] = role("cold", probe.LevelMuted, probe.T("冷备", "cold"), probe.T(
		"平时不测；前面的成员都不可用时才启用", "not tested; used only when none before it is available"))
	switch {
	case p == od.current:
		entry["role"] = role("preferred", probe.LevelOK, probe.T("当前", "in use"), probe.T(
			"组当前选中的成员", "the member the group selects now"))
	case ev.State == probe.Dead && position < slices.Index(od.order, od.current):
		// tested again as its backoff allows while the group is in use, and preferred once back
		entry["role"] = role("recovering", probe.LevelWarn, probe.T("待恢复", "to recover"), probe.T(
			"排在当前选择之前，已不可用；按退避重测，恢复后切回它", "before the member in use and dead: tested again as its backoff allows, selected once back"))
	case p == od.route:
		entry["role"] = role("route", probe.LevelInfo, probe.T("实际拨号", "dialed"), probe.T(
			"当前选择暂缓使用期间，新连接先走它", "new dials start from it while the member in use is set aside"))
	case slices.Contains(pool, p):
		entry["role"] = role("standby", probe.LevelInfo, probe.T("热备", "standby"), probe.T(
			"排在当前选择之后、预先测过，当前选择不可用时直接接替", "after the member in use and tested ahead, to take over at once"))
	case slices.Contains(refill, p):
		entry["role"] = role("candidate", probe.LevelMuted, probe.T("候补", "candidate"), probe.T(
			"热备不够时从这里补测", "tested to fill the standbys when they are too few"))
	}

	var headline []any
	switch ev.State {
	case probe.Alive:
		level := probe.LevelOK
		tip := probe.T("最近一次测试成功", "the last test succeeded")
		if !od.fresh(p, now) {
			level = probe.LevelMuted
			tip = probe.T("最近一次测试成功，但已有一段时间，再用到时会重新确认", "the last test succeeded a while ago: it is confirmed again once used")
		}
		headline = append(headline, badgeTip("evidence", level, probe.Text{"zh": "可用 · " + ago(now, ev.Started)["zh"], "en": "alive · " + ago(now, ev.Started)["en"]}, tip))
	case probe.Dead:
		headline = append(headline, badgeTip("evidence", probe.LevelError, probe.Tf("不可用 · 连续失败 %d 次", "dead · %d failures in a row", ev.Failures), probe.T(
			"最近的测试失败，按退避间隔重测（15 秒起翻倍，最长 30 分钟）", "the last test failed: tested again after a backoff, from 15 seconds doubling up to 30 minutes")))
	default:
		headline = append(headline, badgeTip("evidence", probe.LevelMuted, probe.T("未测试", "not tested"), probe.T(
			"内核本次启动后还没有测过它", "not tested since the core started")))
	}
	if ev.Delegated {
		headline = append(headline, od.delegatedBadge(p))
	}
	if od.excluded(p) {
		headline = append(headline, badgeTip("excluded", probe.LevelMuted, probe.T("已排除", "excluded"), probe.T(
			"所在流量池或经由的依赖被排除表排除，暂不使用", "its traffic pool or a dependency is excluded: not used for now")))
	}
	if s := od.suspended; s != nil && s.member == p {
		headline = append(headline, badgeTip("suspended", probe.LevelWarn, probe.T("暂缓使用", "set aside"), suspendTip))
	}
	if q != nil {
		switch q.standing {
		case standingYield:
			headline = append(headline, badgeTip("standing", probe.LevelWarn, probe.T("让位", "yields"), probe.T(
				"近期异常较多，排到其它成员之后；前面都不可用时照常使用", "too many anomalies lately: after the other members, still used when none before it is available")))
		case standingTrial:
			headline = append(headline, badgeTip("standing", probe.LevelInfo, probe.T("试用中", "on trial"), probe.T(
				"让位后回到原位试用，拨号时同时拨热备", "back in place after yielding, dialed alongside a standby")))
		}
	}
	if slices.Contains(pool, p) && od.trusted(p, now) {
		headline = append(headline, badgeTip("trusted", probe.LevelOK, probe.T("已确认", "confirmed"), probe.T(
			"最近测过且没有异常，可随时接替", "tested lately without anomalies, ready to take over")))
	}
	if counts.Recent > 0 || counts.Long > 0 {
		headline = append(headline, metric("counts", countsLevel(counts), probe.T("异常", "anomalies"),
			probe.Tf("8 小时 %s 次 · 3 天 %s 次", "%s in 8h · %s in 3 days", times(counts.Recent), times(counts.Long)), counts.Recent))
	}

	evidence := []map[string]any{detail(probe.T("结论", "result"), stateText(ev.State), "")}
	if !ev.Started.IsZero() {
		evidence = append(evidence, moment("probedAt", probe.T("测试时间", "tested at"), ev.Started))
	}
	if ms != nil && ms.lastDelay > 0 && ev.State == probe.Alive {
		evidence = append(evidence, metric("delay", probe.LevelInfo, probe.T("延迟", "delay"), probe.Tf("%d ms", "%d ms", ms.lastDelay), ms.lastDelay))
	}
	if ev.State == probe.Dead && !ev.Delegated {
		evidence = append(evidence, metric("failures", probe.LevelError, probe.T("连续失败", "failures in a row"), probe.Tf("%d 次", "%d", ev.Failures), ev.Failures))
		if !ev.NextAllowed.IsZero() {
			evidence = append(evidence, moment("nextAllowed", probe.T("下次重测", "next test at"), ev.NextAllowed))
		}
		if outage := od.coord.DeadFor(probe.Key{Target: p, Cond: od.cond}); outage > 0 {
			kind := probe.T("短暂故障", "short failure")
			if outage >= probe.PersistentAfter {
				kind = probe.T("持续故障：不占候补名额，恢复后先试用", "persistent failure: out of the candidates, on trial once back")
			}
			length := span(outage)
			evidence = append(evidence, detail(probe.T("已持续", "down for"), probe.Text{
				"zh": length["zh"] + " · " + kind["zh"], "en": length["en"] + " · " + kind["en"],
			}, probe.LevelWarn))
		}
	}
	sections := []any{section(probe.T("测试结果", "test"), evidence...)}
	if od.excluded(p) && ms != nil && len(ms.trace) > 0 {
		sections = append(sections, section(probe.T("排除", "exclusion"), exclusionItems(ms.trace)...))
	}

	quality := []map[string]any{
		metric("recent", countsLevel(counts), probe.T("近期异常（8 小时）", "lately (8 hours)"),
			probe.Tf("%s 次，达到 %d 次让位", "%s, yields at %d", times(counts.Recent), probe.YieldRecent), counts.Recent),
		metric("long", countsLevel(counts), probe.T("长期异常（3 天）", "in 3 days"),
			probe.Tf("%s 次，达到 %d 次让位", "%s, yields at %d", times(counts.Long), probe.YieldLong), counts.Long),
	}
	if pools := od.memberPools[p]; len(pools) > 0 {
		quality = append(quality, detail(probe.T("经过的流量池", "traffic pools"), probe.T(strings.Join(pools, "、"), strings.Join(pools, ", ")), ""))
		// the outages counted for each pool, the larger of which adds to the counts
		for _, pool := range pools {
			events := od.coord.PoolEvents(pool, od.cond)
			if len(events) == 0 {
				continue
			}
			pc := probe.CountEvents(events, now)
			last := events[len(events)-1].At.Local().Format("01-02 15:04")
			quality = append(quality, detail(probe.Tf("流量池 %s 的整体故障", "outages of %s", pool), probe.Tf(
				"近期 %s 次，3 天内 %s 次，最近一次 %s", "%s lately, %s in 3 days, the last at %s", times(pc.Recent), times(pc.Long), last), probe.LevelWarn))
		}
	}
	if failures := od.coord.Failures(probe.Key{Target: p, Cond: od.cond}); len(failures) > 0 {
		var at []string
		for _, f := range failures {
			at = append(at, f.At.Local().Format("01-02 15:04"))
		}
		quality = append(quality, detail(probe.T("计入的失效", "failures counted"), probe.T(strings.Join(at, "、"), strings.Join(at, ", ")), ""))
	}
	if q != nil && len(q.degradations) > 0 {
		var at []string
		for _, d := range q.degradations {
			at = append(at, d.At.Local().Format("01-02 15:04"))
		}
		quality = append(quality, metric("degradations", probe.LevelWarn, probe.T("明显慢于热备", "much slower than a standby"), probe.Tf(
			"%d 次（每次计 %s 次异常）：%s", "%d (%s each): %s", len(q.degradations), times(probe.DegradeWeight), strings.Join(at, "、")), len(q.degradations)))
	}
	quality = append(quality, detail(probe.T("说明", "about"), probe.T(
		"异常是归属于成员自身的失效，以及验证时明显慢于热备；本机网络异常或流量池整体故障期间的失效不计入。异常过多会让位，排到其它成员之后",
		"anomalies are failures of the member itself and verifications much slower than a standby; failures while the local network or a pool is out do not count. Too many make it yield to the others"),
		probe.LevelMuted))
	sections = append(sections, section(probe.T("质量", "quality"), quality...))

	if q != nil && q.standing != standingNormal {
		var items []map[string]any
		if od.pinned == p.Name() {
			items = append(items, detail(probe.T("手动固定", "pinned"), probe.T("固定优先：仍排最前", "pinned first: it stays first"), probe.LevelInfo))
		}
		switch q.standing {
		case standingYield:
			items = append(items, detail(probe.T("结论", "standing"), probe.T("让位：排到其它成员之后，前面都不可用时照常使用", "yields: after the other members, still used when none before it is available"), probe.LevelWarn))
			if counts.Recent > probe.ReturnRecent || counts.Long > probe.ReturnLong {
				items = append(items, detail(probe.T("等待", "waiting for"), probe.Tf(
					"异常次数回落到近期不超过 %d、长期不超过 %d（现在 %s、%s），异常会随时间离开统计窗口",
					"anomalies down to at most %d lately and %d in 3 days (now %s and %s), as they leave the windows",
					probe.ReturnRecent, probe.ReturnLong, times(counts.Recent), times(counts.Long)), ""))
			}
			if now.Before(q.readmitAt) {
				items = append(items, detail(probe.T("等待", "waiting for"), probe.Tf(
					"再次试用时刻 %s（试用失败后的等待）", "another trial at %s (after a failed trial)", q.readmitAt.Local().Format("15:04")), ""))
			}
			if !q.readmitAt.IsZero() {
				items = append(items, moment("readmitAt", probe.T("可再试用", "may try again at"), q.readmitAt))
			}
			if q.failedTrials > 0 {
				items = append(items, metric("failedTrials", probe.LevelWarn, probe.T("连续试用失败", "failed trials in a row"), probe.Tf("%d 次", "%d", q.failedTrials), q.failedTrials))
			}
		case standingTrial:
			t := q.trial
			effective := t.effective
			if !t.resumed.IsZero() {
				effective += now.Sub(t.resumed)
			}
			why := trialReasonText(t.reason)
			items = append(items, detail(probe.T("结论", "standing"), probe.Text{
				"zh": "试用（" + why["zh"] + "）：回到原位，拨号时同时拨热备",
				"en": "on trial (" + why["en"] + "): back in place, dialed alongside a standby",
			}, probe.LevelInfo))
			items = append(items, metric("trialProgress", probe.LevelInfo, probe.T("试用进度", "trial"),
				probe.Tf("%d / %d 分钟", "%d / %d min", int(effective.Minutes()), int(probe.TrialLength.Minutes())), int(effective.Minutes())))
			if t.resumed.IsZero() {
				items = append(items, detail(probe.T("计时", "clock"), probe.T("暂停：本机网络或流量池异常期间不计时", "paused while the local network or a pool is out"), probe.LevelWarn))
			}
			if len(t.pending) > 0 {
				var earliest time.Time
				for _, started := range t.pending {
					if earliest.IsZero() || started.Before(earliest) {
						earliest = started
					}
				}
				due := earliest.Add(probe.AttributionDelay)
				if now.Before(due) {
					items = append(items, detail(probe.T("等待", "waiting for"), probe.Tf(
						"%d 次异常待归因（异常发生后等 %d 分钟，看是否属于本机网络或流量池的共同故障），归因前不能通过",
						"%d anomalies to be attributed (%d min after each, to tell a common failure), before it can pass",
						len(t.pending), int(probe.AttributionDelay.Minutes())), probe.LevelWarn))
					items = append(items, moment("attributeAt", probe.T("最早归因", "attributed at the earliest"), due))
				} else {
					items = append(items, detail(probe.T("等待", "waiting for"), probe.Tf(
						"%d 次异常待归因：等待所经流量池整体故障的结论（见页顶提示）",
						"%d anomalies to be attributed: waiting for the outage of a pool they went through to be concluded (see the banners)",
						len(t.pending)), probe.LevelWarn))
				}
			}
			if !t.witness {
				items = append(items, detail(probe.T("等待", "waiting for"), probe.T("还需要一次新建连接收到数据或一次探测成功", "a new connection that receives data, or a successful test"), ""))
			}
		}
		sections = append(sections, section(probe.T("让位与试用", "standing"), items...))
	}
	if ms != nil && (len(ms.signals) > 0 || ms.cooldown != 0 || !ms.lastSuccessDial.IsZero()) {
		var items []map[string]any
		if len(ms.signals) > 0 {
			items = append(items, detail(probe.T("待验证", "to verify"), probe.Tf("%d 个连接失败信号", "%d failure signals", len(ms.signals)), probe.LevelWarn))
			items = append(items, moment("obligation", probe.T("最早的信号", "since"), ms.signals[0]))
		}
		if ms.cooldown != 0 {
			items = append(items, moment("cooldown", probe.T("误报冷却到", "false alarms cool down until"), ms.cooldownUntil))
		}
		if !ms.lastSuccessDial.IsZero() {
			items = append(items, moment("lastSuccess", probe.T("最近成功的新连接", "last new connection with data"), ms.lastSuccessDial))
		}
		sections = append(sections, section(probe.T("连接与信号", "connections"), items...))
	}
	entry["headline"] = headline
	entry["sections"] = sections
	return entry
}

// networkText names a state of the local network that is not normal.
func networkText(state probe.NetworkState) text {
	if state == probe.NetworkDown {
		return probe.T("疑似本机断网", "the local network seems down")
	}
	return probe.T("本机网络不佳", "the local network is poor")
}

func stateText(state probe.State) text {
	switch state {
	case probe.Alive:
		return probe.T("可用", "alive")
	case probe.Dead:
		return probe.T("不可用", "dead")
	}
	return probe.T("未测试", "not tested")
}

// times formats a count of anomalies, which weights may make fractional.
func times(v float64) string {
	return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64)
}

func span(d time.Duration) text {
	switch {
	case d < time.Minute:
		return probe.Tf("%d 秒", "%ds", int(d.Seconds()))
	case d < time.Hour:
		return probe.Tf("%d 分钟", "%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return probe.Tf("%.1f 小时", "%.1fh", d.Hours())
	}
	return probe.Tf("%.1f 天", "%.1fd", d.Hours()/24)
}

func countsLevel(c probe.Counts) string {
	switch {
	case c.Recent >= probe.YieldRecent || c.Long >= probe.YieldLong:
		return probe.LevelError
	case c.Recent > 0:
		return probe.LevelWarn
	}
	return probe.LevelInfo
}

// exclusionItems explains an exclusion by the steps that decided it: the path to the pool or
// the dependency excluded, and when it is evaluated again.
func exclusionItems(trace []probe.TraceStep) []map[string]any {
	// The steps are recorded after the steps they depend on: the first excluded one is the
	// cause, the later ones what it reaches the member through.
	var through []string
	var decided probe.TraceStep
	found := false
	for _, step := range trace {
		if !step.Excluded {
			continue
		}
		if !found {
			decided, found = step, true
			continue
		}
		through = append(through, step.Name)
	}
	why := map[string]probe.Text{
		"pool":      probe.T("所在流量池被排除", "its traffic pool is excluded"),
		"dialer":    probe.T("经由的 dialer-proxy 被排除", "the dialer proxy it goes through is excluded"),
		"selection": probe.T("所选成员被排除", "the member it selects is excluded"),
		"members":   probe.T("全部成员都被排除", "all its members are excluded"),
		"member":    probe.T("成员被排除", "a member is excluded"),
		"cycle":     probe.T("依赖成环", "its dependencies form a cycle"),
	}[decided.By]
	if why == nil {
		why = probe.T(decided.By, decided.By)
	}
	reason := probe.Text{"zh": decided.Name + "：" + why["zh"], "en": decided.Name + ": " + why["en"]}
	if decided.Detail != "" {
		reason = probe.Text{"zh": reason["zh"] + "（" + decided.Detail + "）", "en": reason["en"] + " (" + decided.Detail + ")"}
	}
	items := []map[string]any{detail(probe.T("依据", "by"), reason, probe.LevelMuted)}
	if len(through) > 0 {
		list := strings.Join(through, "、")
		items = append(items, detail(probe.T("影响到", "excluding as well"), probe.T(list, strings.Join(through, ", ")), ""))
	}
	return append(items,
		detail(probe.T("重新判断", "evaluated again"), probe.T(
			"排除表变化或到期、所经组的选择变化时", "when the exclusions change or expire, or a group along the path selects another member"), ""),
	)
}
