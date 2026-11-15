package outboundgroup

import (
	"fmt"
	"sort"
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
		headline = append(headline, badge("use", probe.LevelOK, probe.T("在用", "in use")))
	} else {
		headline = append(headline, badge("use", probe.LevelMuted, probe.T("空闲", "idle")))
	}
	if od.network != probe.NetworkNormal {
		headline = append(headline, badge("network", probe.LevelWarn, probe.Tf("本机网络：%s", "local network: %s", od.network)))
	}
	if s := od.suspended; s != nil {
		headline = append(headline, badge("suspended", probe.LevelWarn, probe.Tf(
			"暂缓 %s，新连接从 %s 开始", "%s set aside, new dials start from %s", nameOf(s.member), nameOf(od.route))))
	}
	if od.search != nil {
		headline = append(headline, badge("search", probe.LevelWarn, probe.T("故障寻找中", "searching")))
	}
	standbyLevel := probe.LevelOK
	if trustedCount < target {
		standbyLevel = probe.LevelWarn
	}
	headline = append(headline, metric("standbys", standbyLevel, probe.T("可信热备", "trusted standbys"),
		probe.Tf("%d/%d（池 %d 个）", "%d/%d (pool of %d)", trustedCount, target, len(pool)), trustedCount))

	routing := []map[string]any{
		detail(probe.T("首选", "preferred"), probe.T(nameOf(od.current), nameOf(od.current)), ""),
	}
	if od.route != od.current {
		why := probe.T("首选暂缓", "the preferred member is set aside")
		if s := od.suspended; s != nil && s.reason == "slowTest" {
			why = probe.Tf("首选的验证已超过快速档，后备 %s 只用了 %d ms", "the verification of the preferred member ran past the fast tier, standby %s took %d ms", nameOf(s.standby), s.standbyElapsed.Milliseconds())
		} else if s != nil {
			why = probe.T("首选在一次拨号中落后于后备，等待验证结果", "the preferred member fell behind a standby in a dial, waiting for its verification")
		}
		routing = append(routing, detail(probe.T("路线", "route"), probe.Text{
			"zh": nameOf(od.route) + "（" + why["zh"] + "）", "en": nameOf(od.route) + " (" + why["en"] + ")",
		}, probe.LevelWarn))
	}
	if d := od.carried.Load(); d != nil {
		carried := probe.T(d.name, d.name)
		if d.hedged {
			carried = probe.Tf("%s（快速档改拨，路线未及时连上）", "%s (by the fast tier, the route was late)", d.name)
		}
		routing = append(routing, detail(probe.T("最近承载", "last carried by"), carried, ""))
	}
	if od.pinned != "" {
		routing = append(routing, detail(probe.T("固定", "pinned"), probe.Tf("%s（固定优先，不受让位与试用影响）", "%s (pinned first, whatever its standing)", od.pinned), probe.LevelInfo))
	}
	if !od.lastSwitch.IsZero() {
		routing = append(routing, moment("lastSwitch", probe.T("最近切换", "last switch"), od.lastSwitch))
	}
	routing = append(routing, detail(probe.T("切换原因", "reason"), causeText(od.cause), ""))

	var poolItems []map[string]any
	poolItems = append(poolItems, metric("trusted", standbyLevel, probe.T("可信热备", "trusted standbys"),
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
			zh, en = append(zh, fmt.Sprintf("%d 个近期有异常（质量记忆，异常过期后再推导）", anomalies)), append(en, fmt.Sprintf("%d with anomalies lately (in their memory, until these expire)", anomalies))
		}
		level := probe.LevelWarn
		if confirming > 0 {
			level = probe.LevelInfo
		}
		poolItems = append(poolItems, detail(probe.T("热备池", "pool"), probe.Text{
			"zh": fmt.Sprintf("已达上限 %d 个，可信热备仍不足：", probe.PoolMax) + strings.Join(zh, "，"),
			"en": fmt.Sprintf("at its limit of %d and still short of trusted standbys: ", probe.PoolMax) + strings.Join(en, ", "),
		}, level))
	}
	var poolNames []string
	for _, p := range pool {
		poolNames = append(poolNames, p.Name())
	}
	poolItems = append(poolItems, detail(probe.T("成员", "members"), probe.T(fmt.Sprint(poolNames), fmt.Sprint(poolNames)), ""))
	sections := []any{
		section(probe.T("选路", "routing"), routing...),
		section(probe.T("热备", "standbys"), poolItems...),
	}
	if s := od.search; s != nil {
		var names []string
		for _, p := range s.members {
			names = append(names, p.Name())
		}
		sections = append(sections, section(probe.T("故障寻找", "search"),
			moment("t0", probe.T("开始", "since"), s.t0),
			detail(probe.T("本批", "batch"), probe.T(fmt.Sprint(names), fmt.Sprint(names)), ""),
		))
	}
	if now.Before(od.repairUntil) {
		sections = append(sections, section(probe.T("依赖修复", "repair"),
			moment("repairUntil", probe.T("窗口到", "open until"), od.repairUntil)))
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

func (od *onDemand) memberView(p C.Proxy, position int, pool, refill []C.Proxy, now time.Time) map[string]any {
	entry := map[string]any{
		"name":     p.Name(),
		"index":    slices.Index(od.members, p),
		"provider": p.ProxyInfo().ProviderName,
		"object":   od.coord.KnownObjectID(p),
		"position": position,
	}
	if probe.IsBuiltin(p) {
		entry["role"] = map[string]any{"id": "builtin", "text": probe.T("内置", "built-in"), "level": probe.LevelMuted}
		entry["headline"] = []any{}
		entry["sections"] = []any{}
		return entry
	}
	ev := od.view[p]
	q := od.quality[p]
	ms := od.states[p]
	counts := od.countsOf(p, now)
	role := map[string]any{"id": "cold", "text": probe.T("冷备", "cold"), "level": probe.LevelMuted}
	switch {
	case p == od.current:
		role = map[string]any{"id": "preferred", "text": probe.T("首选", "preferred"), "level": probe.LevelOK}
	case p == od.route:
		role = map[string]any{"id": "route", "text": probe.T("路线", "route"), "level": probe.LevelInfo}
	case slices.Contains(pool, p):
		role = map[string]any{"id": "standby", "text": probe.T("热备", "standby"), "level": probe.LevelInfo}
	case slices.Contains(refill, p):
		role = map[string]any{"id": "candidate", "text": probe.T("补足范围", "refill range"), "level": probe.LevelMuted}
	}
	entry["role"] = role

	var headline []any
	switch ev.State {
	case probe.Alive:
		level := probe.LevelOK
		if !od.fresh(p, now) {
			level = probe.LevelMuted
		}
		headline = append(headline, badge("evidence", level, probe.Text{"zh": "存活 · " + ago(now, ev.Started)["zh"], "en": "alive · " + ago(now, ev.Started)["en"]}))
	case probe.Dead:
		headline = append(headline, badge("evidence", probe.LevelError, probe.Tf("失效 · 连续 %d 次", "dead · %d in a row", ev.Failures)))
	default:
		headline = append(headline, badge("evidence", probe.LevelMuted, probe.T("未知", "unknown")))
	}
	if od.excluded(p) {
		headline = append(headline, badge("excluded", probe.LevelMuted, probe.T("已排除", "excluded")))
	}
	if s := od.suspended; s != nil && s.member == p {
		headline = append(headline, badge("suspended", probe.LevelWarn, probe.T("暂缓", "set aside")))
	}
	if q != nil {
		switch q.standing {
		case standingYield:
			headline = append(headline, badge("standing", probe.LevelWarn, probe.T("让位", "yields")))
		case standingTrial:
			headline = append(headline, badge("standing", probe.LevelInfo, probe.T("试用中", "on trial")))
		}
	}
	if slices.Contains(pool, p) && od.trusted(p, now) {
		headline = append(headline, badge("trusted", probe.LevelOK, probe.T("可信热备", "trusted standby")))
	}
	if counts.Recent > 0 || counts.Long > 0 {
		headline = append(headline, metric("counts", countsLevel(counts), probe.T("异常计次", "anomalies"),
			probe.Tf("近期 %.1f · 长期 %.1f", "%.1f lately · %.1f in 3 days", counts.Recent, counts.Long), counts.Recent))
	}

	evidence := []map[string]any{detail(probe.T("状态", "state"), probe.T(ev.State.String(), ev.State.String()), "")}
	if !ev.Started.IsZero() {
		evidence = append(evidence, moment("probedAt", probe.T("测于", "tested at"), ev.Started))
	}
	if ms != nil && ms.lastDelay > 0 && ev.State == probe.Alive {
		evidence = append(evidence, metric("delay", probe.LevelInfo, probe.T("延迟", "delay"), probe.Tf("%d ms", "%d ms", ms.lastDelay), ms.lastDelay))
	}
	if ev.State == probe.Dead {
		evidence = append(evidence, metric("failures", probe.LevelError, probe.T("连续失败", "failures in a row"), probe.Tf("%d 次", "%d", ev.Failures), ev.Failures))
		if !ev.NextAllowed.IsZero() {
			evidence = append(evidence, moment("nextAllowed", probe.T("下次可测", "next test at"), ev.NextAllowed))
		}
		if outage := od.coord.DeadFor(probe.Key{Target: p, Cond: od.cond}); outage > 0 {
			kind := probe.T("短暂故障", "short failure")
			if outage >= probe.PersistentAfter {
				kind = probe.T("持续故障：不占补足范围的名额，恢复后先试用", "persistent failure: out of the refill range, on trial once back")
			}
			evidence = append(evidence, detail(probe.T("故障期", "down for"), probe.Text{
				"zh": outage.Round(time.Minute).String() + " · " + kind["zh"], "en": outage.Round(time.Minute).String() + " · " + kind["en"],
			}, probe.LevelWarn))
		}
	}
	sections := []any{section(probe.T("证据", "evidence"), evidence...)}
	if od.excluded(p) && ms != nil && len(ms.trace) > 0 {
		sections = append(sections, section(probe.T("排除", "exclusion"), exclusionItems(ms.trace)...))
	}

	quality := []map[string]any{
		metric("recent", countsLevel(counts), probe.T("近期计次（8 小时）", "lately (8 hours)"),
			probe.Tf("%.1f / 让位阈值 %d", "%.1f / yields at %d", counts.Recent, probe.YieldRecent), counts.Recent),
		metric("long", countsLevel(counts), probe.T("长期计次（3 天）", "in 3 days"),
			probe.Tf("%.1f / 让位阈值 %d", "%.1f / yields at %d", counts.Long, probe.YieldLong), counts.Long),
	}
	if pools := od.memberPools[p]; len(pools) > 0 {
		quality = append(quality, detail(probe.T("经过的流量池", "traffic pools"), probe.T(fmt.Sprint(pools), fmt.Sprint(pools)), ""))
		// the outages counted for each pool, the larger of which adds to the counts
		for _, pool := range pools {
			events := od.coord.PoolEvents(pool, od.cond)
			if len(events) == 0 {
				continue
			}
			pc := probe.CountEvents(events, now)
			last := events[len(events)-1].At.Local().Format("01-02 15:04")
			quality = append(quality, detail(probe.Tf("流量池 %s 的整体故障", "outages of %s", pool), probe.Tf(
				"近期 %.0f 次，3 天内 %.0f 次，最近一次 %s", "%.0f lately, %.0f in 3 days, the last at %s", pc.Recent, pc.Long, last), probe.LevelWarn))
		}
	}
	if failures := od.coord.Failures(probe.Key{Target: p, Cond: od.cond}); len(failures) > 0 {
		var times []string
		for _, f := range failures {
			times = append(times, f.At.Local().Format("01-02 15:04"))
		}
		quality = append(quality, detail(probe.T("计入的失效", "failures counted"), probe.T(fmt.Sprint(times), fmt.Sprint(times)), ""))
	}
	if q != nil && len(q.degradations) > 0 {
		var times []string
		for _, d := range q.degradations {
			times = append(times, d.At.Local().Format("01-02 15:04"))
		}
		quality = append(quality, metric("degradations", probe.LevelWarn, probe.T("本组的劣化", "degradations here"), probe.Tf(
			"%d 次（每次计 %.1f）：%v", "%d (%.1f each): %v", len(q.degradations), probe.DegradeWeight, times), len(q.degradations)))
	}
	sections = append(sections, section(probe.T("质量", "quality"), quality...))

	if q != nil && q.standing != standingNormal {
		var items []map[string]any
		if od.pinned == p.Name() {
			items = append(items, detail(probe.T("固定", "pinned"), probe.T("固定优先：仍排最前", "pinned first: it stays first"), probe.LevelInfo))
		}
		switch q.standing {
		case standingYield:
			items = append(items, detail(probe.T("结论", "standing"), probe.T("让位：排到其它成员之后，前面都不可用时照常使用", "yields: after the other members, still used when none before it is available"), probe.LevelWarn))
			if counts.Recent > probe.ReturnRecent || counts.Long > probe.ReturnLong {
				items = append(items, detail(probe.T("阻塞条件", "waiting for"), probe.Tf(
					"计次回落到近期不超过 %d、长期不超过 %d（现在 %.1f、%.1f），异常随时间离开窗口",
					"counts down to at most %d lately and %d in 3 days (now %.1f and %.1f), as the anomalies leave the windows",
					probe.ReturnRecent, probe.ReturnLong, counts.Recent, counts.Long), ""))
			}
			if now.Before(q.readmitAt) {
				items = append(items, detail(probe.T("阻塞条件", "waiting for"), probe.Tf(
					"再次准入时刻 %s（试用失败后等待）", "the readmission at %s (after a failed trial)", q.readmitAt.Local().Format("15:04")), ""))
			}
			if !q.readmitAt.IsZero() {
				items = append(items, moment("readmitAt", probe.T("再次准入", "may try again at"), q.readmitAt))
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
				"zh": "试用（" + why["zh"] + "）：回到原位，拨号时同时拨后备",
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
					items = append(items, detail(probe.T("阻塞条件", "waiting for"), probe.Tf(
						"%d 次异常待归因（异常发生后等 %d 分钟，看是否属于共同事件），归因前不能通过",
						"%d anomalies to be attributed (%d min after each, to tell a common event), before it can pass",
						len(t.pending), int(probe.AttributionDelay.Minutes())), probe.LevelWarn))
					items = append(items, moment("attributeAt", probe.T("最早归因", "attributed at the earliest"), due))
				} else {
					items = append(items, detail(probe.T("阻塞条件", "waiting for"), probe.Tf(
						"%d 次异常待归因：等待所经流量池整体故障的结论（见页顶提示）",
						"%d anomalies to be attributed: waiting for the outage of a pool they went through to be concluded (see the banners)",
						len(t.pending)), probe.LevelWarn))
				}
			}
			if !t.witness {
				items = append(items, detail(probe.T("阻塞条件", "waiting for"), probe.T("还需要一次新建连接收到数据或一次探测成功", "a new connection that receives data, or a successful test"), ""))
			}
		}
		sections = append(sections, section(probe.T("让位与试用", "standing"), items...))
	}
	if ms != nil && (len(ms.signals) > 0 || ms.cooldown != 0 || !ms.lastSuccessDial.IsZero()) {
		var items []map[string]any
		if len(ms.signals) > 0 {
			items = append(items, detail(probe.T("待验证", "to verify"), probe.Tf("%d 个失败信号", "%d failure signals", len(ms.signals)), probe.LevelWarn))
			items = append(items, moment("obligation", probe.T("义务起于", "since"), ms.signals[0]))
		}
		if ms.cooldown != 0 {
			items = append(items, moment("cooldown", probe.T("信号冷却到", "signals cool down until"), ms.cooldownUntil))
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
