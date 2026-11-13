package provider

// StrategyVersion tells whether the provider published other rules: it goes up once before
// new rules are put in place and once after, so it is odd while they are. Whoever matched
// while it was odd, or before and after it changed, may have matched against both the old
// and the new rules.
func (rp *ruleSetProvider) StrategyVersion() uint64 {
	return rp.version.Load()
}
