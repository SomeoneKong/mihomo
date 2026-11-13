package outboundgroup

// UDPDisabled reports whether the group was set up not to carry UDP, whichever proxy it
// selects. The proxies it selects decide whether it supports UDP otherwise.

func (f *Fallback) UDPDisabled() bool { return f.disableUDP }

func (lb *LoadBalance) UDPDisabled() bool { return lb.disableUDP }

func (s *Selector) UDPDisabled() bool { return s.disableUDP }

func (u *URLTest) UDPDisabled() bool { return u.disableUDP }
