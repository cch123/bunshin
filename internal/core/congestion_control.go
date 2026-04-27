package core

type UDPCongestionControl interface {
	InitialWindow(defaultWindowBytes int) int
	OnFeedback(feedback TransportFeedback, currentWindowBytes, defaultWindowBytes int) int
}

type AIMDUDPCongestionControl struct {
	InitialWindowBytes  int
	MinWindowBytes      int
	MaxWindowBytes      int
	AdditiveIncrease    int
	DecreaseNumerator   int
	DecreaseDenominator int
}

func NewAIMDUDPCongestionControl() *AIMDUDPCongestionControl {
	return &AIMDUDPCongestionControl{}
}

func (c *AIMDUDPCongestionControl) InitialWindow(defaultWindowBytes int) int {
	settings := c.settings(defaultWindowBytes)
	return clampInt(settings.initialWindow, settings.minWindow, settings.maxWindow)
}

func (c *AIMDUDPCongestionControl) OnFeedback(feedback TransportFeedback, currentWindowBytes, defaultWindowBytes int) int {
	settings := c.settings(defaultWindowBytes)
	current := clampInt(currentWindowBytes, settings.minWindow, settings.maxWindow)
	if feedback.RetransmittedFrames > 0 {
		next := current * settings.decreaseNumerator / settings.decreaseDenominator
		return clampInt(next, settings.minWindow, settings.maxWindow)
	}
	if feedback.RTT > 0 {
		return clampInt(current+settings.additiveIncrease, settings.minWindow, settings.maxWindow)
	}
	return current
}

type aimdUDPSettings struct {
	initialWindow       int
	minWindow           int
	maxWindow           int
	additiveIncrease    int
	decreaseNumerator   int
	decreaseDenominator int
}

func (c *AIMDUDPCongestionControl) settings(defaultWindowBytes int) aimdUDPSettings {
	if defaultWindowBytes <= 0 {
		defaultWindowBytes = defaultUDPTransportMTUBytes
	}
	minWindow := c.MinWindowBytes
	if minWindow <= 0 {
		minWindow = defaultUDPTransportMTUBytes
	}
	maxWindow := c.MaxWindowBytes
	if maxWindow <= 0 {
		maxWindow = defaultWindowBytes
	}
	if maxWindow < minWindow {
		maxWindow = minWindow
	}
	initialWindow := c.InitialWindowBytes
	if initialWindow <= 0 {
		initialWindow = maxWindow
	}
	additiveIncrease := c.AdditiveIncrease
	if additiveIncrease <= 0 {
		additiveIncrease = defaultUDPTransportMTUBytes
	}
	decreaseNumerator := c.DecreaseNumerator
	decreaseDenominator := c.DecreaseDenominator
	if decreaseNumerator <= 0 || decreaseDenominator <= 0 || decreaseNumerator >= decreaseDenominator {
		decreaseNumerator = 1
		decreaseDenominator = 2
	}
	return aimdUDPSettings{
		initialWindow:       initialWindow,
		minWindow:           minWindow,
		maxWindow:           maxWindow,
		additiveIncrease:    additiveIncrease,
		decreaseNumerator:   decreaseNumerator,
		decreaseDenominator: decreaseDenominator,
	}
}

func clampInt(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}
