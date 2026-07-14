package domain

// Channel represents a supported notification delivery channel.
type Channel string

const (
	ChannelWebSocket Channel = "websocket"
	// ChannelEmail     Channel = "email"
	// ChannelSMS       Channel = "sms"
)
