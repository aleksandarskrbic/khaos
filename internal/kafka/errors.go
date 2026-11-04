package kafka

import "strings"

// explainHints are the substring-to-hint rules Explain walks, in order; the first entry
// with any substring present in the lowercased error text wins.
//
// The order is load-bearing: "timed out" sits before "ssl" so a TLS handshake that merely
// timed out is reported as a timeout, "unauthorized" before "ssl" so an auth rejection on a
// TLS connection is reported as an authorization failure, and "unknown topic" before
// "topicauthorization" for the same reason.
var explainHints = []struct {
	subs []string
	hint string
}{
	{[]string{"brokertransportfailure", "all brokers are down"},
		"Cannot connect to broker. Check Kafka is running and bootstrap servers are correct."},
	{[]string{"resolve", "unknown host", "nodename"},
		"Cannot resolve broker hostname. Check bootstrap server addresses."},
	{[]string{"connection refused"},
		"Connection refused. Check that Kafka broker is running on the specified port."},
	{[]string{"timed out", "timeout"},
		"Connection timed out. Check network connectivity and firewall rules."},
	{[]string{"authentication", "sasl"},
		"Authentication failed. Check SASL username and password."},
	{[]string{"unauthorized", "not authorized"},
		"Not authorized. Check user permissions and ACLs."},
	{[]string{"ssl", "certificate", "handshake"},
		"SSL/TLS error. Check certificate paths and permissions."},
	{[]string{"unknown topic", "topic not found"},
		"Topic does not exist. Check topic name or create the topic first."},
	{[]string{"topicauthorization"},
		"Not authorized for topic. Check ACLs and permissions."},
}

// Explain turns a client or broker error into an actionable one-line hint by matching
// substrings of the lowercased error text against explainHints. Substring matching is
// fragile, but it covers the messy transport-level failures that have no clean error code
// to switch on instead. Errors that match nothing are returned as-is.
func Explain(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	low := strings.ToLower(msg)

	for _, h := range explainHints {
		for _, s := range h.subs {
			if strings.Contains(low, s) {
				return h.hint
			}
		}
	}
	if strings.Contains(low, "replication") && strings.Contains(low, "factor") {
		return "Invalid replication factor. Cannot exceed the number of available brokers."
	}
	return msg
}
