package kafka

import "strings"

// Explain turns a client or broker error into an actionable one-line hint by matching
// substrings of the lowercased error text. Substring matching is fragile, but it covers the
// messy transport-level failures that have no clean error code to switch on instead.
//
// The ordering of the checks is load-bearing: "timed out" is tested before "ssl" so a TLS
// handshake that merely timed out is reported as a timeout, and "unknown topic" before
// "topicauthorization" for the same reason. Errors that match nothing are returned as-is.
func Explain(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	low := strings.ToLower(msg)

	has := func(subs ...string) bool {
		for _, s := range subs {
			if strings.Contains(low, s) {
				return true
			}
		}
		return false
	}

	switch {
	case has("brokertransportfailure", "all brokers are down"):
		return "Cannot connect to broker. Check Kafka is running and bootstrap servers are correct."
	case has("resolve", "unknown host", "nodename"):
		return "Cannot resolve broker hostname. Check bootstrap server addresses."
	case has("connection refused"):
		return "Connection refused. Check that Kafka broker is running on the specified port."
	case has("timed out", "timeout"):
		return "Connection timed out. Check network connectivity and firewall rules."
	case has("authentication", "sasl"):
		return "Authentication failed. Check SASL username and password."
	case has("unauthorized", "not authorized"):
		return "Not authorized. Check user permissions and ACLs."
	case has("ssl", "certificate", "handshake"):
		return "SSL/TLS error. Check certificate paths and permissions."
	case has("unknown topic", "topic not found"):
		return "Topic does not exist. Check topic name or create the topic first."
	case has("topicauthorization"):
		return "Not authorized for topic. Check ACLs and permissions."
	case strings.Contains(low, "replication") && strings.Contains(low, "factor"):
		return "Invalid replication factor. Cannot exceed the number of available brokers."
	default:
		return msg
	}
}
