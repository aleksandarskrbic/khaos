from confluent_kafka import KafkaException


class KhaosConnectionError(Exception):
    pass


class KhaosConfigError(Exception):
    pass


def format_kafka_error(e: KafkaException | Exception) -> str:
    error_str = str(e).lower()

    if "brokertransportfailure" in error_str or "all brokers are down" in error_str:
        return "Cannot connect to broker. Check Kafka is running and bootstrap servers are correct."

    if "resolve" in error_str or "unknown host" in error_str or "nodename" in error_str:
        return "Cannot resolve broker hostname. Check bootstrap server addresses."

    if "connection refused" in error_str:
        return "Connection refused. Check that Kafka broker is running on the specified port."

    if "timed out" in error_str or "timeout" in error_str:
        return "Connection timed out. Check network connectivity and firewall rules."

    if "authentication" in error_str or "sasl" in error_str:
        return "Authentication failed. Check SASL username and password."

    if "unauthorized" in error_str or "not authorized" in error_str:
        return "Not authorized. Check user permissions and ACLs."

    if "ssl" in error_str or "certificate" in error_str or "handshake" in error_str:
        return "SSL/TLS error. Check certificate paths and permissions."

    if "unknown topic" in error_str or "topic not found" in error_str:
        return "Topic does not exist. Check topic name or create the topic first."

    if "topicauthorization" in error_str:
        return "Not authorized for topic. Check ACLs and permissions."

    if "replication" in error_str and "factor" in error_str:
        return "Invalid replication factor. Cannot exceed number of available brokers (3)."

    return str(e)
