from khaos.errors import (
    KhaosConfigError,
    KhaosConnectionError,
    format_kafka_error,
)


class TestKhaosConnectionError:
    def test_is_exception(self):
        error = KhaosConnectionError("connection failed")
        assert isinstance(error, Exception)
        assert str(error) == "connection failed"


class TestKhaosConfigError:
    def test_is_exception(self):
        error = KhaosConfigError("invalid config")
        assert isinstance(error, Exception)
        assert str(error) == "invalid config"


class TestFormatKafkaError:
    def test_broker_transport_failure(self):
        error = Exception("BrokerTransportFailure: connection lost")
        result = format_kafka_error(error)
        assert "Cannot connect to broker" in result

    def test_all_brokers_down(self):
        error = Exception("All brokers are down")
        result = format_kafka_error(error)
        assert "Cannot connect to broker" in result

    def test_resolve_error(self):
        error = Exception("Failed to resolve hostname")
        result = format_kafka_error(error)
        assert "Cannot resolve broker hostname" in result

    def test_unknown_host(self):
        error = Exception("Unknown host: kafka")
        result = format_kafka_error(error)
        assert "Cannot resolve broker hostname" in result

    def test_nodename_error(self):
        error = Exception("nodename nor servname provided")
        result = format_kafka_error(error)
        assert "Cannot resolve broker hostname" in result

    def test_connection_refused(self):
        error = Exception("Connection refused")
        result = format_kafka_error(error)
        assert "Connection refused" in result

    def test_timeout(self):
        error = Exception("Operation timed out")
        result = format_kafka_error(error)
        assert "Connection timed out" in result

    def test_timeout_variant(self):
        error = Exception("Request timeout")
        result = format_kafka_error(error)
        assert "Connection timed out" in result

    def test_authentication_error(self):
        error = Exception("Authentication failed")
        result = format_kafka_error(error)
        assert "Authentication failed" in result

    def test_sasl_error(self):
        error = Exception("SASL mechanism not supported")
        result = format_kafka_error(error)
        assert "Authentication failed" in result

    def test_unauthorized(self):
        error = Exception("Unauthorized access")
        result = format_kafka_error(error)
        assert "Not authorized" in result

    def test_not_authorized(self):
        error = Exception("Not authorized to access resource")
        result = format_kafka_error(error)
        assert "Not authorized" in result

    def test_ssl_error(self):
        error = Exception("SSL connection failed")
        result = format_kafka_error(error)
        assert "SSL/TLS error" in result

    def test_certificate_error(self):
        error = Exception("Certificate verification failed")
        result = format_kafka_error(error)
        assert "SSL/TLS error" in result

    def test_handshake_error(self):
        error = Exception("TLS handshake failed")
        result = format_kafka_error(error)
        assert "SSL/TLS error" in result

    def test_unknown_topic(self):
        error = Exception("Unknown topic or partition")
        result = format_kafka_error(error)
        assert "Topic does not exist" in result

    def test_topic_not_found(self):
        error = Exception("Topic not found: my-topic")
        result = format_kafka_error(error)
        assert "Topic does not exist" in result

    def test_topic_authorization(self):
        error = Exception("TopicAuthorizationException")
        result = format_kafka_error(error)
        assert "Not authorized for topic" in result

    def test_replication_factor_error(self):
        error = Exception("Replication factor is too high")
        result = format_kafka_error(error)
        assert "Invalid replication factor" in result

    def test_unknown_error_returns_original(self):
        error = Exception("Some random error message")
        result = format_kafka_error(error)
        assert result == "Some random error message"

    def test_kafka_exception_type(self):
        # Test with actual KafkaException if it can be instantiated
        error = Exception("BrokerTransportFailure")
        result = format_kafka_error(error)
        assert "Cannot connect to broker" in result

    def test_case_insensitive_matching(self):
        error = Exception("BROKERTRANSPORTFAILURE")
        result = format_kafka_error(error)
        assert "Cannot connect to broker" in result
