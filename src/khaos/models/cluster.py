from dataclasses import dataclass
from enum import Enum


class SecurityProtocol(str, Enum):
    PLAINTEXT = "PLAINTEXT"
    SSL = "SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SASL_SSL = "SASL_SSL"


class SaslMechanism(str, Enum):
    PLAIN = "PLAIN"
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"


@dataclass
class ClusterConfig:
    bootstrap_servers: str

    # Security protocol
    security_protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

    # SASL configuration
    sasl_mechanism: SaslMechanism | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    # SSL/TLS configuration
    ssl_ca_location: str | None = None
    ssl_cert_location: str | None = None
    ssl_key_location: str | None = None
    ssl_key_password: str | None = None

    def __post_init__(self):
        # SASL validation
        if self.security_protocol in (
            SecurityProtocol.SASL_PLAINTEXT,
            SecurityProtocol.SASL_SSL,
        ):
            if not self.sasl_mechanism:
                raise ValueError("sasl_mechanism required when using SASL security protocol")
            if not self.sasl_username or not self.sasl_password:
                raise ValueError("sasl_username and sasl_password required for SASL")

        if self.ssl_cert_location and not self.ssl_key_location:
            raise ValueError("ssl_key_location required when ssl_cert_location is provided")
        if self.ssl_key_location and not self.ssl_cert_location:
            raise ValueError("ssl_cert_location required when ssl_key_location is provided")

    def to_kafka_config(self) -> dict:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol.value,
        }

        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism.value
            # Validation ensures these are set when sasl_mechanism is present
            if self.sasl_username:
                config["sasl.username"] = self.sasl_username
            if self.sasl_password:
                config["sasl.password"] = self.sasl_password

        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location
        if self.ssl_cert_location:
            config["ssl.certificate.location"] = self.ssl_cert_location
        if self.ssl_key_location:
            config["ssl.key.location"] = self.ssl_key_location
        if self.ssl_key_password:
            config["ssl.key.password"] = self.ssl_key_password

        return config
