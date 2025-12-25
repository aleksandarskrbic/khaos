from __future__ import annotations

from typing import Any, cast

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.schema_registry.protobuf import (
    ProtobufSerializer as ConfluentProtobufSerializer,
)
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf import descriptor_pb2, message_factory
from google.protobuf import descriptor_pool as dp
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from khaos.models.schema import FieldSchema

# Type mapping from FieldSchema to Protobuf field types
FIELD_TYPE_MAP = {
    "string": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "int": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    "float": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
    "boolean": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    "uuid": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "timestamp": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    "faker": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
}


def _sanitize_name(name: str) -> str:
    return name.title().replace("-", "").replace("_", "")


def _add_field_to_message(
    message_type: descriptor_pb2.DescriptorProto,
    field: FieldSchema,
    field_number: int,
    nested_types: list[descriptor_pb2.DescriptorProto],
    enum_types: list[descriptor_pb2.EnumDescriptorProto],
) -> None:
    field_proto = message_type.field.add()
    field_proto.name = field.name
    field_proto.number = field_number

    field_type = field.type

    if field_type == "array":
        field_proto.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED

        if field.items:
            item_type = field.items.type
            if item_type in FIELD_TYPE_MAP:
                field_proto.type = FIELD_TYPE_MAP[item_type]
            elif item_type == "object":
                # Nested message in array
                nested_name = _sanitize_name(field.name) + "Item"
                nested_msg = descriptor_pb2.DescriptorProto()
                nested_msg.name = nested_name

                inner_nested: list[descriptor_pb2.DescriptorProto] = []
                inner_enums: list[descriptor_pb2.EnumDescriptorProto] = []

                for i, nested_field in enumerate(field.items.fields or [], start=1):
                    _add_field_to_message(nested_msg, nested_field, i, inner_nested, inner_enums)

                for nt in inner_nested:
                    nested_msg.nested_type.append(nt)
                for et in inner_enums:
                    nested_msg.enum_type.append(et)

                nested_types.append(nested_msg)
                field_proto.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
                field_proto.type_name = nested_name
            else:
                field_proto.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        else:
            field_proto.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
        return

    # Non-array fields
    field_proto.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

    if field_type in FIELD_TYPE_MAP:
        field_proto.type = FIELD_TYPE_MAP[field_type]

    elif field_type == "enum":
        # Create enum type
        enum_name = _sanitize_name(field.name) + "Enum"
        enum_proto = descriptor_pb2.EnumDescriptorProto()
        enum_proto.name = enum_name

        for i, value in enumerate(field.values or ["UNKNOWN"]):
            enum_value = enum_proto.value.add()
            enum_value.name = value
            enum_value.number = i

        enum_types.append(enum_proto)
        field_proto.type = descriptor_pb2.FieldDescriptorProto.TYPE_ENUM
        field_proto.type_name = enum_name

    elif field_type == "object":
        nested_name = _sanitize_name(field.name) + "Type"
        nested_msg = descriptor_pb2.DescriptorProto()
        nested_msg.name = nested_name

        obj_nested: list[descriptor_pb2.DescriptorProto] = []
        obj_enums: list[descriptor_pb2.EnumDescriptorProto] = []

        for i, nested_field in enumerate(field.fields or [], start=1):
            _add_field_to_message(nested_msg, nested_field, i, obj_nested, obj_enums)

        for nt in obj_nested:
            nested_msg.nested_type.append(nt)
        for et in obj_enums:
            nested_msg.enum_type.append(et)

        nested_types.append(nested_msg)
        field_proto.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        field_proto.type_name = nested_name

    else:
        field_proto.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING


def field_schemas_to_protobuf(
    fields: list[FieldSchema],
    name: str,
) -> tuple[descriptor_pb2.FileDescriptorProto, type]:
    """
    Convert FieldSchema list to a Protobuf FileDescriptor and message class.

    Returns:
        Tuple of (FileDescriptorProto, generated message class)
    """
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = f"khaos_generated_{name.lower()}.proto"
    file_proto.package = "khaos.generated"
    file_proto.syntax = "proto3"

    message_type = file_proto.message_type.add()
    message_type.name = name

    nested_types: list[descriptor_pb2.DescriptorProto] = []
    enum_types: list[descriptor_pb2.EnumDescriptorProto] = []

    for i, field in enumerate(fields, start=1):
        _add_field_to_message(message_type, field, i, nested_types, enum_types)

    for nt in nested_types:
        message_type.nested_type.append(nt)
    for et in enum_types:
        message_type.enum_type.append(et)

    # Create descriptor pool and add the file
    pool = dp.DescriptorPool()
    pool.Add(file_proto)

    # Get the message descriptor
    full_name = f"khaos.generated.{name}"
    message_descriptor = pool.FindMessageTypeByName(full_name)

    # Create the message class using the new API (protobuf 4.x+)
    message_class = message_factory.GetMessageClass(message_descriptor)

    return file_proto, message_class


def _set_field_value(
    message: Message,
    field_name: str,
    value: Any,
    message_class: type,
) -> None:
    """Set a field value on a protobuf message, handling type conversions."""
    field_descriptor = message.DESCRIPTOR.fields_by_name.get(field_name)
    if not field_descriptor:
        return

    # Handle enum fields - convert string to int
    if field_descriptor.enum_type:
        if isinstance(value, str):
            enum_value = field_descriptor.enum_type.values_by_name.get(value)
            value = enum_value.number if enum_value else 0
        setattr(message, field_name, value)
        return

    # Handle nested message fields
    is_repeated = getattr(field_descriptor, "is_repeated", None)
    is_repeated_val = is_repeated() if callable(is_repeated) else is_repeated

    if field_descriptor.message_type:
        if is_repeated_val:
            # Repeated message field (array of objects)
            repeated_field = getattr(message, field_name)
            for item in value:
                if isinstance(item, dict):
                    nested_msg = repeated_field.add()
                    for k, v in item.items():
                        _set_field_value(nested_msg, k, v, type(nested_msg))
                else:
                    repeated_field.append(item)
        else:
            # Single nested message
            nested_msg = getattr(message, field_name)
            if isinstance(value, dict):
                for k, v in value.items():
                    _set_field_value(nested_msg, k, v, type(nested_msg))
        return

    # Handle repeated scalar fields (arrays of primitives)
    if is_repeated_val:
        getattr(message, field_name).extend(value)
        return

    # Handle scalar fields
    setattr(message, field_name, value)


def dict_to_protobuf_message(data: dict[str, Any], message_class: type) -> Message:
    """Convert a dictionary to a protobuf message instance."""
    message = message_class()

    for field_name, value in data.items():
        _set_field_value(message, field_name, value, message_class)

    return message


def protobuf_message_to_dict(message: Message) -> dict[str, Any]:
    """Convert a protobuf message to a dictionary."""
    return cast("dict[str, Any]", MessageToDict(message, preserving_proto_field_name=True))


class ProtobufSerializer:
    """Protobuf serializer with Schema Registry support."""

    def __init__(
        self,
        schema_registry_url: str,
        message_class: type,
        topic: str,
    ):
        self.message_class = message_class
        self.topic = topic
        self.registry_client = SchemaRegistryClient({"url": schema_registry_url})
        self._confluent_serializer = ConfluentProtobufSerializer(
            message_class,
            self.registry_client,
            conf={"auto.register.schemas": True},
        )
        self._confluent_deserializer = ProtobufDeserializer(message_class)

    def serialize(self, data: dict[str, Any]) -> bytes:
        message = dict_to_protobuf_message(data, self.message_class)
        result: bytes = self._confluent_serializer(
            message,
            SerializationContext(self.topic, MessageField.VALUE),
        )
        return result

    def deserialize(self, data: bytes) -> dict[str, Any]:
        message = self._confluent_deserializer(
            data,
            SerializationContext(self.topic, MessageField.VALUE),
        )
        return protobuf_message_to_dict(message)


class ProtobufSerializerNoRegistry:
    """Protobuf serializer without Schema Registry (standalone mode)."""

    def __init__(self, message_class: type):
        self.message_class = message_class

    def serialize(self, data: dict[str, Any]) -> bytes:
        message = dict_to_protobuf_message(data, self.message_class)
        return cast("bytes", message.SerializeToString())

    def deserialize(self, data: bytes) -> dict[str, Any]:
        message = self.message_class()
        message.ParseFromString(data)
        return protobuf_message_to_dict(message)
