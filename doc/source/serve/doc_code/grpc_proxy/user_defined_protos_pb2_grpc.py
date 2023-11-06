# __begin__
# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import user_defined_protos_pb2 as user__defined__protos__pb2


class UserDefinedServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.__call__ = channel.unary_unary(
            "/userdefinedprotos.UserDefinedService/__call__",
            request_serializer=user__defined__protos__pb2.UserDefinedMessage.SerializeToString,  # noqa: E501
            response_deserializer=user__defined__protos__pb2.UserDefinedResponse.FromString,  # noqa: E501
        )
        self.Multiplexing = channel.unary_unary(
            "/userdefinedprotos.UserDefinedService/Multiplexing",
            request_serializer=user__defined__protos__pb2.UserDefinedMessage2.SerializeToString,  # noqa: E501
            response_deserializer=user__defined__protos__pb2.UserDefinedResponse2.FromString,  # noqa: E501
        )
        self.Streaming = channel.unary_stream(
            "/userdefinedprotos.UserDefinedService/Streaming",
            request_serializer=user__defined__protos__pb2.UserDefinedMessage.SerializeToString,  # noqa: E501
            response_deserializer=user__defined__protos__pb2.UserDefinedResponse.FromString,  # noqa: E501
        )


class UserDefinedServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def __call__(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Multiplexing(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Streaming(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_UserDefinedServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "__call__": grpc.unary_unary_rpc_method_handler(
            servicer.__call__,
            request_deserializer=user__defined__protos__pb2.UserDefinedMessage.FromString,  # noqa: E501
            response_serializer=user__defined__protos__pb2.UserDefinedResponse.SerializeToString,  # noqa: E501
        ),
        "Multiplexing": grpc.unary_unary_rpc_method_handler(
            servicer.Multiplexing,
            request_deserializer=user__defined__protos__pb2.UserDefinedMessage2.FromString,  # noqa: E501
            response_serializer=user__defined__protos__pb2.UserDefinedResponse2.SerializeToString,  # noqa: E501
        ),
        "Streaming": grpc.unary_stream_rpc_method_handler(
            servicer.Streaming,
            request_deserializer=user__defined__protos__pb2.UserDefinedMessage.FromString,  # noqa: E501
            response_serializer=user__defined__protos__pb2.UserDefinedResponse.SerializeToString,  # noqa: E501
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "userdefinedprotos.UserDefinedService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class UserDefinedService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def __call__(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/userdefinedprotos.UserDefinedService/__call__",
            user__defined__protos__pb2.UserDefinedMessage.SerializeToString,
            user__defined__protos__pb2.UserDefinedResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Multiplexing(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/userdefinedprotos.UserDefinedService/Multiplexing",
            user__defined__protos__pb2.UserDefinedMessage2.SerializeToString,
            user__defined__protos__pb2.UserDefinedResponse2.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Streaming(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/userdefinedprotos.UserDefinedService/Streaming",
            user__defined__protos__pb2.UserDefinedMessage.SerializeToString,
            user__defined__protos__pb2.UserDefinedResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


class ImageClassificationServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Predict = channel.unary_unary(
            "/userdefinedprotos.ImageClassificationService/Predict",
            request_serializer=user__defined__protos__pb2.ImageData.SerializeToString,
            response_deserializer=user__defined__protos__pb2.ImageClass.FromString,
        )


class ImageClassificationServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Predict(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_ImageClassificationServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Predict": grpc.unary_unary_rpc_method_handler(
            servicer.Predict,
            request_deserializer=user__defined__protos__pb2.ImageData.FromString,
            response_serializer=user__defined__protos__pb2.ImageClass.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "userdefinedprotos.ImageClassificationService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class ImageClassificationService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Predict(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/userdefinedprotos.ImageClassificationService/Predict",
            user__defined__protos__pb2.ImageData.SerializeToString,
            user__defined__protos__pb2.ImageClass.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


# __end__
