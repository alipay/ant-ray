#ifndef RAY_RPC_ASIO_CLIENT_H
#define RAY_RPC_ASIO_CLIENT_H

#include <boost/asio.hpp>
#include <thread>
#include <utility>

#include "src/ray/common/client_connection.h"
#include "src/ray/protobuf/asio.pb.h"
#include "src/ray/rpc/client.h"
#include "src/ray/rpc/common.h"

namespace ray {
namespace rpc {

/// Class that represents a RPC client.
class RpcClient {
 public:
  explicit RpcClient(rpc::RpcServiceType service_type, std::string name,
                     const std::string &address, const int port)
      : service_type_(service_type), name_(name), address_(address), port_(port) {}

  /// Destruct this RPC client.
  virtual ~RpcClient() {}

 protected:
  /// Type of the RPC service.
  const rpc::RpcServiceType service_type_;
  /// Name of this client, used for logging and debugging purpose.
  const std::string name_;
  /// IP address of the server.
  const std::string address_;
  /// Port of the server.
  int port_;
};

/// Class that represents an asio based rpc server.
///
/// An `AsioRpcServer` listens on a specific port.
///
/// Subclasses can register one or multiple services to a `AsioRpcServer`, see
/// `RegisterServices`. And they should also implement `InitServerCallFactories` to decide
/// which kinds of requests this server should accept.
class AsioRpcClient : public RpcClient {
 public:
  explicit AsioRpcClient(rpc::RpcServiceType service_type, const std::string &address,
                         const int port, boost::asio::io_service &io_service)
      : RpcClient(service_type, RpcServiceType_Name(service_type), address, port),
        io_service_(io_service),
        request_id_(0),
        is_connected_(false) {}

  Status Connect();

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam GrpcService Type of the gRPC-generated service class.
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return A `ClientCall` representing the request that was just sent.
  template <class Request, class Reply, class MessageType>
  Status CallMethod(MessageType request_type, MessageType reply_type,
                    const Request &request, const ClientCallback<Reply> &callback) {
    if (connection_ == nullptr || !is_connected_) {
      // There are errors, invoke the callback.
      auto status = Status::Invalid("server is not connected");
      Reply reply;
      callback(status, reply);
      return status;
    }

    RpcRequestMessage request_message;
    auto request_id = ++request_id_;
    request_message.set_request_id(request_id);
    request.SerializeToString(request_message.mutable_request());

    std::shared_ptr<std::string> serialized_message = std::make_shared<std::string>();
    request_message.SerializeToString(serialized_message.get());

    RAY_LOG(DEBUG) << "Calling method for service " << name_
                   << ", request id: " << request_id
                   << ", request type: " << static_cast<int>(request_type);

    // The invocation of `WriteMessageAsync` and its async callback needs to be done
    // in the same thread, thus we need to dispatch it to io_thread_.
    // NOTE(zhijunfu): use `WriteMessageAsync` is noticably faster than `WriteMessage`,
    // about 2X faster on task submission, and 50% faster overall.
    io_service_.dispatch([request_id, callback, request_type, reply_type,
                          serialized_message, this] {
      connection_->WriteMessageAsync(
          request_type, static_cast<int64_t>(serialized_message->size()),
          reinterpret_cast<const uint8_t *>(serialized_message->data()),
          [request_id, callback, request_type, reply_type,
           this](const ray::Status &status) {
            if (status.ok()) {
              // Send succeeds. Add the request to the records, so that
              // we can invoke the callback after receivig the reply.
              std::unique_lock<std::mutex> guard(callback_mutex_);
              pending_callbacks_.emplace(
                  request_id,
                  [callback, reply_type, this](const RpcReplyMessage &reply_message) {
                    const auto request_id = reply_message.request_id();
                    auto error_code = static_cast<StatusCode>(reply_message.error_code());
                    auto error_message = reply_message.error_message();
                    Status status = (error_code == StatusCode::OK)
                                        ? Status::OK()
                                        : Status(error_code, error_message);

                    Reply reply;
                    reply.ParseFromString(reply_message.reply());

                    callback(status, reply);
                    RAY_LOG(DEBUG) << "Calling reply callback for message "
                                   << static_cast<int>(reply_type) << " for service "
                                   << name_ << ", request id " << request_id
                                   << ", status: " << status.ToString();
                  });

            } else {
              // There are errors, invoke the callback.
              Reply reply;
              callback(status, reply);
              RAY_LOG(DEBUG) << "Failed to write request message "
                             << static_cast<int>(request_type) << " for service " << name_
                             << " to " << address_ << ":" << port_ << ", request id "
                             << request_id << ", status: " << status.ToString();
            }
          });
    });

    return Status::OK();
  }

 protected:
  void ProcessServerMessage(const std::shared_ptr<TcpClientConnection> &client,
                            int64_t message_type, uint64_t length,
                            const uint8_t *message_data);

  void ProcessDisconnectClientMessage(const std::shared_ptr<TcpClientConnection> &client);

  using ReplyCallback = std::function<void(const RpcReplyMessage &)>;
  /// Map from request id to the corresponding reply callback, which will be
  /// invoked when the reply is received for the request.
  std::unordered_map<uint64_t, ReplyCallback> pending_callbacks_;
  /// Mutex to protect the `pending_callbacks_` above.
  std::mutex callback_mutex_;

  /// IO service to handle the service calls.
  boost::asio::io_service &io_service_;
  /// Connection to server. Note that TCP is full-duplex, and it's OK for
  /// read and write simultaneously in different threads, provided that
  /// there's only one thread for read and one for write. In this case
  /// we don't need a lock for it.
  std::shared_ptr<TcpClientConnection> connection_;

  // Request sequence id which starts with 1.
  std::atomic_uint64_t request_id_;

  /// Whether we have connected to server.
  std::atomic_bool is_connected_;
};

}  // namespace rpc
}  // namespace ray

#endif