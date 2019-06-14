#ifndef RAY_RPC_OBJECT_MANAGER_CLIENT_H
#define RAY_RPC_OBJECT_MANAGER_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "src/ray/rpc/client_call.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class ObjectManagerClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  ObjectManagerClient(const std::string &address, const int port,
                      ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
        address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
    stub_ = ObjectManagerService::NewStub(channel);
  };

  /// Forward a task and its uncommitted lineage.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  void Push(const PushRequest &request, const ClientCallback<PushReply> &callback) {
    client_call_manager_
        .CreateCall<ObjectManagerService, PushRequest, PushReply>(*stub_,
                                                                &ObjectManagerService::Stub::PrepareAsyncPush, request,
                                                                           callback);
  }

  void Pull(const PullRequest &request, const ClientCallback<PullReply> &callback) {
    client_call_manager_
        .CreateCall<ObjectManagerService, PullRequest, PullReply>(*stub_,
                                                                &ObjectManagerService::Stub::PrepareAsyncPull, request,
                                                                           callback);
  }

 private:
  /// The gRPC-generated stub.
  std::unique_ptr<ObjectManagerService::Stub> stub_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_OBJECT_MANAGER_CLIENT_H
