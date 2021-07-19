#pragma once

#include <ray/api/common_types.h>
#include <ray/api/function_manager.h>
#include <boost/dll.hpp>
#include <memory>
#include <msgpack.hpp>
#include <string>
#include <unordered_map>

using namespace ::ray::internal;

namespace ray {
namespace api {

using EntryFuntion = std::function<msgpack::sbuffer(
    const std::string &, const std::vector<msgpack::sbuffer> &, msgpack::sbuffer *)>;

class FunctionHelper {
 public:
  static FunctionHelper &GetInstance() {
    // We use `new` here because we don't want to destruct this instance forever.
    // If we do destruct, the shared libraries will be unloaded. And Maybe the unloading
    // will bring some errors which hard to debug.
    static auto *instance = new FunctionHelper();
    return *instance;
  }

  void LoadDll(const boost::filesystem::path &lib_path);
  void LoadFunctionsFromPaths(const std::vector<std::string> &paths);
  const EntryFuntion &GetExecutableFunctions(const std::string &function_name);
  const EntryFuntion &GetExecutableMemberFunctions(const std::string &function_name);

 private:
  FunctionHelper() = default;
  ~FunctionHelper() = default;
  FunctionHelper(FunctionHelper const &) = delete;
  FunctionHelper(FunctionHelper &&) = delete;
  std::string LoadAllRemoteFunctions(const std::string lib_path,
                                     const boost::dll::shared_library &lib,
                                     const EntryFuntion &entry_function);
  std::unordered_map<std::string, std::shared_ptr<boost::dll::shared_library>> libraries_;
  // Map from remote function name to executable entry function.
  std::unordered_map<std::string, EntryFuntion> remote_funcs_;
  // Map from remote member function name to executable entry function.
  std::unordered_map<std::string, EntryFuntion> remote_member_funcs_;
};
}  // namespace api
}  // namespace ray