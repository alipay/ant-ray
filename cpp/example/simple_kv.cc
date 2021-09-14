/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.

/// including the `<ray/api.h>` header
#include <ray/api.h>

#include <chrono>
#include <thread>

std::string MAIN_SERVER_NAME = "main_actor";
std::string BACKUP_SERVER_NAME = "backup_actor";

namespace common {
inline std::pair<bool, std::string> Get(
    const std::string &key, const std::unordered_map<std::string, std::string> &data) {
  auto it = data.find(key);
  if (it == data.end()) {
    return std::pair<bool, std::string>{};
  }

  return {true, it->second};
}
}  // namespace common

class MainServer;

class BackupServer {
 public:
  BackupServer() {
    // Handle failover when the actor restarted.
    if (ray::WasCurrentActorRestarted()) {
      HanldeFailover();
    }
    RAYLOG(INFO) << "BackupServer created";
  }

  // The main server will get all BackupServer's data when it restarted.
  std::unordered_map<std::string, std::string> GetAllData();

  // The main server will sync data at first before puting data.
  void SyncData(const std::string &key, const std::string &val);

 private:
  // Get all data from MainServer.
  void HanldeFailover();

  std::unordered_map<std::string, std::string> data_;
  std::mutex mtx_;
};

std::unordered_map<std::string, std::string> BackupServer::GetAllData() {
  std::unique_lock<std::mutex> lock(mtx_);
  return data_;
}

void BackupServer::SyncData(const std::string &key, const std::string &val) {
  std::unique_lock<std::mutex> lock(mtx_);
  data_[key] = val;
}

class MainServer {
 public:
  MainServer() {
    if (ray::WasCurrentActorRestarted()) {
      HanldeFailover();
    }

    dest_actor_ = ray::GetActor<BackupServer>(BACKUP_SERVER_NAME);
    RAYLOG(INFO) << "MainServer created";
  }

  std::unordered_map<std::string, std::string> GetAllData();

  std::pair<bool, std::string> Get(const std::string &key);

  void Put(const std::string &key, const std::string &val);

 private:
  void HanldeFailover();

  std::unordered_map<std::string, std::string> data_;
  std::mutex mtx_;

  boost::optional<ray::ActorHandle<BackupServer>> dest_actor_;
};

std::unordered_map<std::string, std::string> MainServer::GetAllData() {
  std::unique_lock<std::mutex> lock(mtx_);
  return data_;
}

void BackupServer::HanldeFailover() {
  // Get all data from MainServer.
  auto dest_actor = *ray::GetActor<MainServer>(MAIN_SERVER_NAME);
  data_ = *dest_actor.Task(&MainServer::GetAllData).Remote().Get();
  RAYLOG(INFO) << "BackupServer get all data from MainServer";
}

std::pair<bool, std::string> MainServer::Get(const std::string &key) {
  std::unique_lock<std::mutex> lock(mtx_);
  return common::Get(key, data_);
}

void MainServer::Put(const std::string &key, const std::string &val) {
  // SyncData before put data.
  auto r = (*dest_actor_).Task(&BackupServer::SyncData).Remote(key, val);
  std::vector<ray::ObjectRef<void>> objects{r};
  auto result = ray::Wait(objects, 1, 2000);
  if (result.ready.empty()) {
    RAYLOG(WARNING) << "MainServer SyncData failed.";
  }

  std::unique_lock<std::mutex> lock(mtx_);
  data_[key] = val;
}

void MainServer::HanldeFailover() {
  // Get all data from BackupServer.
  dest_actor_ = ray::GetActor<BackupServer>(BACKUP_SERVER_NAME);
  data_ = *dest_actor_.get().Task(&BackupServer::GetAllData).Remote().Get();
  RAYLOG(INFO) << "MainServer get all data from BackupServer";
}

static MainServer *CreateMainServer() { return new MainServer(); }

static BackupServer *CreateBackupServer() { return new BackupServer(); }

RAY_REMOTE(CreateMainServer, &MainServer::GetAllData, &MainServer::Get, &MainServer::Put);

RAY_REMOTE(CreateBackupServer, &BackupServer::GetAllData, &BackupServer::SyncData);

ray::PlacementGroup CreateSimplePlacementGroup(const std::string &name) {
  std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 1}, {"CPU", 1}}};

  ray::internal::PlacementGroupCreationOptions options{
      false, name, bundles, ray::internal::PlacementStrategy::SPREAD};
  return ray::CreatePlacementGroup(options);
}

void StartServer() {
  auto placement_group = CreateSimplePlacementGroup("first_placement_group");
  assert(placement_group.Wait(10));

  ray::Actor(CreateMainServer)
      .SetMaxRestarts(1)
      .SetPlacementGroup(placement_group, 0)
      .SetName(MAIN_SERVER_NAME)
      .Remote();
  ray::Actor(CreateBackupServer)
      .SetMaxRestarts(1)
      .SetPlacementGroup(placement_group, 1)
      .SetName(BACKUP_SERVER_NAME)
      .Remote();
}

void KillMainServer() {
  auto main_server = *ray::GetActor<MainServer>(MAIN_SERVER_NAME);
  main_server.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(2));
}

class Client {
 public:
  Client() : main_actor_(*ray::GetActor<MainServer>(MAIN_SERVER_NAME)) {}

  void Put(const std::string &key, const std::string &val) {
    main_actor_.Task(&MainServer::Put).Remote(key, val).Get();
  }

  std::pair<bool, std::string> Get(const std::string &key) {
    return *main_actor_.Task(&MainServer::Get).Remote(key).Get();
  }

 private:
  ray::ActorHandle<MainServer> main_actor_;
};

int main(int argc, char **argv) {
  /// Start ray cluster and ray runtime.
  ray::Init();

  StartServer();

  Client client;
  client.Put("hello", "ray");

  auto get_result = [&client](const std::string &key) {
    bool ok;
    std::string result;
    std::tie(ok, result) = client.Get("hello");
    assert(ok);
    assert(result == "ray");
  };

  get_result("hello");

  // Kill main server and then get result.
  KillMainServer();
  get_result("hello");

  /// Stop ray cluster and ray runtime.
  ray::Shutdown();
  return 0;
}
