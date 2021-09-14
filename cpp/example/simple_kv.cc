/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.

/// including the `<ray/api.h>` header
#include <ray/api.h>

#include <chrono>
#include <thread>

enum Role {
  SLAVE = 0,
  MASTER = 1,
};
MSGPACK_ADD_ENUM(Role);

inline std::string RoleName(Role role) {
  return role == Role::SLAVE ? "slave_actor" : "master_actor";
}

inline std::string DestRoleName(Role role) {
  return role == Role::SLAVE ? "master_actor" : "slave_actor";
}

class KVStore {
 public:
  KVStore(Role role) : role_(role) {
    was_restared_ = ray::WasCurrentActorRestarted();

    if (was_restared_) {
      HanldeFaileover();
    }
  }

  std::unordered_map<std::string, std::string> GetAllData() {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_;
  }

  std::pair<bool, std::string> Get(const std::string &key) {
    std::unique_lock<std::mutex> lock(mtx_);
    auto it = data_.find(key);
    if (it == data_.end()) {
      return std::pair<bool, std::string>{};
    }

    return {true, it->second};
  }

  virtual void Put(const std::string &key, const std::string &val) = 0;

  virtual bool Del(const std::string &key) = 0;

  virtual void SyncData(const std::string &key, const std::string &val) = 0;

  Role GetRole() const { return role_; }

  bool WasRestarted() const { return was_restared_; }

  bool Empty() {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_.empty();
  }

  size_t Size() {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_.size();
  }

 protected:
  void HanldeFaileover() {
    dest_actor_ = ray::GetActor<KVStore>(DestRoleName(role_));
    data_ = *dest_actor_.get().Task(&KVStore::GetAllData).Remote().Get();
    RAYLOG(INFO) << RoleName(role_) << " pull all data from " << DestRoleName(role_)
                 << " was_restared:" << was_restared_ << ", data size: " << data_.size();
  }

  Role role_ = Role::SLAVE;
  bool was_restared_ = false;
  std::unordered_map<std::string, std::string> data_;
  std::mutex mtx_;

  boost::optional<ray::ActorHandle<KVStore>> dest_actor_;
};

class Master : public KVStore {
 public:
  Master() : KVStore(Role::MASTER) {
    dest_actor_ = ray::GetActor<KVStore>(RoleName(Role::SLAVE));
    RAYLOG(INFO) << RoleName(role_) << "Master created";
  }

  void Put(const std::string &key, const std::string &val) {
    auto r = (*dest_actor_).Task(&KVStore::SyncData).Remote(key, val);
    std::vector<ray::ObjectRef<void>> objects{r};
    auto result = ray::Wait(objects, 1, 2000);
    if (result.ready.empty()) {
      RAYLOG(WARNING) << RoleName(role_) << " SyncData failed.";
    }

    std::unique_lock<std::mutex> lock(mtx_);
    data_[key] = val;
  }

  bool Del(const std::string &key) {
    auto r = (*dest_actor_).Task(&KVStore::Del).Remote(key);
    std::vector<ray::ObjectRef<bool>> objects{r};
    auto result = ray::Wait(objects, 1, 2000);
    if (result.ready.empty()) {
      RAYLOG(WARNING) << RoleName(role_) << " Del Slave data failed.";
    }

    std::unique_lock<std::mutex> lock(mtx_);
    return data_.erase(key) > 0;
  }

  void SyncData(const std::string &key, const std::string &val) {
    throw std::logic_error("SyncData is not supported in Master.");
  }
};

class Slave : public KVStore {
 public:
  Slave() : KVStore(Role::SLAVE) { RAYLOG(INFO) << RoleName(role_) << "Slave created"; }

  bool Del(const std::string &key) {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_.erase(key) > 0;
  }

  void SyncData(const std::string &key, const std::string &val) {
    std::unique_lock<std::mutex> lock(mtx_);
    data_[key] = val;
  }

  void Put(const std::string &key, const std::string &val) {
    throw std::logic_error("Put is not supported in Slave.");
  }
};

static KVStore *Create(Role role) {
  if (role == Role::MASTER) {
    return new Master();
  }
  return new Slave();
}

RAY_REMOTE(Create, &KVStore::GetAllData, &KVStore::Get, &KVStore::Put, &KVStore::SyncData,
           &KVStore::Del, &KVStore::GetRole, &KVStore::WasRestarted, &KVStore::Empty,
           &KVStore::Size);

void PrintActor(ray::ActorHandle<KVStore> &actor) {
  Role role = *actor.Task(&KVStore::GetRole).Remote().Get();
  bool empty = *actor.Task(&KVStore::Empty).Remote().Get();
  bool was_restarted = *actor.Task(&KVStore::WasRestarted).Remote().Get();

  if (empty) {
    std::cout << RoleName(role) << " is empty, "
              << "was_restarted: " << was_restarted << "\n";
    return;
  }

  auto pair = *actor.Task(&KVStore::Get).Remote("hello").Get();
  size_t size = *actor.Task(&KVStore::Size).Remote().Get();

  std::cout << RoleName(role) << " was_restarted: " << was_restarted << ", size: " << size
            << ", value: " << pair.second << "\n";
}

void BasiceUsage() {
  ray::ActorHandle<KVStore> master_actor = ray::Actor(Create)
                                               .SetMaxRestarts(1)
                                               .SetName(RoleName(Role::MASTER))
                                               .Remote(Role::MASTER);
  ray::ActorHandle<KVStore> slave_actor = ray::Actor(Create)
                                              .SetMaxRestarts(1)
                                              .SetName(RoleName(Role::SLAVE))
                                              .Remote(Role::SLAVE);

  master_actor.Task(&KVStore::Put).Remote("hello", "world").Get();

  PrintActor(master_actor);
  PrintActor(slave_actor);

  master_actor.Task(&KVStore::Del).Remote("hello").Get();
  PrintActor(master_actor);
  PrintActor(slave_actor);

  master_actor.Task(&KVStore::Put).Remote("hello", "world").Get();
  PrintActor(master_actor);
  PrintActor(slave_actor);
}

void Faileover() {
  auto master_actor = *ray::GetActor<KVStore>(RoleName(Role::MASTER));
  master_actor.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  PrintActor(master_actor);

  auto slave_actor = *ray::GetActor<KVStore>(RoleName(Role::SLAVE));
  PrintActor(slave_actor);

  slave_actor.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  PrintActor(slave_actor);

  master_actor.Kill();
  slave_actor.Kill();
}

ray::PlacementGroup CreateSimplePlacementGroup(const std::string &name) {
  std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 1}, {"CPU", 1}}};

  ray::internal::PlacementGroupCreationOptions options{
      false, name, bundles, ray::internal::PlacementStrategy::SPREAD};
  return ray::CreatePlacementGroup(options);
}

void PlacementGroup() {
  auto placement_group = CreateSimplePlacementGroup("first_placement_group");
  assert(placement_group.Wait(10));

  ray::ActorHandle<KVStore> master_actor = ray::Actor(Create)
                                               .SetMaxRestarts(1)
                                               .SetPlacementGroup(placement_group, 0)
                                               .SetName(RoleName(Role::MASTER))
                                               .Remote(Role::MASTER);
  ray::ActorHandle<KVStore> slave_actor = ray::Actor(Create)
                                              .SetMaxRestarts(1)
                                              .SetPlacementGroup(placement_group, 1)
                                              .SetName(RoleName(Role::SLAVE))
                                              .Remote(Role::SLAVE);

  master_actor.Task(&KVStore::Put).Remote("hello", "world").Get();
  PrintActor(master_actor);
  PrintActor(slave_actor);

  master_actor.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  PrintActor(master_actor);
  PrintActor(slave_actor);

  ray::RemovePlacementGroup(placement_group.GetID());
}

int main(int argc, char **argv) {
  /// initialization
  ray::Init();

  BasiceUsage();
  Faileover();
  PlacementGroup();

  /// shutdown
  ray::Shutdown();
  return 0;
}
