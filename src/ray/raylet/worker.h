// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

#include "absl/memory/memory.h"
#include "gtest/gtest_prod.h"
#include "ray/common/client_connection.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/util/process.h"

namespace ray {

namespace raylet {

/// \class WorkerPoolInterface
///
/// Used for new scheduler unit tests.
class WorkerInterface {
 public:
  /// A destructor responsible for freeing all worker state.
  virtual ~WorkerInterface() {}
  virtual rpc::WorkerType GetWorkerType() const = 0;
  virtual void MarkDead() = 0;
  virtual bool IsDead() const = 0;
  virtual void MarkBlocked() = 0;
  virtual void MarkUnblocked() = 0;
  virtual bool IsBlocked() const = 0;
  /// Return the worker's ID.
  virtual WorkerID WorkerId() const = 0;
  /// Return the worker process.
  virtual Process GetProcess() const = 0;
  /// Return the worker process's startup token
  virtual StartupToken GetStartupToken() const = 0;
  virtual void SetProcess(Process proc) = 0;
  /// Return the worker shim process.
  virtual Process GetShimProcess() const = 0;
  virtual void SetShimProcess(Process proc) = 0;
  virtual Language GetLanguage() const = 0;
  virtual const std::string IpAddress() const = 0;
  /// Connect this worker's gRPC client.
  virtual void Connect(int port) = 0;
  /// Testing-only
  virtual void Connect(std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client) = 0;
  virtual int Port() const = 0;
  virtual int AssignedPort() const = 0;
  virtual void SetAssignedPort(int port) = 0;
  virtual void AssignTaskId(const TaskID &task_id) = 0;
  virtual const TaskID &GetAssignedTaskId() const = 0;
  virtual bool AddBlockedTaskId(const TaskID &task_id) = 0;
  virtual bool RemoveBlockedTaskId(const TaskID &task_id) = 0;
  virtual const std::unordered_set<TaskID> &GetBlockedTaskIds() const = 0;
  virtual const JobID &GetAssignedJobId() const = 0;
  virtual int GetRuntimeEnvHash() const = 0;
  virtual void AssignActorId(const ActorID &actor_id) = 0;
  virtual const ActorID &GetActorId() const = 0;
  virtual void MarkDetachedActor() = 0;
  virtual bool IsDetachedActor() const = 0;
  virtual const std::shared_ptr<ClientConnection> Connection() const = 0;
  virtual void SetOwnerAddress(const rpc::Address &address) = 0;
  virtual const rpc::Address &GetOwnerAddress() const = 0;

  virtual const ResourceIdSet &GetLifetimeResourceIds() const = 0;
  virtual void SetLifetimeResourceIds(ResourceIdSet &resource_ids) = 0;
  virtual void ResetLifetimeResourceIds() = 0;

  virtual const ResourceIdSet &GetTaskResourceIds() const = 0;
  virtual void SetTaskResourceIds(ResourceIdSet &resource_ids) = 0;
  virtual void ResetTaskResourceIds() = 0;
  virtual ResourceIdSet ReleaseTaskCpuResources() = 0;
  virtual void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources) = 0;

  virtual void DirectActorCallArgWaitComplete(int64_t tag) = 0;

  virtual const BundleID &GetBundleId() const = 0;
  virtual void SetBundleId(const BundleID &bundle_id) = 0;

  // Setter, geter, and clear methods  for allocated_instances_.
  virtual void SetAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) = 0;

  virtual std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() = 0;

  virtual void ClearAllocatedInstances() = 0;

  virtual void SetLifetimeAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) = 0;
  virtual std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() = 0;

  virtual void ClearLifetimeAllocatedInstances() = 0;

  virtual RayTask &GetAssignedTask() = 0;

  virtual void SetAssignedTask(const RayTask &assigned_task) = 0;

  virtual bool IsRegistered() = 0;

  virtual rpc::CoreWorkerClientInterface *rpc_client() = 0;

 protected:
  virtual void SetStartupToken(StartupToken startup_token) = 0;

  FRIEND_TEST(WorkerPoolTest, PopWorkerMultiTenancy);
  FRIEND_TEST(WorkerPoolTest, TestWorkerCapping);
  FRIEND_TEST(WorkerPoolTest, TestWorkerCappingLaterNWorkersNotOwningObjects);
  FRIEND_TEST(WorkerPoolTest, MaximumStartupConcurrency);
};

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker : public WorkerInterface {
 public:
  /// A constructor that initializes a worker object.
  /// NOTE: You MUST manually set the worker process.
  Worker(const JobID &job_id, const int runtime_env_hash, const WorkerID &worker_id,
         const Language &language, rpc::WorkerType worker_type,
         const std::string &ip_address, std::shared_ptr<ClientConnection> connection,
         rpc::ClientCallManager &client_call_manager, StartupToken startup_token);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  rpc::WorkerType GetWorkerType() const;
  void MarkDead();
  bool IsDead() const;
  void MarkBlocked();
  void MarkUnblocked();
  bool IsBlocked() const;
  /// Return the worker's ID.
  WorkerID WorkerId() const;
  /// Return the worker process.
  Process GetProcess() const;
  /// Return the worker process's startup token
  StartupToken GetStartupToken() const;
  void SetProcess(Process proc);
  /// Return this worker shim process.
  Process GetShimProcess() const;
  void SetShimProcess(Process proc);
  Language GetLanguage() const;
  const std::string IpAddress() const;
  /// Connect this worker's gRPC client.
  void Connect(int port);
  /// Testing-only
  void Connect(std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client);
  int Port() const;
  int AssignedPort() const;
  void SetAssignedPort(int port);
  void AssignTaskId(const TaskID &task_id);
  const TaskID &GetAssignedTaskId() const;
  bool AddBlockedTaskId(const TaskID &task_id);
  bool RemoveBlockedTaskId(const TaskID &task_id);
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const;
  const JobID &GetAssignedJobId() const;
  int GetRuntimeEnvHash() const;
  void AssignActorId(const ActorID &actor_id);
  const ActorID &GetActorId() const;
  void MarkDetachedActor();
  bool IsDetachedActor() const;
  const std::shared_ptr<ClientConnection> Connection() const;
  void SetOwnerAddress(const rpc::Address &address);
  const rpc::Address &GetOwnerAddress() const;

  const ResourceIdSet &GetLifetimeResourceIds() const;
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids);
  void ResetLifetimeResourceIds();

  const ResourceIdSet &GetTaskResourceIds() const;
  void SetTaskResourceIds(ResourceIdSet &resource_ids);
  void ResetTaskResourceIds();
  ResourceIdSet ReleaseTaskCpuResources();
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources);

  void DirectActorCallArgWaitComplete(int64_t tag);

  const BundleID &GetBundleId() const;
  void SetBundleId(const BundleID &bundle_id);

  // Setter, geter, and clear methods  for allocated_instances_.
  void SetAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    allocated_instances_ = allocated_instances;
  };

  std::shared_ptr<TaskResourceInstances> GetAllocatedInstances() {
    return allocated_instances_;
  };

  void ClearAllocatedInstances() { allocated_instances_ = nullptr; };

  void SetLifetimeAllocatedInstances(
      const std::shared_ptr<TaskResourceInstances> &allocated_instances) {
    lifetime_allocated_instances_ = allocated_instances;
  };

  std::shared_ptr<TaskResourceInstances> GetLifetimeAllocatedInstances() {
    return lifetime_allocated_instances_;
  };

  void ClearLifetimeAllocatedInstances() { lifetime_allocated_instances_ = nullptr; };

  RayTask &GetAssignedTask() { return assigned_task_; };

  void SetAssignedTask(const RayTask &assigned_task) { assigned_task_ = assigned_task; };

  bool IsRegistered() { return rpc_client_ != nullptr; }

  rpc::CoreWorkerClientInterface *rpc_client() {
    RAY_CHECK(IsRegistered());
    return rpc_client_.get();
  }

 protected:
  void SetStartupToken(StartupToken startup_token);

 private:
  /// The worker's ID.
  WorkerID worker_id_;
  /// The worker's process.
  Process proc_;
  /// The worker's process's startup_token
  StartupToken startup_token_;
  /// The worker's shim process. The shim process PID is the same with worker process PID,
  /// except starting worker process in container.
  Process shim_proc_;
  /// The language type of this worker.
  Language language_;
  /// The type of the worker.
  rpc::WorkerType worker_type_;
  /// IP address of this worker.
  std::string ip_address_;
  /// Port assigned to this worker by the raylet. If this is 0, the actual
  /// port the worker listens (port_) on will be a random one. This is required
  /// because a worker could crash before announcing its port, in which case
  /// we still need to be able to mark that port as free.
  int assigned_port_;
  /// Port that this worker listens on.
  int port_;
  /// Connection state of a worker.
  std::shared_ptr<ClientConnection> connection_;
  /// The worker's currently assigned task.
  TaskID assigned_task_id_;
  /// Job ID for the worker's current assigned task.
  const JobID assigned_job_id_;
  /// The hash of the worker's assigned runtime env.  We use this in the worker
  /// pool to cache and reuse workers with the same runtime env, because
  /// installing runtime envs from scratch can be slow.
  const int runtime_env_hash_;
  /// The worker's actor ID. If this is nil, then the worker is not an actor.
  ActorID actor_id_;
  /// The worker's placement group bundle. It is used to detect if the worker is
  /// associated with a placement group bundle.
  BundleID bundle_id_;
  /// Whether the worker is dead.
  bool dead_;
  /// Whether the worker is blocked. Workers become blocked in a `ray.get`, if
  /// they require a data dependency while executing a task.
  bool blocked_;
  /// The specific resource IDs that this worker owns for its lifetime. This is
  /// only used for actors.
  ResourceIdSet lifetime_resource_ids_;
  /// The specific resource IDs that this worker currently owns for the duration
  // of a task.
  ResourceIdSet task_resource_ids_;
  std::unordered_set<TaskID> blocked_task_ids_;
  /// The `ClientCallManager` object that is shared by `CoreWorkerClient` from all
  /// workers.
  rpc::ClientCallManager &client_call_manager_;
  /// The rpc client to send tasks to this worker.
  std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client_;
  /// Whether the worker is detached. This is applies when the worker is actor.
  /// Detached actor means the actor's creator can exit without killing this actor.
  bool is_detached_actor_;
  /// The address of this worker's owner. The owner is the worker that
  /// currently holds the lease on this worker, if any.
  rpc::Address owner_address_;
  /// The capacity of each resource instance allocated to this worker in order
  /// to satisfy the resource requests of the task is currently running.
  std::shared_ptr<TaskResourceInstances> allocated_instances_;
  /// The capacity of each resource instance allocated to this worker
  /// when running as an actor.
  std::shared_ptr<TaskResourceInstances> lifetime_allocated_instances_;
  /// RayTask being assigned to this worker.
  RayTask assigned_task_;
};

}  // namespace raylet

}  // namespace ray
