import abc
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Callable, TypeVar, List, Optional, Dict, Union, Type, Tuple

import ray
from ray import cloudpickle
from ray.exceptions import RayActorError
from ray.ray_constants import env_integer
from ray.train.checkpoint import CheckpointStrategy
from ray.train.constants import ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, \
    TUNE_INSTALLED, TUNE_CHECKPOINT_FILE_NAME, \
    TUNE_CHECKPOINT_ID, ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV
from ray.train.session import TrainingResultType, TrainingResult
from ray.train.session import init_session, get_session, shutdown_session
from ray.train.utils import construct_path, check_for_failure
from ray.train.delayed_worker_group import DelayedWorkerGroup
from ray.train.utils import PropagatingThread, RayDataset


from typing import Callable, List, Any, Dict, Optional
import logging
import socket
import time
import os
import random
import math
import threading

from horovod.runner.common.util import timeout, secret

from horovod.runner.http.http_server import RendezvousServer
from horovod.ray.utils import detect_nics
from horovod.runner.gloo_run import (create_slot_env_vars, create_run_env_vars,
                                     _get_min_start_hosts)
from horovod.runner.elastic.rendezvous import create_rendezvous_handler
from horovod.runner.elastic.discovery import HostDiscovery
from ray.train.worker_group import WorkerGroup
from horovod.runner.elastic.driver import ElasticDriver

if TUNE_INSTALLED:
    from ray import tune
else:
    tune = None

T = TypeVar("T")

logger = logging.getLogger(__name__)


class BackendConfig:
    """Parent class for configurations of training backend."""

    @property
    def backend_cls(self):
        raise NotImplementedError


class TrainBackendError(Exception):
    """Errors with BackendExecutor that should not be exposed to user."""


class CheckpointManager:
    """Manages checkpoint processing, writing, and loading.


    - A ``checkpoints`` directory is created in the ``run_dir`` and contains
    all the checkpoint files.

    The full default path will be:

    ~/ray_results/train_<datestring>/run_<run_id>/checkpoints/
    checkpoint_<checkpoint_id>

    Attributes:
        latest_checkpoint_dir (Optional[Path]): Path to the file directory for
            the checkpoints from the latest run. Configured through
            ``start_training``.
        latest_checkpoint_filename (Optional[str]): Filename for the latest
            checkpoint.
        latest_checkpoint_path (Optional[Path]): Path to the latest persisted
            checkpoint from the latest run.
        latest_checkpoint_id (Optional[int]): The id of the most recently
            saved checkpoint.
        latest_checkpoint (Optional[Dict]): The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    def on_init(self):
        """Checkpoint code executed during BackendExecutor init."""
        self.latest_checkpoint = None

        # Incremental unique checkpoint ID of this run.
        self._latest_checkpoint_id = 0

    def on_start_training(
            self,
            checkpoint_strategy: Optional[CheckpointStrategy],
            run_dir: Path,
            latest_checkpoint_id: Optional[int] = None,
    ):
        """Checkpoint code executed during BackendExecutor start_training."""
        # Restart checkpointing.
        self._latest_checkpoint_id = latest_checkpoint_id if \
            latest_checkpoint_id else 0
        self._checkpoint_strategy = CheckpointStrategy() if \
            checkpoint_strategy is None else checkpoint_strategy
        self.run_dir = run_dir

    def _process_checkpoint(self,
                            checkpoint_results: List[TrainingResult]) -> None:
        """Perform all processing for a checkpoint. """

        # Get checkpoint from first worker.
        checkpoint = checkpoint_results[0].data

        # Increment checkpoint id.
        self._latest_checkpoint_id += 1

        # Store checkpoint in memory.
        self.latest_checkpoint = checkpoint

        self.write_checkpoint(checkpoint)

    def _load_checkpoint(self,
                         checkpoint_to_load: Optional[Union[Dict, str, Path]]
                         ) -> Optional[Dict]:
        """Load the checkpoint dictionary from the input dict or path."""
        if checkpoint_to_load is None:
            return None
        if isinstance(checkpoint_to_load, Dict):
            return checkpoint_to_load
        else:
            # Load checkpoint from path.
            checkpoint_path = Path(checkpoint_to_load).expanduser()
            if not checkpoint_path.exists():
                raise ValueError(f"Checkpoint path {checkpoint_path} "
                                 f"does not exist.")
            with checkpoint_path.open("rb") as f:
                return cloudpickle.load(f)

    def write_checkpoint(self, checkpoint: Dict):
        """Writes checkpoint to disk."""
        if self._checkpoint_strategy.num_to_keep == 0:
            # Checkpoints should not be persisted to disk.
            return

        # TODO(matt): Implement additional checkpoint strategy functionality.
        # Get or create checkpoint dir.
        self.latest_checkpoint_dir.mkdir(parents=True, exist_ok=True)
        # Write checkpoint to disk.
        with self.latest_checkpoint_path.open("wb") as f:
            cloudpickle.dump(checkpoint, f)
            logger.debug(f"Checkpoint successfully written to: "
                         f"{self.latest_checkpoint_path}")

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the latest checkpoint directory."""
        checkpoint_dir = Path("checkpoints")
        return construct_path(checkpoint_dir, self.run_dir)

    @property
    def latest_checkpoint_file_name(self) -> Optional[str]:
        """Filename to use for the latest checkpoint."""
        if self._latest_checkpoint_id > 0:
            return f"checkpoint_{self._latest_checkpoint_id:06d}"
        else:
            return None

    @property
    def latest_checkpoint_path(self) -> Optional[Path]:
        """Path to the latest persisted checkpoint."""
        if self._latest_checkpoint_id > 0:
            checkpoint_file = self.latest_checkpoint_file_name
            return self.latest_checkpoint_dir.joinpath(checkpoint_file)
        else:
            return None

class RayHostDiscovery(HostDiscovery):
    """Uses Ray global state to obtain host mapping.

    Assumes that the whole global state is available for usage."""

    def __init__(self, use_gpu=False, cpus_per_slot=1, gpus_per_slot=1):
        self.use_gpu = use_gpu
        self.cpus_per_slot = cpus_per_slot
        self.gpus_per_slot = gpus_per_slot
        logger.debug(f"Discovery started with {cpus_per_slot} CPU / "
                     f"{gpus_per_slot} GPU per slot.")

    def find_available_hosts_and_slots(self) -> Dict[str, int]:
        """Returns a dict mapping <hostname> -> <number of slots>."""
        alive_nodes = [k for k in ray.nodes() if k["alive"]]
        host_mapping = {}
        for node in alive_nodes:
            hostname = node["NodeManagerAddress"]
            resources = node["Resources"]
            slots = resources.get("CPU", 0) // self.cpus_per_slot
            if self.use_gpu:
                gpu_slots = resources.get("GPU", 0) // self.gpus_per_slot
                slots = min(slots, gpu_slots)
            slots = int(math.ceil(slots))
            if slots:
                host_mapping[hostname] = slots

        if host_mapping and sum(host_mapping.values()) == 0:
            logger.info(f"Detected {len(host_mapping)} hosts, but no hosts "
                        "have available slots.")
            logger.debug(f"Alive nodes: {alive_nodes}")
        return host_mapping


class TrainingWorkerError(Exception):
    """Raised if a worker fails during training."""


class ElasticBackendExecutor:
    """Main execution class for training backends.

    This class holds a worker group and is responsible for executing the
    training function on the workers, and collecting intermediate results
    from ``train.report()`` and ``train.checkpoint()``.

    Args:
        backend_config (BackendConfig): The configurations for this
            specific backend.
        num_workers (int): Number of workers to use for training.
        num_cpus_per_worker (float): Number of CPUs to use per worker.
        num_gpus_per_worker (float): Number of GPUs to use per worker.
        additional_resources_per_worker (Optional[Dict[str, float]]):
            Dictionary specifying the extra resources that will be
            requested for each worker in addition to ``num_cpus_per_worker``
            and ``num_gpus_per_worker``.
        max_retries (int): Number of retries when Ray actors fail.
            Defaults to 3. Set to -1 for unlimited retries.

    Attributes:
        latest_checkpoint_dir (Optional[Path]): Path to the file directory for
            the checkpoints from the latest run. Configured through
            ``start_training``.
        latest_checkpoint_path (Optional[Path]): Path to the latest persisted
            checkpoint from the latest run.
        latest_checkpoint (Optional[Dict]): The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    def __init__(
            self,
            backend_config: BackendConfig,
            min_np: int = 1,
            max_np: Optional[int] = None,
            num_cpus_per_worker: float = 1,
            num_gpus_per_worker: float = 0,
            additional_resources_per_worker: Optional[Dict[str, float]] = None,
            max_retries: int = 3):
        self._backend_config = backend_config
        self._backend = self._backend_config.backend_cls()
        self.min_np = min_np
        self.max_np = max_np
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker
        self._additional_resources_per_worker = additional_resources_per_worker
        self._max_failures = max_retries
        if self._max_failures < 0:
            self._max_failures = float("inf")
        self._num_failures = 0
        self._initialization_hook = None

        self.checkpoint_manager = CheckpointManager()

        self.worker_group = InactiveWorkerGroup()
        self.dataset_shards = None

        self.checkpoint_manager.on_init()

    def start(self,
              initialization_hook: Optional[Callable[[], None]] = None,
              train_cls: Optional[Type] = None,
              train_cls_args: Optional[Tuple] = None,
              train_cls_kwargs: Optional[Dict] = None):
        """Starts the worker group."""
        self.rendezvous = RendezvousServer(self._backend_config.verbose)
        discovery = RayHostDiscovery(
            use_gpu=self._num_gpus_per_worker > 0,
            cpus_per_slot=self._num_cpus_per_worker,
            gpus_per_slot=self._num_gpus_per_worker)
        self.driver = ElasticDriver(
            rendezvous=self.rendezvous,
            discovery=discovery,
            min_np=self.min_np,
            max_np=self.max_np,
            timeout=self._backend_config.elastic_timeout,
            reset_limit=self._backend_config.reset_limit,
            verbose=self._backend_config.verbose)
        handler = create_rendezvous_handler(self.driver)
        logger.debug("[ray] starting rendezvous")
        global_rendezv_port = self.rendezvous.start(handler)

        logger.debug(f"[ray] waiting for {self.min_np} to start.")
        self.driver.wait_for_available_slots(self.min_np)

        # Host-to-host common interface detection
        # requires at least 2 hosts in an elastic job.
        min_hosts = _get_min_start_hosts(self._backend_config)
        current_hosts = self.driver.wait_for_available_slots(
            self.min_np, min_hosts=min_hosts)
        logger.debug("[ray] getting common interfaces")
        nics = detect_nics(
            self._backend_config,
            all_host_names=current_hosts.host_assignment_order,
        )
        logger.debug("[ray] getting driver IP")
        server_ip = socket.gethostbyname(socket.gethostname())
        self.run_env_vars = create_run_env_vars(
            server_ip, nics, global_rendezv_port, elastic=True)

        self.worker_group = DelayedWorkerGroup(
            num_workers=self.min_np,
            num_cpus_per_worker=self._num_cpus_per_worker,
            num_gpus_per_worker=self._num_gpus_per_worker,
            additional_resources_per_worker=self.
            _additional_resources_per_worker,
            actor_cls=train_cls,
            actor_cls_args=train_cls_args,
            actor_cls_kwargs=train_cls_kwargs)
        try:
            if initialization_hook:
                self._initialization_hook = initialization_hook
                # self.worker_group.execute(initialization_hook)

            # share_cuda_visible_devices_enabled = bool(
            #     env_integer(ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
            #                 self._backend.share_cuda_visible_devices))

            if (self._num_gpus_per_worker > 0):
                self._share_cuda_visible_devices()
            # self._backend.on_start(self.worker_group, self._backend_config)
        except RayActorError as exc:
            logger.exception(str(exc))
            self._increment_failures()
            self._restart()

    def _share_cuda_visible_devices(self):
        """Sets CUDA_VISIBLE_DEVICES on all workers.

        For each worker, CUDA_VISIBLE_DEVICES will be set to the GPU IDs
        visible to all workers on that worker's node.

        This allows GPU workers on the same node to communicate with one
        another.

        Example:

            Setup:
            - Node1:
                - Worker1: {0, 1}
                - Worker2: {2, 3}
            - Node2:
                - Worker3: {0, 1}

            CUDA_VISIBLE_DEVICES:
            - Worker1: "0,1,2,3"
            - Worker2: "0,1,2,3"
            - Worker2: "0,1"

        """

        node_ids_and_gpu_ids = [(w.metadata.node_id, w.metadata.gpu_ids)
                                for w in self.worker_group.workers]

        node_id_to_worker_id = defaultdict(set)
        node_id_to_gpu_ids = defaultdict(set)

        for worker_id, (node_id, gpu_ids) in enumerate(node_ids_and_gpu_ids):
            node_id_to_worker_id[node_id].add(worker_id)
            node_id_to_gpu_ids[node_id].update(gpu_ids)

        futures = []
        for node_id, gpu_ids in node_id_to_gpu_ids.items():
            all_gpu_ids = ",".join([str(gpu_id) for gpu_id in gpu_ids])

            def set_gpu_ids():
                os.environ["CUDA_VISIBLE_DEVICES"] = all_gpu_ids

            for worker_id in node_id_to_worker_id[node_id]:
                futures.append(
                    self.worker_group.execute_single_async(
                        worker_id, set_gpu_ids))
        ray.get(futures)

    def _create_local_rank_map(self) -> Dict:
        """Create mapping from worker world_rank to local_rank.

        Example:
            Worker 0: 0.0.0.0
            Worker 1: 0.0.0.0
            Worker 2: 0.0.0.1
            Worker 3: 0.0.0.0
            Worker 4: 0.0.0.1

            Workers 0, 1, 3 are on 0.0.0.0.
            Workers 2, 4 are on 0.0.0.1.

            Expected Output:
            {
                0 -> 0,
                1 -> 1,
                2 -> 0,
                3 -> 2,
                4 -> 1
            }
        """
        rank_mapping = {}
        ip_dict = defaultdict(int)
        for world_rank in range(len(self.worker_group)):
            worker = self.worker_group.workers[world_rank]
            node_ip = worker.metadata.node_ip
            rank_mapping[world_rank] = ip_dict[node_ip]
            ip_dict[node_ip] += 1
        return rank_mapping

    def _get_dataset_shards(self, dataset_or_dict):

        if dataset_or_dict is None:
            # Return None for each shard.
            return [None] * len(self.worker_group)

        def split_dataset(dataset_or_pipeline):
            actors = [worker.actor for worker in self.worker_group.workers]
            return dataset_or_pipeline.split(
                len(self.worker_group), equal=True, locality_hints=actors)

        if isinstance(dataset_or_dict, dict):
            # Return a smaller dict for each shard.
            dataset_shards = [{} for _ in range(len(self.worker_group))]
            for key, dataset in dataset_or_dict.items():
                split_datasets = split_dataset(dataset)
                assert len(split_datasets) == len(self.worker_group)
                for i in range(len(split_datasets)):
                    dataset_shards[i][key] = split_datasets[i]
            return dataset_shards
        else:
            # return a smaller RayDataset for each shard.
            return split_dataset(dataset_or_dict)

    def start_training(
            self,
            train_func: Callable[[], T],
            run_dir: Path,
            dataset: Optional[Union[RayDataset, Dict[str, RayDataset]]] = None,
            checkpoint: Optional[Union[Dict, str, Path]] = None,
            checkpoint_strategy: Optional[CheckpointStrategy] = None,
            latest_checkpoint_id: Optional[int] = None,
    ) -> None:
        """Executes a training function on all workers in a separate thread.

        ``finish_training`` should be called after this.

        Args:
            train_func (Callable): The training function to run on each worker.
            run_dir (Path): The directory to use for this run.
            dataset (Optional[Union[Dataset, DatasetPipeline]])
                Distributed Ray Dataset or DatasetPipeline to pass into
                worker, which can be accessed from the training function via
                ``train.get_dataset_shard()``. Sharding will automatically be
                handled by the Trainer. Multiple Datasets can be passed in as
                a ``Dict`` that maps each name key to a Dataset value,
                and each Dataset can be accessed from the training function
                by passing in a `dataset_name` argument to
                ``train.get_dataset_shard()``.
            checkpoint (Optional[Dict|str|Path]): The checkpoint data that
                should be loaded onto each worker and accessed by the
                training function via ``train.load_checkpoint()``. If this is a
                ``str`` or ``Path`` then the value is expected to be a path
                to a file that contains a serialized checkpoint dict. If this
                is ``None`` then no checkpoint will be loaded.
            checkpoint_strategy (Optional[CheckpointStrategy]): The
                configurations for saving checkpoints.
            latest_checkpoint_id (Optional[int]): The checkpoint id of the
                most recently saved checkpoint.
        """
        self.checkpoint_manager.on_start_training(
            checkpoint_strategy=checkpoint_strategy,
            run_dir=run_dir,
            latest_checkpoint_id=latest_checkpoint_id)

        use_detailed_autofilled_metrics = env_integer(
            ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, 0)

        # First initialize the session.
        def initialize_session(train_func, world_rank, local_rank, checkpoint,
                               dataset_shard):
            try:
                init_session(
                    training_func=train_func,
                    world_rank=world_rank,
                    local_rank=local_rank,
                    dataset_shard=dataset_shard,
                    checkpoint=checkpoint,
                    detailed_autofilled_metrics=use_detailed_autofilled_metrics
                )
            except ValueError:
                raise TrainBackendError(
                    "Attempting to start training but a "
                    "previous training run is still ongoing. "
                    "You must call `finish_training` before "
                    "calling `start_training` again.")

        if self.dataset_shards is None:
            self.dataset_shards = self._get_dataset_shards(dataset)

        checkpoint_dict = self.checkpoint_manager._load_checkpoint(checkpoint)

        local_rank_map = self._create_local_rank_map()

        # futures = []
        # for index in range(len(self.worker_group)):
        #     futures.append(
        #         self.worker_group.execute_single_async(
        #             index,
        #             initialize_session,
        #             world_rank=index,
        #             local_rank=local_rank_map[index],
        #             train_func=train_func,
        #             dataset_shard=self.dataset_shards[index],
        #             checkpoint=checkpoint_dict))

        # self.get_with_failure_handling(futures)
        # create slot-id --> workers map
        # Find the correct worker and pipe its events to the session
        # Run the training function asynchronously in its own thread.

        # pass train_async as the worker loop
        # it should run till the thread joins
        # Event killer check:
        # train_async should have a second thread that periodically check on the events.
        # get own event like this(for failures):
        # shutdown_event = self._shutdown(driver failure)
        # host_event = self._host_manager.get_host_event(slot_info.hostname)(this worker failed)
        # it should return the result
        # Invoke with elastic_driver: worker_group[hostname].execute (not async)
        # get the exit code and return to this
        # What if this gets invoked on a new worker?
        # we need to initialize session and run training on that new worker
        # that worker must be added to the workergroup
        # Later, dataset partitioning can also be done in the workergroup
        # no need for anything special in the reset callback yet


        def check_and_kill(events, worker, rank):
            for e in events:
                if e.is_set():
                    self.worker_group.remove_workers([rank])
                    ray.kill(worker)
                    return True
                e.wait(0.1)

        def train_async(events, worker, rank):
            session = get_session()
            session.start()
            hvd_event_thread = PropagatingThread(target=check_and_kill, args=[events, worker, rank],
                daemon=True)
            hvd_event_thread.start()

        def launch_worker(slot_info, events):
            """This function will be invoked by the horovod elastic driver when a worker is added or removed.
               Therefore it needs to perform all the necessary initialization of a worker
               And add the worker to the worker_group
               In addition, it needs to listen for worker or driver termination events.
            """
            worker_env_vars = {}
            worker_env_vars.update(self.run_env_vars.copy())
            worker_env_vars.update({"PYTHONUNBUFFERED": "1"})
            worker = self.worker_group.add_worker(slot_info)
            worker.update_env_vars.remote(worker_env_vars)
            worker.update_env_vars.remote(create_slot_env_vars(slot_info))
            if self.use_gpu:
                visible_devices = ",".join(
                    [str(i) for i in range(slot_info.local_size)])
                worker.update_env_vars.remote({
                    "CUDA_VISIBLE_DEVICES":
                    visible_devices
                })
            # initialize session
            self.get_with_failure_handling(
                worker.actor._BaseWorkerMixin__execute.remote(
                    initialize_session,
                    world_rank=slot_info.rank,
                    local_rank=local_rank_map[slot_info.rank],
                    train_func=train_func,
                    dataset_shard=dataset,
                    checkpoint=checkpoint_dict))
            # train in a separate thread
            worker.actor._BaseWorkerMixin__execute.remote(train_async, events, worker, slot_info.rank)



        self.driver.start(self.min_np, launch_worker)

    def _get_next_results(self) -> Optional[List[TrainingResult]]:
        """Fetches the next ``TrainingResult`` from each worker.

        Each ``TrainingResult`` is expected to correspond to the same step from
        each worker (e.g. the same call to ``train.report()`` or
        ``train.checkpoint()``).

        Returns:
            A list of ``TrainingResult``s with the same
            ``TrainingResultType``, or ``None`` if there are no more results.
        """

        def get_next():
            # Get the session for this worker.
            try:
                session = get_session()
            except ValueError:
                # Session is not initialized yet.
                raise TrainBackendError("`fetch_next_result` has been called "
                                        "before `start_training`. Please call "
                                        "`start_training` before "
                                        "`fetch_next_result`.")

            try:
                result = session.get_next()
            except RuntimeError:
                # Training thread has not been started yet.
                raise TrainBackendError("`fetch_next_result` has been called "
                                        "before `start_training`. Please call "
                                        "`start_training` before "
                                        "`fetch_next_result`.")

            return result

        # Get next result from each worker.
        futures = self.worker_group.execute_async(get_next)
        results = self.get_with_failure_handling(futures)

        # Check if any worker returned None.
        if any(r is None for r in results):
            # Either all workers have results or none of them do.
            if not all(r is None for r in results):
                raise RuntimeError(
                    "Some workers returned results while "
                    "others didn't. Make sure that "
                    "`train.report()` and `train.checkpoint()` "
                    "are called the same number of times on all "
                    "workers.")
            else:
                # Return None if all results are None.
                return None
        first_result = results[0]
        result_type = first_result.type
        if any(r.type != result_type for r in results):
            raise RuntimeError("Some workers returned results with "
                               "different types. Make sure `train.report()` "
                               "and `train.save_checkpoint()` are called the "
                               "same number of times and in the same order on "
                               "each worker.")
        return results

    def fetch_next_result(self) -> Optional[List[Dict]]:
        """Fetch next results produced by ``train.report()`` from each worker.

        Assumes ``start_training`` has already been called.

        Returns:
            A list of dictionaries of values passed to ``train.report()`` from
                each worker. Each item corresponds to an intermediate result
                a single worker. If there are no more items to fetch,
                returns None.
        """

        while True:
            results = self._get_next_results()
            if results is None:
                return None
            first_result = results[0]
            result_type = first_result.type
            if result_type is TrainingResultType.REPORT:
                result_data = [r.data for r in results]
                return result_data
            elif result_type is TrainingResultType.CHECKPOINT:
                self.checkpoint_manager._process_checkpoint(results)
                # Iterate until next REPORT call or training has finished.
            else:
                raise TrainBackendError(f"Unexpected result type: "
                                        f"{result_type}. "
                                        f"Expected one of "
                                        f"{[type in TrainingResultType]}")

    def finish_training(self) -> List[T]:
        """Finish training and return final results. Propagate any exceptions.

        Blocks until training is finished on all workers.

        Assumes `start_training` has already been called.

        Returns:
            A list of return values from calling ``train_func`` on each worker.
                Each item corresponds to the return value from a single worker.
        """

        def pause_reporting():
            # Get the session for this worker.
            try:
                session = get_session()
            except ValueError:
                # Session is not initialized yet.
                raise TrainBackendError("`finish_training` has been called "
                                        "before `start_training`. Please call "
                                        "`start_training` before "
                                        "`finish_training`.")

            return session.pause_reporting()

        def end_training():
            # Get the session for this worker.
            try:
                session = get_session()
            except ValueError:
                # Session is not initialized yet.
                raise TrainBackendError("`finish_training` has been called "
                                        "before `start_training`. Please call "
                                        "`start_training` before "
                                        "`finish_training`.")

            try:
                # session.finish raises any Exceptions from training.
                output = session.finish()
            finally:
                # Shutdown session even if session.finish() raises an
                # Exception.
                shutdown_session()

            return output

        # Disable workers from enqueuing results from `train.report()`.
        # Results will not be processed during the execution of `finish`.
        # Note: Reported results may still be enqueued at this point,
        #       and should be handled appropriately.
        futures = self.worker_group.execute_async(pause_reporting)
        self.get_with_failure_handling(futures)

        # Finish up processing checkpoints. Reporting has been disabled.
        while True:
            results = self._get_next_results()
            if results is None:
                break
            result_type = results[0].type
            # Process checkpoints and ignore other result types.
            if result_type is TrainingResultType.CHECKPOINT:
                self.checkpoint_manager._process_checkpoint(results)

        futures = self.worker_group.execute_async(end_training)
        results = self.get_with_failure_handling(futures)
        self.driver.stop()
        return results

    def get_with_failure_handling(self, remote_values):
        """Gets the remote values while handling for worker failures.

        This method should be called instead of ``ray.get()`` directly in
        order to handle worker failures.

        If a worker failure is identified, backend specific failure handling
        is executed and a ``TrainingWorkerError`` is raised.

        Args:
            remote_values (list): List of object refs representing functions
                that may fail in the middle of execution. For example, running
                a Train training loop in multiple parallel actor calls.
        Returns:
            The resolved objects represented by the passed in ObjectRefs.
        """
        success, failed_worker_indexes = check_for_failure(remote_values)
        if success:
            return ray.get(remote_values)
        else:
            self._increment_failures()
            try:
                self._backend.handle_failure(self.worker_group,
                                             failed_worker_indexes,
                                             self._backend_config)
            except RayActorError as exc:
                logger.exception(str(exc))
                self._restart()
            raise TrainingWorkerError

    def shutdown(self):
        """Shuts down the workers in the worker group."""
        try:
            self._backend.on_shutdown(self.worker_group, self._backend_config)
        except RayActorError:
            logger.warning("Graceful shutdown of backend failed. This is "
                           "expected if one of the workers has crashed.")
        self.worker_group.shutdown()
        self.worker_group = InactiveWorkerGroup()
        self.dataset_shards = None

    @property
    def is_started(self):
        return not isinstance(self.worker_group, InactiveWorkerGroup)

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the latest checkpoint directory."""
        return self.checkpoint_manager.latest_checkpoint_dir

    @property
    def latest_checkpoint_path(self) -> Optional[Path]:
        """Path to the latest persisted checkpoint."""
        return self.checkpoint_manager.latest_checkpoint_path

    @property
    def latest_checkpoint_id(self) -> Optional[int]:
        """The checkpoint id of most recently saved checkpoint.

        If no checkpoint has been saved yet, then return None.
        """
        checkpoint_id = self.checkpoint_manager._latest_checkpoint_id
        if checkpoint_id == 0:
            return None
        else:
            return checkpoint_id

    @property
    def latest_checkpoint(self) -> Optional[Dict]:
        """Latest checkpoint object."""
        return self.checkpoint_manager.latest_checkpoint

    def _restart(self):
        self.worker_group.shutdown()
        if self._initialization_hook is not None:
            initialization_hook = self._initialization_hook
        else:
            initialization_hook = None
        self.start(initialization_hook=initialization_hook)

    def _increment_failures(self):
        self._num_failures += 1
        if self._num_failures >= self._max_failures:
            raise RuntimeError("Training has failed even after "
                               f"{self._num_failures} "
                               "attempts. You can change the number of max "
                               "failure attempts by setting the "
                               "`max_retries` arg in your `Trainer`.") \
                from None


class Backend(metaclass=abc.ABCMeta):
    """Metaclass for distributed communication backend.

    Attributes:
        share_cuda_visible_devices (bool): If True, each worker
            process will have CUDA_VISIBLE_DEVICES set as the visible device
            IDs of all workers on the same node for this training instance.
            If False, each worker will have CUDA_VISIBLE_DEVICES set to the
            device IDs allocated by Ray for that worker.
    """

    share_cuda_visible_devices: bool = False

    def on_start(self, worker_group: WorkerGroup,
                 backend_config: BackendConfig):
        """Logic for starting this backend."""
        pass

    def on_shutdown(self, worker_group: WorkerGroup,
                    backend_config: BackendConfig):
        """Logic for shutting down the backend."""
        pass

    def handle_failure(self, worker_group: WorkerGroup,
                       failed_worker_indexes: List[int],
                       backend_config: BackendConfig):
        """Logic for handling failures.

        By default, restart all workers.
        """
        worker_group.shutdown()
        worker_group.start()
        self.on_start(worker_group, backend_config)


class InactiveWorkerGroupError(Exception):
    """Raised when underlying worker group is inactive."""


class InactiveWorkerGroup():
    # TODO: fix inheritence. perhaps create WorkerGroupInterface.

    # Need to define getstate and setstate so that getattr does not screwup
    # pickling. See https://stackoverflow.com/a/50888571/11249691
    def __getstate__(self):
        return vars(self)

    def __setstate__(self, state):
        vars(self).update(state)

    def __getattr__(self, name):
        raise InactiveWorkerGroupError()

    def __len__(self):
        raise InactiveWorkerGroupError()
