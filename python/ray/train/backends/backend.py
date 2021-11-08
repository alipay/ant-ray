import abc
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Callable, TypeVar, List, Optional, Dict, Union, Type, Tuple

import ray
from ray.exceptions import RayActorError
from ray.ray_constants import env_integer
from ray.train.checkpoint import CheckpointManager, CheckpointStrategy, \
    TuneCheckpointManager
from ray.train.constants import ENABLE_DETAILED_AUTOFILLED_METRICS_ENV, \
    TUNE_INSTALLED, ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV
from ray.train.session import TrainingResultType, TrainingResult
from ray.train.session import init_session, get_session, shutdown_session
from ray.train.utils import RayDataset
from ray.train.utils import check_for_failure
from ray.train.worker_group import WorkerGroup

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


class TrainingWorkerError(Exception):
    """Raised if a worker fails during training."""


class BackendExecutor:
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
            ``start_training``
        best_checkpoint_path (Optional[Path]): Path to the best persisted
            checkpoint from the latest run.
        latest_checkpoint (Optional[Dict]): The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    def __init__(
            self,
            backend_config: BackendConfig,
            num_workers: int = 1,
            num_cpus_per_worker: float = 1,
            num_gpus_per_worker: float = 0,
            additional_resources_per_worker: Optional[Dict[str, float]] = None,
            max_retries: int = 3):
        self._backend_config = backend_config
        self._backend = self._backend_config.backend_cls()
        self._num_workers = num_workers
        self._num_cpus_per_worker = num_cpus_per_worker
        self._num_gpus_per_worker = num_gpus_per_worker
        self._additional_resources_per_worker = additional_resources_per_worker
        self._max_failures = max_retries
        if self._max_failures < 0:
            self._max_failures = float("inf")
        self._num_failures = 0
        self._initialization_hook = None

        if tune is not None and tune.is_session_enabled():
            self.checkpoint_manager = TuneCheckpointManager()
        else:
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
        self.worker_group = WorkerGroup(
            num_workers=self._num_workers,
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
                self.worker_group.execute(initialization_hook)

            share_cuda_visible_devices_enabled = bool(
                env_integer(ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
                            self._backend.share_cuda_visible_devices))

            if (self._num_gpus_per_worker > 0
                    and share_cuda_visible_devices_enabled):
                self._share_cuda_visible_devices()
            self._backend.on_start(self.worker_group, self._backend_config)
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

        futures = []
        for index in range(len(self.worker_group)):
            futures.append(
                self.worker_group.execute_single_async(
                    index,
                    initialize_session,
                    world_rank=index,
                    local_rank=local_rank_map[index],
                    train_func=train_func,
                    dataset_shard=self.dataset_shards[index],
                    checkpoint=checkpoint_dict))

        self.get_with_failure_handling(futures)

        # Run the training function asynchronously in its own thread.
        def train_async():
            session = get_session()
            session.start()

        self.worker_group.execute_async(train_async)

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
    def best_checkpoint_path(self) -> Optional[Path]:
        """Path to the best persisted checkpoint."""
        return self.checkpoint_manager.best_checkpoint_path

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
