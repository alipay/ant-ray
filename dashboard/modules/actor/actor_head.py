import asyncio
import logging
import aiohttp.web
import ray._private.utils
from ray.new_dashboard.modules.actor import actor_utils
from aioredis.pubsub import Receiver
from grpc.experimental import aio as aiogrpc

import ray.gcs_utils
import ray.new_dashboard.modules.stats_collector.stats_collector_consts \
    as stats_collector_consts
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.utils import rest_response
from ray.new_dashboard.modules.actor.actor_utils import \
    actor_classname_from_task_spec
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
from ray.new_dashboard.datacenter import DataSource, DataOrganizer

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def actor_table_data_to_dict(message):
    return dashboard_utils.message_to_dict(
        message, {
            "actorId", "parentId", "jobId", "workerId", "rayletId",
            "actorCreationDummyObjectId", "callerId", "taskId", "parentTaskId",
            "sourceActorId", "placementGroupId"
        },
        including_default_value_fields=True)


class ActorHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        # ActorInfoGcsService
        self._gcs_actor_info_stub = None
        DataSource.nodes.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, node_info = change.old
            self._stubs.pop(node_id)
        if change.new:
            # TODO(fyrestone): Handle exceptions.
            node_id, node_info = change.new
            address = "{}:{}".format(node_info["nodeManagerAddress"],
                                     int(node_info["nodeManagerPort"]))
            options = (("grpc.enable_http_proxy", 0), )
            channel = aiogrpc.insecure_channel(address, options=options)
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._stubs[node_id] = stub

    async def _update_actors(self):
        # Subscribe actor channel.
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        key = "{}:*".format(stats_collector_consts.ACTOR_CHANNEL)
        pattern = receiver.pattern(key)
        await aioredis_client.psubscribe(pattern)
        logger.info("Subscribed to %s", key)

        def _process_actor_table_data(data):
            actor_class = actor_classname_from_task_spec(
                data.get("taskSpec", {}))
            data["actorClass"] = actor_class

        # Get all actor info.
        while True:
            try:
                logger.info("Getting all actor info from GCS.")
                request = gcs_service_pb2.GetAllActorInfoRequest()
                reply = await self._gcs_actor_info_stub.GetAllActorInfo(
                    request, timeout=5)
                if reply.status.code == 0:
                    actors = {}
                    for message in reply.actor_table_data:
                        actor_table_data = actor_table_data_to_dict(message)
                        _process_actor_table_data(actor_table_data)
                        actors[actor_table_data["actorId"]] = actor_table_data
                    # Update actors.
                    DataSource.actors.reset(actors)
                    # Update node actors and job actors.
                    job_actors = {}
                    node_actors = {}
                    for actor_id, actor_table_data in actors.items():
                        job_id = actor_table_data["jobId"]
                        node_id = actor_table_data["address"]["rayletId"]
                        job_actors.setdefault(job_id,
                                              {})[actor_id] = actor_table_data
                        # Update only when node_id is not Nil.
                        if node_id != stats_collector_consts.NIL_NODE_ID:
                            node_actors.setdefault(
                                node_id, {})[actor_id] = actor_table_data
                    DataSource.job_actors.reset(job_actors)
                    DataSource.node_actors.reset(node_actors)
                    logger.info("Received %d actor info from GCS.",
                                len(actors))
                    break
                else:
                    raise Exception(
                        f"Failed to GetAllActorInfo: {reply.status.message}")
            except Exception:
                logger.exception("Error Getting all actor info from GCS.")
                await asyncio.sleep(stats_collector_consts.
                                    RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS)

        # Receive actors from channel.
        state_keys = ("state", "address", "numRestarts", "timestamp", "pid")
        async for sender, msg in receiver.iter():
            try:
                actor_id, actor_table_data = msg
                pubsub_message = ray.gcs_utils.PubSubMessage.FromString(
                    actor_table_data)
                message = ray.gcs_utils.ActorTableData.FromString(
                    pubsub_message.data)
                actor_table_data = actor_table_data_to_dict(message)
                _process_actor_table_data(actor_table_data)
                # If actor is not new registered but updated, we only update
                # states related fields.
                if actor_table_data["state"] != "DEPENDENCIES_UNREADY":
                    actor_id = actor_id.decode("UTF-8")[len(
                        ray.gcs_utils.TablePrefix_ACTOR_string + ":"):]
                    actor_table_data_copy = dict(DataSource.actors[actor_id])
                    for k in state_keys:
                        actor_table_data_copy[k] = actor_table_data[k]
                    actor_table_data = actor_table_data_copy
                actor_id = actor_table_data["actorId"]
                job_id = actor_table_data["jobId"]
                node_id = actor_table_data["address"]["rayletId"]
                # Update actors.
                DataSource.actors[actor_id] = actor_table_data
                # Update node actors (only when node_id is not Nil).
                if node_id != stats_collector_consts.NIL_NODE_ID:
                    node_actors = dict(DataSource.node_actors.get(node_id, {}))
                    node_actors[actor_id] = actor_table_data
                    DataSource.node_actors[node_id] = node_actors
                # Update job actors.
                job_actors = dict(DataSource.job_actors.get(job_id, {}))
                job_actors[actor_id] = actor_table_data
                DataSource.job_actors[job_id] = job_actors
            except Exception:
                logger.exception("Error receiving actor info.")

    @routes.get("/logical/actor_groups")
    async def get_actor_groups(self, req) -> aiohttp.web.Response:
        actors = await DataOrganizer.get_all_actors()
        actor_creation_tasks = await DataOrganizer.get_actor_creation_tasks()
        # actor_creation_tasks have some common interface with actors,
        # and they get processed and shown in tandem in the logical view
        # hence we merge them together before constructing actor groups.
        actors.update(actor_creation_tasks)
        actor_groups = actor_utils.construct_actor_groups(actors)
        return rest_response(
            success=True,
            message="Fetched actor groups.",
            actor_groups=actor_groups)

    @routes.get("/logical/actors")
    @dashboard_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        return dashboard_utils.rest_response(
            success=True,
            message="All actors fetched.",
            actors=DataSource.actors)

    @routes.get("/logical/kill_actor")
    async def kill_actor(self, req) -> aiohttp.web.Response:
        try:
            actor_id = req.query["actorId"]
            ip_address = req.query["ipAddress"]
            port = req.query["port"]
        except KeyError:
            return rest_response(success=False, message="Bad Request")
        try:
            options = (("grpc.enable_http_proxy", 0), )
            channel = aiogrpc.insecure_channel(
                f"{ip_address}:{port}", options=options)
            stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

            await stub.KillActor(
                core_worker_pb2.KillActorRequest(
                    intended_actor_id=ray._private.utils.hex_to_binary(
                        actor_id)))

        except aiogrpc.AioRpcError:
            # This always throws an exception because the worker
            # is killed and the channel is closed on the worker side
            # before this handler, however it deletes the actor correctly.
            pass

        return rest_response(
            success=True, message=f"Killed actor with id {actor_id}")

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._gcs_actor_info_stub = \
            gcs_service_pb2_grpc.ActorInfoGcsServiceStub(gcs_channel)

        await asyncio.gather(self._update_actors())
