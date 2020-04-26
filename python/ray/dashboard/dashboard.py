try:
    import aiohttp.web
except ImportError:
    print("The dashboard requires aiohttp to run.")
    import sys

    sys.exit(1)

import argparse
import asyncio
import errno
import logging
import os
import traceback
import uuid

import ray
import ray.dashboard.master as dashboard_master
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray.services
import ray.utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def setup_static_dir(app):
    build_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "client/build")
    if not os.path.isdir(build_dir):
        raise OSError(
                errno.ENOENT, "Dashboard build directory not found. If installing "
                              "from source, please follow the additional steps "
                              "required to build the dashboard"
                              "(cd python/ray/dashboard/client "
                              "&& npm install "
                              "&& npm ci "
                              "&& npm run build)", build_dir)

    static_dir = os.path.join(build_dir, "static")
    app.router.add_static("/static", static_dir)
    return build_dir


def setup_speedscope_dir(app, build_dir):
    speedscope_dir = os.path.join(build_dir, "speedscope-1.5.3")
    app.router.add_static("/speedscope", speedscope_dir)


class Dashboard:
    """A dashboard process for monitoring Ray nodes.

    This dashboard is made up of a REST API which collates data published by
        Reporter processes on nodes into a json structure, and a webserver
        which polls said API for display purposes.

    Args:
        host(str): Host address of dashboard aiohttp server.
        port(int): Port number of dashboard aiohttp server.
        redis_address(str): GCS address of a Ray cluster
        temp_dir (str): The temporary directory used for log files and
            information for this Ray session.
        redis_password(str): Redis password to access GCS
    """

    def __init__(self,
                 host,
                 port,
                 redis_address,
                 temp_dir,
                 redis_password=None):
        self.host = host
        self.port = port
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)
        self.temp_dir = temp_dir
        self.dashboard_id = str(uuid.uuid4())
        self.dashboard_master = dashboard_master.DashboardMaster(
                redis_address=redis_address,
                redis_password=redis_password)

        # Setting the environment variable RAY_DASHBOARD_DEV=1 disables some
        # security checks in the dashboard server to ease development while
        # using the React dev server. Specifically, when this option is set, we
        # allow cross-origin requests to be made.
        self.is_dev = os.environ.get("RAY_DASHBOARD_DEV") == "1"

        self.app = aiohttp.web.Application()
        self.app.add_routes(routes=routes.routes())

        # Setup Dashboard Routes
        build_dir = setup_static_dir(self.app)
        setup_speedscope_dir(self.app, build_dir)
        dashboard_utils.ClassMethodRouteTable.bind(self)

    def log_dashboard_url(self):
        url = ray.services.get_webui_url_from_redis(self.redis_client)
        if url is None:
            raise ValueError("WebUI URL is not present in GCS.")
        with open(os.path.join(self.temp_dir, "dashboard_url"), "w") as f:
            f.write(url)
        logger.info("Dashboard running on {}".format(url))

    @routes.get("/{_}")
    async def get_forbidden(self, _) -> aiohttp.web.Response:
        return aiohttp.web.Response(status=403, text="403 Forbidden")

    @routes.get("/")
    async def get_index(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
                os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "client/build/index.html"))

    @routes.get("/favicon.ico")
    async def get_favicon(self, req) -> aiohttp.web.FileResponse:
        return aiohttp.web.FileResponse(
                os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "client/build/favicon.ico"))

    async def run(self):
        # self.log_dashboard_url()
        coroutines = [self.dashboard_master.run(),
                      aiohttp.web._run_app(self.app, host=self.host, port=self.port)]
        await asyncio.gather(*coroutines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description=("Parse Redis server for the "
                         "dashboard to connect to."))
    parser.add_argument(
            "--host",
            required=True,
            type=str,
            help="The host to use for the HTTP server.")
    parser.add_argument(
            "--port",
            required=True,
            type=int,
            help="The port to use for the HTTP server.")
    parser.add_argument(
            "--redis-address",
            required=True,
            type=str,
            help="The address to use for Redis.")
    parser.add_argument(
            "--redis-password",
            required=False,
            type=str,
            default=None,
            help="the password to use for Redis")
    parser.add_argument(
            "--logging-level",
            required=False,
            type=lambda s: logging.getLevelName(s.upper()),
            default=ray_constants.LOGGER_LEVEL,
            choices=ray_constants.LOGGER_LEVEL_CHOICES,
            help=ray_constants.LOGGER_LEVEL_HELP)
    parser.add_argument(
            "--logging-format",
            required=False,
            type=str,
            default=ray_constants.LOGGER_FORMAT,
            help=ray_constants.LOGGER_FORMAT_HELP)
    parser.add_argument(
            "--temp-dir",
            required=False,
            type=str,
            default=None,
            help="Specify the path of the temporary directory use by Ray process.")
    args = parser.parse_args()
    logging.basicConfig(level=args.logging_level, format=args.logging_format)

    # TODO(sang): Add a URL validation.
    metrics_export_address = os.environ.get("METRICS_EXPORT_ADDRESS")

    try:
        dashboard = Dashboard(
                args.host,
                args.port,
                args.redis_address,
                args.temp_dir,
                redis_password=args.redis_password)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(dashboard.run())
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
                args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The dashboard on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
                redis_client, ray_constants.DASHBOARD_DIED_ERROR, message)
        if isinstance(e, OSError) and e.errno == errno.ENOENT:
            logger.warning(message)
        else:
            raise e
