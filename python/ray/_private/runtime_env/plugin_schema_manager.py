import os
import jsonschema
import logging
from typing import List
#import json
# from ray._private.runtime_env.constants import (
#     RAY_RUNTIME_ENV_SCHEMA_VALIDATION_ENV_VAR,
#     RAY_RUNTIME_ENV_PLUGIN_SCHEMAS_ENV_VAR,
#     RAY_RUNTIME_ENV_PLUGIN_SCHEMA_SUFFIX,
# )

# logger = logging.getLogger(__name__)


class RuntimeEnvPluginSchemaManager:
    """This manager is used to load plugin json schemas."""

    default_schema_path = os.path.join(os.path.dirname(__file__), "schemas")
    schemas = {}
    loaded = False

    # @classmethod
    # def _load_schemas(cls, schema_paths: List[str]):
    #     for schema_path in schema_paths:
    #         try:
    #             schema = json.load(open(schema_path))
    #         except json.decoder.JSONDecodeError:
    #             logger.error("Invalid runtime env schema %s, skip it.", schema_path)
    #         if "title" not in schema:
    #             logger.error(
    #                 "No valid title in runtime env schema %s, skip it.", schema_path
    #             )
    #             continue
    #         if schema["title"] in cls.schemas:
    #             logger.error(
    #                 "The 'title' of runtime env schema %s conflicts with %s, skip it.",
    #                 schema_path,
    #                 cls.schemas[schema["title"]],
    #             )
    #             continue
    #         cls.schemas[schema["title"]] = schema

    # @classmethod
    # def _load_default_schemas(cls):
    #     schema_json_files = list()
    #     for root, _, files in os.walk(cls.default_schema_path):
    #         for f in files:
    #             if f.endswith(RAY_RUNTIME_ENV_PLUGIN_SCHEMA_SUFFIX):
    #                 schema_json_files.append(os.path.join(root, f))
    #         logger.info(
    #             f"Loading the default runtime env schemas: {schema_json_files}."
    #         )
    #         cls._load_schemas(schema_json_files)

    # @classmethod
    # def _load_schemas_from_env_var(cls):
    #     # The format of env var:
    #     # "/path/to/env_1_schema.json,/path/to/env_2_schema.json,/path/to/schemas_dir/"
    #     schema_paths = os.environ.get(RAY_RUNTIME_ENV_PLUGIN_SCHEMAS_ENV_VAR)
    #     if schema_paths:
    #         schema_json_files = list()
    #         for path in schema_paths.split(","):
    #             if path.endswith(RAY_RUNTIME_ENV_PLUGIN_SCHEMA_SUFFIX):
    #                 schema_json_files.append(path)
    #             elif os.path.isdir(path):
    #                 for root, _, files in os.walk(path):
    #                     for f in files:
    #                         if f.endswith(RAY_RUNTIME_ENV_PLUGIN_SCHEMA_SUFFIX):
    #                             schema_json_files.append(os.path.join(root, f))
    #         logger.info(
    #             f"Loading the runtime env schemas from env var: {schema_json_files}."
    #         )
    #         cls._load_schemas(schema_json_files)

    # @classmethod
    # def validate(cls, name, instance):
    #     # TODO(SongGuyang): We add this flag because the test `serve:test_runtime_env`
    #     # is flakey. Remove this flag after we address the flakey test issue.
    #     if os.environ.get(RAY_RUNTIME_ENV_SCHEMA_VALIDATION_ENV_VAR, "false") != "true":
    #         return
    #     if not cls.loaded:
    #         # Load the schemas lazily.
    #         cls._load_default_schemas()
    #         cls._load_schemas_from_env_var()
    #         cls.loaded = True
    #     # if no schema matches, skip the validation.
    #     if name in cls.schemas:
    #         jsonschema.validate(instance=instance, schema=cls.schemas[name])

    # @classmethod
    # def clear(cls):
    #     cls.schemas.clear()
    #     cls.loaded = False
