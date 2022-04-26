from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray import Language
from ray.util.annotations import PublicAPI
from ray._raylet import JavaFunctionDescriptor

__all__ = [
    "java_function",
    "java_actor_class",
]


def format_args(worker, args, kwargs):
    """Format args for various languages.

    Args:
        worker: The global worker instance.
        args: The arguments for cross language.
        kwargs: The keyword arguments for cross language.

    Returns:
        List of args and kwargs (if supported).
    """
    if not worker.load_code_from_local:
        raise ValueError(
            "Cross language feature needs --load-code-from-local to be set."
        )
    if kwargs:
        raise TypeError("Cross language remote functions does not support kwargs.")
    return args


def get_function_descriptor_for_actor_method(
    language, actor_creation_function_descriptor, method_name, signature: str
):
    """Get function descriptor for cross language actor method call.

    Args:
        language: Target language.
        actor_creation_function_descriptor:
            The function signature for actor creation.
        method_name: The name of actor method.
        signature: The signature for the actor method. When calling Java from Python,
            it should be string in the form of "{length_of_args}".

    Returns:
        Function descriptor for cross language actor method call.
    """
    if language == Language.JAVA:
        return JavaFunctionDescriptor(
            actor_creation_function_descriptor.class_name,
            method_name,
            signature,
        )
    else:
        raise NotImplementedError(
            "Cross language remote actor method " f"not support language {language}"
        )


@PublicAPI(stability="beta")
def java_function(class_name, function_name):
    """Define a Java function.

    Args:
        class_name (str): Java class name.
        function_name (str): Java function name.
    """
    from ray.remote_function import RemoteFunction

    return RemoteFunction(
        Language.JAVA,
        lambda *args, **kwargs: None,
        JavaFunctionDescriptor(class_name, function_name, ""),
        {},
    )


@PublicAPI(stability="beta")
def java_actor_class(class_name):
    """Define a Java actor class.

    Args:
        class_name (str): Java class name.
    """
    from ray.actor import ActorClass

    return ActorClass._ray_from_function_descriptor(
        Language.JAVA,
        JavaFunctionDescriptor(class_name, "<init>", ""),
        {},
    )