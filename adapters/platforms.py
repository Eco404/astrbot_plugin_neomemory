"""Look up an adapter's declared type by its configured AstrBot instance ID."""


def platform_name(context, instance_id):
    get_instance = getattr(context, "get_platform_inst", None)
    platform = get_instance(instance_id) if get_instance else None
    return platform.meta().name if platform is not None else None
