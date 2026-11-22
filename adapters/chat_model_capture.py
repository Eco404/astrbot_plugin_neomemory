"""Capture the requested chat route and the final response's separately reported model."""

from collections.abc import Mapping


async def requested_chat_route(context, event, request):
    extra = getattr(event, "get_extra", lambda key: None)
    explicit = extra("selected_provider")
    if explicit:
        provider_id, basis = explicit, "request_provider"
        provider = context.get_provider_by_id(provider_id)
    else:
        lookup = getattr(context, "get_using_provider_async", None)
        if not lookup:
            return {}
        locale = extra("locale")
        provider = await lookup(umo=event.unified_msg_origin, **({"locale": locale} if isinstance(locale, str) else {}))
        if not provider:
            return {}
        provider_id, basis = provider.meta().id, "session_config"
    route = {"provider_id": provider_id, "basis": basis}
    model = getattr(request, "model", None) or (provider.get_model() if provider else None)
    if isinstance(model, str) and model:
        route["requested_model"] = model
    return route


def observed_chat_route(event, response):
    route = dict(getattr(event, "_neomemory_chat_route", {}))
    raw = getattr(response, "raw_completion", None)
    for key in ("model", "model_version", "modelVersion"):
        model = raw.get(key) if isinstance(raw, Mapping) else getattr(raw, key, None)
        if isinstance(model, str) and model:
            route["reported_model"] = model
            break
    return route
