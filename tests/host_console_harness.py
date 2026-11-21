"""Isolated real-host routing, auth, assets and plugin runtime for console probes.

The parent is a minimal implementation of the host Vue page's GET/POST bridge
contract, not the Vue application. No real accounts, configuration or data are
opened. The iframe SDK, routers and authentication code are unmodified AstrBot.
"""

from __future__ import annotations

import json
import secrets
import time
from pathlib import Path
from types import SimpleNamespace

import jwt
from astrbot.core.config.astrbot_config import AstrBotConfig
from astrbot.core.star.star import StarMetadata
from astrbot.dashboard.api.plugins import legacy_router, router
from astrbot.dashboard.responses import ApiError
from astrbot.dashboard.server import AstrBotDashboard
from astrbot.dashboard.services.plugin_page_service import PluginPageService
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

import main as plugin_module
from core.config import NeoMemoryConfig
from tests.test_recall_runtime import RuntimeContext, RuntimeEvent, seed

PLUGIN_ID = "astrbot_plugin_neomemory"
API_PREFIX = f"/api/v1/plugins/extensions/{PLUGIN_ID}"
PROJECT = Path(__file__).resolve().parents[1]


class HostConsoleHarness:
    def __init__(self, directory: Path):
        self.directory = directory
        self.secret = secrets.token_urlsafe(40)
        self.dashboard_token = jwt.encode(
            {"username": "synthetic-admin", "exp": int(time.time()) + 3600},
            self.secret, algorithm="HS256",
        )
        self.plugin = None
        metadata = StarMetadata(name=PLUGIN_ID, root_dir_name=PROJECT.name)
        manager = SimpleNamespace(
            context=SimpleNamespace(get_all_stars=lambda: [metadata]),
            plugin_store_path=str(PROJECT.parent), reserved_plugin_path=str(PROJECT.parent),
        )
        self.pages = PluginPageService(manager, config={"dashboard": {"jwt_secret": self.secret}})
        self.asset_token = self.pages.issue_plugin_page_asset_token(
            plugin_name=PLUGIN_ID, view_name="neomemory", username="synthetic-admin", locale="zh-CN",
        )
        self.asset_prefix = f"/api/v1/plugins/{PLUGIN_ID}/views/neomemory/_t/{self.asset_token}/"
        self.app = FastAPI()
        self.app.state.jwt_secret = self.secret
        self.app.state.services = SimpleNamespace(plugin_pages=self.pages)
        self.app.state.core_lifecycle = SimpleNamespace(star_context=None)
        dashboard = AstrBotDashboard.__new__(AstrBotDashboard)
        dashboard._jwt_secret = self.secret
        dashboard.config = {"dashboard": {"auth_rate_limit": {"enable": False}}}

        @self.app.middleware("http")
        async def auth(request: Request, call_next):
            request.state.dashboard_g = SimpleNamespace()
            rejection = await dashboard.auth_middleware(request)
            return rejection if rejection is not None else await call_next(request)

        @self.app.exception_handler(ApiError)
        async def api_error(_request, exc):
            return JSONResponse({"status": "error", "message": exc.message}, status_code=exc.status_code)

        self.app.include_router(router, prefix="/api/v1")
        self.app.include_router(legacy_router)

        @self.app.get("/host-console")
        async def parent_page():
            # Only the parent has the temporary admin JWT. The sandboxed iframe
            # authenticates assets with a scoped path token, as the host does.
            config = json.dumps({"src": self.asset_prefix, "prefix": API_PREFIX, "token": self.dashboard_token})
            return HTMLResponse("""<!doctype html><html><body style="margin:0">
<iframe id="plugin" title="NeoMemory" style="width:100%;height:100vh;border:0"
 sandbox="allow-scripts allow-forms allow-downloads"></iframe><script>
const cfg=CONFIG, frame=document.getElementById('plugin');
window.addEventListener('message',async(event)=>{
 if(event.source!==frame.contentWindow)return;
 const m=event.data;if(m?.channel!=='astrbot-plugin-page'||m.kind!=='request')return;
 let ok=false,data,error;
 try{
  if(!['api:get','api:post'].includes(m.action))throw Error('Unsupported harness action');
  if(typeof m.endpoint!=='string'||m.endpoint.split('/').some(p=>!p||p==='.'||p==='..'))throw Error('Invalid endpoint');
  const url=new URL(cfg.prefix+'/'+m.endpoint.split('/').map(encodeURIComponent).join('/'),location.origin);
  if(m.action==='api:get')Object.entries(m.params||{}).forEach(([k,v])=>url.searchParams.set(k,v));
  const r=await fetch(url,{method:m.action==='api:post'?'POST':'GET',headers:{Authorization:'Bearer '+cfg.token,'Content-Type':'application/json'},body:m.action==='api:post'?JSON.stringify(m.body||{}):undefined});
  const payload=await r.json();if(!r.ok||payload.status==='error')throw Error(payload.message||'API failed');
  data=payload.data??payload;ok=true;
 }catch(e){error=e.message;}
 frame.contentWindow.postMessage({channel:'astrbot-plugin-page',kind:'response',requestId:m.requestId,ok,data,error},'*');
});
frame.src=cfg.src;
</script></body></html>""".replace("CONFIG", config))

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.dashboard_token}"}

    async def start(self, *, seed_data=True):
        defaults = {**NeoMemoryConfig().as_dict(), "automatic_extraction_enabled": False, "job_poll_seconds": 0.05}
        config = AstrBotConfig(str(self.directory / "config.json"), default_config=defaults)
        context = RuntimeContext()
        previous = plugin_module.StarTools.get_data_dir
        plugin_module.StarTools.get_data_dir = staticmethod(lambda _: self.directory / "data")
        try:
            self.plugin = plugin_module.NeoMemoryPlugin(context, config)
            await self.plugin.initialize()
        finally:
            plugin_module.StarTools.get_data_dir = staticmethod(previous)
        self.app.state.core_lifecycle.star_context = context
        if seed_data:
            self.space, self.node = await seed(
                self.plugin, RuntimeEvent(message_id="synthetic-host-seed", text="Synthetic host conversation"),
                content="Synthetic host memory <img src=x onerror=alert(1)>",
            )
            self.other_space, _ = await seed(
                self.plugin, RuntimeEvent(message_id="synthetic-other-seed", persona="other-persona"),
                content="Isolated other persona memory",
            )

    async def stop(self):
        if self.plugin is not None:
            await self.plugin.terminate()
            self.plugin = None

    async def reload(self):
        await self.stop()
        await self.start(seed_data=False)
