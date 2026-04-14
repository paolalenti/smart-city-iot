import os
import httpx
import json
from redis.asyncio import Redis
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html


app = FastAPI(title="IoT API Gateway", docs_url=None, openapi_url=None)

redis_client = Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

SERVICES = {
    "device_manager": os.getenv("DEVICE_MANAGER_URL", "http://device_manager:8000"),
    "telemetry": os.getenv("TELEMETRY_URL", "http://telemetry_ingestor:8001"),
    "historical": os.getenv("HISTORICAL_URL", "http://historical_service:8002"),
}


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    ip = request.client.host
    key = f"gateway_limit:{ip}"

    current_requests = await redis_client.incr(key)
    if current_requests == 1:
        await redis_client.expire(key, 60)

    if current_requests > 100:
        return Response(content="Too Many Requests", status_code=429)

    return await call_next(request)


@app.get("/openapi.json", include_in_schema=False)
async def merged_openapi():
    paths, schemas, tags = {}, {}, []

    async with httpx.AsyncClient(timeout=5) as client:
        for name, url in SERVICES.items():
            try:
                spec = (await client.get(f"{url}/openapi.json")).json()
            except Exception:
                continue

            spec = json.loads(
                json.dumps(spec).replace(
                    "#/components/schemas/",
                    f"#/components/schemas/{name}__"
                )
            )

            for path_item in spec.get("paths", {}).values():
                for operation in path_item.values():
                    if isinstance(operation, dict):
                        operation["tags"] = [name]

            paths |= {f"/{name}{p}": v for p, v in spec.get("paths", {}).items()}
            schemas |= {f"{name}__{k}": v for k, v in spec.get("components", {}).get("schemas", {}).items()}
            tags.append({"name": name})

    return JSONResponse({
        "openapi": "3.1.0",
        "info": {"title": "IoT API Gateway — All Services", "version": "1.0.0"},
        "tags": tags,
        "paths": paths,
        "components": {"schemas": schemas},
    })


@app.get("/docs", include_in_schema=False)
async def swagger_ui():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="IoT Gateway Docs")


@app.api_route("/{service_name}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_router(request: Request, service_name: str, path: str):
    """ Маршрутизатор (Router) """

    if service_name not in SERVICES:
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found")

    target_base_url = SERVICES[service_name]
    target_url = f"{target_base_url}/{path}"

    async with httpx.AsyncClient() as client:
        headers = dict(request.headers)
        headers.pop("host", None)
        try:
            proxy_req = client.build_request(
                method=request.method,
                url=target_url,
                params=request.query_params,
                headers=headers,
                content=await request.body()
            )

            response = await client.send(proxy_req)

            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=dict(response.headers)
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Service Unavailable: {e}")
