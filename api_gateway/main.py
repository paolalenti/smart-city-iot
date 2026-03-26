import os
import httpx
from redis.asyncio import Redis
from fastapi import FastAPI, Request, Response, HTTPException


app = FastAPI(title="IoT API Gateway")

redis_client = Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

DEVICE_MANAGER_URL = os.getenv("DEVICE_MANAGER_URL", "http://device_manager:8000")
TELEMETRY_URL = os.getenv("TELEMETRY_URL", "http://telemetry_ingestor:8001")


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


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_router(request: Request, path: str):
    """ Маршрутизатор (Router) """

    if path.startswith("devices") or path == "docs" or path == "openapi.json":
        target_url = f"{DEVICE_MANAGER_URL}/{path}"
    elif path.startswith("telemetry"):
        target_url = f"{TELEMETRY_URL}/{path}"
    else:
        raise HTTPException(status_code=404, detail="Service not found")

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
