import logging
import uuid
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Optional
from src.exchanges import load_exchanges, get_adapter, EXCHANGE_REGISTRY
from src.models import MarketType
from src.stream_manager import StreamManager


logging.basicConfig(level=logging.INFO, format='%(levelname)s:     %(name)s - %(message)s')

stream_manager = StreamManager()

SERVICE_VERSION = "1.0.3"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logging.info("Starting Exchange Router Service...")
    load_exchanges()
    yield
    logging.info("Shutting down. Closing exchange connections...")
    await stream_manager.shutdown()
    for name, adapter in EXCHANGE_REGISTRY.items():
        logging.info(f"   Closing {name}...")
        await adapter.shutdown()


app = FastAPI(title="Exchange Router Service", version=SERVICE_VERSION, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(ValueError)
async def value_error_exception_handler(_request: Request, exc: ValueError):
    return JSONResponse(
        status_code=400,
        content={"error": "Invalid Request", "detail": str(exc)},
    )


def validate_request(exchange: str, market_type: MarketType = None):
    adapter = get_adapter(exchange)
    if not adapter:
        raise HTTPException(status_code=404, detail=f"Exchange '{exchange}' not found or not enabled.")
    if market_type and market_type not in adapter.supported_market_types:
        raise HTTPException(status_code=400, detail=f"Market type '{market_type.value}' not supported on {exchange}.")
    return adapter


@app.get("/status")
def service_status():
    return {"status": "ok", "service": "exchange-router-service"}


@app.get("/version")
def service_version():
    return {"version": SERVICE_VERSION}


@app.get("/exchanges")
def list_exchanges():
    return {"count": len(EXCHANGE_REGISTRY), "exchanges": list(EXCHANGE_REGISTRY.keys())}


@app.get("/{exchange}/status")
async def exchange_status(exchange: str):
    adapter = validate_request(exchange)
    return await adapter.get_status()


@app.get("/{exchange}/capabilities")
def exchange_capabilities(exchange: str):
    adapter = validate_request(exchange)
    return adapter.get_capabilities()


@app.get("/{exchange}/market_types")
def list_market_types(exchange: str):
    adapter = validate_request(exchange)
    return {"market_types": adapter.supported_market_types}


@app.get("/{exchange}/{market_type}/info")
async def get_exchange_info(exchange: str, market_type: MarketType):
    adapter = validate_request(exchange, market_type)
    return await adapter.get_exchange_info(market_type)


@app.get("/{exchange}/{market_type}/markets")
async def get_markets(exchange: str, market_type: MarketType):
    adapter = validate_request(exchange, market_type)
    return await adapter.get_markets(market_type)


@app.get("/{exchange}/{market_type}/ticker/{symbol}")
async def get_ticker(exchange: str, market_type: MarketType, symbol: str):
    adapter = validate_request(exchange, market_type)
    return await adapter.get_ticker(market_type, symbol)


@app.get("/{exchange}/{market_type}/book_ticker/{symbol}")
async def get_book_ticker(exchange: str, market_type: MarketType, symbol: str):
    adapter = validate_request(exchange, market_type)
    return await adapter.get_book_ticker(market_type, symbol)


@app.get("/{exchange}/{market_type}/mark_price/{symbol}")
async def get_mark_price(exchange: str, market_type: MarketType, symbol: str):
    adapter = validate_request(exchange, market_type)
    try:
        return await adapter.get_mark_price(market_type, symbol)
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))


@app.get("/{exchange}/{market_type}/orderbook/{symbol}")
async def get_orderbook(exchange: str, market_type: MarketType, symbol: str, depth: int = Query(20, ge=1, le=100)):
    adapter = validate_request(exchange, market_type)
    return await adapter.get_orderbook(market_type, symbol, depth)


@app.get("/{exchange}/{market_type}/trades/{symbol}")
async def get_trades(exchange: str, market_type: MarketType, symbol: str, limit: int = Query(100, ge=1, le=1000)):
    adapter = validate_request(exchange, market_type)
    return await adapter.get_trades(market_type, symbol, limit)


@app.get("/{exchange}/{market_type}/agg_trades/{symbol}")
async def get_agg_trades(exchange: str, market_type: MarketType, symbol: str, start: Optional[int] = None, limit: int = Query(500, ge=1, le=10000)):
    adapter = validate_request(exchange, market_type)
    try:
        return await adapter.get_agg_trades(market_type, symbol, start, limit)
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))


@app.get("/{exchange}/{market_type}/candles/{symbol}")
async def get_candles(exchange: str, market_type: MarketType, symbol: str, interval: str = "1h", start: Optional[int] = None, limit: int = Query(100, ge=1, le=10000)):
    adapter = validate_request(exchange, market_type)
    return await adapter.get_candles(market_type, symbol, interval, start, limit)


@app.get("/{exchange}/{market_type}/open_interest/{symbol}")
async def get_open_interest(exchange: str, market_type: MarketType, symbol: str, period: str = Query("1h"), start: Optional[int] = None, limit: int = Query(30, ge=1, le=5000)):
    adapter = validate_request(exchange, market_type)
    try:
        return await adapter.get_open_interest(market_type, symbol, period, start, limit)
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))


@app.get("/{exchange}/{market_type}/funding_rate/{symbol}")
async def get_funding_rate(exchange: str, market_type: MarketType, symbol: str, start: Optional[int] = None, limit: int = Query(100, ge=1, le=5000)):
    adapter = validate_request(exchange, market_type)
    try:
        return await adapter.get_funding_rate(market_type, symbol, start, limit)
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))


@app.get("/{exchange}/{market_type}/liquidations/{symbol}")
async def get_liquidations(exchange: str, market_type: MarketType, symbol: str, start: Optional[int] = None, limit: int = Query(100, ge=1, le=1000)):
    adapter = validate_request(exchange, market_type)
    try:
        return await adapter.get_liquidations(market_type, symbol, start, limit)
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))


@app.get("/{exchange}/{market_type}/long_short_ratio/{symbol}")
async def get_long_short_ratio(exchange: str, market_type: MarketType, symbol: str, period: str = Query("5m"), start: Optional[int] = None, limit: int = Query(30, ge=1, le=5000)):
    adapter = validate_request(exchange, market_type)
    try:
        return await adapter.get_long_short_ratio(market_type, symbol, period, start, limit)
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))


@app.websocket("/ws/{exchange}/{market_type}")
async def websocket_endpoint(websocket: WebSocket, exchange: str, market_type: MarketType):
    adapter = get_adapter(exchange)
    if not adapter or market_type not in adapter.supported_market_types:
        await websocket.close(code=1008)
        return

    await websocket.accept()

    key: Optional[str] = None
    client_id: Optional[uuid.UUID] = None

    try:
        data = await websocket.receive_json()
        channel, symbol = data.get("channel"), data.get("symbol")
        if not channel or not symbol:
            await websocket.close(code=1003)
            return

        key = f"{exchange}:{market_type.value}:{channel}:{symbol}"
        client_id = await stream_manager.subscribe(key, websocket, adapter, market_type, channel, symbol)

        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logging.error(f"WS Error: {e}")
    finally:
        if key and client_id:
            await stream_manager.unsubscribe(key, client_id)
