"""
=============================================================================
  FOOD DELIVERY SYSTEM — Integrated Monolith
=============================================================================
  Combines 9 modules into one deployable FastAPI application:

  1. User Module        — Registration & Auth (devyani1512/user_module)
  2. Payment Module     — Stripe payment intents (Siriusspec/PaymentModule)
  3. Payment Service    — Idempotency, circuit breaker, retries (Bajaj041)
  4. Payment Success    — Event publishing on payment success (IshaanAgarwal2704)
  5. Payment Consumer   — Event handler, restaurant + dispatch trigger (KartikAg13)
  6. Restaurant Service — Order queue, DLQ, SNS/SQS simulation (rishugoyal805)
  7. Dispatch Service   — Rider assignment with SSE streaming (KanishkRichhariya107)
  8. Cloud Module       — Priority queue, saga, rate limiter (PranviPandey)
  9. Frontend Bridge    — API endpoints matching files/ React app (Ishikad01)

  Run locally:
    pip install -r requirements.txt
    uvicorn main:app --reload --port 8000

  Environment variables (.env):
    DATABASE_URL=sqlite:///./fooddelivery.db     (SQLite — zero setup)
    STRIPE_SECRET_KEY=sk_test_...               (optional, mock used if absent)
    STRIPE_WEBHOOK_SECRET=whsec_...             (optional)
    SECRET_KEY=change-me-in-production
=============================================================================
"""

# ─── Standard library ──────────────────────────────────────────────────────
import asyncio
import hashlib
import json
import logging
import math
import os
import queue
import random
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from queue import Queue as SyncQueue
from typing import Any, Dict, List, Literal, Optional, AsyncGenerator

# ─── Third-party ───────────────────────────────────────────────────────────
from dotenv import load_dotenv
from fastapi import (
    APIRouter, BackgroundTasks, Depends, FastAPI, Header,
    HTTPException, Request, status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer, String, Text,
    create_engine, select, update,
)
from sqlalchemy.orm import DeclarativeBase, Session, Mapped, mapped_column, relationship, sessionmaker

load_dotenv()

# ─── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("FoodDelivery")

# =============================================================================
# DATABASE SETUP  (SQLite by default, PostgreSQL in production)
# =============================================================================

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fooddelivery.db")

# Fix Render's postgres:// prefix
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# SQLite needs check_same_thread=False; PostgreSQL doesn't need it
connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =============================================================================
# DATABASE MODELS
# =============================================================================

class Tenant(Base):
    __tablename__ = "tenants"
    tenant_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), nullable=False)
    plan = Column(String(30), default="free")
    created_at = Column(DateTime, default=datetime.utcnow)
    users = relationship("User", back_populates="tenant")


class User(Base):
    __tablename__ = "users"
    user_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(255), unique=True, nullable=False)
    username = Column(String(100), nullable=False)
    password_hash = Column(Text, nullable=False)
    tenant_id = Column(String(36), ForeignKey("tenants.tenant_id"), nullable=False)
    region = Column(String(50), default="ap-south-1")
    status = Column(String(20), default="active")
    mfa_enabled = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="users")


class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, index=True)
    payment_intent_id = Column(String(255), unique=True)
    order_id = Column(String(255))
    amount = Column(Integer)
    currency = Column(String(10), default="inr")
    customer_email = Column(String(255))
    status = Column(String(50), default="created")
    idempotency_key = Column(String(255), unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    event_id = Column(String(255), primary_key=True)
    order_id = Column(String(255), nullable=False)
    status = Column(String(50), default="IN_PROGRESS")
    processed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class OrderRecord(Base):
    __tablename__ = "order_records"
    order_id = Column(String(255), primary_key=True)
    current_state = Column(String(50), default="PENDING")
    restaurant_triggered_at = Column(DateTime, nullable=True)
    dispatch_triggered_at = Column(DateTime, nullable=True)
    rider_assigned_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)
    # Seed a default tenant so registration works out-of-the-box
    db = SessionLocal()
    try:
        existing = db.query(Tenant).filter_by(name="default").first()
        if not existing:
            t = Tenant(tenant_id="00000000-0000-0000-0000-000000000001", name="default", plan="free")
            db.add(t)
            db.commit()
            logger.info("Seeded default tenant: 00000000-0000-0000-0000-000000000001")
    finally:
        db.close()


# =============================================================================
# AUTH / PASSWORD UTILITIES
# =============================================================================

SECRET_KEY = os.getenv("SECRET_KEY", "change-this-secret-key-in-production")


def hash_password(password: str) -> str:
    """Hash password using SHA-256 + HMAC (bcrypt-compatible fallback)."""
    import hmac as _hmac
    salt = os.urandom(16).hex()
    h = _hmac.new((SECRET_KEY + salt).encode(), password[:72].encode(), hashlib.sha256).hexdigest()
    return f"sha256${salt}${h}"


def verify_password(plain: str, hashed: str) -> bool:
    try:
        _, salt, stored = hashed.split("$")
        import hmac as _hmac
        h = _hmac.new((SECRET_KEY + salt).encode(), plain[:72].encode(), hashlib.sha256).hexdigest()
        return h == stored
    except Exception:
        return False


def make_token(user_id: str) -> str:
    """Simple HMAC-based token (swap for JWT in production)."""
    import hmac
    payload = f"{user_id}:{int(time.time())}"
    sig = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}"


# =============================================================================
# STRIPE / PAYMENT GATEWAY MOCK
# =============================================================================

STRIPE_KEY = os.getenv("STRIPE_SECRET_KEY", "")
USE_STRIPE = bool(STRIPE_KEY)

if USE_STRIPE:
    try:
        import stripe
        stripe.api_key = STRIPE_KEY
    except ImportError:
        USE_STRIPE = False
        logger.warning("stripe package not installed — using mock gateway")

IDEMPOTENCY_DB = os.getenv("IDEMPOTENCY_DB", "/tmp/food_idempotency.db")


class IdempotencyStore:
    def __init__(self):
        conn = sqlite3.connect(IDEMPOTENCY_DB)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS idempotency_keys (
                key TEXT PRIMARY KEY,
                transaction_id TEXT,
                status TEXT,
                response TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def get(self, key: str) -> Optional[Dict]:
        conn = sqlite3.connect(IDEMPOTENCY_DB)
        row = conn.execute(
            "SELECT transaction_id, status, response FROM idempotency_keys "
            "WHERE key=? AND expires_at > datetime('now')",
            (key,)
        ).fetchone()
        conn.close()
        if row:
            return {"transaction_id": row[0], "status": row[1], "response": json.loads(row[2])}
        return None

    def set(self, key: str, transaction_id: str, status: str, response: dict, ttl_h: int = 24):
        conn = sqlite3.connect(IDEMPOTENCY_DB)
        expires = datetime.utcnow() + timedelta(hours=ttl_h)
        conn.execute(
            "INSERT OR REPLACE INTO idempotency_keys(key,transaction_id,status,response,expires_at) "
            "VALUES(?,?,?,?,?)",
            (key, transaction_id, status, json.dumps(response), expires)
        )
        conn.commit()
        conn.close()


idempotency_store = IdempotencyStore()


# ─── Circuit Breaker ────────────────────────────────────────────────────────

class CircuitBreaker:
    def __init__(self, threshold: int = 5, timeout: int = 60):
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure: Optional[datetime] = None
        self.state = "CLOSED"

    def can_attempt(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if self.last_failure and (datetime.utcnow() - self.last_failure).seconds > self.timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        return True  # HALF_OPEN

    def success(self):
        self.failures = 0
        self.state = "CLOSED"

    def failure(self):
        self.failures += 1
        self.last_failure = datetime.utcnow()
        if self.failures >= self.threshold:
            self.state = "OPEN"


gateway_breaker = CircuitBreaker()


async def call_gateway(transaction_id: str, amount: float, retries: int = 3) -> Dict:
    """Call payment gateway with retries + circuit breaker."""
    for attempt in range(retries):
        if not gateway_breaker.can_attempt():
            return {"success": False, "error": "Circuit breaker OPEN — gateway unavailable"}
        try:
            await asyncio.sleep(0.05)  # simulate network
            if USE_STRIPE:
                # Real Stripe — PaymentIntent already created; just simulate confirm here
                gateway_breaker.success()
                return {"success": True, "gateway_txn": f"stripe_{transaction_id}"}
            else:
                # Mock: 80% success rate
                if random.random() < 0.80:
                    gateway_breaker.success()
                    return {"success": True, "gateway_txn": f"MOCK_{transaction_id}"}
                raise Exception("Mock gateway timeout")
        except Exception as exc:
            gateway_breaker.failure()
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                return {"success": False, "error": str(exc)}
    return {"success": False, "error": "Unknown"}


# =============================================================================
# IN-MEMORY EVENT BUS  (replaces RabbitMQ/SQS/Kafka in this demo)
# =============================================================================

class EventBus:
    """Simple in-process pub/sub. Swap for Celery + Redis in production."""

    def __init__(self):
        self._events: List[Dict] = []
        self._handlers: Dict[str, List] = {}
        self._lock = threading.Lock()

    def subscribe(self, event_type: str, handler):
        with self._lock:
            self._handlers.setdefault(event_type, []).append(handler)

    def publish(self, event_type: str, payload: Dict):
        event = {"event_type": event_type, "timestamp": datetime.utcnow().isoformat(), **payload}
        with self._lock:
            self._events.append(event)
            handlers = list(self._handlers.get(event_type, []))
        for h in handlers:
            try:
                h(event)
            except Exception as exc:
                logger.error(f"EventBus handler error [{event_type}]: {exc}")

    def get_all(self) -> List[Dict]:
        with self._lock:
            return list(self._events)


event_bus = EventBus()


# =============================================================================
# RESTAURANT SERVICE  (rishugoyal805/Restaurant_service_project)
# =============================================================================

class RestaurantSQSQueue:
    def __init__(self):
        self._q = SyncQueue()
        self._dlq: List[Dict] = []
        self._retry_counts: Dict[str, int] = {}

    def send(self, order: Dict):
        self._q.put(order)

    def receive(self) -> Optional[Dict]:
        try:
            return self._q.get(timeout=1)
        except Exception:
            return None

    def send_to_dlq(self, order: Dict):
        self._dlq.append(order)
        logger.warning(f"[DLQ] Order {order.get('order_id')} moved to dead-letter queue")

    def view_dlq(self) -> List[Dict]:
        return list(self._dlq)

    def should_retry(self, order: Dict, max_retries: int = 3) -> bool:
        key = order.get("order_id", str(id(order)))
        self._retry_counts[key] = self._retry_counts.get(key, 0) + 1
        return self._retry_counts[key] <= max_retries


restaurant_queue = RestaurantSQSQueue()
_restaurant_cache: set = set()  # idempotency cache


def sns_notify(order: Dict):
    logger.info(f"[SNS] Restaurant notified — order {order.get('order_id')}, amount ₹{order.get('amount')}")
    event_bus.publish("RestaurantNotified", order)


def restaurant_worker():
    logger.info("[Restaurant Worker] Started")
    while True:
        order = restaurant_queue.receive()
        if order is None:
            continue
        try:
            if order.get("amount", 0) < 0:
                raise Exception("Invalid order amount")
            sns_notify(order)
        except Exception as exc:
            logger.error(f"[Restaurant Worker] Processing failed: {exc}")
            if restaurant_queue.should_retry(order):
                restaurant_queue.send(order)
            else:
                restaurant_queue.send_to_dlq(order)


# =============================================================================
# RIDER / DISPATCH SERVICE  (KanishkRichhariya107/cloudPBL)
# =============================================================================

class RiderStatus(str, Enum):
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"


@dataclass
class Rider:
    id: str
    name: str
    phone: str
    x: float
    y: float
    status: RiderStatus = RiderStatus.IDLE
    rating: float = 4.5
    last_active: float = field(default_factory=time.time)

    def is_available(self) -> bool:
        return self.status == RiderStatus.IDLE

    def distance_to(self, x: float, y: float) -> float:
        return math.sqrt((self.x - x) ** 2 + (self.y - y) ** 2)

    def to_dict(self) -> Dict:
        return {
            "id": self.id, "name": self.name, "phone": self.phone,
            "x": self.x, "y": self.y,
            "status": self.status.value, "rating": self.rating,
        }


def compute_rider_score(rider: Rider, ux: float, uy: float, rx: float, ry: float) -> float:
    return round(rider.distance_to(rx, ry) + math.sqrt((rx - ux) ** 2 + (ry - uy) ** 2), 4)


class RiderStore:
    def __init__(self):
        self._riders: Dict[str, Rider] = {}
        self._lock = threading.Lock()

    def seed(self, count: int = 5):
        with self._lock:
            self._riders.clear()
            for i in range(1, count + 1):
                r = Rider(
                    id=f"R{i}", name=f"Rider {i}",
                    phone=f"+91-9810000{i:03d}",
                    x=round(random.uniform(0, 10), 1),
                    y=round(random.uniform(0, 10), 1),
                    rating=round(random.uniform(4.0, 5.0), 1),
                )
                self._riders[r.id] = r

    def get_available(self) -> List[Rider]:
        with self._lock:
            return [r for r in self._riders.values() if r.is_available()]

    def assign(self, rider_id: str) -> bool:
        with self._lock:
            r = self._riders.get(rider_id)
            if r and r.is_available():
                r.status = RiderStatus.BUSY
                return True
            return False

    def free(self, rider_id: str):
        with self._lock:
            r = self._riders.get(rider_id)
            if r:
                r.status = RiderStatus.IDLE
                r.last_active = time.time()

    def snapshot(self) -> Dict:
        with self._lock:
            return {rid: r.to_dict() for rid, r in self._riders.items()}

    def all(self) -> List[Rider]:
        with self._lock:
            return list(self._riders.values())


rider_store = RiderStore()


async def simulate_rider_response(rider_id: str, rider_name: str, force_accept: bool = False) -> str:
    delay = random.uniform(0.3, 1.5)
    await asyncio.sleep(delay)
    if force_accept:
        return "accepted"
    roll = random.random()
    if roll < 0.65:
        return "accepted"
    return "rejected"


async def dispatch_order_stream(
    order_id: str, customer_name: str, amount: float,
    user_x: float, user_y: float, rest_x: float, rest_y: float,
) -> AsyncGenerator[Dict, None]:
    yield {"type": "log", "level": "info", "msg": f"📦 Dispatching order {order_id}"}
    await asyncio.sleep(0.2)

    available = rider_store.get_available()
    if not available:
        yield {"type": "order_failed", "order_id": order_id, "reason": "No riders available"}
        return

    ranked = sorted(
        [(compute_rider_score(r, user_x, user_y, rest_x, rest_y), r) for r in available],
        key=lambda x: x[0]
    )

    yield {"type": "log", "level": "info", "msg": f"🔍 Found {len(ranked)} riders — ranking..."}
    yield {"type": "riders_ranked", "riders": [
        {"id": r.id, "name": r.name, "x": r.x, "y": r.y, "score": round(s, 2), "rating": r.rating}
        for s, r in ranked
    ]}

    for i, (score, rider) in enumerate(ranked):
        is_last = i == len(ranked) - 1
        yield {"type": "log", "level": "info", "msg": f"🏍️ Contacting {rider.name}..."}

        outcome = await simulate_rider_response(rider.id, rider.name, force_accept=is_last)

        if outcome == "accepted":
            if rider_store.assign(rider.id):
                eta = int((rider.distance_to(rest_x, rest_y) + math.sqrt((rest_x-user_x)**2+(rest_y-user_y)**2)) * 2) + 5
                yield {"type": "rider_assigned", "order_id": order_id, "rider": rider.to_dict(), "eta_minutes": eta}
                event_bus.publish("RiderAssigned", {"order_id": order_id, "rider_id": rider.id})
                return
        else:
            yield {"type": "log", "level": "warn", "msg": f"🚫 {rider.name} {'rejected' if outcome=='rejected' else 'timed out'}"}

    yield {"type": "order_failed", "order_id": order_id, "reason": "All riders rejected"}


# =============================================================================
# CLOUD MODULE — Saga, Broker, Rate Limiter (PranviPandey/cloud-module2)
# =============================================================================

class SagaCoordinator:
    def __init__(self):
        self._state: Dict[str, Any] = {}

    def start(self, order_id: str):
        self._state[order_id] = {"payment": False, "restaurant": False, "dispatch": False}

    def complete_step(self, order_id: str, step: str):
        if order_id in self._state and isinstance(self._state[order_id], dict):
            self._state[order_id][step] = True

    def compensate(self, order_id: str):
        self._state[order_id] = "COMPENSATED"
        logger.warning(f"[SAGA] Rollback for order {order_id}")

    def get(self, order_id: str) -> Any:
        return self._state.get(order_id)

    def all(self) -> Dict:
        return dict(self._state)


class RateLimiter:
    def __init__(self, max_per_window: int = 10, window_seconds: int = 60):
        self._counts: Dict[str, List[float]] = {}
        self.max = max_per_window
        self.window = window_seconds
        self._lock = threading.Lock()

    def is_allowed(self, key: str) -> bool:
        with self._lock:
            now = time.time()
            timestamps = [t for t in self._counts.get(key, []) if now - t < self.window]
            if len(timestamps) >= self.max:
                return False
            timestamps.append(now)
            self._counts[key] = timestamps
            return True


saga = SagaCoordinator()
rate_limiter = RateLimiter()


# =============================================================================
# METRICS
# =============================================================================

class Metrics:
    def __init__(self):
        self._data: Dict[str, int] = {
            "total_requests": 0, "successful_payments": 0,
            "failed_payments": 0, "duplicate_requests": 0,
            "retries": 0, "circuit_breaker_trips": 0,
            "orders_placed": 0, "riders_assigned": 0,
        }
        self._lock = threading.Lock()

    def inc(self, key: str):
        with self._lock:
            self._data[key] = self._data.get(key, 0) + 1

    def get(self) -> Dict:
        with self._lock:
            return dict(self._data)


metrics = Metrics()


# =============================================================================
# EVENT HANDLERS (wires modules together)
# =============================================================================

def on_payment_success(event: Dict):
    """When payment succeeds: notify restaurant and trigger dispatch."""
    order_id = event.get("order_id", "")
    amount = event.get("amount", 0)
    saga.complete_step(order_id, "payment")

    # Notify restaurant queue (rishugoyal805 module)
    restaurant_queue.send({
        "order_id": order_id,
        "amount": amount,
        "idempotency_key": event.get("idempotency_key", str(uuid.uuid4())),
        "restaurant_id": event.get("restaurant_id", "REST-001"),
        "timestamp": datetime.utcnow().isoformat(),
    })
    saga.complete_step(order_id, "restaurant")

    # Update order state in DB
    db = SessionLocal()
    try:
        rec = db.query(OrderRecord).filter_by(order_id=order_id).first()
        if not rec:
            rec = OrderRecord(order_id=order_id)
            db.add(rec)
        rec.current_state = "PAYMENT_SUCCESS_RECEIVED"
        rec.restaurant_triggered_at = datetime.utcnow()
        db.commit()
    finally:
        db.close()

    logger.info(f"[EventHandler] PaymentSuccess handled for {order_id}")


event_bus.subscribe("PaymentSuccess", on_payment_success)


def on_rider_assigned(event: Dict):
    saga.complete_step(event.get("order_id", ""), "dispatch")
    db = SessionLocal()
    try:
        rec = db.query(OrderRecord).filter_by(order_id=event.get("order_id")).first()
        if rec:
            rec.current_state = "RIDER_ASSIGNED"
            rec.rider_assigned_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()
    metrics.inc("riders_assigned")


event_bus.subscribe("RiderAssigned", on_rider_assigned)


# =============================================================================
# PYDANTIC SCHEMAS
# =============================================================================

# ─── User schemas ───────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: EmailStr
    username: str
    password: str
    tenant_id: str = "00000000-0000-0000-0000-000000000001"
    region: str = "ap-south-1"

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 3:
            raise ValueError("Username must be at least 3 characters")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one digit")
        return v


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


# ─── Payment schemas ────────────────────────────────────────────────────────

class CreatePaymentRequest(BaseModel):
    order_id: str
    amount: int = Field(..., gt=0, description="Amount in smallest currency unit (paise for INR)")
    currency: str = "inr"
    customer_email: str
    idempotency_key: Optional[str] = None
    payment_method: str = "card"
    description: Optional[str] = None


class ConfirmPaymentRequest(BaseModel):
    payment_intent_id: str


class PaymentWebhookRequest(BaseModel):
    payment_intent_id: str
    status: str


# ─── Order / Dispatch schemas ────────────────────────────────────────────────

class PlaceOrderRequest(BaseModel):
    customer_name: str
    user_x: float = 5.0
    user_y: float = 5.0
    restaurant: str = "Spice Garden"
    restaurant_x: float = 3.0
    restaurant_y: float = 3.0
    items: List[str]
    amount: float


class RestaurantWebhookRequest(BaseModel):
    order_id: str
    amount: float
    restaurant_id: str = "REST-001"
    idempotency_key: Optional[str] = None


# =============================================================================
# ROUTERS
# =============================================================================

# ─── Health ─────────────────────────────────────────────────────────────────
health_router = APIRouter(tags=["Health"])

@health_router.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "FoodDelivery Integrated System",
        "timestamp": datetime.utcnow().isoformat(),
        "circuit_breaker": gateway_breaker.state,
        "modules": [
            "user", "payment", "payment_service", "payment_success",
            "payment_consumer", "restaurant", "dispatch", "cloud", "frontend_bridge",
        ],
    }


@health_router.get("/")
def root():
    return {
        "message": "🍕 Food Delivery System — All Modules Integrated",
        "docs": "/docs",
        "redoc": "/redoc",
        "modules": {
            "users": "/auth/register  /auth/login  /users",
            "payments": "/payments/create  /payments/confirm  /payments/{id}",
            "payment_service": "/api/v1/payments  /api/v1/payments/{id}/refund",
            "payment_success": "/payment-success  /events",
            "restaurant": "/restaurant/webhook  /restaurant/dlq",
            "dispatch": "/api/order/place  /api/order/{id}/stream  /api/riders",
            "cloud_module": "/cloud/order  /cloud/metrics  /cloud/saga",
            "system": "/metrics  /events  /health",
        },
    }


# ─── User / Auth router ─────────────────────────────────────────────────────
auth_router = APIRouter(prefix="/auth", tags=["Authentication"])

@auth_router.post("/register", status_code=201)
def register(payload: RegisterRequest, db: Session = Depends(get_db)):
    tenant = db.query(Tenant).filter_by(tenant_id=payload.tenant_id).first()
    if not tenant:
        raise HTTPException(404, detail={"code": "TENANT_NOT_FOUND", "message": f"Tenant {payload.tenant_id} not found"})

    if db.query(User).filter_by(email=payload.email.lower()).first():
        raise HTTPException(409, detail={"code": "EMAIL_ALREADY_EXISTS", "message": "Email already registered"})

    if db.query(User).filter_by(username=payload.username, tenant_id=payload.tenant_id).first():
        raise HTTPException(409, detail={"code": "USERNAME_TAKEN", "message": "Username already taken"})

    user = User(
        email=payload.email.lower(),
        username=payload.username,
        password_hash=hash_password(payload.password),
        tenant_id=payload.tenant_id,
        region=payload.region,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    logger.info(f"[Auth] Registered user {user.email}")
    return {
        "user_id": user.user_id,
        "email": user.email,
        "username": user.username,
        "tenant_id": user.tenant_id,
        "region": user.region,
        "created_at": user.created_at.isoformat(),
    }


@auth_router.post("/login")
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter_by(email=payload.email.lower()).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(401, detail={"code": "INVALID_CREDENTIALS", "message": "Invalid email or password"})
    token = make_token(user.user_id)
    return {"token": token, "user_id": user.user_id, "email": user.email, "username": user.username}


# ─── User CRUD ──────────────────────────────────────────────────────────────
users_router = APIRouter(prefix="/users", tags=["Users"])

@users_router.get("")
@users_router.get("/")
def get_users(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [{"user_id": u.user_id, "email": u.email, "username": u.username,
             "status": u.status, "region": u.region, "created_at": u.created_at} for u in users]

@users_router.get("/{user_id}")
def get_user(user_id: str, db: Session = Depends(get_db)):
    user = db.query(User).filter_by(user_id=user_id).first()
    if not user:
        raise HTTPException(404, detail="User not found")
    return {"user_id": user.user_id, "email": user.email, "username": user.username,
            "status": user.status, "region": user.region, "created_at": user.created_at}

@users_router.delete("/{user_id}")
def delete_user(user_id: str, db: Session = Depends(get_db)):
    user = db.query(User).filter_by(user_id=user_id).first()
    if not user:
        raise HTTPException(404, detail="User not found")
    db.delete(user)
    db.commit()
    return {"deleted": user_id}


# ─── Also mount users at /api/users for the React frontend (Ishikad01) ─────
api_users_router = APIRouter(prefix="/api/users", tags=["API Users (Frontend)"])

@api_users_router.post("/register", status_code=201)
def api_register(payload: RegisterRequest, db: Session = Depends(get_db)):
    return register(payload, db)

@api_users_router.post("/login")
def api_login(payload: LoginRequest, db: Session = Depends(get_db)):
    return login(payload, db)

@api_users_router.get("")
def api_get_users(db: Session = Depends(get_db)):
    return get_users(db)

@api_users_router.get("/{user_id}")
def api_get_user(user_id: str, db: Session = Depends(get_db)):
    return get_user(user_id, db)

@api_users_router.delete("/{user_id}")
def api_delete_user(user_id: str, db: Session = Depends(get_db)):
    return delete_user(user_id, db)


# ─── Payments router (Siriusspec/PaymentModule) ─────────────────────────────
payments_router = APIRouter(prefix="/payments", tags=["Payments (Stripe Module)"])


def _update_tx_status(payment_intent_id: str, status: str):
    db = SessionLocal()
    try:
        tx = db.query(Transaction).filter_by(payment_intent_id=payment_intent_id).first()
        if tx:
            tx.status = status
            tx.updated_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


@payments_router.get("/history")
def list_payments(limit: int = 20, db: Session = Depends(get_db)):
    txs = db.query(Transaction).order_by(Transaction.created_at.desc()).limit(limit).all()
    return [{"payment_intent_id": t.payment_intent_id, "order_id": t.order_id,
             "amount": t.amount, "currency": t.currency, "status": t.status,
             "customer_email": t.customer_email, "created_at": t.created_at} for t in txs]


@payments_router.post("/create")
async def create_payment(data: CreatePaymentRequest, db: Session = Depends(get_db)):
    if USE_STRIPE:
        try:
            import stripe as stripe_lib
            intent = stripe_lib.PaymentIntent.create(
                amount=data.amount, currency=data.currency,
                metadata={"order_id": data.order_id, "customer_email": data.customer_email},
            )
            pi_id = intent.id
            client_secret = intent.client_secret
        except Exception as exc:
            raise HTTPException(400, detail=f"Stripe error: {exc}")
    else:
        pi_id = f"pi_mock_{uuid.uuid4().hex[:16]}"
        client_secret = f"cs_mock_{uuid.uuid4().hex}"

    tx = Transaction(
        payment_intent_id=pi_id,
        order_id=data.order_id,
        amount=data.amount,
        currency=data.currency,
        customer_email=data.customer_email,
        status="created",
        idempotency_key=data.idempotency_key,
    )
    db.add(tx)
    db.commit()

    saga.start(data.order_id)
    return {"payment_intent_id": pi_id, "client_secret": client_secret,
            "amount": data.amount, "currency": data.currency, "status": "created"}


@payments_router.post("/confirm")
async def confirm_payment(data: ConfirmPaymentRequest, db: Session = Depends(get_db)):
    tx = db.query(Transaction).filter_by(payment_intent_id=data.payment_intent_id).first()
    if not tx:
        raise HTTPException(404, detail="Payment not found")

    if USE_STRIPE:
        try:
            import stripe as stripe_lib
            intent = stripe_lib.PaymentIntent.retrieve(data.payment_intent_id)
            new_status = intent.status
        except Exception as exc:
            raise HTTPException(400, detail=f"Stripe error: {exc}")
    else:
        # Mock: mark as succeeded
        new_status = "succeeded"

    tx.status = new_status
    tx.updated_at = datetime.utcnow()
    db.commit()

    if new_status == "succeeded":
        event_bus.publish("PaymentSuccess", {
            "order_id": tx.order_id, "amount": tx.amount / 100,
            "payment_intent_id": tx.payment_intent_id,
            "customer_email": tx.customer_email,
        })
        metrics.inc("successful_payments")

    return {"payment_intent_id": tx.payment_intent_id, "status": new_status,
            "amount": tx.amount, "currency": tx.currency}


@payments_router.get("/{payment_intent_id}")
def get_payment_status(payment_intent_id: str, db: Session = Depends(get_db)):
    tx = db.query(Transaction).filter_by(payment_intent_id=payment_intent_id).first()
    if not tx:
        raise HTTPException(404, detail="Payment not found")
    return {"payment_intent_id": tx.payment_intent_id, "order_id": tx.order_id,
            "amount": tx.amount, "currency": tx.currency, "status": tx.status,
            "customer_email": tx.customer_email, "created_at": tx.created_at}


@payments_router.post("/webhook")
async def stripe_webhook(request: Request, stripe_signature: str = Header(None)):
    """Stripe calls this endpoint automatically on payment events."""
    payload = await request.body()
    webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET", "")

    if USE_STRIPE and webhook_secret and stripe_signature:
        try:
            import stripe as stripe_lib
            event = stripe_lib.Webhook.construct_event(payload, stripe_signature, webhook_secret)
        except Exception as exc:
            raise HTTPException(400, detail=f"Webhook error: {exc}")
    else:
        # Mock webhook — parse JSON body directly
        try:
            event = json.loads(payload)
        except Exception:
            raise HTTPException(400, detail="Invalid payload")

    event_type = event.get("type", "")
    obj = event.get("data", {}).get("object", event)
    pi_id = obj.get("id") or obj.get("payment_intent_id")

    if event_type == "payment_intent.succeeded" or event.get("status") == "succeeded":
        _update_tx_status(pi_id, "succeeded")
        logger.info(f"[Webhook] Payment succeeded: {pi_id}")
    elif event_type == "payment_intent.payment_failed":
        _update_tx_status(pi_id, "failed")
        logger.info(f"[Webhook] Payment failed: {pi_id}")

    return {"status": "webhook received"}


# ─── Payment Service router (Bajaj041/payment-service) ──────────────────────
payment_service_router = APIRouter(prefix="/api/v1/payments", tags=["Payment Service (Idempotent)"])

# In-memory transaction store for this module
_tx_store: Dict[str, Dict] = {}


@payment_service_router.post("")
async def process_payment_idempotent(req: CreatePaymentRequest, background_tasks: BackgroundTasks):
    """Idempotent payment processing with circuit breaker and retry logic."""
    metrics.inc("total_requests")

    idem_key = req.idempotency_key or str(uuid.uuid4())
    cached = idempotency_store.get(idem_key)
    if cached:
        metrics.inc("duplicate_requests")
        return {**cached["response"], "message": "[IDEMPOTENT] Already processed"}

    txn_id = uuid.uuid4().hex[:8].upper()
    _tx_store[txn_id] = {
        "transaction_id": txn_id, "order_id": req.order_id,
        "amount": req.amount, "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }

    gw = await call_gateway(txn_id, req.amount)

    if gw["success"]:
        _tx_store[txn_id]["status"] = "success"
        metrics.inc("successful_payments")
        response = {
            "transaction_id": txn_id, "order_id": req.order_id,
            "status": "success", "amount": req.amount,
            "timestamp": datetime.utcnow().isoformat(),
            "idempotency_key": idem_key, "message": "Payment successful",
        }
        idempotency_store.set(idem_key, txn_id, "success", response)
        background_tasks.add_task(
            event_bus.publish, "PaymentSuccess",
            {"order_id": req.order_id, "amount": req.amount / 100,
             "transaction_id": txn_id, "idempotency_key": idem_key}
        )
        return response
    else:
        _tx_store[txn_id]["status"] = "failed"
        metrics.inc("failed_payments")
        return {
            "transaction_id": txn_id, "order_id": req.order_id,
            "status": "failed", "amount": req.amount,
            "timestamp": datetime.utcnow().isoformat(),
            "idempotency_key": idem_key, "message": gw.get("error", "Gateway failed"),
        }


@payment_service_router.get("/{txn_id}")
def get_txn_status(txn_id: str):
    tx = _tx_store.get(txn_id)
    if not tx:
        raise HTTPException(404, detail="Transaction not found")
    return tx


@payment_service_router.post("/{txn_id}/refund")
def refund_payment(txn_id: str):
    tx = _tx_store.get(txn_id)
    if not tx:
        raise HTTPException(404, detail="Transaction not found")
    if tx["status"] != "success":
        raise HTTPException(400, detail="Can only refund successful payments")
    tx["status"] = "refunded"
    event_bus.publish("PaymentRefunded", {"transaction_id": txn_id, "order_id": tx["order_id"]})
    return {"transaction_id": txn_id, "status": "refunded", "original_amount": tx["amount"]}


# ─── Payment Success module (IshaanAgarwal2704/payment_success_module) ───────
payment_success_router = APIRouter(tags=["Payment Success Event"])

_processed_orders: set = set()
_event_log: List[Dict] = []


class PaymentSuccessRequest(BaseModel):
    order_id: str
    amount: float


@payment_success_router.post("/payment-success")
def handle_payment_success(req: PaymentSuccessRequest):
    if req.order_id in _processed_orders:
        return {"status": "duplicate", "message": "Already processed", "order_id": req.order_id}

    _processed_orders.add(req.order_id)
    ev = {
        "event": "PaymentSuccess", "order_id": req.order_id,
        "amount": req.amount, "timestamp": str(datetime.now()),
    }
    _event_log.append(ev)
    event_bus.publish("PaymentSuccess", {"order_id": req.order_id, "amount": req.amount})
    return {"status": "success", "message": "Event created", "event": ev}


@payment_success_router.get("/events")
def get_events():
    return {"events": event_bus.get_all(), "count": len(event_bus.get_all())}


# ─── Restaurant router (rishugoyal805/Restaurant_service_project) ────────────
restaurant_router = APIRouter(prefix="/restaurant", tags=["Restaurant Service"])


@restaurant_router.post("/webhook")
def restaurant_webhook(order: RestaurantWebhookRequest):
    key = order.idempotency_key or order.order_id
    if key in _restaurant_cache:
        raise HTTPException(409, detail="Duplicate request")
    _restaurant_cache.add(key)

    db = SessionLocal()
    try:
        rec = db.query(OrderRecord).filter_by(order_id=order.order_id).first()
        if not rec:
            rec = OrderRecord(order_id=order.order_id, current_state="RESTAURANT_TRIGGERED")
            db.add(rec)
        else:
            rec.current_state = "RESTAURANT_TRIGGERED"
            rec.restaurant_triggered_at = datetime.utcnow()
        db.commit()
    finally:
        db.close()

    restaurant_queue.send({
        "order_id": order.order_id, "amount": order.amount,
        "restaurant_id": order.restaurant_id,
        "idempotency_key": key,
    })
    return {"status": "Order Accepted", "order_id": order.order_id}


@restaurant_router.get("/dlq")
def view_dlq():
    return {"failed_messages": restaurant_queue.view_dlq()}


@restaurant_router.get("/queue/status")
def queue_status():
    return {"queue_size": restaurant_queue._q.qsize(), "dlq_size": len(restaurant_queue.view_dlq())}


# ─── Dispatch / Order router (KanishkRichhariya107/cloudPBL) ─────────────────
dispatch_router = APIRouter(tags=["Dispatch & Rider Assignment"])


@dispatch_router.post("/api/order/place")
async def place_order(req: PlaceOrderRequest):
    order_id = f"ORD-{uuid.uuid4().hex[:6].upper()}"
    metrics.inc("orders_placed")
    saga.start(order_id)

    # Fire-and-forget dispatch in background
    asyncio.create_task(_dispatch_background(
        order_id, req.customer_name, req.amount,
        req.user_x, req.user_y, req.restaurant_x, req.restaurant_y
    ))

    return {
        "order_id": order_id, "customer_name": req.customer_name,
        "restaurant": req.restaurant, "items": req.items,
        "amount": req.amount, "status": "pending",
        "stream_url": f"/api/order/{order_id}/stream",
    }


async def _dispatch_background(order_id, customer, amount, ux, uy, rx, ry):
    async for _ in dispatch_order_stream(order_id, customer, amount, ux, uy, rx, ry):
        pass  # Events are published to event_bus inside the generator


@dispatch_router.get("/api/order/{order_id}/stream")
async def stream_dispatch(order_id: str, user_x: float = 5.0, user_y: float = 5.0,
                           restaurant_x: float = 3.0, restaurant_y: float = 3.0,
                           amount: float = 0.0):
    """Server-Sent Events stream for real-time dispatch updates."""
    async def generator():
        async for event in dispatch_order_stream(
            order_id, "Customer", amount, user_x, user_y, restaurant_x, restaurant_y
        ):
            yield f"data: {json.dumps(event)}\n\n"
        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(generator(), media_type="text/event-stream",
                              headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@dispatch_router.get("/api/riders")
def get_riders():
    return {"riders": rider_store.snapshot()}


@dispatch_router.post("/api/riders/{rider_id}/free")
def free_rider(rider_id: str):
    rider_store.free(rider_id)
    return {"ok": True}


@dispatch_router.post("/api/reset")
def reset_riders():
    for r in rider_store.all():
        rider_store.free(r.id)
    return {"ok": True, "message": "All riders reset to IDLE"}


@dispatch_router.post("/api/init")
def init_riders(count: int = 5):
    rider_store.seed(count)
    return {"ok": True, "riders": rider_store.snapshot()}


# ─── Cloud Module router (PranviPandey/cloud-module2) ────────────────────────
cloud_router = APIRouter(prefix="/cloud", tags=["Cloud Module (Priority Queue, Saga)"])


class CloudOrderRequest(BaseModel):
    user_id: str
    items: List[str]
    amount: float
    vip: bool = False
    idempotency_key: Optional[str] = None


_cloud_orders: Dict[str, Dict] = {}
_cloud_dlq: List[Dict] = []


@cloud_router.post("/order")
def place_cloud_order(req: CloudOrderRequest):
    if not rate_limiter.is_allowed(req.user_id):
        raise HTTPException(429, detail=f"Rate limit exceeded for user {req.user_id}")

    idem = req.idempotency_key or str(uuid.uuid4())
    if idem in _cloud_orders:
        return {"status": "duplicate", "order_id": _cloud_orders[idem]["order_id"]}

    order_id = str(uuid.uuid4())
    order = {
        "order_id": order_id, "user_id": req.user_id, "items": req.items,
        "amount": req.amount, "vip": req.vip, "priority": 1 if req.vip else 2,
        "status": "placed", "idempotency_key": idem,
        "created_at": datetime.utcnow().isoformat(),
    }
    _cloud_orders[idem] = order
    saga.start(order_id)

    # Publish to event bus as if the broker dispatched it
    event_bus.publish("OrderPlaced", order)
    saga.complete_step(order_id, "payment")  # simulated step

    return {"order_id": order_id, "status": "placed", "priority": "VIP" if req.vip else "Normal"}


@cloud_router.get("/saga")
def get_saga_state():
    return {"saga_transactions": saga.all()}


@cloud_router.get("/orders")
def list_cloud_orders():
    return {"orders": list(_cloud_orders.values())}


# ─── System-wide endpoints ──────────────────────────────────────────────────
system_router = APIRouter(tags=["System"])


@system_router.get("/metrics")
def get_metrics():
    return {
        "service": "FoodDelivery",
        "timestamp": datetime.utcnow().isoformat(),
        "circuit_breaker": gateway_breaker.state,
        "metrics": metrics.get(),
    }


@system_router.get("/events")
def list_events(limit: int = 50):
    all_events = event_bus.get_all()
    return {"events": all_events[-limit:], "total": len(all_events)}


@system_router.get("/orders/{order_id}/status")
def order_status(order_id: str, db: Session = Depends(get_db)):
    rec = db.query(OrderRecord).filter_by(order_id=order_id).first()
    if not rec:
        raise HTTPException(404, detail="Order not found")
    return {
        "order_id": rec.order_id, "state": rec.current_state,
        "saga": saga.get(order_id),
        "restaurant_triggered_at": rec.restaurant_triggered_at,
        "dispatch_triggered_at": rec.dispatch_triggered_at,
        "rider_assigned_at": rec.rider_assigned_at,
        "created_at": rec.created_at,
    }


# =============================================================================
# APP FACTORY & STARTUP
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    logger.info("🚀 Starting Food Delivery System...")
    init_db()
    rider_store.seed(5)
    logger.info("✅ Database tables created")
    logger.info("✅ Rider store seeded with 5 riders")

    # Start restaurant background worker thread
    t = threading.Thread(target=restaurant_worker, daemon=True)
    t.start()
    logger.info("✅ Restaurant worker thread started")

    logger.info("🎉 All modules initialised — system ready!")
    yield
    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("Shutting down Food Delivery System...")


app = FastAPI(
    title="Food Delivery System — All Modules Integrated",
    description=(
        "A single-file FastAPI application integrating 9 microservice modules:\n\n"
        "- **User Module** (devyani1512): Registration, login, user CRUD\n"
        "- **Payment Module** (Siriusspec): Stripe PaymentIntent lifecycle\n"
        "- **Payment Service** (Bajaj041): Idempotency, circuit breaker, retries\n"
        "- **Payment Success** (IshaanAgarwal2704): Event publishing\n"
        "- **Payment Consumer** (KartikAg13): Restaurant + Dispatch trigger on success\n"
        "- **Restaurant Service** (rishugoyal805): SQS/SNS simulation, DLQ\n"
        "- **Dispatch Service** (KanishkRichhariya107): Rider assignment with SSE\n"
        "- **Cloud Module** (PranviPandey): Saga, priority queue, rate limiting\n"
        "- **Frontend Bridge** (Ishikad01): `/api/users` routes for React frontend"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register all routers
app.include_router(health_router)
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(api_users_router)
app.include_router(payments_router)
app.include_router(payment_service_router)
app.include_router(payment_success_router)
app.include_router(restaurant_router)
app.include_router(dispatch_router)
app.include_router(cloud_router)
app.include_router(system_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
