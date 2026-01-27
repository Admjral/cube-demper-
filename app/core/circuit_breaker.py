"""
Circuit Breaker implementation for external service calls.

Prevents cascading failures when Kaspi API is unavailable by:
- Tracking failures and opening circuit after threshold
- Rejecting requests immediately when circuit is open
- Automatically trying to recover after timeout period

States:
- CLOSED: Normal operation, requests go through
- OPEN: All requests rejected immediately
- HALF_OPEN: Testing if service recovered with limited requests
"""
import asyncio
import time
import logging
from enum import Enum
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # All requests rejected
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5      # Failures before opening circuit
    success_threshold: int = 2       # Successes needed to close from half-open
    timeout_seconds: float = 60.0    # Time before trying half-open
    half_open_max_calls: int = 3     # Max concurrent calls in half-open state


class CircuitOpenError(Exception):
    """Raised when circuit is open and request is rejected."""
    pass


class CircuitBreaker:
    """
    Circuit Breaker for protecting against cascading failures.

    Usage:
        breaker = get_kaspi_circuit_breaker()

        try:
            async with breaker:
                result = await make_api_call()
        except CircuitOpenError:
            # Circuit is open, request rejected immediately
            logger.warning("Circuit open, skipping request")
            return None

    The circuit breaker tracks failures and automatically:
    - Opens after failure_threshold consecutive failures
    - Tries to recover after timeout_seconds
    - Closes after success_threshold successful requests in half-open state
    """

    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        """
        Initialize circuit breaker.

        Args:
            name: Identifier for this circuit (for logging)
            config: Configuration options
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """
        Get current circuit state, checking for timeout.

        Automatically transitions from OPEN to HALF_OPEN if
        enough time has passed since last failure.
        """
        if self._state == CircuitState.OPEN:
            if self._should_try_half_open():
                return CircuitState.HALF_OPEN
        return self._state

    def _should_try_half_open(self) -> bool:
        """Check if enough time passed to try half-open recovery."""
        if self._last_failure_time is None:
            return False
        elapsed = time.monotonic() - self._last_failure_time
        return elapsed >= self.config.timeout_seconds

    async def __aenter__(self):
        """
        Check if request can proceed through the circuit.

        Raises:
            CircuitOpenError: If circuit is open or half-open limit reached
        """
        async with self._lock:
            state = self.state

            if state == CircuitState.OPEN:
                logger.warning(f"Circuit {self.name} is OPEN, rejecting request")
                raise CircuitOpenError(f"Circuit {self.name} is open")

            if state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    raise CircuitOpenError(f"Circuit {self.name} half-open limit reached")
                self._half_open_calls += 1
                self._state = CircuitState.HALF_OPEN
                logger.debug(f"Circuit {self.name} HALF_OPEN, allowing test request")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Record result of the request.

        Updates circuit state based on success/failure.
        """
        async with self._lock:
            if exc_type is None:
                # Success
                await self._on_success()
            else:
                # Failure (but not CircuitOpenError which we raised)
                if exc_type is not CircuitOpenError:
                    await self._on_failure()

        return False  # Don't suppress exceptions

    async def _on_success(self):
        """Handle successful request."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            logger.debug(f"Circuit {self.name} success in HALF_OPEN: {self._success_count}/{self.config.success_threshold}")
            if self._success_count >= self.config.success_threshold:
                logger.info(f"Circuit {self.name} CLOSED (recovered after {self.config.timeout_seconds}s)")
                self._state = CircuitState.CLOSED
                self._reset_counts()
        else:
            # Reset failure count on success in closed state
            self._failure_count = 0

    async def _on_failure(self):
        """Handle failed request."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            logger.warning(f"Circuit {self.name} OPEN (half-open test failed)")
            self._state = CircuitState.OPEN
            self._reset_counts()
        elif self._failure_count >= self.config.failure_threshold:
            logger.warning(
                f"Circuit {self.name} OPEN (threshold reached: "
                f"{self._failure_count} failures)"
            )
            self._state = CircuitState.OPEN

    def _reset_counts(self):
        """Reset all counters."""
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0

    def get_stats(self) -> dict:
        """
        Get circuit breaker stats for monitoring.

        Returns:
            Dictionary with current state and counters
        """
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure": self._last_failure_time,
        }

    def force_open(self):
        """Manually force circuit to open (for testing/maintenance)."""
        self._state = CircuitState.OPEN
        self._last_failure_time = time.monotonic()
        logger.warning(f"Circuit {self.name} manually forced OPEN")

    def force_close(self):
        """Manually force circuit to close (for recovery)."""
        self._state = CircuitState.CLOSED
        self._reset_counts()
        logger.info(f"Circuit {self.name} manually forced CLOSED")


# Global circuit breakers registry
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str, config: CircuitBreakerConfig = None) -> CircuitBreaker:
    """
    Get or create a circuit breaker by name.

    Args:
        name: Unique identifier for the circuit
        config: Optional configuration (only used on first call)

    Returns:
        CircuitBreaker instance
    """
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(name, config)
    return _circuit_breakers[name]


def get_all_circuit_breakers() -> dict[str, CircuitBreaker]:
    """Get all registered circuit breakers (for monitoring)."""
    return _circuit_breakers.copy()


# Pre-configured circuit breakers for common services

def get_kaspi_circuit_breaker() -> CircuitBreaker:
    """
    Get circuit breaker for Kaspi API calls.

    Configuration:
    - Opens after 5 consecutive failures
    - Tries to recover after 60 seconds
    - Closes after 2 successful requests
    """
    return get_circuit_breaker(
        "kaspi_api",
        CircuitBreakerConfig(
            failure_threshold=5,
            success_threshold=2,
            timeout_seconds=60.0,
            half_open_max_calls=3,
        )
    )


def get_kaspi_auth_circuit_breaker() -> CircuitBreaker:
    """
    Get circuit breaker for Kaspi auth calls.

    Has longer timeout since auth failures are more critical
    and we want to be more careful about retry attempts.
    """
    return get_circuit_breaker(
        "kaspi_auth",
        CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=1,
            timeout_seconds=120.0,  # Longer timeout for auth
            half_open_max_calls=1,
        )
    )
