from fastapi import HTTPException, status


class AuthenticationError(HTTPException):
    """Raised when authentication fails"""
    def __init__(self, detail: str = "Could not validate credentials"):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


class AuthorizationError(HTTPException):
    """Raised when user lacks permission"""
    def __init__(self, detail: str = "Not enough permissions"):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


class NotFoundError(HTTPException):
    """Raised when resource not found"""
    def __init__(self, detail: str = "Resource not found"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class ConflictError(HTTPException):
    """Raised when resource already exists"""
    def __init__(self, detail: str = "Resource already exists"):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail)


class ValidationError(HTTPException):
    """Raised when validation fails"""
    def __init__(self, detail: str = "Validation error"):
        super().__init__(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail)


class KaspiAuthError(HTTPException):
    """Raised when Kaspi authentication fails"""
    def __init__(self, detail: str = "Kaspi authentication failed"):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


class KaspiAPIError(HTTPException):
    """Raised when Kaspi API call fails"""
    def __init__(self, detail: str = "Kaspi API error", status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR):
        super().__init__(status_code=status_code, detail=detail)


class WahaError(HTTPException):
    """Raised when WAHA operation fails"""
    def __init__(self, detail: str = "WhatsApp operation failed"):
        super().__init__(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail)


class SubscriptionError(HTTPException):
    """Raised when subscription limit reached"""
    def __init__(self, detail: str = "Subscription limit reached"):
        super().__init__(status_code=status.HTTP_402_PAYMENT_REQUIRED, detail=detail)
