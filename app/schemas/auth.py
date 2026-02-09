"""Authentication schemas for API requests and responses"""

from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional
from datetime import datetime


class UserRegister(BaseModel):
    """Schema for user registration"""
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=100)
    full_name: Optional[str] = Field(None, max_length=255)
    phone: str = Field(..., min_length=10, max_length=15)

    @field_validator('password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        """Validate password strength"""
        if not any(char.isdigit() for char in v):
            raise ValueError('Password must contain at least one digit')
        if not any(char.isupper() for char in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(char.islower() for char in v):
            raise ValueError('Password must contain at least one lowercase letter')
        return v

    @field_validator('phone')
    @classmethod
    def validate_phone(cls, v: str) -> str:
        """Validate phone - strip non-digits, must be 10-15 digits"""
        digits = ''.join(c for c in v if c.isdigit())
        if len(digits) < 10 or len(digits) > 15:
            raise ValueError('Phone must be 10-15 digits')
        return digits


class UserLogin(BaseModel):
    """Schema for user login"""
    email: EmailStr
    password: str


class Token(BaseModel):
    """Schema for JWT token response"""
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    """Schema for user data response"""
    id: str
    email: str
    full_name: Optional[str]
    phone: Optional[str] = None
    phone_verified: bool = False
    role: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class VerifyOtpRequest(BaseModel):
    """Schema for OTP verification"""
    code: str = Field(..., min_length=6, max_length=6)


class ForgotPasswordRequest(BaseModel):
    """Schema for forgot password request"""
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    """Schema for password reset"""
    token: str
    new_password: str = Field(..., min_length=8, max_length=100)

    @field_validator('new_password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        """Validate password strength"""
        if not any(char.isdigit() for char in v):
            raise ValueError('Password must contain at least one digit')
        if not any(char.isupper() for char in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(char.islower() for char in v):
            raise ValueError('Password must contain at least one lowercase letter')
        return v
