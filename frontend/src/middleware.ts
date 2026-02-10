import { NextResponse, type NextRequest } from 'next/server'

export async function middleware(request: NextRequest) {
  // Force HTTPS in production (skip for Railway internal healthchecks)
  const proto = request.headers.get('x-forwarded-proto')
  const host = request.headers.get('host') || ''
  const isInternalRequest = host.includes('.railway.internal') || !host.includes('.')
  if (proto === 'http' && !isInternalRequest) {
    const url = request.nextUrl.clone()
    url.protocol = 'https'
    return NextResponse.redirect(url, 301)
  }

  const token = request.cookies.get('auth_token')?.value
  const partnerToken = request.cookies.get('partner_token')?.value
  const { pathname } = request.nextUrl

  // Protected routes - redirect to login if not authenticated
  if (pathname.startsWith('/dashboard') || pathname.startsWith('/admin')) {
    if (!token) {
      const url = request.nextUrl.clone()
      url.pathname = '/login'
      return NextResponse.redirect(url)
    }

    // For admin routes, check role from JWT payload
    if (pathname.startsWith('/admin')) {
      try {
        // Decode JWT payload (base64)
        const payloadBase64 = token.split('.')[1]
        const payload = JSON.parse(atob(payloadBase64))

        if (payload.role !== 'admin') {
          const url = request.nextUrl.clone()
          url.pathname = '/dashboard'
          return NextResponse.redirect(url)
        }
      } catch {
        // Invalid token - redirect to login
        const url = request.nextUrl.clone()
        url.pathname = '/login'
        return NextResponse.redirect(url)
      }
    }
  }

  // Partner/Referral routes - use regular auth_token (any logged in user)
  if (pathname.startsWith('/partner')) {
    if (!token) {
      const url = request.nextUrl.clone()
      url.pathname = '/login'
      return NextResponse.redirect(url)
    }
  }

  // Verify phone - requires auth but don't redirect to dashboard
  if (pathname.startsWith('/verify-phone')) {
    if (!token) {
      const url = request.nextUrl.clone()
      url.pathname = '/login'
      return NextResponse.redirect(url)
    }
    return NextResponse.next()
  }

  // Auth routes - redirect to dashboard if already authenticated
  if (
    pathname.startsWith('/login') ||
    pathname.startsWith('/register') ||
    pathname.startsWith('/forgot-password')
  ) {
    if (token) {
      const url = request.nextUrl.clone()
      url.pathname = '/dashboard'
      return NextResponse.redirect(url)
    }
  }

  return NextResponse.next()
}

export const config = {
  matcher: [
    '/dashboard/:path*',
    '/admin/:path*',
    '/partner/:path*',
    '/login',
    '/register',
    '/forgot-password',
    '/reset-password',
    '/verify-phone',
  ],
}
