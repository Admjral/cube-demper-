import { NextResponse, type NextRequest } from 'next/server'

export async function middleware(request: NextRequest) {
  const token = request.cookies.get('auth_token')?.value
  const { pathname } = request.nextUrl

  // Protected routes - redirect to login if not authenticated
  if (pathname.startsWith('/dashboard')) {
    if (!token) {
      const url = request.nextUrl.clone()
      url.pathname = '/login'
      return NextResponse.redirect(url)
    }

    // Check if user is admin
    try {
      const payloadBase64 = token.split('.')[1]
      const payload = JSON.parse(atob(payloadBase64))

      if (payload.role !== 'admin') {
        const url = request.nextUrl.clone()
        url.pathname = '/login'
        url.searchParams.set('error', 'admin_required')
        return NextResponse.redirect(url)
      }
    } catch {
      // Invalid token - redirect to login
      const url = request.nextUrl.clone()
      url.pathname = '/login'
      return NextResponse.redirect(url)
    }
  }

  // Auth routes - redirect to dashboard if already authenticated
  if (pathname.startsWith('/login')) {
    if (token) {
      try {
        const payloadBase64 = token.split('.')[1]
        const payload = JSON.parse(atob(payloadBase64))

        if (payload.role === 'admin') {
          const url = request.nextUrl.clone()
          url.pathname = '/dashboard'
          return NextResponse.redirect(url)
        }
      } catch {
        // Invalid token - allow login
      }
    }
  }

  return NextResponse.next()
}

export const config = {
  matcher: [
    '/dashboard/:path*',
    '/login',
  ],
}
