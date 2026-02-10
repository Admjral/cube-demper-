"use client"

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  return (
    <html>
      <body>
        <div style={{
          display: "flex",
          minHeight: "100vh",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          gap: "1rem",
          textAlign: "center",
          fontFamily: "system-ui, sans-serif",
        }}>
          <h2 style={{ fontSize: "1.25rem", fontWeight: 600 }}>
            Что-то пошло не так
          </h2>
          <p style={{ color: "#666", maxWidth: "28rem" }}>
            Произошла критическая ошибка. Попробуйте обновить страницу.
          </p>
          <button
            onClick={reset}
            style={{
              padding: "0.5rem 1rem",
              borderRadius: "0.375rem",
              border: "1px solid #ccc",
              cursor: "pointer",
              fontSize: "0.875rem",
            }}
          >
            Попробовать снова
          </button>
        </div>
      </body>
    </html>
  )
}
