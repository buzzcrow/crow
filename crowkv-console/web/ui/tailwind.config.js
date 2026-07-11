/** @type {import('tailwindcss').Config} */
export default {
  prefix: 'tw-',
  important: '.crowkv-console',
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: 'var(--color-bg, #0b0d10)',
        panel: 'var(--color-panel, #161a1f)',
        accent: 'var(--color-accent, #88c0d0)',
        'brand-accent': 'var(--brand-accent, var(--color-accent, #88c0d0))',
        accent2: 'var(--color-accent2, #81a1c1)',
        muted: 'var(--color-muted, #a0a9bd)',
        text: 'var(--color-text, #d8dee9)',
        border: 'var(--color-border, #2e3440)',
        healthy: "#10b981",
        degraded: "#f59e0b",
        failed: "#ef4444",
        unknown: "#6b7280",
        remote: "#8b5cf6",
      },
      fontFamily: {
        mono: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      animation: {
        'pulse-slow': 'pulse 1.5s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'pulse-fast': 'pulse 0.5s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'slide-in-right': 'slideInRight 0.3s ease-out',
        'fade-in': 'fadeIn 0.2s ease-out',
        'scale-in': 'scaleIn 0.1s ease-out',
      },
      keyframes: {
        slideInRight: {
          '0%': { transform: 'translateX(100%)', opacity: '0' },
          '100%': { transform: 'translateX(0)', opacity: '1' },
        },
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        scaleIn: {
          '0%': { transform: 'scale(0.95)', opacity: '0' },
          '100%': { transform: 'scale(1)', opacity: '1' },
        },
      },
    },
  },
  plugins: [],
};
