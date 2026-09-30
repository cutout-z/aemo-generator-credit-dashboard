/** Tailwind theme for the AEMO credit dashboard.
 *
 *  Every colour is a CSS variable, so dark/light is one attribute flip on <html>.
 *  The token values live in assets/css/tailwind.src.css (the single source).
 *  No raw hex in docs/index.html — use these names.
 *
 *  Chart colours are NOT here: Plotly reads them from the same CSS variables through
 *  assets/js/chart-tokens.js, so a chart mark and its legend swatch can never drift apart.
 */
module.exports = {
  // The page, its inline <script> (the class strings it builds live in the same file),
  // and the unpublished design pages under design/.
  content: ["./docs/**/*.html", "./docs/**/*.js", "./design/**/*.html"],
  // Preflight ON since BRIEF.md step 1: the page's own inline reset (`* { margin:0; padding:0 }`,
  // the body font) is gone, so this is the only reset. The panels not yet migrated set their own
  // spacing and type explicitly, so they are unaffected.
  corePlugins: { preflight: true },
  theme: {
    extend: {
      colors: {
        bg: "var(--bg)",
        canvas: "var(--canvas)",
        surface: "var(--surface)",
        "surface-2": "var(--surface-2)",
        "surface-3": "var(--surface-3)",
        line: "var(--line)",
        "line-strong": "var(--line-strong)",
        "line-soft": "var(--line-soft)",
        ink: "var(--text)",
        muted: "var(--muted)",
        faint: "var(--faint)",
        accent: "var(--accent)",
        "accent-soft": "var(--accent-soft)",
        good: "var(--good)",
        warn: "var(--warn)",
        bad: "var(--bad)",
        info: "var(--info)",
        neutral: "var(--neutral)",
        "accent-wash": "var(--accent-wash)",
        "good-wash": "var(--good-wash)",
        "warn-wash": "var(--warn-wash)",
        "bad-wash": "var(--bad-wash)",
        "neutral-wash": "var(--neutral-wash)",
      },
      // Preflight gives every element a border colour; make a bare `border` a hairline, not gray-200.
      borderColor: { DEFAULT: "var(--line)" },
      borderRadius: { control: "8px", card: "12px", panel: "14px" },
      boxShadow: { pop: "var(--shadow-pop)" },
      spacing: { 18: "4.5rem" },
      fontFamily: {
        sans: ['-apple-system', 'BlinkMacSystemFont', '"SF Pro Text"', '"Segoe UI"', 'Inter', 'system-ui', 'sans-serif'],
        mono: ['ui-monospace', '"SF Mono"', '"JetBrains Mono"', 'Menlo', 'Consolas', 'monospace'],
      },
    },
  },
  plugins: [],
};
