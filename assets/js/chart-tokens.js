/* Chart tokens — the bridge between the CSS design tokens and Plotly.
 *
 * Why this exists: this dashboard's design language lives in CSS variables (assets/css/tailwind.src.css).
 * Plotly needs literal colours. If the two are maintained separately they drift, and a chart mark stops
 * matching the swatch in its own legend. So every chart colour is READ FROM THE SAME VARIABLES.
 *
 * Rules this enforces:
 *   - Colour is assigned to an ENTITY, not to a series index. `series('good','accent')` always returns
 *     those two colours in that order, whatever the chart.
 *   - Chart chrome (gridlines, ticks, fonts, hover) is one layout object, so 14 charts cannot disagree.
 *   - Explicit heights: Plotly renders 0px if a panel is hidden when the chart is created. Always pass
 *     an explicit height (see HEIGHTS) or re-layout on reveal.
 *
 * Usage:
 *   Plotly.newPlot(el, traces, ChartTokens.layout({ height: ChartTokens.HEIGHTS.md }), ChartTokens.config);
 *   trace.line.color = ChartTokens.color('accent');
 *
 * No build step: this file is served as-is and reads the live CSS variables, so a theme flip
 * (`<html data-theme="light">`) is picked up with no extra code.
 */
(function () {
  "use strict";

  var SEMANTIC = {
    good: "--good", warn: "--warn", bad: "--bad", info: "--info",
    accent: "--accent", neutral: "--neutral",
    goodWash: "--good-wash", warnWash: "--warn-wash", badWash: "--bad-wash",
    accentWash: "--accent-wash", neutralWash: "--neutral-wash",
    surface: "--surface", surface2: "--surface-2", surface3: "--surface-3",
    line: "--line", lineSoft: "--line-soft", lineStrong: "--line-strong",
    ink: "--text", muted: "--muted", faint: "--faint",
    bg: "--bg", canvas: "--canvas"
  };

  // Explicit heights, in the dashboard's rhythm. Never let Plotly infer one.
  var HEIGHTS = { sm: 200, md: 280, lg: 340, xl: 420 };

  // Plotly cannot read font-family from a colour variable, so the stack is named once here and
  // kept in step with tailwind.config.js fontFamily.sans.
  var FONT = '-apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", Inter, system-ui, sans-serif';

  var _lightCache = null;

  function _read(el, name) {
    if (!el) return "";
    return getComputedStyle(el).getPropertyValue(name).trim();
  }

  /** Current theme's value of a token ('' if unknown — the caller sees it, nothing is invented). */
  function color(name) {
    var varName = SEMANTIC[name] || (String(name).indexOf("--") === 0 ? name : null);
    if (!varName) return "";
    return _read(document.documentElement, varName);
  }

  /** Same token under the light theme, without needing the page to be in light mode. */
  function lightColor(name) {
    if (!_lightCache) {
      var probe = document.createElement("div");
      probe.setAttribute("data-theme", "light");
      probe.style.cssText = "position:absolute;left:-9999px;top:0;width:0;height:0";
      document.body.appendChild(probe);
      _lightCache = probe;
    }
    return color(name) ? _read(_lightCache, SEMANTIC[name] || name) : color(name);
  }

  /** Inline style for .swatch / .swatch-bar, carrying both themes so the dot tracks the theme. */
  function swatchStyle(name) {
    var d = color(name), l = lightColor(name);
    return "--c-dark:" + (d || "transparent") + ";--c-light:" + (l || d || "transparent");
  }

  /** Legend/mark HTML: <span class="swatch" style="..."> */
  function swatchHTML(name, kind) {
    var cls = kind === "bar" ? "swatch-bar" : "swatch";
    return '<span class="' + cls + '" style="' + swatchStyle(name) + '"></span>';
  }

  /** Entity colours, in the order asked for. Unknown names return transparent, never a guess. */
  function series() {
    return Array.prototype.map.call(arguments, function (n) { return color(n) || "transparent"; });
  }

  /** Sequential ramp step 0-7 (heatmaps, day-state grids). Mirrors the .seq-N classes. */
  function seq(n, which) {
    var v = _read(document.documentElement, (which === "text" ? "--seq-t" : "--seq-") + n);
    return v || "";
  }
  function seqScale() {
    var out = [];
    for (var i = 0; i < 8; i++) out.push([i / 7, seq(i)]);
    return out;
  }

  /** The shared Plotly layout. Pass at least { height }. */
  function layout(overrides) {
    var base = {
      height: HEIGHTS.md,
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { family: FONT, size: 12, color: color("muted") },
      margin: { l: 48, r: 16, t: 8, b: 32 },
      hovermode: "x unified",
      hoverlabel: {
        bgcolor: color("surface-2"), bordercolor: color("line"),
        font: { size: 12, color: color("ink") }
      },
      xaxis: {
        gridcolor: color("line-soft"), zerolinecolor: color("line"),
        linecolor: color("line"), tickcolor: color("line"),
        tickfont: { size: 11, color: color("faint") },
        automargin: true, showline: false
      },
      yaxis: {
        gridcolor: color("line-soft"), zerolinecolor: color("line"),
        linecolor: color("line"), tickcolor: color("line"),
        tickfont: { size: 11, color: color("faint") },
        automargin: true, showline: false
      },
      legend: {
        orientation: "h", x: 0, y: 1.06, xanchor: "left", yanchor: "bottom",
        font: { size: 11, color: color("muted") }, bgcolor: "rgba(0,0,0,0)", borderwidth: 0
      },
      showlegend: true,
      bargap: 0.25,
      colorway: series("info", "accent", "good", "warn", "bad", "neutral")
    };
    // Shallow-merge the caller's overrides one level deep, so `xaxis: {title:...}` keeps the tokens.
    var out = {};
    var k;
    for (k in base) if (Object.prototype.hasOwnProperty.call(base, k)) out[k] = base[k];
    for (k in overrides || {}) {
      if (!Object.prototype.hasOwnProperty.call(overrides, k)) continue;
      var b = base[k], o = overrides[k];
      if (b && typeof b === "object" && !Array.isArray(b) && o && typeof o === "object" && !Array.isArray(o)) {
        var merged = {}, kk;
        for (kk in b) merged[kk] = b[kk];
        for (kk in o) merged[kk] = o[kk];
        out[k] = merged;
      } else {
        out[k] = o;
      }
    }
    return out;
  }

  var config = { responsive: true, displayModeBar: false, displaylogo: false, doubleClick: "reset+autosize" };

  /** Re-apply the theme-dependent chrome after a theme flip, without disturbing any data layout.
   *  Only the keys that read tokens are touched, so a chart's own height/axes settings survive. */
  function restyle() {
    if (!window.Plotly) return;
    var plots = document.querySelectorAll(".js-plotly-plot");
    Array.prototype.forEach.call(plots, function (el) {
      try {
        Plotly.relayout(el, {
          "font.color": color("muted"),
          "font.family": FONT,
          "hoverlabel.bgcolor": color("surface-2"),
          "hoverlabel.bordercolor": color("line"),
          "hoverlabel.font.color": color("ink"),
          "legend.font.color": color("muted"),
          "xaxis.gridcolor": color("line-soft"),
          "xaxis.linecolor": color("line"),
          "xaxis.tickcolor": color("line"),
          "xaxis.tickfont.color": color("faint"),
          "yaxis.gridcolor": color("line-soft"),
          "yaxis.linecolor": color("line"),
          "yaxis.tickcolor": color("line"),
          "yaxis.tickfont.color": color("faint")
        });
      } catch (e) { /* a panel mid-teardown is not an error worth surfacing */ }
    });
  }

  window.ChartTokens = {
    color: color, lightColor: lightColor, swatchStyle: swatchStyle, swatchHTML: swatchHTML,
    series: series, seq: seq, seqScale: seqScale, layout: layout, config: config,
    HEIGHTS: HEIGHTS, restyle: restyle, TOKENS: SEMANTIC
  };
})();
