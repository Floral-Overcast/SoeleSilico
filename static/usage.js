// Presentational plan-usage components (props in, pixels out). No data fetching here.
// Two variants: renderPopover (compact) and renderSettings (full). Both return HTML
// strings; the caller sets innerHTML and re-renders on a 60s interval so countdowns tick.
window.HubUsage = (function () {
  const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g,
    c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  // clamp 0-100; null/NaN -> null (a missing window)
  function clampPct(p) {
    if (p == null || isNaN(p)) return null;
    return Math.max(0, Math.min(100, Number(p)));
  }
  const pctText = p => p == null ? "—" : Math.round(p) + "%";

  // reset text from a unix-seconds timestamp, local time
  function fmtResets(resets_at, now) {
    if (!resets_at) return "";
    const ms = resets_at * 1000 - (now || Date.now());
    if (ms < 24 * 3600 * 1000) {
      const mins = Math.max(0, Math.round(ms / 60000));
      const h = Math.floor(mins / 60), m = mins % 60;
      return "Resets in " + (h ? h + " hr " : "") + m + " min";
    }
    const d = new Date(resets_at * 1000);
    const wd = d.toLocaleDateString(undefined, { weekday: "short" });
    let hr = d.getHours(); const ap = hr >= 12 ? "PM" : "AM"; hr = hr % 12 || 12;
    return `Resets ${wd} ${hr}:${String(d.getMinutes()).padStart(2, "0")} ${ap}`;
  }

  // staleness stamp from captured_at (unix seconds); "as of —" when no snapshot exists
  function fmtStamp(captured_at) {
    if (!captured_at) return "as of —";
    const d = new Date(captured_at * 1000);
    return "as of " + d.getHours() + ":" + String(d.getMinutes()).padStart(2, "0");
  }

  // one bar; missing (p==null) -> empty track. role=progressbar per spec.
  // oi = optional second fill (the Fable/Opus weekly) drawn purple on the same track;
  // fills are absolutely positioned, wider one first so the narrower paints on top.
  function bar(p, cls, oi) {
    const w = p == null ? 0 : p;
    let fills = `<i style="width:${w}%"></i>`;
    if (oi != null) {
      const b = `<i class="hu-fill-oi" style="width:${oi}%"></i>`;
      fills = oi > w ? b + fills : fills + b;
    }
    return `<div class="hu-bar ${cls}" role="progressbar" aria-valuemin="0" ` +
      `aria-valuemax="100" aria-valuenow="${Math.round(w)}">${fills}</div>`;
  }

  // The per-model weekly bucket (seven_day_oi / seven_day_fable / ...) is not its own
  // row: it rides the all-models weekly bar as the purple fill.
  const isOi = id => /^seven_day_./.test(String(id || ""));

  // Variant A. opts: { context_window?, expand?:bool, now? }
  function renderPopover(props, opts) {
    opts = opts || {};
    const now = opts.now || Date.now();
    const hasContext = !!opts.context_window || !!(props && props.context_window);
    const cw = opts.context_window || (props && props.context_window) || { used_tokens: 0, used_percentage: 0 };
    const cwPct = clampPct(cw.used_percentage) || 0;
    const context = hasContext ? `<div class="hu-ctxrow"><span>Context window</span>` +
      `<span>${(cw.used_tokens || 0).toLocaleString()}</span></div>` + bar(cwPct, "hu-ctxbar") : "";
    // extra_usage (the pay-per-use overflow meter) is noise here - hide it
    const all = ((props && props.limits) || [])
      .filter(l => !/extra/i.test(String(l.id || "")) && !/extra/i.test(String(l.label || "")));
    const oiLim = all.find(l => isOi(l.id));
    const oiPct = oiLim ? clampPct(oiLim.used_percentage) : null;
    const limits = all.filter(l => !isOi(l.id));
    const blocks = limits.map(l => {
      const p = clampPct(l.used_percentage);
      const oi = l.id === "seven_day" ? oiPct : null;
      const oiTag = oi == null ? "" :
        `<span class="hu-pct hu-oi" title="Fable/Opus weekly">${pctText(oi)}</span>`;
      return `<div class="hu-lim"><div class="hu-top">` +
        `<span class="hu-name">${esc(l.label)}</span>` +
        `<span class="hu-right"><span class="hu-reset">${esc(fmtResets(l.resets_at, now))}</span>` +
        `<span class="hu-pct">${pctText(p)}</span>` + oiTag + `</span></div>` +
        bar(p, "", oi) + `</div>`;
    }).join("");
    const expand = opts.expand === false ? "" :
      `<button type="button" data-hu-expand title="Plan usage limits">→</button>`;
    return `<div class="hu hu-pop">` +
      context +
      `<div class="hu-hdr"><span>Plan usage limits · ${esc(props && props.plan)}</span>` +
      `<span class="hu-hdr-right"><span class="hu-stamp">${fmtStamp(props && props.captured_at)}</span>${expand}</span></div>` +
      blocks + `</div>`;
  }

  // Variant B. Fixed rows, relabeled from the shared data by id. The per-model weekly
  // is not a row of its own - it overlays the All models bar in purple.
  const SETTINGS_ROWS = [
    { id: "five_hour", label: "Current session", group: "session" },
    { id: "seven_day", label: "All models", group: "weekly" },
  ];
  function renderSettings(props, opts) {
    opts = opts || {};
    const now = opts.now || Date.now();
    const byId = {};
    let oiPct = null;
    for (const l of (props && props.limits) || []) {
      byId[l.id] = l;
      if (isOi(l.id)) oiPct = clampPct(l.used_percentage);
    }
    const rowHtml = def => {
      const l = byId[def.id];
      const p = l ? clampPct(l.used_percentage) : null;
      const oi = def.id === "seven_day" ? oiPct : null;
      const oiTag = oi == null ? "" :
        `<div class="hu-oi" title="Fable/Opus weekly">${Math.round(oi)}% Fable/Opus</div>`;
      return `<div class="hu-row"><div><div class="hu-label">${esc(def.label)}</div>` +
        `<div class="hu-reset">${l ? esc(fmtResets(l.resets_at, now)) : ""}</div></div>` +
        bar(p, "", oi) +
        `<div class="hu-pct">${p == null ? "—" : Math.round(p) + "% used"}${oiTag}</div></div>`;
    };
    const session = SETTINGS_ROWS.filter(r => r.group === "session");
    const weekly = SETTINGS_ROWS.filter(r => r.group === "weekly");
    return `<section class="hu hu-set">` +
      `<h2 class="hu-h">Plan usage limits<span class="hu-plan">${esc(props && props.plan)}</span></h2>` +
      `<div class="hu-stamp hu-set-stamp">${fmtStamp(props && props.captured_at)}</div>` +
      `<div class="hu-session">${session.map(rowHtml).join("")}</div>` +
      `<div class="hu-sub">Weekly limits</div>` +
      `<a class="hu-learn" href="https://support.anthropic.com/en/articles/11145838-usage-limit-best-practices" target="_blank" rel="noopener">Learn more about usage limits</a>` +
      `<div class="hu-weekly">${weekly.map(rowHtml).join("")}</div>` +
      `</section>`;
  }

  return { renderPopover, renderSettings, fmtResets, fmtStamp, clampPct, pctText };
})();
