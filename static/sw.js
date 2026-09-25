// Service worker: PWA installability + web push. It deliberately does NOT cache anything -
// hub deploys via `git push prod` and expects a plain reload to pick up new code, so a
// caching SW would strand stale assets. Pass every request straight through to the network.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));
self.addEventListener("fetch", () => {});  // no respondWith = browser handles it normally

// Push (daemon /api/push/* + /api/notify). The daemon always sends on turn end; the
// visibility check here is the "am I already looking at it" gate - a notification only
// shows when the app is closed or backgrounded.
self.addEventListener("push", (e) => {
  let d = {};
  try { d = e.data ? e.data.json() : {}; } catch (err) {}
  e.waitUntil((async () => {
    const wins = await clients.matchAll({ type: "window", includeUncontrolled: true });
    if (wins.some((w) => w.visibilityState === "visible")) return;
    await self.registration.showNotification(d.title || "hub", {
      body: d.body || "",
      tag: d.sid || "hub",          // same-session pings collapse into one
      icon: "/icon-192.png",
      data: { sid: d.sid || "" },
    });
  })());
});

// Tap -> that convo. App already open: focus it and post the sid (the page listens and
// opens the chat). App closed: open "/#sid=<sid>" - the page picks the hash up after its
// first session-list load.
self.addEventListener("notificationclick", (e) => {
  e.notification.close();
  const sid = (e.notification.data || {}).sid || "";
  e.waitUntil((async () => {
    const wins = await clients.matchAll({ type: "window", includeUncontrolled: true });
    if (wins.length) {
      await wins[0].focus();
      if (sid) wins[0].postMessage({ type: "open-session", sid });
      return;
    }
    return clients.openWindow(sid ? "/#sid=" + sid : "/");
  })());
});
