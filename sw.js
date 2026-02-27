self.addEventListener("install", (e) => {
  e.waitUntil(
    caches.open("bungalows-v1").then((cache) =>
      cache.addAll(["./", "./index.html", "./manifest.webmanifest", "./sw.js"])
    )
  );
});

self.addEventListener("fetch", (e) => {
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
