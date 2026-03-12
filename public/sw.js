self.addEventListener("push", function(event) {
  const data = event.data ? event.data.json() : {};

  self.registration.showNotification(data.title || "Steam Deal Alert", {
    body: data.body || "",
    icon: "/icon.png",
    data: { url: data.url || "/" },
  });
});

self.addEventListener("notificationclick", function(event) {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || "/";
  event.waitUntil(clients.openWindow(targetUrl));
});

