document.addEventListener("htmx:afterRequest", (event) => {
  document.body.classList.toggle("stale", !event.detail.successful);
});
