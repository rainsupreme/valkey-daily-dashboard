(function () {
  var body = document.getElementById("scorecard-body");
  if (!body) return;
  var rows = Array.prototype.slice.call(body.querySelectorAll("tr"));
  var classSel = document.getElementById("sc-class");
  var catSel = document.getElementById("sc-cat");

  // Populate category filter from the rows present.
  var cats = {};
  rows.forEach(function (r) {
    cats[r.getAttribute("data-cat")] = true;
  });
  Object.keys(cats)
    .sort()
    .forEach(function (c) {
      var o = document.createElement("option");
      o.value = c;
      o.textContent = c;
      catSel.appendChild(o);
    });

  function applyFilter() {
    var cv = classSel.value,
      catv = catSel.value;
    rows.forEach(function (r) {
      var ok =
        (!cv || r.getAttribute("data-class") === cv) &&
        (!catv || r.getAttribute("data-cat") === catv);
      r.style.display = ok ? "" : "none";
    });
  }
  classSel.addEventListener("change", applyFilter);
  catSel.addEventListener("change", applyFilter);

  var sortState = {};
  function sortBy(key) {
    var desc = (sortState[key] = !sortState[key]); // first click = descending
    rows
      .slice()
      .sort(function (a, b) {
        var av = parseFloat(a.getAttribute("data-" + key)) || 0;
        var bv = parseFloat(b.getAttribute("data-" + key)) || 0;
        return desc ? bv - av : av - bv;
      })
      .forEach(function (r) {
        body.appendChild(r);
      });
  }
  document
    .querySelectorAll("#scorecard-controls button[data-sort]")
    .forEach(function (btn) {
      btn.addEventListener("click", function () {
        sortBy(btn.getAttribute("data-sort"));
      });
    });
})();

// Tab switching (Heatmap / Scorecard / Run Details / Regressions).
// The active tab is encoded in the URL hash (e.g. #regressions) so it is
// deep-linkable and survives reload / back-forward navigation.
(function () {
  var tabs = Array.prototype.slice.call(document.querySelectorAll(".tab"));
  if (!tabs.length) return;
  var panels = Array.prototype.slice.call(document.querySelectorAll(".tab-panel"));

  function activate(name) {
    var target = "tab-" + name;
    if (!panels.some(function (p) { return p.id === target; })) return false;
    tabs.forEach(function (t) {
      t.classList.toggle("active", t.getAttribute("data-tab") === name);
    });
    panels.forEach(function (p) {
      p.classList.toggle("active", p.id === target);
    });
    // When the Heatmap tab becomes visible, (re)apply the scroll-right +
    // fade indicators — a hidden panel has no measurable width at load.
    if (name === "heatmap" && typeof initHeatmapScroll === "function") {
      requestAnimationFrame(initHeatmapScroll);
    }
    return true;
  }

  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      var name = tab.getAttribute("data-tab");
      activate(name);
      // replaceState avoids spamming browser history on every tab click.
      if (window.history && history.replaceState) {
        history.replaceState(null, "", "#" + name);
      } else {
        location.hash = name;
      }
    });
  });

  function fromHash() {
    return (location.hash || "").replace(/^#/, "");
  }
  // Honor a deep link on load; fall back to the default (heatmap).
  if (!activate(fromHash())) activate("heatmap");
  // React to back/forward or manual hash edits.
  window.addEventListener("hashchange", function () {
    activate(fromHash());
  });
})();

// Wide CI (per-commit) heatmaps: start scrolled fully right (newest visible)
// and show left/right fade indicators so it's obvious there's off-screen
// content. Defined at top level so tab activation can re-run it.
function updateHeatmapFade(el) {
  var wrap = el.parentNode;
  if (!wrap || !wrap.classList.contains("heatmap-scroll-wrap")) return;
  var maxScroll = el.scrollWidth - el.clientWidth;
  wrap.classList.toggle("show-left", el.scrollLeft > 2);
  wrap.classList.toggle("show-right", el.scrollLeft < maxScroll - 2);
}

function initHeatmapScroll() {
  document.querySelectorAll(".heatmap-scroll.scroll-right").forEach(function (el) {
    el.scrollLeft = el.scrollWidth; // newest runs on the right
    updateHeatmapFade(el);
    if (!el.__fadeWired) {
      el.addEventListener("scroll", function () {
        updateHeatmapFade(el);
      });
      el.__fadeWired = true;
    }
  });
}

initHeatmapScroll();
// Re-apply once layout/fonts settle (rotated SHA headers affect width).
window.addEventListener("load", initHeatmapScroll);
