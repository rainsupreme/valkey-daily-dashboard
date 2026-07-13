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

// Start wide heatmaps (CI per-commit) scrolled fully right so the most
// recent runs are visible on load. Narrow tables are a no-op.
(function () {
  function scrollRight() {
    document.querySelectorAll(".heatmap-scroll.scroll-right").forEach(function (el) {
      el.scrollLeft = el.scrollWidth;
    });
  }
  scrollRight();
  // Re-apply once layout/fonts settle (rotated SHA headers affect width).
  window.addEventListener("load", scrollRight);
})();
