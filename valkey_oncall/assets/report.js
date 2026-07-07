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

// Tab switching (Heatmap / Scorecard / Run Details).
(function () {
  var tabs = Array.prototype.slice.call(document.querySelectorAll(".tab"));
  if (!tabs.length) return;
  var panels = Array.prototype.slice.call(document.querySelectorAll(".tab-panel"));
  tabs.forEach(function (tab) {
    tab.addEventListener("click", function () {
      var target = "tab-" + tab.getAttribute("data-tab");
      tabs.forEach(function (t) {
        t.classList.remove("active");
      });
      panels.forEach(function (p) {
        p.classList.toggle("active", p.id === target);
      });
      tab.classList.add("active");
    });
  });
})();
