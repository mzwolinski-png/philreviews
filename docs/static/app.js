/* PhilReviews — client-side filter / sort / paginate */
(function () {
  "use strict";

  let allReviews = [];
  let filtered = [];
  let state = { page: 1, perPage: 25, sortKey: "date", sortDir: "desc", expandedIdx: null };

  const esc = (s) => {
    if (!s) return "";
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  };

  /* ---------- DOM refs ---------- */
  const $ = (id) => document.getElementById(id);
  let els;

  /* ---------- Init ---------- */
  document.addEventListener("DOMContentLoaded", () => {
    allReviews = JSON.parse(document.getElementById("review-data").textContent);
    els = {
      globalSearch: $("global-search"),
      titleFilter: $("filter-title"),
      authorFilter: $("filter-author"),
      reviewerFilter: $("filter-reviewer"),
      journalFilter: $("filter-journal"),
      yearFrom: $("filter-year-from"),
      yearTo: $("filter-year-to"),
      accessFilter: $("filter-access"),
      clearBtn: $("clear-filters"),
      advancedToggle: $("advanced-toggle"),
      advancedPanel: $("advanced-panel"),
      resultCount: $("result-count"),
      perPageSelect: $("per-page"),
      tbody: $("review-tbody"),
      pagination: $("pagination"),
    };

    /* event listeners */
    els.advancedToggle.addEventListener("click", () => {
      const open = els.advancedPanel.classList.toggle("open");
      els.advancedToggle.textContent = open ? "Hide Advanced Search" : "Advanced Search";
    });

    els.clearBtn.addEventListener("click", clearFilters);
    els.perPageSelect.addEventListener("change", () => {
      state.perPage = parseInt(els.perPageSelect.value, 10);
      state.page = 1;
      render();
    });

    /* debounced text inputs */
    let timer;
    const debounced = () => { clearTimeout(timer); timer = setTimeout(() => { state.page = 1; update(); }, 200); };
    [els.globalSearch, els.titleFilter, els.authorFilter, els.reviewerFilter].forEach(
      (el) => el.addEventListener("input", debounced)
    );

    /* instant selects / number inputs */
    [els.journalFilter, els.yearFrom, els.yearTo, els.accessFilter].forEach(
      (el) => el.addEventListener("change", () => { state.page = 1; update(); })
    );

    /* sortable headers */
    document.querySelectorAll("th[data-sort]").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.sort;
        if (state.sortKey === key) {
          state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        } else {
          state.sortKey = key;
          state.sortDir = key === "date" ? "desc" : "asc";
        }
        state.page = 1;
        render();
      });
    });

    /* source card clicks — filter by journal */
    document.querySelectorAll(".source-card").forEach((btn) => {
      btn.addEventListener("click", () => {
        clearFilters();
        els.journalFilter.value = btn.dataset.journal;
        state.sortKey = "date";
        state.sortDir = "desc";
        state.page = 1;
        update();
        document.querySelector(".table-wrap").scrollIntoView({ behavior: "smooth" });
      });
    });

    /* browse recent reviews */
    const recentBtn = document.getElementById("browse-recent");
    if (recentBtn) {
      recentBtn.addEventListener("click", () => {
        clearFilters();
        state.sortKey = "date";
        state.sortDir = "desc";
        state.page = 1;
        update();
        document.querySelector(".table-wrap").scrollIntoView({ behavior: "smooth" });
      });
    }

    update();
  });

  /* ---------- Filtering ---------- */
  function applyFilters() {
    const g = els.globalSearch.value.toLowerCase().trim();
    const ft = els.titleFilter.value.toLowerCase().trim();
    const fa = els.authorFilter.value.toLowerCase().trim();
    const fr = els.reviewerFilter.value.toLowerCase().trim();
    const fj = els.journalFilter.value;
    const fy1 = els.yearFrom.value ? parseInt(els.yearFrom.value, 10) : null;
    const fy2 = els.yearTo.value ? parseInt(els.yearTo.value, 10) : null;
    const fac = els.accessFilter.value.toLowerCase();

    filtered = allReviews.filter((r) => {
      if (g) {
        const blob = (r.title + " " + r.author + " " + r.reviewer + " " + r.journal + " " + r.date).toLowerCase();
        if (!blob.includes(g)) return false;
      }
      if (ft && !r.title.toLowerCase().includes(ft)) return false;
      if (fa && !r.author.toLowerCase().includes(fa)) return false;
      if (fr && !r.reviewer.toLowerCase().includes(fr)) return false;
      if (fj && r.journal !== fj) return false;
      if (fac && r.access.toLowerCase() !== fac) return false;
      if (fy1 || fy2) {
        const y = parseInt((r.date || "").substring(0, 4), 10);
        if (isNaN(y)) return false;
        if (fy1 && y < fy1) return false;
        if (fy2 && y > fy2) return false;
      }
      return true;
    });
  }

  /* ---------- Sorting ---------- */
  function applySort() {
    const key = state.sortKey;
    const dir = state.sortDir === "asc" ? 1 : -1;
    filtered.sort((a, b) => {
      const va = (a[key] || "").toLowerCase();
      const vb = (b[key] || "").toLowerCase();
      return dir * va.localeCompare(vb);
    });
  }

  /* ---------- Render ---------- */
  function update() {
    applyFilters();
    render();
  }

  function render() {
    applySort();

    const total = allReviews.length;
    const count = filtered.length;
    els.resultCount.textContent = "Showing " + count.toLocaleString() + " of " + total.toLocaleString() + " reviews";

    /* sort indicators */
    document.querySelectorAll("th[data-sort]").forEach((th) => {
      th.classList.remove("sort-asc", "sort-desc");
      if (th.dataset.sort === state.sortKey) {
        th.classList.add(state.sortDir === "asc" ? "sort-asc" : "sort-desc");
      }
    });

    /* pagination math */
    const totalPages = Math.max(1, Math.ceil(count / state.perPage));
    if (state.page > totalPages) state.page = totalPages;
    const start = (state.page - 1) * state.perPage;
    const pageItems = filtered.slice(start, start + state.perPage);

    /* build rows */
    let html = "";
    pageItems.forEach((r, i) => {
      const idx = start + i;
      const expanded = state.expandedIdx === idx;
      html += '<tr class="review-row' + (expanded ? " expanded" : "") + '" data-idx="' + idx + '">';
      html += "<td>" + esc(r.title) + "</td>";
      html += "<td>" + esc(r.author) + "</td>";
      html += "<td>" + esc(r.reviewer) + "</td>";
      html += "<td>" + esc(r.journal) + "</td>";
      html += "<td>" + esc(r.date) + "</td>";
      html += "</tr>";
      if (expanded) {
        html += '<tr class="detail-row"><td colspan="5"><div class="detail-content">';
        if (r.summary) html += '<p class="summary">' + esc(r.summary) + "</p>";
        if (r.access) {
          const cls = r.access.toLowerCase() === "open" ? "badge-open" : "badge-restricted";
          html += '<span class="access-badge ' + cls + '">' + esc(r.access) + "</span> ";
        }
        if (r.link) html += '<a class="read-link" href="' + esc(r.link) + '" target="_blank" rel="noopener">Read Review &rarr;</a>';
        html += "</div></td></tr>";
      }
    });
    els.tbody.innerHTML = html;

    /* row click handlers */
    els.tbody.querySelectorAll(".review-row").forEach((tr) => {
      tr.addEventListener("click", () => {
        const idx = parseInt(tr.dataset.idx, 10);
        state.expandedIdx = state.expandedIdx === idx ? null : idx;
        render();
      });
    });

    /* GoatCounter click tracking on review links */
    els.tbody.querySelectorAll(".read-link").forEach((a) => {
      a.addEventListener("click", (e) => {
        if (typeof goatcounter !== "undefined" && goatcounter.count) {
          const label = a.closest(".detail-row").previousElementSibling.querySelector("td").textContent || "unknown";
          goatcounter.count({ path: "click-" + label, event: true });
        }
      });
    });

    /* pagination */
    renderPagination(totalPages);
  }

  function renderPagination(totalPages) {
    if (totalPages <= 1) { els.pagination.innerHTML = ""; return; }

    const pages = [];
    const p = state.page;
    pages.push(1);
    if (p > 3) pages.push("...");
    for (let i = Math.max(2, p - 1); i <= Math.min(totalPages - 1, p + 1); i++) pages.push(i);
    if (p < totalPages - 2) pages.push("...");
    if (totalPages > 1) pages.push(totalPages);

    let html = '<button class="page-btn" data-p="' + (p - 1) + '"' + (p === 1 ? " disabled" : "") + '>&laquo;</button>';
    pages.forEach((pg) => {
      if (pg === "...") {
        html += '<span class="page-ellipsis">&hellip;</span>';
      } else {
        html += '<button class="page-btn' + (pg === p ? " active" : "") + '" data-p="' + pg + '">' + pg + "</button>";
      }
    });
    html += '<button class="page-btn" data-p="' + (p + 1) + '"' + (p === totalPages ? " disabled" : "") + '>&raquo;</button>';

    els.pagination.innerHTML = html;
    els.pagination.querySelectorAll(".page-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const np = parseInt(btn.dataset.p, 10);
        if (np >= 1 && np <= totalPages) { state.page = np; render(); }
      });
    });
  }

  /* ---------- Clear ---------- */
  function clearFilters() {
    els.globalSearch.value = "";
    els.titleFilter.value = "";
    els.authorFilter.value = "";
    els.reviewerFilter.value = "";
    els.journalFilter.value = "";
    els.yearFrom.value = "";
    els.yearTo.value = "";
    els.accessFilter.value = "";
    state.page = 1;
    state.expandedIdx = null;
    update();
  }
})();
