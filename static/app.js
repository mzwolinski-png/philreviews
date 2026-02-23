/* PhilReviews — client-side filter / sort / paginate */
(function () {
  "use strict";

  let allReviews = [];
  let filtered = [];
  let allJournals = [];
  let selectedJournals = new Set();
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

    /* Build journal list from data */
    const journalSet = new Set();
    allReviews.forEach((r) => journalSet.add(r.journal));
    allJournals = Array.from(journalSet).sort((a, b) => a.localeCompare(b));
    selectedJournals = new Set(allJournals);

    els = {
      globalSearch: $("global-search"),
      titleFilter: $("filter-title"),
      authorFilter: $("filter-author"),
      reviewerFilter: $("filter-reviewer"),
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
      journalBtn: $("journal-select-btn"),
      journalDropdown: $("journal-dropdown"),
      journalSelectAll: $("journal-select-all"),
      journalSelectNone: $("journal-select-none"),
      filterIndicator: $("filter-indicator"),
    };

    /* event listeners */
    els.advancedToggle.addEventListener("click", () => {
      const open = els.advancedPanel.classList.toggle("open");
      els.advancedToggle.textContent = open ? "Hide Advanced Search" : "Advanced Search";
    });

    els.clearBtn.addEventListener("click", () => {
      clearFilters();
      syncToUrl();
    });
    els.perPageSelect.addEventListener("change", () => {
      state.perPage = parseInt(els.perPageSelect.value, 10);
      state.page = 1;
      render();
      syncToUrl();
    });

    /* debounced text inputs */
    let timer;
    const debounced = () => {
      clearTimeout(timer);
      timer = setTimeout(() => { state.page = 1; update(); syncToUrl(); }, 200);
    };
    [els.globalSearch, els.titleFilter, els.authorFilter, els.reviewerFilter].forEach(
      (el) => el.addEventListener("input", debounced)
    );

    /* instant selects / number inputs */
    [els.yearFrom, els.yearTo, els.accessFilter].forEach(
      (el) => el.addEventListener("change", () => { state.page = 1; update(); syncToUrl(); })
    );

    /* journal multi-select dropdown */
    els.journalBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      els.journalDropdown.classList.toggle("open");
    });
    document.addEventListener("click", (e) => {
      if (!els.journalDropdown.contains(e.target) && e.target !== els.journalBtn) {
        els.journalDropdown.classList.remove("open");
      }
    });
    els.journalSelectAll.addEventListener("click", () => {
      selectedJournals = new Set(allJournals);
      syncCheckboxes();
      updateJournalBtnLabel();
      state.page = 1;
      update();
      syncToUrl();
    });
    els.journalSelectNone.addEventListener("click", () => {
      selectedJournals.clear();
      syncCheckboxes();
      updateJournalBtnLabel();
      state.page = 1;
      update();
      syncToUrl();
    });
    els.journalDropdown.querySelectorAll(".journal-option input").forEach((cb) => {
      cb.addEventListener("change", () => {
        if (cb.checked) {
          selectedJournals.add(cb.value);
        } else {
          selectedJournals.delete(cb.value);
        }
        updateJournalBtnLabel();
        state.page = 1;
        update();
        syncToUrl();
      });
    });

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
        syncToUrl();
      });
    });

    /* sources toggle — expand/collapse */
    const sourcesToggle = document.getElementById("sources-toggle");
    const sourcesGrid = document.getElementById("sources-grid");
    if (sourcesToggle && sourcesGrid) {
      sourcesToggle.addEventListener("click", () => {
        const icon = sourcesToggle.querySelector(".toggle-icon");
        if (sourcesGrid.style.display === "none") {
          sourcesGrid.style.display = "flex";
          if (icon) icon.classList.add("open");
        } else {
          sourcesGrid.style.display = "none";
          if (icon) icon.classList.remove("open");
        }
      });
    }

    /* source card clicks — filter by journal */
    document.querySelectorAll(".source-card").forEach((btn) => {
      btn.addEventListener("click", () => {
        clearFilters();
        selectedJournals = new Set([btn.dataset.journal]);
        syncCheckboxes();
        updateJournalBtnLabel();
        state.sortKey = "date";
        state.sortDir = "desc";
        state.page = 1;
        update();
        syncToUrl();
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
        syncToUrl();
        document.querySelector(".table-wrap").scrollIntoView({ behavior: "smooth" });
      });
    }

    /* popstate for browser back/forward */
    window.addEventListener("popstate", () => {
      readFromUrl();
      update();
    });

    /* Read URL state, then render */
    readFromUrl();
    update();
  });

  /* ---------- Journal multi-select helpers ---------- */
  function syncCheckboxes() {
    els.journalDropdown.querySelectorAll(".journal-option input").forEach((cb) => {
      cb.checked = selectedJournals.has(cb.value);
    });
  }

  function updateJournalBtnLabel() {
    if (selectedJournals.size === 0) {
      els.journalBtn.textContent = "No journals selected";
    } else if (selectedJournals.size === allJournals.length) {
      els.journalBtn.textContent = "All Journals";
    } else if (selectedJournals.size === 1) {
      els.journalBtn.textContent = Array.from(selectedJournals)[0];
    } else if (selectedJournals.size <= 2) {
      els.journalBtn.textContent = Array.from(selectedJournals).join(", ");
    } else {
      const first = Array.from(selectedJournals)[0];
      els.journalBtn.textContent = first + " +" + (selectedJournals.size - 1) + " more";
    }
  }

  /* ---------- URL persistence ---------- */
  function syncToUrl() {
    const params = new URLSearchParams();

    const g = els.globalSearch.value.trim();
    if (g) params.set("q", g);

    const ft = els.titleFilter.value.trim();
    if (ft) params.set("title", ft);

    const fa = els.authorFilter.value.trim();
    if (fa) params.set("author", fa);

    const fr = els.reviewerFilter.value.trim();
    if (fr) params.set("reviewer", fr);

    if (selectedJournals.size > 0 && selectedJournals.size < allJournals.length) {
      params.set("journals", Array.from(selectedJournals).join(","));
    }

    const y1 = els.yearFrom.value;
    const y2 = els.yearTo.value;
    if (y1 || y2) {
      params.set("year", (y1 || "") + "-" + (y2 || ""));
    }

    const ac = els.accessFilter.value;
    if (ac) params.set("access", ac);

    const defaultSort = state.sortKey === "date" && state.sortDir === "desc";
    if (!defaultSort) params.set("sort", state.sortKey + "-" + state.sortDir);

    if (state.page > 1) params.set("page", state.page);

    const hash = params.toString();
    const url = window.location.pathname + (hash ? "#" + hash : "");
    history.replaceState(null, "", url);
  }

  function readFromUrl() {
    const hash = window.location.hash.slice(1);
    if (!hash) return;

    const params = new URLSearchParams(hash);

    if (params.has("q")) els.globalSearch.value = params.get("q");
    if (params.has("title")) els.titleFilter.value = params.get("title");
    if (params.has("author")) els.authorFilter.value = params.get("author");
    if (params.has("reviewer")) els.reviewerFilter.value = params.get("reviewer");

    if (params.has("journals")) {
      const names = params.get("journals").split(",");
      selectedJournals = new Set(names.filter((n) => allJournals.includes(n)));
      syncCheckboxes();
      updateJournalBtnLabel();
    }

    if (params.has("year")) {
      const parts = params.get("year").split("-");
      if (parts[0]) els.yearFrom.value = parts[0];
      if (parts[1]) els.yearTo.value = parts[1];
    }

    if (params.has("access")) els.accessFilter.value = params.get("access");

    if (params.has("sort")) {
      const sp = params.get("sort").split("-");
      if (sp.length === 2) {
        state.sortKey = sp[0];
        state.sortDir = sp[1];
      }
    }

    if (params.has("page")) {
      state.page = parseInt(params.get("page"), 10) || 1;
    }
  }

  /* ---------- Filtering ---------- */
  function applyFilters() {
    const g = els.globalSearch.value.toLowerCase().trim();
    const ft = els.titleFilter.value.toLowerCase().trim();
    const fa = els.authorFilter.value.toLowerCase().trim();
    const fr = els.reviewerFilter.value.toLowerCase().trim();
    const fy1 = els.yearFrom.value ? parseInt(els.yearFrom.value, 10) : null;
    const fy2 = els.yearTo.value ? parseInt(els.yearTo.value, 10) : null;
    const fac = els.accessFilter.value.toLowerCase();
    const filterByJournal = selectedJournals.size < allJournals.length;

    filtered = allReviews.filter((r) => {
      if (g) {
        const blob = (r.title + " " + r.author + " " + r.reviewer + " " + r.journal + " " + r.date).toLowerCase();
        if (!blob.includes(g)) return false;
      }
      if (ft && !r.title.toLowerCase().includes(ft)) return false;
      if (fa && !r.author.toLowerCase().includes(fa)) return false;
      if (fr && !r.reviewer.toLowerCase().includes(fr)) return false;
      if (filterByJournal && !selectedJournals.has(r.journal)) return false;
      if (fac && r.access.toLowerCase() !== fac) return false;
      if (fy1 || fy2) {
        const y = parseInt((r.date || "").substring(0, 4), 10);
        if (isNaN(y)) return false;
        if (fy1 && y < fy1) return false;
        if (fy2 && y > fy2) return false;
      }
      return true;
    });

    updateFilterIndicator();
  }

  function updateFilterIndicator() {
    const active = [];
    if (els.globalSearch.value.trim()) active.push("search");
    if (els.titleFilter.value.trim()) active.push("title");
    if (els.authorFilter.value.trim()) active.push("author");
    if (els.reviewerFilter.value.trim()) active.push("reviewer");
    if (selectedJournals.size < allJournals.length) active.push("journals");
    if (els.yearFrom.value || els.yearTo.value) active.push("year");
    if (els.accessFilter.value) active.push("access");

    if (active.length > 0) {
      els.filterIndicator.textContent = active.length + " filter" + (active.length > 1 ? "s" : "") + " active";
    } else {
      els.filterIndicator.textContent = "";
    }
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
    els.resultCount.textContent = "Showing " + count.toLocaleString() + " of " + total.toLocaleString() + " entries";

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
        if (r.type === "symposium") {
          html += '<span class="access-badge badge-symposium">Symposium</span> ';
        }
        if (r.access) {
          const cls = r.access.toLowerCase() === "open" ? "badge-open" : "badge-restricted";
          html += '<span class="access-badge ' + cls + '">' + esc(r.access) + "</span> ";
        }
        if (r.link) html += '<a class="read-link" href="' + esc(r.link) + '" target="_blank" rel="noopener">' + (r.type === "symposium" ? "Read Contribution" : "Read Review") + ' &rarr;</a>';
        if (r.type === "symposium" && r.symposium_group) {
          const peers = allReviews.filter(function(p) {
            return p.symposium_group === r.symposium_group && p !== r;
          });
          if (peers.length > 0) {
            html += '<div class="symposium-peers"><strong>Other contributions:</strong><ul>';
            peers.forEach(function(p) {
              html += "<li>" + esc(p.reviewer);
              if (p.title && p.title !== r.title) html += ' — "' + esc(p.title) + '"';
              if (p.link) html += ' <a href="' + esc(p.link) + '" target="_blank" rel="noopener">Read &rarr;</a>';
              html += "</li>";
            });
            html += "</ul></div>";
          }
        }
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
        if (np >= 1 && np <= totalPages) {
          state.page = np;
          render();
          syncToUrl();
        }
      });
    });
  }

  /* ---------- Clear ---------- */
  function clearFilters() {
    els.globalSearch.value = "";
    els.titleFilter.value = "";
    els.authorFilter.value = "";
    els.reviewerFilter.value = "";
    els.yearFrom.value = "";
    els.yearTo.value = "";
    els.accessFilter.value = "";
    selectedJournals = new Set(allJournals);
    syncCheckboxes();
    updateJournalBtnLabel();
    state.page = 1;
    state.expandedIdx = null;
    update();
  }
})();
