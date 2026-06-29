/* PhilReviews — async API client with server-side search */
(function () {
  "use strict";

  let allJournals = [];
  let selectedJournals = new Set();
  let allSubfields = [];
  let selectedSubfields = new Set();
  let totalReviews = 0;
  let state = { page: 1, perPage: 25, sortKey: "date", sortDir: "desc", expandedId: null };
  let currentController = null;

  const SUBFIELD_NAMES = {
    "ethics": "Ethics & Moral Philosophy",
    "applied-ethics": "Applied & Professional Ethics",
    "political": "Political & Social Philosophy",
    "legal": "Philosophy of Law",
    "epistemology": "Epistemology & Philosophy of Mind",
    "metaphysics": "Metaphysics & Logic",
    "science": "Philosophy of Science",
    "aesthetics": "Aesthetics & Philosophy of Art",
    "religion": "Philosophy of Religion & Theology",
    "history": "History of Philosophy",
    "ancient": "Ancient & Medieval Philosophy",
    "modern": "Early Modern Philosophy",
    "continental": "Continental & Phenomenological",
    "feminist": "Feminist Philosophy",
    "non-western": "Non-Western & Comparative",
  };

  const esc = (s) => {
    if (!s) return "";
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  };
  const stripThe = (s) => s && (s.startsWith("The ") || s.startsWith("the ")) ? s.slice(4) : s;
  const nameLink = (type, value) => {
    if (!value) return "";
    const display = type === "journal" ? stripThe(value) : value;
    return '<a class="name-link" tabindex="0" role="link" data-type="' + type + '" data-value="' + esc(value) + '">' + esc(display) + "</a>";
  };

  const $ = (id) => document.getElementById(id);
  let els;

  /* ---------- Init ---------- */
  document.addEventListener("DOMContentLoaded", async () => {
    els = {
      globalSearch: $("global-search"),
      titleFilter: $("filter-title"),
      authorFilter: $("filter-author"),
      reviewerFilter: $("filter-reviewer"),
      yearFrom: $("filter-year-from"),
      yearTo: $("filter-year-to"),
      accessFilter: $("filter-access"),
      typeReview: $("filter-type-review"),
      typeSymposium: $("filter-type-symposium"),
      clearBtn: $("clear-filters"),
      advancedToggle: $("advanced-toggle"),
      advancedPanel: $("advanced-panel"),
      resultCount: $("result-count"),
      perPageSelect: $("per-page"),
      tbody: $("review-tbody"),
      pagination: $("pagination"),
      loading: $("loading"),
      resultsMessage: $("results-message"),
      journalBtn: $("journal-select-btn"),
      journalDropdown: $("journal-dropdown"),
      journalList: $("journal-list"),
      journalFilter: $("journal-filter"),
      journalSelectAll: $("journal-select-all"),
      journalSelectNone: $("journal-select-none"),
      subfieldBtn: $("subfield-select-btn"),
      subfieldDropdown: $("subfield-dropdown"),
      subfieldList: $("subfield-list"),
      subfieldSelectAll: $("subfield-select-all"),
      subfieldSelectNone: $("subfield-select-none"),
      filterIndicator: $("filter-indicator"),
      sourcesGrid: $("sources-grid"),
      sourceTotal: $("source-total"),
      shareBtn: $("share-search"),
    };

    await fetchMetadata();

    els.advancedToggle.addEventListener("click", () => {
      const open = els.advancedPanel.classList.toggle("open");
      els.advancedToggle.textContent = open ? "Hide Advanced Search" : "Advanced Search";
    });

    els.clearBtn.addEventListener("click", () => {
      clearFilters();
      syncToUrl();
      fetchAndRender();
    });
    els.shareBtn.addEventListener("click", () => {
      const url = window.location.href;
      navigator.clipboard.writeText(url).then(() => {
        els.shareBtn.textContent = "Link Copied!";
        els.shareBtn.classList.add("copied");
        setTimeout(() => {
          els.shareBtn.textContent = "Share This Search";
          els.shareBtn.classList.remove("copied");
        }, 2000);
      });
    });
    els.perPageSelect.addEventListener("change", () => {
      state.perPage = parseInt(els.perPageSelect.value, 10);
      state.page = 1;
      syncToUrl();
      fetchAndRender();
    });

    let timer;
    const debounced = () => {
      clearTimeout(timer);
      timer = setTimeout(() => { state.page = 1; syncToUrl(); fetchAndRender(); }, 300);
    };
    [els.globalSearch, els.titleFilter, els.authorFilter, els.reviewerFilter].forEach(
      (el) => el.addEventListener("input", debounced)
    );

    [els.yearFrom, els.yearTo, els.accessFilter].forEach(
      (el) => el.addEventListener("change", () => { state.page = 1; syncToUrl(); fetchAndRender(); })
    );
    [els.typeReview, els.typeSymposium].forEach(
      (el) => el.addEventListener("change", () => { state.page = 1; syncToUrl(); fetchAndRender(); })
    );

    els.subfieldBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      const open = els.subfieldDropdown.classList.toggle("open");
      els.subfieldBtn.setAttribute("aria-expanded", open ? "true" : "false");
    });
    els.subfieldSelectAll.addEventListener("click", () => {
      selectedSubfields = new Set(allSubfields);
      syncSubfieldCheckboxes();
      updateSubfieldBtnLabel();
      state.page = 1;
      syncToUrl();
      fetchAndRender();
    });
    els.subfieldSelectNone.addEventListener("click", () => {
      selectedSubfields.clear();
      syncSubfieldCheckboxes();
      updateSubfieldBtnLabel();
      state.page = 1;
      syncToUrl();
      fetchAndRender();
    });

    els.journalBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      const open = els.journalDropdown.classList.toggle("open");
      els.journalBtn.setAttribute("aria-expanded", open ? "true" : "false");
      if (open && els.journalFilter) els.journalFilter.focus();
    });
    document.addEventListener("click", (e) => {
      if (!els.journalDropdown.contains(e.target) && e.target !== els.journalBtn
          && !els.subfieldDropdown.contains(e.target) && e.target !== els.subfieldBtn) {
        els.journalDropdown.classList.remove("open");
        els.subfieldDropdown.classList.remove("open");
        els.journalBtn.setAttribute("aria-expanded", "false");
        els.subfieldBtn.setAttribute("aria-expanded", "false");
      }
    });
    // Typeahead filter inside the journal dropdown (live-filters the checkboxes)
    if (els.journalFilter) {
      els.journalFilter.addEventListener("input", () => {
        const q = els.journalFilter.value.trim().toLowerCase();
        els.journalList.querySelectorAll(".journal-option").forEach((label) => {
          label.style.display = label.textContent.toLowerCase().includes(q) ? "" : "none";
        });
      });
      els.journalFilter.addEventListener("click", (e) => e.stopPropagation());
    }
    els.journalSelectAll.addEventListener("click", () => {
      selectedJournals = new Set(allJournals);
      syncCheckboxes();
      updateJournalBtnLabel();
      state.page = 1;
      syncToUrl();
      fetchAndRender();
    });
    els.journalSelectNone.addEventListener("click", () => {
      selectedJournals.clear();
      syncCheckboxes();
      updateJournalBtnLabel();
      state.page = 1;
      syncToUrl();
      fetchAndRender();
    });

    document.querySelectorAll("th[data-sort]").forEach((th) => {
      const sortBy = () => {
        const key = th.dataset.sort;
        if (state.sortKey === key) {
          state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        } else {
          state.sortKey = key;
          state.sortDir = key === "date" ? "desc" : "asc";
        }
        state.page = 1;
        syncToUrl();
        fetchAndRender();
      };
      th.addEventListener("click", sortBy);
      th.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); sortBy(); }
      });
    });

    const sourcesToggle = $("sources-toggle");
    if (sourcesToggle) {
      sourcesToggle.addEventListener("click", () => {
        const icon = sourcesToggle.querySelector(".toggle-icon");
        if (els.sourcesGrid.style.display === "none") {
          els.sourcesGrid.style.display = "flex";
          if (icon) icon.classList.add("open");
        } else {
          els.sourcesGrid.style.display = "none";
          if (icon) icon.classList.remove("open");
        }
      });
    }

    const recentBtn = $("browse-recent");
    if (recentBtn) {
      recentBtn.addEventListener("click", () => {
        clearFilters();
        state.sortKey = "date";
        state.sortDir = "desc";
        state.page = 1;
        syncToUrl();
        fetchAndRender();
        document.querySelector(".table-wrap").scrollIntoView({ behavior: "smooth" });
      });
    }

    window.addEventListener("popstate", () => {
      readFromUrl();
      fetchAndRender();
    });

    readFromUrl();
    fetchAndRender();
  });

  /* ---------- API calls ---------- */
  async function fetchMetadata() {
    try {
      const resp = await fetch("/api/metadata");
      const data = await resp.json();
      totalReviews = data.total;

      // Populate journal dropdown
      const sortKey = s => s.replace(/^The /i, '');
      const sorted = data.journals.slice().sort((a, b) => sortKey(a.name).localeCompare(sortKey(b.name)));
      allJournals = sorted.map(j => j.name);
      selectedJournals = new Set(allJournals);

      let journalHtml = "";
      sorted.forEach((j) => {
        journalHtml += '<label class="journal-option"><input type="checkbox" value="' +
          esc(j.name) + '" checked> ' + esc(stripThe(j.name)) + '</label>';
      });
      els.journalList.innerHTML = journalHtml;

      let checkboxTimer;
      const debouncedCheckbox = () => {
        clearTimeout(checkboxTimer);
        checkboxTimer = setTimeout(() => { state.page = 1; syncToUrl(); fetchAndRender(); }, 200);
      };

      els.journalList.querySelectorAll("input").forEach((cb) => {
        cb.addEventListener("change", () => {
          if (cb.checked) selectedJournals.add(cb.value);
          else selectedJournals.delete(cb.value);
          updateJournalBtnLabel();
          debouncedCheckbox();
        });
      });

      // Populate subfield dropdown
      const subfieldsSorted = Object.keys(SUBFIELD_NAMES).sort(
        (a, b) => SUBFIELD_NAMES[a].localeCompare(SUBFIELD_NAMES[b])
      );
      allSubfields = subfieldsSorted;
      selectedSubfields = new Set(allSubfields);

      let subfieldHtml = "";
      subfieldsSorted.forEach((code) => {
        subfieldHtml += '<label class="journal-option"><input type="checkbox" value="' +
          code + '" checked> ' + esc(SUBFIELD_NAMES[code]) + '</label>';
      });
      els.subfieldList.innerHTML = subfieldHtml;

      els.subfieldList.querySelectorAll("input").forEach((cb) => {
        cb.addEventListener("change", () => {
          if (cb.checked) selectedSubfields.add(cb.value);
          else selectedSubfields.delete(cb.value);
          updateSubfieldBtnLabel();
          debouncedCheckbox();
        });
      });

      // Populate sources grid
      let sourcesHtml = "";
      data.journals.forEach((j) => {
        sourcesHtml += '<button class="source-card" data-journal="' + esc(j.name) + '">' +
          '<span class="source-name">' + esc(stripThe(j.name)) + '</span>' +
          '<span class="source-count">' + j.count.toLocaleString() + '</span></button>';
      });
      els.sourcesGrid.innerHTML = sourcesHtml;
      els.sourceTotal.textContent = "(" + data.journals.length + " journals)";

      els.sourcesGrid.querySelectorAll(".source-card").forEach((btn) => {
        btn.addEventListener("click", () => {
          clearFilters();
          selectedJournals = new Set([btn.dataset.journal]);
          syncCheckboxes();
          updateJournalBtnLabel();
          state.sortKey = "date";
          state.sortDir = "desc";
          state.page = 1;
          syncToUrl();
          fetchAndRender();
          document.querySelector(".table-wrap").scrollIntoView({ behavior: "smooth" });
        });
      });
    } catch (e) {
      console.error("Failed to fetch metadata:", e);
    }
  }

  async function fetchAndRender() {
    if (currentController) currentController.abort();
    currentController = new AbortController();

    els.loading.style.display = "block";
    updateFilterIndicator();

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
    if (selectedSubfields.size > 0 && selectedSubfields.size < allSubfields.length) {
      params.set("subfield", Array.from(selectedSubfields).join(","));
    }

    const y1 = els.yearFrom.value;
    if (y1) params.set("year_from", y1);
    const y2 = els.yearTo.value;
    if (y2) params.set("year_to", y2);

    const ac = els.accessFilter.value;
    if (ac) params.set("access", ac);
    const showReviews = els.typeReview.checked;
    const showSymposia = els.typeSymposium.checked;
    if (showReviews && !showSymposia) params.set("type", "review");
    else if (showSymposia && !showReviews) params.set("type", "symposium");

    params.set("sort", state.sortKey);
    params.set("sort_dir", state.sortDir);
    params.set("page", state.page);
    params.set("per_page", state.perPage);

    try {
      const resp = await fetch("/api/reviews?" + params.toString(), {
        signal: currentController.signal,
      });
      const data = await resp.json();
      els.loading.style.display = "none";
      renderResults(data);
    } catch (e) {
      if (e.name === "AbortError") return;
      console.error("Failed to fetch reviews:", e);
      els.loading.style.display = "none";
      els.tbody.innerHTML = "";
      if (els.resultsMessage) {
        els.resultsMessage.textContent = "Something went wrong loading reviews. Please try again.";
        els.resultsMessage.hidden = false;
      }
    }
  }

  /* ---------- Journal multi-select helpers ---------- */
  function syncCheckboxes() {
    els.journalList.querySelectorAll("input").forEach((cb) => {
      cb.checked = selectedJournals.has(cb.value);
    });
  }

  function updateJournalBtnLabel() {
    if (selectedJournals.size === 0) {
      els.journalBtn.textContent = "No journals selected";
    } else if (selectedJournals.size === allJournals.length) {
      els.journalBtn.textContent = "All Journals";
    } else if (selectedJournals.size === 1) {
      els.journalBtn.textContent = stripThe(Array.from(selectedJournals)[0]);
    } else if (selectedJournals.size <= 2) {
      els.journalBtn.textContent = Array.from(selectedJournals).map(stripThe).join(", ");
    } else {
      const first = stripThe(Array.from(selectedJournals)[0]);
      els.journalBtn.textContent = first + " +" + (selectedJournals.size - 1) + " more";
    }
  }

  /* ---------- Subfield multi-select helpers ---------- */
  function syncSubfieldCheckboxes() {
    els.subfieldList.querySelectorAll("input").forEach((cb) => {
      cb.checked = selectedSubfields.has(cb.value);
    });
  }

  function updateSubfieldBtnLabel() {
    if (selectedSubfields.size === 0) {
      els.subfieldBtn.textContent = "No subfields selected";
    } else if (selectedSubfields.size === allSubfields.length) {
      els.subfieldBtn.textContent = "All Subfields";
    } else if (selectedSubfields.size === 1) {
      const code = Array.from(selectedSubfields)[0];
      els.subfieldBtn.textContent = SUBFIELD_NAMES[code] || code;
    } else if (selectedSubfields.size === 2) {
      els.subfieldBtn.textContent = Array.from(selectedSubfields).map(c => SUBFIELD_NAMES[c] || c).join(", ");
    } else {
      const first = Array.from(selectedSubfields)[0];
      els.subfieldBtn.textContent = (SUBFIELD_NAMES[first] || first) + " +" + (selectedSubfields.size - 1) + " more";
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

    if (selectedSubfields.size > 0 && selectedSubfields.size < allSubfields.length) {
      params.set("subfield", Array.from(selectedSubfields).join(","));
    }
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
    if (els.typeReview.checked && !els.typeSymposium.checked) params.set("type", "review");
    else if (els.typeSymposium.checked && !els.typeReview.checked) params.set("type", "symposium");

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

    if (params.has("subfield")) {
      const codes = params.get("subfield").split(",");
      selectedSubfields = new Set(codes.filter((c) => allSubfields.includes(c)));
      syncSubfieldCheckboxes();
      updateSubfieldBtnLabel();
    }

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
    if (params.has("type")) {
      const tp = params.get("type");
      els.typeReview.checked = (tp === "review");
      els.typeSymposium.checked = (tp === "symposium");
    }

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

    if (params.has("per_page")) {
      state.perPage = parseInt(params.get("per_page"), 10) || 25;
      els.perPageSelect.value = state.perPage;
    }
  }

  /* ---------- Filter indicator ---------- */
  function updateFilterIndicator() {
    const active = [];
    if (els.globalSearch.value.trim()) active.push("search");
    if (els.titleFilter.value.trim()) active.push("title");
    if (els.authorFilter.value.trim()) active.push("author");
    if (els.reviewerFilter.value.trim()) active.push("reviewer");
    if (selectedSubfields.size < allSubfields.length) active.push("subfield");
    if (selectedJournals.size < allJournals.length) active.push("journals");
    if (els.yearFrom.value || els.yearTo.value) active.push("year");
    if (els.accessFilter.value) active.push("access");
    if (!els.typeReview.checked || !els.typeSymposium.checked) active.push("type");

    if (active.length > 0) {
      els.filterIndicator.textContent = active.length + " filter" + (active.length > 1 ? "s" : "") + " active";
      els.shareBtn.style.display = "";
    } else {
      els.filterIndicator.textContent = "";
      els.shareBtn.style.display = "none";
    }
  }

  /* ---------- Render ---------- */
  function renderResults(data) {
    const total = data.total;
    const reviews = data.reviews;
    const totalPages = data.total_pages;

    if (total < totalReviews) {
      els.resultCount.textContent = "Page " + data.page + " of " + totalPages.toLocaleString() +
        " \u2014 " + total.toLocaleString() + " results";
    } else {
      els.resultCount.textContent = total.toLocaleString() + " entries";
    }

    document.querySelectorAll("th[data-sort]").forEach((th) => {
      th.classList.remove("sort-asc", "sort-desc");
      if (th.dataset.sort === state.sortKey) {
        th.classList.add(state.sortDir === "asc" ? "sort-asc" : "sort-desc");
        th.setAttribute("aria-sort", state.sortDir === "asc" ? "ascending" : "descending");
      } else {
        th.setAttribute("aria-sort", "none");
      }
    });

    // Empty state
    if (els.resultsMessage) {
      if (total === 0) {
        els.resultsMessage.textContent = "No reviews match your search. Try broadening your filters or clearing them.";
        els.resultsMessage.hidden = false;
      } else {
        els.resultsMessage.hidden = true;
      }
    }

    let html = "";
    reviews.forEach((r) => {
      const expanded = state.expandedId === r.id;
      html += '<tr class="review-row' + (expanded ? " expanded" : "") + '" data-id="' + r.id + '">';
      html += "<td>";
      if (r.link) {
        html += '<a class="title-link" href="' + esc(r.link) + '" target="_blank" rel="noopener">' + esc(r.title) + "</a>";
      } else {
        html += esc(r.title);
      }
      html += ' <button class="info-btn">' + (expanded ? "Less" : "Info") + "</button>";
      html += "</td>";
      html += "<td>" + nameLink("author", r.author) + "</td>";
      html += "<td>" + nameLink("reviewer", r.reviewer) + "</td>";
      html += "<td>" + nameLink("journal", r.journal) + "</td>";
      html += "<td>" + esc(r.date) + "</td>";
      html += "</tr>";
      if (expanded) {
        html += '<tr class="detail-row"><td colspan="5"><div class="detail-content">';
        if (r.summary) html += '<p class="summary">' + esc(r.summary) + "</p>";
        if (r.subfield) {
          html += '<span class="subfield-badge subfield-' + r.subfield + '">' + esc(SUBFIELD_NAMES[r.subfield] || r.subfield) + '</span> ';
        }
        if (r.subfield2) {
          html += '<span class="subfield-badge subfield-' + r.subfield2 + '">' + esc(SUBFIELD_NAMES[r.subfield2] || r.subfield2) + '</span> ';
        }
        if (r.type === "symposium") {
          html += '<span class="access-badge badge-symposium">Symposium</span> ';
        }
        if (r.access) {
          const cls = r.access.toLowerCase() === "open" ? "badge-open" : "badge-restricted";
          html += '<span class="access-badge ' + cls + '">' + esc(r.access) + "</span> ";
        }
        if (r.link) {
          const readLabel = r.link_search ? "Search for this review"
            : (r.type === "symposium" ? "Read Contribution" : "Read Review");
          html += '<a class="read-link" href="' + esc(r.link) + '" target="_blank" rel="noopener">' + readLabel + ' &rarr;</a> ';
        }
        if (r.title) html += '<a class="read-link find-book-link" tabindex="0" role="link" data-title="' + esc(r.title) + '">Other reviews of this book &rarr;</a>';
        if (r.type === "symposium" && r.peers && r.peers.length > 0) {
          html += '<div class="symposium-peers"><strong>Other contributions:</strong><ul>';
          r.peers.forEach(function(p) {
            html += "<li>" + esc(p.reviewer);
            if (p.title && p.title !== r.title) html += ' &mdash; &ldquo;' + esc(p.title) + '&rdquo;';
            if (p.link) html += ' <a href="' + esc(p.link) + '" target="_blank" rel="noopener">Read &rarr;</a>';
            html += "</li>";
          });
          html += "</ul></div>";
        }
        html += "</div></td></tr>";
      }
    });
    els.tbody.innerHTML = html;

    els.tbody.querySelectorAll(".review-row").forEach((tr) => {
      tr.addEventListener("click", (e) => {
        // Don't expand when clicking external links or name-links
        if (e.target.closest(".title-link") || e.target.closest(".name-link")) return;
        const id = parseInt(tr.dataset.id, 10);
        const opening = state.expandedId !== id;
        state.expandedId = opening ? id : null;
        renderResults(data);
        if (opening && typeof goatcounter !== "undefined" && goatcounter.count) {
          const label = tr.querySelector("td").textContent.replace(/\s*Info\s*$/, "").trim() || "unknown";
          goatcounter.count({ path: "info-" + label, event: true });
        }
      });
    });

    const onActivate = (a, fn) => {
      a.addEventListener("click", fn);
      a.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fn(e); }
      });
    };

    els.tbody.querySelectorAll(".find-book-link").forEach((a) => {
      onActivate(a, (e) => {
        e.stopPropagation();
        const title = a.dataset.title;
        clearFilters();
        els.titleFilter.value = title;
        if (!els.advancedPanel.classList.contains("open")) {
          els.advancedPanel.classList.add("open");
          els.advancedToggle.textContent = "Hide Advanced Search";
        }
        state.sortKey = "date";
        state.sortDir = "desc";
        state.page = 1;
        state.expandedId = null;
        syncToUrl();
        fetchAndRender();
      });
    });

    els.tbody.querySelectorAll(".name-link").forEach((a) => {
      onActivate(a, (e) => {
        e.stopPropagation();
        const type = a.dataset.type;
        const value = a.dataset.value || a.textContent;
        if (type === "journal") {
          selectedJournals = new Set([value]);
          syncCheckboxes();
          updateJournalBtnLabel();
        } else if (type === "author") {
          els.authorFilter.value = value;
          if (!els.advancedPanel.classList.contains("open")) {
            els.advancedPanel.classList.add("open");
            els.advancedToggle.textContent = "Hide Advanced Search";
          }
        } else if (type === "reviewer") {
          els.reviewerFilter.value = value;
          if (!els.advancedPanel.classList.contains("open")) {
            els.advancedPanel.classList.add("open");
            els.advancedToggle.textContent = "Hide Advanced Search";
          }
        }
        state.page = 1;
        state.expandedId = null;
        syncToUrl();
        fetchAndRender();
      });
    });

    els.tbody.querySelectorAll(".read-link").forEach((a) => {
      a.addEventListener("click", () => {
        if (typeof goatcounter !== "undefined" && goatcounter.count) {
          const label = a.closest(".detail-row").previousElementSibling.querySelector("td").textContent || "unknown";
          goatcounter.count({ path: "click-" + label, event: true });
        }
      });
    });

    state.page = data.page;
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
          syncToUrl();
          fetchAndRender();
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
    els.typeReview.checked = true;
    els.typeSymposium.checked = true;
    selectedSubfields = new Set(allSubfields);
    syncSubfieldCheckboxes();
    updateSubfieldBtnLabel();
    selectedJournals = new Set(allJournals);
    syncCheckboxes();
    updateJournalBtnLabel();
    state.page = 1;
    state.expandedId = null;
  }
})();
