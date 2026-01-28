"""UI components for the McMurdo Shiny dashboard.

Provides reusable UI elements: detail panels, badges, and
localStorage JavaScript for "new since last visit" tracking.
"""

from __future__ import annotations

from shiny import ui


def posting_detail_panel(posting: dict) -> ui.Tag:
    """Render a detailed view of a single posting.

    Args:
        posting: A posting dict from the database.

    Returns:
        A Shiny UI Tag with the detail panel content.
    """
    elements = []

    # Title and link
    title = posting.get("job_title") or "Untitled posting"
    url = posting.get("url_original", "#")
    elements.append(
        ui.h3(ui.a(title, href=url, target="_blank"))
    )

    # Institution and location
    meta_parts = []
    if posting.get("institution"):
        meta_parts.append(posting["institution"])
    if posting.get("department"):
        meta_parts.append(posting["department"])
    location_parts = []
    if posting.get("city"):
        location_parts.append(posting["city"])
    if posting.get("country"):
        location_parts.append(posting["country"])
    if location_parts:
        meta_parts.append(", ".join(location_parts))
    if meta_parts:
        elements.append(ui.p(" -- ".join(meta_parts), class_="text-muted"))

    # Badges
    badges = []
    if posting.get("relevance_score") is not None:
        score = posting["relevance_score"]
        score_pct = f"{score * 100:.0f}%"
        if score >= 0.7:
            badges.append(ui.span(f"{score_pct} match", class_="badge bg-success me-1"))
        elif score >= 0.5:
            badges.append(ui.span(f"{score_pct} match", class_="badge bg-warning me-1"))
        else:
            badges.append(ui.span(f"{score_pct} match", class_="badge bg-danger me-1"))

    if posting.get("seniority_match"):
        badges.append(ui.span("Seniority match", class_="badge bg-info me-1"))

    if posting.get("rank_bucket") and posting["rank_bucket"] != "other":
        label = posting["rank_bucket"].replace("_", " ").title()
        badges.append(ui.span(label, class_="badge bg-secondary me-1"))

    if badges:
        elements.append(ui.div(*badges, class_="mb-2"))

    # Key details table
    details = []
    if posting.get("contract_type"):
        details.append(("Contract", posting["contract_type"].title()))
    if posting.get("fte") is not None:
        details.append(("FTE", f"{posting['fte']:.1f}"))
    if posting.get("salary_min") or posting.get("salary_max"):
        salary_parts = []
        if posting.get("salary_min"):
            salary_parts.append(f"{posting['salary_min']:,.0f}")
        if posting.get("salary_max"):
            salary_parts.append(f"{posting['salary_max']:,.0f}")
        salary_str = " - ".join(salary_parts)
        if posting.get("currency"):
            salary_str = f"{posting['currency']} {salary_str}"
        details.append(("Salary", salary_str))
    if posting.get("closing_date"):
        details.append(("Closing date", posting["closing_date"]))
    if posting.get("interview_date"):
        details.append(("Interview date", posting["interview_date"]))
    if posting.get("language") and posting["language"] != "en":
        details.append(("Language", posting["language"].upper()))
    if posting.get("source_id"):
        details.append(("Source", posting["source_id"]))

    if details:
        rows = [ui.tags.tr(ui.tags.td(ui.strong(k)), ui.tags.td(v)) for k, v in details]
        elements.append(
            ui.tags.table(
                ui.tags.tbody(*rows),
                class_="table table-sm table-borderless",
            )
        )

    # Topic tags
    tags = posting.get("topic_tags", [])
    if tags and isinstance(tags, list):
        tag_badges = [
            ui.span(tag, class_="badge bg-light text-dark me-1 mb-1")
            for tag in tags
        ]
        elements.append(ui.div(ui.strong("Topics: "), *tag_badges, class_="mb-2"))

    # Rationale
    if posting.get("relevance_rationale"):
        elements.append(
            ui.div(
                ui.strong("Relevance rationale: "),
                posting["relevance_rationale"],
                class_="mb-2 fst-italic",
            )
        )

    # Synopsis
    if posting.get("synopsis"):
        elements.append(
            ui.div(
                ui.strong("Synopsis: "),
                posting["synopsis"],
                class_="mb-2",
            )
        )

    return ui.div(*elements, class_="posting-detail p-3")


def new_since_last_visit_js() -> str:
    """Return JavaScript for tracking 'new since last visit' via localStorage.

    The script stores the current timestamp in localStorage on page load,
    and exposes the previous timestamp as a Shiny input value.
    """
    return """
    (function() {
        const STORAGE_KEY = 'mcmurdo_last_seen_ts';
        const previous = localStorage.getItem(STORAGE_KEY) || '1970-01-01T00:00:00';
        const now = new Date().toISOString();

        // Set Shiny input value so the server can use it
        if (typeof Shiny !== 'undefined') {
            Shiny.setInputValue('last_seen_ts', previous);
        } else {
            document.addEventListener('shiny:connected', function() {
                Shiny.setInputValue('last_seen_ts', previous);
            });
        }

        // Update the stored timestamp
        localStorage.setItem(STORAGE_KEY, now);
    })();
    """


def diagnostics_panel(diag: dict) -> ui.Tag:
    """Render the diagnostics view.

    Args:
        diag: Diagnostics data dict from data_access.get_diagnostics().

    Returns:
        A Shiny UI Tag with diagnostics content.
    """
    elements = []

    # Summary cards
    elements.append(
        ui.layout_columns(
            ui.value_box("Total postings", str(diag["total_postings"]), theme="primary"),
            ui.value_box("Open", str(diag["open_postings"]), theme="success"),
            ui.value_box("Closed", str(diag["closed_postings"]), theme="secondary"),
            ui.value_box("Enrichments", str(diag["enrichment_count"]), theme="info"),
            col_widths=[3, 3, 3, 3],
        )
    )

    # By source
    if diag.get("sources"):
        source_rows = [
            ui.tags.tr(ui.tags.td(s["source_id"]), ui.tags.td(str(s["n"])))
            for s in diag["sources"]
        ]
        elements.append(
            ui.div(
                ui.h4("Postings by source"),
                ui.tags.table(
                    ui.tags.thead(ui.tags.tr(ui.tags.th("Source"), ui.tags.th("Count"))),
                    ui.tags.tbody(*source_rows),
                    class_="table table-sm table-striped",
                ),
                class_="mb-4",
            )
        )

    # By rank
    if diag.get("ranks"):
        rank_rows = [
            ui.tags.tr(
                ui.tags.td(r["rank_bucket"].replace("_", " ").title()),
                ui.tags.td(str(r["n"])),
            )
            for r in diag["ranks"]
        ]
        elements.append(
            ui.div(
                ui.h4("Postings by rank"),
                ui.tags.table(
                    ui.tags.thead(ui.tags.tr(ui.tags.th("Rank"), ui.tags.th("Count"))),
                    ui.tags.tbody(*rank_rows),
                    class_="table table-sm table-striped",
                ),
                class_="mb-4",
            )
        )

    # By country
    if diag.get("countries"):
        country_rows = [
            ui.tags.tr(ui.tags.td(c["country"]), ui.tags.td(str(c["n"])))
            for c in diag["countries"]
        ]
        elements.append(
            ui.div(
                ui.h4("Postings by country"),
                ui.tags.table(
                    ui.tags.thead(ui.tags.tr(ui.tags.th("Country"), ui.tags.th("Count"))),
                    ui.tags.tbody(*country_rows),
                    class_="table table-sm table-striped",
                ),
                class_="mb-4",
            )
        )

    # Latest pipeline run
    if diag.get("latest_run"):
        run = diag["latest_run"]
        elements.append(
            ui.div(
                ui.h4("Latest pipeline run"),
                ui.p(f"Started: {run.get('started_at', 'N/A')}"),
                ui.p(f"Status: {run.get('status', 'N/A')}"),
                ui.p(f"Found: {run.get('postings_found', 0)}, New: {run.get('postings_new', 0)}"),
                class_="mb-4",
            )
        )

    return ui.div(*elements)
