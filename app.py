"""McMurdo -- Academic Psychology Job Discovery Dashboard.

A Shiny for Python application displaying psychology job postings
collected, verified, and enriched by the McMurdo pipeline.

Deployed on Posit Connect Cloud.
"""

from __future__ import annotations

from pathlib import Path

from shiny import App, Inputs, Outputs, Session, reactive, render, ui

from dashboard.data_access import (
    get_all_postings,
    get_connection,
    get_diagnostics,
    get_distinct_values,
    get_filtered_postings,
    get_posting_detail,
)
from dashboard.filters import RANK_LABELS, get_filter_choices
from dashboard.ui_components import (
    diagnostics_panel,
    new_since_last_visit_js,
    posting_detail_panel,
)

_CSS_PATH = Path(__file__).parent / "dashboard" / "styles.css"


# -- UI --

app_ui = ui.page_navbar(
    # Custom CSS
    ui.head_content(
        ui.tags.style(_CSS_PATH.read_text(encoding="utf-8")),
        ui.tags.script(new_since_last_visit_js()),
    ),

    # Tab 1: Postings
    ui.nav_panel(
        "Postings",
        ui.layout_sidebar(
            ui.sidebar(
                ui.h5("Filters"),
                ui.input_text("search_text", "Search", placeholder="Free-text search..."),
                ui.input_select("region", "Region", choices=["All regions"]),
                ui.input_select("rank", "Rank", choices=["All ranks"]),
                ui.input_select("language", "Language", choices=["All languages"]),
                ui.input_select("status", "Status", choices=[
                    ("open", "Open"),
                    ("closed", "Closed"),
                ]),
                ui.input_slider(
                    "min_relevance", "Min. relevance",
                    min=0, max=100, value=0, step=5, post="%",
                ),
                ui.hr(),
                ui.input_action_button("refresh", "Refresh data", class_="btn-outline-primary w-100"),
                width=280,
            ),
            ui.layout_columns(
                ui.card(
                    ui.card_header("Job Postings"),
                    ui.output_data_frame("postings_table"),
                ),
                ui.card(
                    ui.card_header("Details"),
                    ui.output_ui("detail_panel"),
                ),
                col_widths=[7, 5],
            ),
        ),
    ),

    # Tab 2: Diagnostics
    ui.nav_panel(
        "Diagnostics",
        ui.card(
            ui.card_header("Pipeline Diagnostics"),
            ui.output_ui("diagnostics_view"),
        ),
    ),

    # Tab 3: About
    ui.nav_panel(
        "About",
        ui.card(
            ui.card_header("About McMurdo"),
            ui.card_body(
                ui.h3("McMurdo -- Academic Psychology Job Discovery"),
                ui.p(
                    "McMurdo is an automated job discovery system for academic psychology "
                    "positions. It crawls eight international job sources every six hours, "
                    "deduplicates and verifies postings, enriches them with AI-powered "
                    "relevance scoring and structured field extraction, and delivers "
                    "email digests of the most relevant opportunities."
                ),
                ui.h4("Target profile"),
                ui.tags.ul(
                    ui.tags.li("Psychology broadly, with priority for psychosis research, "
                               "organisational/occupational/work/I-O psychology, and "
                               "health psychology/behaviour change"),
                    ui.tags.li("Target seniority: Senior Lecturer, Reader, Principal Lecturer, "
                               "Associate Professor (and international equivalents)"),
                    ui.tags.li("Languages: English, Danish, Swedish, Norwegian"),
                ),
                ui.h4("Sources"),
                ui.tags.ul(
                    ui.tags.li("jobs.ac.uk (UK)"),
                    ui.tags.li("HigherEdJobs (US)"),
                    ui.tags.li("APA PsycCareers (US)"),
                    ui.tags.li("EURAXESS (EU)"),
                    ui.tags.li("Academic Positions (EU/global)"),
                    ui.tags.li("Jobindex.dk (Denmark)"),
                    ui.tags.li("Scandinavian university career pages"),
                    ui.tags.li("Seek.com.au (Australia)"),
                ),
                ui.h4("Technology"),
                ui.tags.ul(
                    ui.tags.li("Pipeline: Python, httpx, feedparser, BeautifulSoup, Gemini 1.5 Flash"),
                    ui.tags.li("Dashboard: Shiny for Python on Posit Connect Cloud"),
                    ui.tags.li("Storage: SQLite (committed to git)"),
                    ui.tags.li("CI/CD: GitHub Actions (crawl every 6 hours)"),
                    ui.tags.li("Notifications: Resend email digests"),
                ),
                ui.h4("AI Assistance Statement"),
                ui.p(
                    "This dashboard was vibe coded with the assistance of Claude Code "
                    "powered by Opus 4.5."
                ),
                ui.hr(),
                ui.p(
                    ui.a("GitHub repository", href="https://github.com/ricardotwumasi/mcmurdo", target="_blank"),
                    class_="text-muted",
                ),
            ),
        ),
    ),

    title="McMurdo",
    id="main_nav",
)


# -- Server --

def server(input: Inputs, output: Outputs, session: Session) -> None:
    """Shiny server function."""

    # Reactive database connection
    @reactive.Calc
    def db_conn():
        # Re-read on refresh button click
        input.refresh()
        return get_connection()

    # Populate filter dropdowns on load
    @reactive.Effect
    def _populate_filters():
        conn = db_conn()
        choices = get_filter_choices(conn)

        regions = {code: label for code, label in choices["regions"]}
        ui.update_select("region", choices=regions)

        ranks = {code: label for code, label in choices["ranks"]}
        ui.update_select("rank", choices=ranks)

        languages = {code: label for code, label in choices["languages"]}
        ui.update_select("language", choices=languages)

    # Filtered postings
    @reactive.Calc
    def filtered_postings():
        conn = db_conn()
        return get_filtered_postings(
            conn,
            region=input.region() if input.region() else None,
            rank_bucket=input.rank() if input.rank() else None,
            language=input.language() if input.language() else None,
            status=input.status(),
            search_text=input.search_text() if input.search_text() else None,
            min_relevance=input.min_relevance() / 100.0 if input.min_relevance() > 0 else None,
        )

    # Postings table
    @render.data_frame
    def postings_table():
        postings = filtered_postings()

        # Build display data
        rows = []
        for p in postings:
            score = f"{p['relevance_score'] * 100:.0f}%" if p.get("relevance_score") is not None else ""
            rank = RANK_LABELS.get(p.get("rank_bucket", ""), p.get("rank_bucket", ""))
            rows.append({
                "posting_id": p["posting_id"],
                "Title": p.get("job_title") or "(No title)",
                "Institution": p.get("institution") or "",
                "Country": p.get("country") or "",
                "Rank": rank,
                "Relevance": score,
                "Closing": p.get("closing_date") or "",
            })

        import pandas as pd
        df = pd.DataFrame(rows)
        if "posting_id" in df.columns:
            display_df = df.drop(columns=["posting_id"])
        else:
            display_df = df

        return render.DataGrid(
            display_df,
            selection_mode="row",
            height="600px",
        )

    # Detail panel
    @render.ui
    def detail_panel():
        selected = postings_table.cell_selection()
        if not selected or "rows" not in selected or not selected["rows"]:
            return ui.p("Select a posting to view details.", class_="text-muted p-3")

        row_idx = selected["rows"][0]
        postings = filtered_postings()
        if row_idx >= len(postings):
            return ui.p("No posting selected.", class_="text-muted p-3")

        posting = postings[row_idx]
        return posting_detail_panel(posting)

    # Diagnostics
    @render.ui
    def diagnostics_view():
        conn = db_conn()
        diag = get_diagnostics(conn)
        return diagnostics_panel(diag)


# -- App --

app = App(app_ui, server)
