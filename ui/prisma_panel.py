# ui/prisma_panel.py

import streamlit as st
import plotly.graph_objects as go
from storage.repository import ArticleRepository

repo = ArticleRepository()


def render_prisma_diagram(review_id: int) -> None:
    """
    Render a PRISMA 2020-style flow diagram.

    BUG FIXED — blank diagram:
        The original code used ONLY fig.add_annotation() calls with no shapes
        and no explicit axis ranges. In Plotly ≥ 5.x, a figure that contains
        only annotations and no traces/shapes has no data extent, so Plotly
        cannot determine the axes range automatically and renders a completely
        blank canvas. The annotations exist in the figure object but are
        invisible because the plot area collapses to zero size.

        Fix:
          1. Add explicit xaxis/yaxis range (range=[0,1]) so the canvas
             has a defined size.
          2. Replace pure annotation boxes with go.Scatter traces using
             mode="markers+text" and marker symbol "square" — these ARE
             real data points that Plotly renders reliably.
          3. Alternatively (and more robustly): draw boxes as filled
             rect shapes + separate annotation labels, which is the
             approach used here and matches PRISMA visual conventions.
    """
    st.subheader("PRISMA 2020 Flow Diagram")

    counts = repo.get_screening_counts(review_id)
    identified = counts["total"]
    excluded   = counts["excluded"]
    unsure     = counts["unsure"]
    included   = counts["included"]
    pending    = counts["pending"]
    screened   = identified  # before full-text stage

    fig = go.Figure()

    # ── Helper: draw a filled rectangle box with a centred label ─────────────
    def add_box(x_centre: float, y_centre: float, label: str,
                fill: str = "#1B4F8A", font_color: str = "white",
                w: float = 0.38, h: float = 0.10) -> None:
        x0, x1 = x_centre - w / 2, x_centre + w / 2
        y0, y1 = y_centre - h / 2, y_centre + h / 2
        fig.add_shape(
            type="rect",
            x0=x0, y0=y0, x1=x1, y1=y1,
            fillcolor=fill,
            line=dict(color="#0B2545", width=1.5),
        )
        fig.add_annotation(
            x=x_centre, y=y_centre,
            text=label,
            showarrow=False,
            font=dict(color=font_color, size=11),
            align="center",
            xref="x", yref="y",
        )

    # ── Helper: draw a downward arrow ────────────────────────────────────────
    def add_arrow(x: float, y_top: float, y_bottom: float) -> None:
        fig.add_annotation(
            x=x, y=y_bottom + 0.06,
            ax=x, ay=y_top - 0.06,
            axref="x", ayref="y",
            xref="x",  yref="y",
            showarrow=True,
            arrowhead=2, arrowwidth=2,
            arrowcolor="#0B2545",
        )

    # ── Helper: horizontal connector to a side exclusion box ─────────────────
    def add_side_connector(main_x: float, side_x: float, y: float) -> None:
        fig.add_shape(
            type="line",
            x0=main_x + 0.20, y0=y,
            x1=side_x - 0.19, y1=y,
            line=dict(color="#0B2545", width=1.5),
        )

    # ── Main flow (left-centre column) ───────────────────────────────────────
    main_x = 0.35

    add_box(main_x, 0.88,
            f"<b>IDENTIFICATION</b><br>Records identified: {identified}",
            fill="#0B2545")

    add_arrow(main_x, 0.83, 0.70)

    add_box(main_x, 0.65,
            f"<b>SCREENING</b><br>Records screened: {screened}",
            fill="#1B4F8A")

    add_arrow(main_x, 0.60, 0.47)

    add_box(main_x, 0.42,
            f"<b>ELIGIBILITY</b><br>Full texts assessed: {included + unsure}",
            fill="#028090")

    add_arrow(main_x, 0.37, 0.24)

    add_box(main_x, 0.19,
            f"<b>INCLUDED</b><br>Studies included: {included}",
            fill="#2D6A4F")

    # ── Side exclusion boxes (right column) ──────────────────────────────────
    side_x = 0.78

    # Screening exclusions
    add_side_connector(main_x, side_x, 0.65)
    add_box(side_x, 0.65,
            f"<b>Excluded</b><br>(title/abstract): {excluded}",
            fill="#FEF3F2", font_color="#C0392B", w=0.34)

    # Eligibility exclusions (unsure = needs full text)
    add_side_connector(main_x, side_x, 0.42)
    add_box(side_x, 0.42,
            f"<b>Excluded</b><br>(full text / unsure): {unsure}",
            fill="#FEF3F2", font_color="#C0392B", w=0.34)

    # ── Layout — CRITICAL: explicit axis ranges prevent blank canvas ──────────
    fig.update_layout(
        height=520,
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        title=dict(text="PRISMA 2020 Flow Diagram", font=dict(size=15, color="#0B2545")),
        xaxis=dict(
            visible=False,
            range=[0, 1.1],   # ← REQUIRED: without this, blank canvas
            fixedrange=True,
        ),
        yaxis=dict(
            visible=False,
            range=[0.05, 1.0],  # ← REQUIRED: without this, blank canvas
            fixedrange=True,
        ),
    )

    # A single invisible scatter point is added as a "dummy trace" to ensure
    # Plotly treats this as a proper data figure, not an annotation-only figure.
    # Some Plotly/Streamlit version combinations refuse to display annotation-
    # only figures even with explicit axis ranges. The dummy trace costs nothing.
    fig.add_trace(go.Scatter(
        x=[0], y=[0],
        mode="markers",
        marker=dict(size=0.001, color="rgba(0,0,0,0)"),
        showlegend=False,
        hoverinfo="skip",
    ))

    st.plotly_chart(fig, use_container_width=True)

    # ── Summary metrics below the diagram ────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Identified",  identified)
    c2.metric("Screened",    screened)
    c3.metric("Excluded",    excluded)
    c4.metric("Included",    included)
    c5.metric("Pending",     pending)
