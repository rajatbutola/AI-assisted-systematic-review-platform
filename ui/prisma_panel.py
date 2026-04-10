# ui/prisma_panel.py v1 - 25th March
#
# Redesigned PRISMA diagram panel.
#
# Key improvements over previous version:
#   1. Dual output modes: Black & White (publication-standard) and Colour (customisable)
#   2. Dynamic database sources: "Records identified" box updates with db name and N
#   3. Scalable architecture: add more databases without refactoring the diagram logic
#   4. Fixed rendering bugs: consistent box sizing, proper arrow anchoring
#   5. Publication-quality export: high-DPI PNG and vector SVG
#   6. Clean PRISMA 2020 structure matching the attached journal example

# ui/prisma_panel.py v2 - 27th March
# ui/prisma_panel.py
#
# ── FIX 4: PRISMA B&W TOGGLE LOOP + CUSTOMISATION NOT APPLYING ───────────────
#
# Root cause of the infinite rerun loop:
#   The mode radio widget was inside the diagram sub-tab. When the user clicked
#   "Black & White", the code detected new_mode != current_mode, called
#   settings_repo.save_settings() and then st.rerun(). On the rerun, Streamlit
#   re-read the DB value (which was now "bw") and re-rendered the radio as "bw"
#   — but the widget value also read "bw" now, so new_mode == current_mode and
#   no further rerun was triggered. That should have worked... EXCEPT that
#   Streamlit's st.radio with an explicit `index` parameter ignores the widget's
#   stored value on the FIRST render of a new widget key. The result was the
#   toggle appeared stuck and the page kept refreshing.
#
# Fix:
#   Mode is stored in st.session_state immediately when the radio changes.
#   The figure is built from session_state mode, NOT from the DB settings mode.
#   No st.rerun() is called from the mode toggle. DB is only written when the
#   user explicitly clicks "Apply Changes". This separates the "preview" state
#   from the "persisted" state and eliminates the rerun loop entirely.
#
# Additional fixes:
#   - Dynamic database label: auto-populated from st.session_state["prisma_db_{review_id}"]
#     which is written by app.py after every PubMed/PMC search.
#   - Customisation changes apply IMMEDIATELY to the live diagram preview because
#     the figure is rebuilt from the UI widget values before the DB write.
# ─────────────────────────────────────────────────────────────────────────────

# ui/prisma_panel.py v3 - 27th March
# New features:
#   - Arrow length (vertical gap between boxes) slider
#   - Arrow width slider
#   - Horizontal gap (main col ↔ side col) slider
#   - Vertical spacing between all boxes is now fully parameterised
#   - Custom extra box: user can add one free-form box anywhere on the canvas
#   - Font family selector for box text AND side labels
#   - Side label (Identification/Screening/Eligibility/Included) font size slider
#   - All new settings persisted in the same JSON blob alongside existing ones

# FIXES IN THIS VERSION:
#
# FIX A — "Database Sources" expander resets all other customisation to defaults
#   Root cause: The expander was set to `expanded=True`. In Streamlit, when an
#   expander is open by default, any widget inside it is rendered on the first
#   render cycle. But ALL other expanders (colours, layout, typography, etc.)
#   are closed, so their widgets are NOT rendered — meaning new_bc, new_fc,
#   new_bw_, etc. are undefined when preview_settings is assembled.
#   Fix: All expanders start `expanded=False`. The db section is kept open by
#   detecting whether databases is empty (first-time setup). Widget default
#   values always fall back to `settings` dict, so unopened expanders never
#   cause KeyErrors.
#
# FIX B — Arrows not touching box edges
#   Root cause: Arrow endpoints used a fixed ±0.005 gap regardless of bh.
#   When bh was large the arrow stopped well short of the box.
#   Fix: Arrow starts/ends exactly at box edge (y ± bh/2) with zero gap.
#   Plotly renders the arrowhead ON the endpoint coordinate, so setting
#   the endpoint to exactly the box edge looks correct visually.
#
# FIX C — Label X position stays fixed when main column moves
#   Root cause: Stage labels were hardcoded at x=0.03. When box width was
#   reduced the gap between label and box grew because main_x was also
#   reduced visually but the label stayed at 0.03.
#   Fix: Label x = main_x - bw/2 - label_margin, where label_margin is a
#   user-controlled slider "Label distance from diagram".
#
# FIX D — Extra custom box inherits diagram defaults
#   The extra box now uses the same bw/bh/fs/ff as the diagram unless
#   the user explicitly overrides them (via scale factors and the override
#   toggle). This means changing global font size also changes the extra box.
#
# FIX E — Extra box arrow connection
#   User can choose which standard box to connect the extra box to (or none),
#   pick direction (from/to), and the arrow is drawn automatically.
#
# FIX F — "Add Database" button no longer resets other settings
#   Instead of calling save_settings (which triggers a rerun with only the
#   partial settings object), the new entry is added to the in-memory list
#   and the full preview_settings is saved atomically on "Apply Changes".
#   The button itself now just appends to a session_state list so the new
#   row appears immediately without losing other widget values.
 


# ui/prisma_panel.py — v6 1st April, 2026
#

# ═══════════════════════════════════════════════════════════════════
# COMPLETE REWRITE OF THE FIGURE BUILDER
# ═══════════════════════════════════════════════════════════════════
#
# ROOT CAUSE OF ARROW MISALIGNMENT (definitive fix):
#
# All previous versions used Plotly "annotation" objects with
# axref/ayref="x", yref/xref="x" to draw arrows. This approach has
# a fundamental flaw: the "tail" (ax, ay) uses DATA coordinates, but
# data coordinates are mapped to pixel coordinates by Plotly's internal
# axis scaling, which depends on the figure size, margins, and the
# visible range. Small rounding differences cause the arrow tail to
# stop 5–15 pixels short of the box edge.
#
# THE FIX: Switch the entire figure to PAPER coordinates (0.0–1.0,
# normalised to the figure dimensions). In paper coordinates:
#   - Boxes are drawn as go.layout.Shape with xref="paper", yref="paper"
#   - Text is added with go.layout.Annotation with xref="paper", yref="paper"
#   - Arrows are drawn as go.layout.Shape type="path" using SVG path strings
#     e.g. "M x1 y1 L x2 y2" in paper coordinates
# Because everything uses the same paper coordinate system, there is no
# coordinate-system mismatch and arrows touch boxes exactly.
#
# PRISMA 2020 LAYOUT (from images 1–5 provided by user):
#   Stage labels: vertical text on left (blue bars)
#   Single source: Identification → [Duplicates right] → Screening → [Excluded right]
#                  → Eligibility → [Full-text excl. right] → Included
#   Multi source:  [DB1 box] [DB2 box] → merge → [Dedup box right] → Screening → ...
#   All side boxes connected by horizontal arrows FROM main column
#   All stage boxes connected by vertical arrows between rows
#
# ═══════════════════════════════════════════════════════════════════



# ui/prisma_panel.py — v5
#
# ═══════════════════════════════════════════════════════════════════
# COMPLETE REWRITE OF THE FIGURE BUILDER
# ═══════════════════════════════════════════════════════════════════
#
# ROOT CAUSE OF ARROW MISALIGNMENT (definitive fix):
#
# All previous versions used Plotly "annotation" objects with
# axref/ayref="x", yref/xref="x" to draw arrows. This approach has
# a fundamental flaw: the "tail" (ax, ay) uses DATA coordinates, but
# data coordinates are mapped to pixel coordinates by Plotly's internal
# axis scaling, which depends on the figure size, margins, and the
# visible range. Small rounding differences cause the arrow tail to
# stop 5–15 pixels short of the box edge.
#
# THE FIX: Switch the entire figure to PAPER coordinates (0.0–1.0,
# normalised to the figure dimensions). In paper coordinates:
#   - Boxes are drawn as go.layout.Shape with xref="paper", yref="paper"
#   - Text is added with go.layout.Annotation with xref="paper", yref="paper"
#   - Arrows are drawn as go.layout.Shape type="path" using SVG path strings
#     e.g. "M x1 y1 L x2 y2" in paper coordinates
# Because everything uses the same paper coordinate system, there is no
# coordinate-system mismatch and arrows touch boxes exactly.
#
# PRISMA 2020 LAYOUT (from images 1–5 provided by user):
#   Stage labels: vertical text on left (blue bars)
#   Single source: Identification → [Duplicates right] → Screening → [Excluded right]
#                  → Eligibility → [Full-text excl. right] → Included
#   Multi source:  [DB1 box] [DB2 box] → merge → [Dedup box right] → Screening → ...
#   All side boxes connected by horizontal arrows FROM main column
#   All stage boxes connected by vertical arrows between rows
#
# ═══════════════════════════════════════════════════════════════════

import json
import logging
import math
from typing import Dict, List, Optional

import plotly.graph_objects as go
import streamlit as st

from storage.repository import ArticleRepository, PrismaSettingsRepository

logger = logging.getLogger(__name__)

article_repo  = ArticleRepository()
settings_repo = PrismaSettingsRepository()

# ── Colour palettes ───────────────────────────────────────────────────────────

_COLOUR_BOXES = {
    "identification": "#1B4F8A",
    "screening":      "#2E7D4F",
    "eligibility":    "#6A3093",
    "included":       "#1B4F8A",
    "excluded":       "#8B2500",
    "unsure":         "#7A6000",
    "duplicates":     "#8B2500",
    "dedup":          "#5D4037",
}
_COLOUR_FONTS = {k: "#FFFFFF" for k in _COLOUR_BOXES}
_BW_BOXES     = {k: "#FFFFFF" for k in _COLOUR_BOXES}
_BW_FONTS     = {k: "#000000" for k in _COLOUR_BOXES}

FONT_FAMILIES = [
    "Arial, sans-serif",
    "Times New Roman, serif",
    "Courier New, monospace",
    "Georgia, serif",
    "Verdana, sans-serif",
    "Helvetica, sans-serif",
]

STANDARD_BOXES = [
    "None", "Identification", "Screening", "Eligibility", "Included",
    "Duplicates (right)", "Excluded (right)", "Unsure (right)",
]

_DEFAULT_EXTRA_BOX = {
    "enabled": False, "text": "Custom Box",
    "x": 0.50, "y": 0.10,
    "fill": "", "font_color": "",
    "override_size": False, "w_scale": 1.0, "h_scale": 1.0,
    "arrow_to": "None", "arrow_direction": "from",
}

_DEFAULT_SETTINGS = {
    "mode":                 "colour",
    "box_colors":           dict(_COLOUR_BOXES),
    "font_colors":          dict(_COLOUR_FONTS),
    # Layout — all in PAPER coordinates (0–1)
    "box_width":            0.36,   # width of main column boxes
    "box_height":           0.09,   # height of all boxes
    "side_box_width":       0.22,   # width of side (exclusion) boxes
    "font_size":            12,
    "font_family":          "Arial, sans-serif",
    "label_font_size":      10,
    "label_font_family":    "Arial, sans-serif",
    "label_col_x":          0.04,   # x-centre of the stage label column
    "show_unsure_box":      True,
    "custom_labels":        {},
    "databases":            [],
    "additional_sources_n": 0,
    "v_gap":                0.17,   # vertical gap BETWEEN boxes (not centres)
    "h_gap":                0.08,   # horizontal gap between main box right edge and side box left edge
    "arrow_width":          2,
    "arrow_color":          "",     # empty = auto from mode
    "extra_box":            dict(_DEFAULT_EXTRA_BOX),
}


# ── Main entry point ──────────────────────────────────────────────────────────

def render_prisma_diagram(review_id: int) -> None:
    db_settings = settings_repo.get_settings(review_id) or {}
    # Back-fill defaults
    for k, v in _DEFAULT_SETTINGS.items():
        if k not in db_settings:
            db_settings[k] = v
    eb = db_settings.get("extra_box", {})
    for k, v in _DEFAULT_EXTRA_BOX.items():
        if k not in eb:
            eb[k] = v
    db_settings["extra_box"] = eb

    mode_key    = f"prisma_mode_{review_id}"
    add_db_flag = f"prisma_adding_db_{review_id}"
    db_list_key = f"prisma_dbs_{review_id}"

    if mode_key not in st.session_state:
        st.session_state[mode_key] = db_settings.get("mode", "colour")

    # Sync database list from DB settings (unless mid-add)
    if not st.session_state.get(add_db_flag, False):
        st.session_state[db_list_key] = list(db_settings.get("databases", []))

    # ── Live search counts always override saved database list ────────────
    # search_db is set by app.py after every search via _refresh_prisma_from_db.
    # Format: {"PubMed": 10, "Europe PMC": 8, "duplicates_removed": 2}
    # We ALWAYS prefer the live session_state value over whatever was saved in
    # db_settings.databases, because db_settings is persisted from a previous
    # session and may be stale (e.g. single source when user has now done two).
    search_db = st.session_state.get(f"prisma_db_{review_id}")
    if search_db and isinstance(search_db, dict):
        if "name" in search_db:
            # Legacy single-source format: {"name": "PubMed", "n": 10}
            db_settings["databases"] = [search_db]
        else:
            # Multi-source format: {"PubMed": 10, "Europe PMC": 8, ...}
            live_dbs = [
                {"name": k, "n": v}
                for k, v in search_db.items()
                if k != "duplicates_removed" and isinstance(v, int) and v > 0
            ]
            if live_dbs:
                db_settings["databases"] = live_dbs
            if "duplicates_removed" in search_db:
                db_settings["_duplicates_removed"] = int(search_db["duplicates_removed"])

    counts = article_repo.get_screening_counts(review_id)

    diag_tab, cust_tab, export_tab = st.tabs(
        ["📊 Diagram", "🎨 Customise", "📥 Export"]
    )
    with diag_tab:
        _render_diagram_tab(review_id, counts, db_settings, mode_key)
    with cust_tab:
        _render_customisation_panel(review_id, db_settings, mode_key,
                                    db_list_key, add_db_flag)
    with export_tab:
        _render_export_panel(review_id, db_settings, counts)


# ── Diagram tab ───────────────────────────────────────────────────────────────

def _render_diagram_tab(review_id, counts, settings, mode_key):
    dark_mode    = st.session_state.get("dark_mode", False)
    current_mode = st.session_state[mode_key]

    c_radio, c_hint = st.columns([2, 4])
    with c_radio:
        new_mode = st.radio(
            "Style", ["colour", "bw"],
            index=0 if current_mode == "colour" else 1,
            format_func=lambda x: "🎨 Colour" if x == "colour" else "⬛ B&W",
            key=f"mode_radio_{review_id}", horizontal=True,
        )
    with c_hint:
        st.caption("Switch style instantly. Save in 🎨 Customise → Apply Changes.")
    if new_mode != st.session_state[mode_key]:
        st.session_state[mode_key] = new_mode

    eff = _effective_settings(settings, st.session_state[mode_key])
    fig = _build_figure(counts, eff, dark_mode)
    st.plotly_chart(fig, use_container_width=True, key=f"prisma_diag_{review_id}")

    id_, excl, uns, incl, pend = (
        counts["total"], counts["excluded"], counts["unsure"],
        counts["included"], counts["pending"],
    )
    screened = incl + excl + uns
    cols = st.columns(6)
    for col, lbl, val in zip(cols,
        ["Identified","Screened","✅ Included","❌ Excluded","🟡 Unsure","⏳ Pending"],
        [id_, screened, incl, excl, uns, pend]
    ):
        col.metric(lbl, val)

    if counts.get("conflict", 0) > 0:
        st.warning(f"⚠️ {counts['conflict']} conflict(s) excluded from counts.")


def _effective_settings(settings: Dict, live_mode: str) -> Dict:
    s = dict(settings)
    if live_mode == "bw":
        s.update(mode="bw", box_colors=dict(_BW_BOXES), font_colors=dict(_BW_FONTS))
    else:
        s["mode"] = "colour"
        if settings.get("mode") == "bw":
            s.update(box_colors=dict(_COLOUR_BOXES), font_colors=dict(_COLOUR_FONTS))
    return s


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE BUILDER — PAPER COORDINATES, SVG PATH ARROWS
# ══════════════════════════════════════════════════════════════════════════════

def _build_figure(counts: Dict, settings: Dict, dark_mode: bool) -> go.Figure:
    """
    Build the PRISMA 2020 flow diagram entirely in paper coordinates (0–1).

    Paper coordinate system:
      (0, 0) = bottom-left corner of the figure
      (1, 1) = top-right corner of the figure

    All shapes (boxes) and annotations (text) use xref="paper", yref="paper".
    Arrows are drawn as go.layout.Shape objects with type="path" using
    SVG M (moveto) and L (lineto) commands plus a custom arrowhead shape.
    This guarantees pixel-perfect alignment regardless of figure size.
    """
    identified = counts["total"]
    excluded   = counts["excluded"]
    unsure     = counts["unsure"]
    included   = counts["included"]
    conflict   = counts.get("conflict", 0)
    screened   = included + excluded + unsure

    is_bw = settings.get("mode", "colour") == "bw"

    # Colours
    if dark_mode and not is_bw:
        bg = paper_bg = "#0E1117"
        line_c = text_c = arrow_c = label_c = "#CCCCCC"
        border_c = "#CCCCCC"
    elif is_bw:
        bg = paper_bg = "#FFFFFF"
        line_c = text_c = arrow_c = label_c = border_c = "#000000"
    else:
        bg = paper_bg = "#FFFFFF"
        line_c = text_c = arrow_c = label_c = border_c = "#2C3E50"

    # Override arrow colour if set
    custom_arrow = settings.get("arrow_color", "")
    if custom_arrow:
        arrow_c = custom_arrow

    bc  = settings.get("box_colors",  _COLOUR_BOXES)
    fc  = settings.get("font_colors", _COLOUR_FONTS)
    lbl = settings.get("custom_labels", {})

    # ── Layout parameters (all in paper 0–1 space) ────────────────────────
    BW   = float(settings.get("box_width",      0.36))   # main box width
    BH   = float(settings.get("box_height",     0.09))   # box height
    SBW  = float(settings.get("side_box_width", 0.22))   # side box width
    FS   = int(settings.get("font_size",         12))
    FF   = settings.get("font_family",           "Arial, sans-serif")
    LFS  = int(settings.get("label_font_size",   10))
    LFF  = settings.get("label_font_family",     "Arial, sans-serif")
    VG   = float(settings.get("v_gap",           0.17))  # gap BETWEEN box bottom and next box top
    HG   = float(settings.get("h_gap",           0.08))  # gap between main right edge and side left edge
    AW   = int(settings.get("arrow_width",        2))
    LX   = float(settings.get("label_col_x",     0.04))  # x-centre of stage label column

    # Margin for top/bottom padding
    TOP_PAD = 0.04
    databases = settings.get("databases", [])
    other_n   = int(settings.get("additional_sources_n", 0))
    dupes_from_search = int(settings.get("_duplicates_removed", 0))

    n_src = len(databases)  # number of source databases

    # ── Y positions (top of figure = 1, we place boxes from top down) ────
    # For multi-source we add an extra row at the top for the source boxes
    extra_top = BH + VG if n_src >= 2 else 0

    # Y centres of the four main stage boxes (in paper coords, y=0 at bottom)
    # We'll compute from the top down:
    # y = 1 - TOP_PAD - BH/2 is the centre of the topmost row
    if n_src >= 2:
        y_src_row   = 1 - TOP_PAD - BH / 2                  # source boxes
        y_ident     = y_src_row - BH - VG                    # identification (after dedup)
    else:
        y_ident     = 1 - TOP_PAD - BH / 2

    y_screen = y_ident  - BH - VG
    y_elig   = y_screen - BH - VG
    y_incl   = y_elig   - BH - VG

    # X positions
    MAIN_CX  = 0.50              # centre of main column
    SIDE_CX  = MAIN_CX + BW / 2 + HG + SBW / 2   # centre of side boxes

    # Cap SIDE_CX so it doesn't go off-screen
    SIDE_CX  = min(SIDE_CX, 0.95 - SBW / 2)

    fig = go.Figure()

    # ════════════════════════════════════════════════════════════════════
    # HELPER FUNCTIONS
    # ════════════════════════════════════════════════════════════════════

    def _box(cx, cy, w, h, fill, line_color=None):
        """Draw a filled rectangle in paper coordinates."""
        fig.add_shape(
            type="rect",
            xref="paper", yref="paper",
            x0=cx - w / 2, y0=cy - h / 2,
            x1=cx + w / 2, y1=cy + h / 2,
            fillcolor=fill,
            line=dict(color=line_color or border_c, width=1.5),
            layer="below",
        )

    def _text(cx, cy, text, color, size=None, family=None, angle=0,
              box_w=None, box_h=None):
        """
        Add centred text annotation in paper coordinates.

        FIX — Text centring:
        Plotly paper-coordinate annotations must have xanchor="center" and
        yanchor="middle" explicitly set. Without these, the anchor defaults
        to "left"/"bottom" and the text appears offset from the box centre.

        FIX — Text overflow:
        When box_w and box_h are given, the font size is auto-capped so
        text never renders larger than the box can contain. We estimate
        the maximum font size from the box height: roughly figure_height_px
        × box_h gives the box height in pixels; each line of text needs
        ~1.4× its font size in pixels. Multi-line text (counted by <br>)
        divides the available height across lines.
        """
        txt_size = size or FS

        if box_h is not None:
            # Estimate available pixel height for this box
            # figure is ~650px tall, paper coords are 0-1
            px_height = 650 * box_h
            n_lines = max(1, text.count("<br>") + 1)
            # Each line needs ~font_size * 1.4 px; cap so all lines fit
            max_size_from_height = max(6, int(px_height / (n_lines * 1.5)))
            txt_size = min(txt_size, max_size_from_height)

        fig.add_annotation(
            xref="paper", yref="paper",
            x=cx, y=cy,
            text=text,
            showarrow=False,
            font=dict(color=color, size=txt_size, family=family or FF),
            align="center",
            xanchor="center",   # FIX: explicit horizontal anchor
            yanchor="middle",   # FIX: explicit vertical anchor
            textangle=angle,
        )

    def _v_arrow(cx, y_from_box_top, y_to_box_bottom):
        """
        Vertical downward arrow from BOTTOM edge of upper box to TOP edge of lower box.
        y_from_box_top  = cy_upper - BH/2  (bottom edge of the box above)
        y_to_box_bottom = cy_lower + BH/2  (top edge of the box below)

        Uses go.layout.Shape type="path" for pixel-perfect rendering.
        The arrowhead is a small filled triangle drawn as a separate shape.
        """
        # Arrow shaft
        fig.add_shape(
            type="line",
            xref="paper", yref="paper",
            x0=cx, y0=y_from_box_top,
            x1=cx, y1=y_to_box_bottom + 0.012,  # stop short for arrowhead
            line=dict(color=arrow_c, width=AW),
        )
        # Arrowhead (downward-pointing triangle)
        tip_y  = y_to_box_bottom
        half   = 0.012
        fig.add_shape(
            type="path",
            xref="paper", yref="paper",
            path=f"M {cx - half} {tip_y + half*1.8} "
                 f"L {cx + half} {tip_y + half*1.8} "
                 f"L {cx} {tip_y} Z",
            fillcolor=arrow_c,
            line=dict(color=arrow_c, width=0),
        )

    def _h_arrow(y, x_from_right_edge, x_to_left_edge):
        """
        Horizontal rightward arrow from RIGHT edge of main box to LEFT edge of side box.
        """
        # Shaft
        fig.add_shape(
            type="line",
            xref="paper", yref="paper",
            x0=x_from_right_edge, y0=y,
            x1=x_to_left_edge - 0.010, y1=y,
            line=dict(color=arrow_c, width=AW),
        )
        # Arrowhead (rightward triangle)
        tip_x = x_to_left_edge
        half  = 0.010
        fig.add_shape(
            type="path",
            xref="paper", yref="paper",
            path=f"M {tip_x - half*1.8} {y + half} "
                 f"L {tip_x - half*1.8} {y - half} "
                 f"L {tip_x} {y} Z",
            fillcolor=arrow_c,
            line=dict(color=arrow_c, width=0),
        )

    def _merge_arrow(cx_src, y_src_bottom, cx_dest, y_dest_top):
        """
        L-shaped arrow: go down from source, then horizontally to destination cx,
        then continue down. Used for multi-source boxes merging into dedup.
        """
        mid_y = (y_src_bottom + y_dest_top) / 2
        # Vertical segment down from source
        fig.add_shape(type="line", xref="paper", yref="paper",
                      x0=cx_src, y0=y_src_bottom,
                      x1=cx_src, y1=mid_y,
                      line=dict(color=arrow_c, width=AW))
        # Horizontal segment to dest cx
        fig.add_shape(type="line", xref="paper", yref="paper",
                      x0=cx_src, y0=mid_y,
                      x1=cx_dest, y1=mid_y,
                      line=dict(color=arrow_c, width=AW))
        # Vertical segment down to dest
        fig.add_shape(type="line", xref="paper", yref="paper",
                      x0=cx_dest, y0=mid_y,
                      x1=cx_dest, y1=y_dest_top + 0.012,
                      line=dict(color=arrow_c, width=AW))
        # Arrowhead
        half = 0.012
        fig.add_shape(type="path", xref="paper", yref="paper",
                      path=f"M {cx_dest - half} {y_dest_top + half*1.8} "
                           f"L {cx_dest + half} {y_dest_top + half*1.8} "
                           f"L {cx_dest} {y_dest_top} Z",
                      fillcolor=arrow_c, line=dict(color=arrow_c, width=0))

    def main_box(cy, text, color_key, w=None):
        w = w or BW
        fill = bc.get(color_key, _COLOUR_BOXES.get(color_key, "#1B4F8A"))
        font = fc.get(color_key, "#FFFFFF")
        _box(MAIN_CX, cy, w, BH, fill)
        _text(MAIN_CX, cy, text, font, box_w=w, box_h=BH)

    def side_box(cy, text, color_key, w=None):
        w = w or SBW
        fill = bc.get(color_key, _COLOUR_BOXES.get(color_key, "#8B2500"))
        font = fc.get(color_key, "#FFFFFF")
        _box(SIDE_CX, cy, w, BH, fill)
        _text(SIDE_CX, cy, text, font, box_w=w, box_h=BH)
        # Arrow from main box right edge to side box left edge
        _h_arrow(
            y=cy,
            x_from_right_edge=MAIN_CX + BW / 2,
            x_to_left_edge=SIDE_CX - w / 2,
        )

    # ════════════════════════════════════════════════════════════════════
    # STAGE LABEL BARS (left side, vertical text)
    # FIX: In B&W mode, bars must be white with black border and black text,
    # not the coloured version. is_bw is already computed above.
    # ════════════════════════════════════════════════════════════════════
    label_bar_w = 0.025
    bar_x0 = LX - label_bar_w / 2
    bar_x1 = LX + label_bar_w / 2

    if n_src >= 2:
        stage_regions = [
            ("Identification", y_src_row, y_ident),
            ("Screening",      y_screen + BH / 2, y_screen - BH / 2),
            ("Eligibility",    y_elig + BH / 2,   y_elig   - BH / 2),
            ("Included",       y_incl + BH / 2,   y_incl   - BH / 2),
        ]
    else:
        stage_regions = [
            ("Identification", y_ident + BH / 2, y_ident - BH / 2),
            ("Screening",      y_screen + BH / 2, y_screen - BH / 2),
            ("Eligibility",    y_elig + BH / 2,   y_elig   - BH / 2),
            ("Included",       y_incl + BH / 2,   y_incl   - BH / 2),
        ]

    STAGE_COLOURS = {
        "Identification": "#1565C0",
        "Screening":      "#2E7D32",
        "Eligibility":    "#6A1B9A",
        "Included":       "#1565C0",
    }

    for s_label, y_top, y_bot in stage_regions:
        pad = 0.01
        if is_bw:
            bar_fill   = "#FFFFFF"
            bar_border = "#000000"
            bar_text   = "#000000"
        else:
            bar_fill   = STAGE_COLOURS.get(s_label, "#1565C0")
            bar_border = bar_fill
            bar_text   = "#FFFFFF"

        fig.add_shape(
            type="rect", xref="paper", yref="paper",
            x0=bar_x0, y0=y_bot - pad,
            x1=bar_x1, y1=y_top + pad,
            fillcolor=bar_fill,
            line=dict(color=bar_border, width=1.0),
            layer="below",
        )
        cy_bar = (y_top + y_bot) / 2
        _text(LX, cy_bar, f"<b>{s_label}</b>", bar_text,
              size=LFS, family=LFF, angle=-90)

    # ════════════════════════════════════════════════════════════════════
    # IDENTIFICATION ROW
    # ════════════════════════════════════════════════════════════════════
    dupes_n = dupes_from_search if dupes_from_search > 0 else max(0, identified - screened)

    if n_src >= 2:
        # ── Multi-source: side-by-side source boxes at the top ─────────
        # Source boxes are evenly spaced
        src_w   = min(BW * 0.90, 0.35)
        spacing = src_w + 0.04
        n       = min(n_src, 4)
        total_w = n * spacing - 0.04
        x_starts = [MAIN_CX - total_w / 2 + i * spacing for i in range(n)]

        for i, (db, sx) in enumerate(zip(databases[:n], x_starts)):
            src_fill = bc.get("identification", _COLOUR_BOXES["identification"])
            src_font = fc.get("identification", "#FFFFFF")
            _box(sx, y_src_row, src_w, BH, src_fill)
            _text(sx, y_src_row,
                  lbl.get(f"id_src_{i}",
                          f"<b>{db['name']}</b><br>(n = {db['n']:,})"),
                  src_font, box_w=src_w, box_h=BH)

        # Dedup box (main column, between source row and screening row)
        total_all = sum(db["n"] for db in databases) + other_n
        after_dedup = max(0, total_all - dupes_n)
        main_box(y_ident,
                 lbl.get("identification",
                         f"<b>Records after deduplication</b>"
                         f"<br>(n = {after_dedup:,})"),
                 "identification")

        # Side box: duplicates removed
        side_box(y_ident,
                 lbl.get("duplicates",
                         f"<b>Duplicates removed</b><br>(n = {dupes_n:,})"),
                 "duplicates")

        # Arrows: each source box → dedup box (merge arrows)
        for sx in x_starts[:n]:
            _merge_arrow(sx, y_src_row - BH / 2, MAIN_CX, y_ident + BH / 2)

    else:
        # ── Single source layout ──────────────────────────────────────
        if databases:
            db_lines = "<br>".join(
                f"Records from {db['name']} (n = {db['n']:,})"
                for db in databases
            )
            if other_n > 0:
                db_lines += f"<br>Other sources (n = {other_n:,})"
            ident_txt = lbl.get("identification",
                f"<b>Records identified</b><br>{db_lines}"
                f"<br><i>Total: n = {identified:,}</i>")
        else:
            ident_txt = lbl.get("identification",
                f"<b>Records identified</b><br>(n = {identified:,})")

        main_box(y_ident, ident_txt, "identification")
        side_box(y_ident,
                 lbl.get("duplicates",
                         f"<b>Duplicates removed</b><br>(n = {dupes_n:,})"),
                 "duplicates")

    # Vertical arrow from identification to screening
    _v_arrow(MAIN_CX, y_ident - BH / 2, y_screen + BH / 2)

    # ════════════════════════════════════════════════════════════════════
    # SCREENING
    # ════════════════════════════════════════════════════════════════════
    main_box(y_screen,
             lbl.get("screening",
                     f"<b>Records screened</b><br>(n = {screened:,})"),
             "screening")
    side_box(y_screen,
             lbl.get("excluded",
                     f"<b>Records excluded</b><br>"
                     f"(title/abstract)<br>(n = {excluded:,})"),
             "excluded")

    _v_arrow(MAIN_CX, y_screen - BH / 2, y_elig + BH / 2)

    # ════════════════════════════════════════════════════════════════════
    # ELIGIBILITY
    # ════════════════════════════════════════════════════════════════════
    elig_n = included + unsure
    main_box(y_elig,
             lbl.get("eligibility",
                     f"<b>Full-text articles assessed</b><br>(n = {elig_n:,})"),
             "eligibility")

    if settings.get("show_unsure_box", True) and unsure > 0:
        side_box(y_elig,
                 lbl.get("unsure",
                         f"<b>Full-text excluded</b><br>(n = {unsure:,})"),
                 "unsure")

    _v_arrow(MAIN_CX, y_elig - BH / 2, y_incl + BH / 2)

    # ════════════════════════════════════════════════════════════════════
    # INCLUDED
    # ════════════════════════════════════════════════════════════════════
    main_box(y_incl,
             lbl.get("included",
                     f"<b>Studies included</b><br>(n = {included:,})"),
             "included")

    # ════════════════════════════════════════════════════════════════════
    # EXTRA CUSTOM BOX
    # ════════════════════════════════════════════════════════════════════
    eb = settings.get("extra_box", {})
    if eb.get("enabled"):
        eb_cx  = float(eb.get("x", MAIN_CX))
        eb_cy  = float(eb.get("y", y_incl - BH - VG))
        eb_w   = BW * float(eb.get("w_scale", 1.0)) if eb.get("override_size") else BW
        eb_h   = BH * float(eb.get("h_scale", 1.0)) if eb.get("override_size") else BH
        eb_fill = eb.get("fill") or bc.get("included", _COLOUR_BOXES["included"])
        eb_fc  = eb.get("font_color") or fc.get("included", "#FFFFFF")
        _box(eb_cx, eb_cy, eb_w, eb_h, eb_fill)
        _text(eb_cx, eb_cy, eb.get("text", "Custom Box"), eb_fc)

        # Arrow to target
        box_centres_map = {
            "Identification": (MAIN_CX, y_ident),
            "Screening":      (MAIN_CX, y_screen),
            "Eligibility":    (MAIN_CX, y_elig),
            "Included":       (MAIN_CX, y_incl),
        }
        target = eb.get("arrow_to", "None")
        if target and target != "None" and target in box_centres_map:
            tx, ty = box_centres_map[target]
            if eb.get("arrow_direction", "from") == "from":
                src_x, src_y = eb_cx, eb_cy
                dst_x, dst_y = tx, ty
            else:
                src_x, src_y = tx, ty
                dst_x, dst_y = eb_cx, eb_cy
            fig.add_shape(type="line", xref="paper", yref="paper",
                          x0=src_x, y0=src_y, x1=dst_x, y1=dst_y,
                          line=dict(color=arrow_c, width=AW))

    # ════════════════════════════════════════════════════════════════════
    # CONFLICT NOTE
    # ════════════════════════════════════════════════════════════════════
    if conflict > 0:
        _text(MAIN_CX, max(y_incl - BH, 0.02),
              f"⚠️ {conflict} conflict(s) excluded from counts",
              "#E67E22", size=9)

    # ════════════════════════════════════════════════════════════════════
    # LAYOUT
    # ════════════════════════════════════════════════════════════════════
    # Compute figure height dynamically
    bottom_y = y_incl - BH / 2 - 0.04
    top_y    = (y_src_row + BH / 2 + 0.04) if n_src >= 2 else (y_ident + BH / 2 + 0.04)
    # Paper coords go 0-1, so the content spans (1 - top_y) to bottom_y
    # We want about 600px for the default layout; scale proportionally
    content_span = top_y - bottom_y
    fig_height   = max(500, int(650 * content_span / 0.80))

    fig.update_layout(
        height=fig_height,
        showlegend=False,
        margin=dict(l=20, r=20, t=15, b=15),
        plot_bgcolor=bg,
        paper_bgcolor=paper_bg,
        xaxis=dict(visible=False, range=[0, 1], fixedrange=True,
                   showgrid=False, zeroline=False),
        yaxis=dict(visible=False, range=[0, 1], fixedrange=True,
                   showgrid=False, zeroline=False),
    )
    # Invisible scatter to prevent blank canvas
    fig.add_trace(go.Scatter(
        x=[0.5], y=[0.5], mode="markers",
        marker=dict(size=0.001, opacity=0),
        showlegend=False, hoverinfo="skip",
    ))
    return fig


# ── Customisation panel ───────────────────────────────────────────────────────

def _render_customisation_panel(review_id, settings, mode_key, db_list_key, add_db_flag):
    rid = review_id

    # Initialise all output variables from stored settings
    bc       = settings.get("box_colors",  dict(_COLOUR_BOXES))
    fc       = settings.get("font_colors", dict(_COLOUR_FONTS))
    new_bc   = dict(bc)
    new_fc   = dict(fc)
    new_bw_      = float(settings.get("box_width",      0.36))
    new_bh_      = float(settings.get("box_height",     0.09))
    new_sbw_     = float(settings.get("side_box_width", 0.22))
    new_vg_      = float(settings.get("v_gap",          0.17))
    new_hg_      = float(settings.get("h_gap",          0.08))
    new_aw_      = int(settings.get("arrow_width",       2))
    new_fs_      = int(settings.get("font_size",         12))
    new_ff_      = settings.get("font_family",           "Arial, sans-serif")
    new_lfs_     = int(settings.get("label_font_size",   10))
    new_lff_     = settings.get("label_font_family",     "Arial, sans-serif")
    new_lx_      = float(settings.get("label_col_x",    0.04))
    new_show_uns = settings.get("show_unsure_box",       True)
    new_labels   = dict(settings.get("custom_labels",   {}))
    new_other_n  = int(settings.get("additional_sources_n", 0))
    eb           = settings.get("extra_box", dict(_DEFAULT_EXTRA_BOX))
    new_eb       = dict(eb)

    # ── 1. Database Sources ────────────────────────────────────────────
    with st.expander("🗄️ Database Sources", expanded=False):
        st.caption("These appear in the Identification boxes. Auto-filled from search. Click Apply to save.")
        databases_ss = st.session_state.get(db_list_key, [])
        updated_dbs  = []
        for i, db in enumerate(databases_ss):
            c1, c2, c3 = st.columns([3, 2, 1])
            db_name = c1.text_input("Database", value=db.get("name", ""),
                                    key=f"db_name_{rid}_{i}",
                                    label_visibility="collapsed",
                                    placeholder="e.g. PubMed")
            db_n    = c2.number_input("N", value=int(db.get("n", 0)),
                                      min_value=0, key=f"db_n_{rid}_{i}",
                                      label_visibility="collapsed")
            if c3.button("🗑️", key=f"db_rm_{rid}_{i}"):
                pass
            else:
                updated_dbs.append({"name": db_name, "n": db_n})

        if st.button("➕ Add Database", key=f"add_db_{rid}"):
            st.session_state[add_db_flag] = True
            st.session_state[db_list_key] = updated_dbs + [{"name": "", "n": 0}]
            st.rerun()
        else:
            st.session_state[db_list_key] = updated_dbs
            st.session_state[add_db_flag] = False

        new_other_n = st.number_input("Additional records (other sources)",
                                      value=int(settings.get("additional_sources_n", 0)),
                                      min_value=0, key=f"other_n_{rid}")

    # ── 2. Box Colours ─────────────────────────────────────────────────
    with st.expander("🎨 Box Colours (Colour mode)", expanded=False):
        BOX_KEYS = [
            ("identification", "Identification"), ("screening", "Screening"),
            ("eligibility",    "Eligibility"),    ("included",  "Included"),
            ("excluded",       "Excluded"),       ("unsure",    "Unsure"),
            ("duplicates",     "Duplicates"),
        ]
        col1, col2, col3 = st.columns(3)
        for i, (key, label) in enumerate(BOX_KEYS):
            col = [col1, col2, col3][i % 3]
            with col:
                new_bc[key] = st.color_picker(f"{label} fill",
                    value=bc.get(key, _COLOUR_BOXES.get(key, "#1B4F8A")),
                    key=f"cp_fill_{rid}_{key}")
                new_fc[key] = st.color_picker(f"{label} text",
                    value=fc.get(key, "#FFFFFF"),
                    key=f"cp_text_{rid}_{key}")

    # ── 3. Layout & Spacing ────────────────────────────────────────────
    with st.expander("📐 Layout, Spacing & Arrows", expanded=False):
        st.markdown("**Box dimensions** (paper coordinates 0–1)")
        r1c1, r1c2, r1c3 = st.columns(3)
        new_bw_  = r1c1.slider("Main box width",  0.00, 1.00, new_bw_,  0.01, key=f"sl_bw_{rid}")
        new_bh_  = r1c2.slider("Box height",      0.00, 0.60, new_bh_,  0.01, key=f"sl_bh_{rid}")
        new_sbw_ = r1c3.slider("Side box width",  0.00, 0.60, new_sbw_, 0.01, key=f"sl_sbw_{rid}")
        st.markdown("**Spacing**")
        r2c1, r2c2 = st.columns(2)
        new_vg_ = r2c1.slider("Vertical gap between boxes", 0.00, 0.60, new_vg_, 0.01, key=f"sl_vg_{rid}",
                               help="Gap between bottom of one box and top of next.")
        new_hg_ = r2c2.slider("Horizontal gap (main→side)", 0.00, 0.60, new_hg_, 0.01, key=f"sl_hg_{rid}",
                               help="Gap between right edge of main box and left edge of side box.")
        st.markdown("**Stage label position**")
        new_lx_ = st.slider("Stage label column X", 0.00, 0.60, new_lx_, 0.005, key=f"sl_lx_{rid}",
                             help="Horizontal position of the Identification/Screening/... labels.")
        st.markdown("**Arrows**")
        r3c1, r3c2 = st.columns(2)
        new_aw_      = r3c1.slider("Arrow width (px)", 1, 10, new_aw_, key=f"sl_aw_{rid}")
        new_show_uns = r3c2.checkbox("Show unsure/full-text exclusion box",
                                     value=new_show_uns, key=f"cb_uns_{rid}")

    # ── 4. Typography ──────────────────────────────────────────────────
    with st.expander("🔤 Typography", expanded=False):
        st.markdown("**Box text**")
        tc1, tc2 = st.columns(2)
        new_fs_ = tc1.slider("Font size", 1, 30, new_fs_, key=f"sl_fs_{rid}")
        ff_idx  = FONT_FAMILIES.index(new_ff_) if new_ff_ in FONT_FAMILIES else 0
        new_ff_ = tc2.selectbox("Font family", FONT_FAMILIES, index=ff_idx, key=f"sel_ff_{rid}")
        st.markdown("**Stage labels**")
        lc1, lc2 = st.columns(2)
        new_lfs_ = lc1.slider("Label font size", 1, 30, new_lfs_, key=f"sl_lfs_{rid}")
        lff_idx  = FONT_FAMILIES.index(new_lff_) if new_lff_ in FONT_FAMILIES else 0
        new_lff_ = lc2.selectbox("Label font family", FONT_FAMILIES, index=lff_idx, key=f"sel_lff_{rid}")

    # ── 5. Custom Labels ───────────────────────────────────────────────
    with st.expander("✏️ Custom Labels", expanded=False):
        st.caption("Leave blank for auto-generated labels.")
        LABEL_KEYS = [
            ("identification", "Identification"),
            ("screening",      "Screening"),
            ("eligibility",    "Eligibility"),
            ("included",       "Included"),
            ("excluded",       "Excluded (right side)"),
            ("unsure",         "Unsure / full-text excl. (right side)"),
            ("duplicates",     "Duplicates removed (right side)"),
        ]
        new_labels = {}
        for key, label in LABEL_KEYS:
            new_labels[key] = st.text_input(label,
                value=settings.get("custom_labels", {}).get(key, ""),
                placeholder="Leave blank for auto", key=f"lbl_{rid}_{key}")
        new_labels = {k: v for k, v in new_labels.items() if v.strip()}

    # ── 6. Extra Custom Box ────────────────────────────────────────────
    with st.expander("➕ Extra Custom Box", expanded=False):
        eb_en = st.checkbox("Enable extra box", value=bool(eb.get("enabled", False)),
                            key=f"eb_en_{rid}")
        if eb_en:
            eb_txt = st.text_area("Box text (HTML: <b>bold</b>)",
                                  value=eb.get("text", "Custom Box"),
                                  key=f"eb_txt_{rid}", height=70)
            ep1, ep2 = st.columns(2)
            eb_x = ep1.slider("X (left→right)", 0.05, 0.95, float(eb.get("x", 0.50)), 0.01, key=f"eb_x_{rid}")
            eb_y = ep2.slider("Y (bottom→top)", 0.02, 0.98, float(eb.get("y", 0.10)), 0.01, key=f"eb_y_{rid}")
            eb_ov = st.checkbox("Override size", value=bool(eb.get("override_size")), key=f"eb_ov_{rid}")
            if eb_ov:
                es1, es2 = st.columns(2)
                eb_ws = es1.slider("Width scale", 0.3, 2.5, float(eb.get("w_scale", 1.0)), 0.05, key=f"eb_ws_{rid}")
                eb_hs = es2.slider("Height scale", 0.3, 2.5, float(eb.get("h_scale", 1.0)), 0.05, key=f"eb_hs_{rid}")
            else:
                eb_ws = eb.get("w_scale", 1.0)
                eb_hs = eb.get("h_scale", 1.0)
            ec1, ec2, ec3 = st.columns(3)
            eb_inh = ec1.checkbox("Inherit colours", value=not bool(eb.get("fill")), key=f"eb_inh_{rid}")
            if eb_inh:
                eb_fill = ""; eb_fc_ = ""
            else:
                eb_fill = ec2.color_picker("Fill", value=eb.get("fill") or _COLOUR_BOXES["included"], key=f"eb_fill_{rid}")
                eb_fc_  = ec3.color_picker("Text", value=eb.get("font_color") or "#FFFFFF",           key=f"eb_fc_{rid}")
            at1, at2 = st.columns(2)
            arrow_to_idx = STANDARD_BOXES.index(eb.get("arrow_to","None")) if eb.get("arrow_to") in STANDARD_BOXES else 0
            eb_at  = at1.selectbox("Connect to", STANDARD_BOXES, index=arrow_to_idx, key=f"eb_at_{rid}")
            eb_adir = at2.radio("Arrow direction", ["from extra → box", "from box → extra"],
                                index=0 if eb.get("arrow_direction","from")=="from" else 1, key=f"eb_adir_{rid}")
            new_eb = {"enabled": True, "text": eb_txt, "x": eb_x, "y": eb_y,
                      "override_size": eb_ov, "w_scale": eb_ws, "h_scale": eb_hs,
                      "fill": eb_fill, "font_color": eb_fc_,
                      "arrow_to": eb_at, "arrow_direction": "from" if "from extra" in eb_adir else "to"}
        else:
            new_eb = dict(eb); new_eb["enabled"] = False

    # ── Assemble preview settings ──────────────────────────────────────
    # CRITICAL: databases must come from the live search session state
    # (prisma_db_{review_id}), which is what the Diagram tab uses.
    # db_list_key holds the manual widget values from the "Database Sources"
    # expander — these are used only when the user has manually edited them.
    # If the manual list is empty or out of date, fall back to the live counts.
    manual_dbs = st.session_state.get(db_list_key, [])
    live_search_db = st.session_state.get(f"prisma_db_{review_id}", {})

    # Build the live databases list from the search session state
    if isinstance(live_search_db, dict) and live_search_db:
        if "name" in live_search_db:
            live_dbs = [live_search_db]
        else:
            live_dbs = [
                {"name": k, "n": v}
                for k, v in live_search_db.items()
                if k != "duplicates_removed" and isinstance(v, int) and v > 0
            ]
    else:
        live_dbs = []

    # Use manual list only if the user has actually edited it
    # (non-empty AND different from what live search provides)
    effective_dbs = manual_dbs if manual_dbs else live_dbs

    # Duplicates removed: prefer live value, fall back to settings
    live_dupes = int(live_search_db.get("duplicates_removed", 0)) if isinstance(live_search_db, dict) else 0
    effective_dupes = live_dupes or int(settings.get("_duplicates_removed", 0))

    preview = {
        "mode":                 st.session_state.get(mode_key, "colour"),
        "box_colors":           new_bc, "font_colors": new_fc,
        "box_width":            new_bw_,  "box_height":     new_bh_,
        "side_box_width":       new_sbw_, "font_size":      new_fs_,
        "font_family":          new_ff_,  "label_font_size": new_lfs_,
        "label_font_family":    new_lff_, "label_col_x":    new_lx_,
        "show_unsure_box":      new_show_uns,
        "custom_labels":        new_labels,
        "databases":            effective_dbs,
        "additional_sources_n": new_other_n,
        "v_gap":                new_vg_, "h_gap":  new_hg_,
        "arrow_width":          new_aw_, "extra_box": new_eb,
        "_duplicates_removed":  effective_dupes,
    }

    # ── Live preview ───────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 👁️ Live Preview")
    st.caption("All changes reflected instantly. Click **Apply Changes** to save.")
    counts  = article_repo.get_screening_counts(review_id)
    eff     = _effective_settings(preview, st.session_state.get(mode_key, "colour"))
    fig_pre = _build_figure(counts, eff, st.session_state.get("dark_mode", False))
    st.plotly_chart(fig_pre, use_container_width=True, key=f"prisma_preview_{review_id}")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("💾 Apply Changes", type="primary", use_container_width=True, key=f"apply_{rid}"):
            settings_repo.save_settings(review_id, preview)
            st.session_state[add_db_flag] = False
            st.success("✅ Settings saved.")
            st.rerun()
    with b2:
        if st.button("↩️ Reset to Defaults", use_container_width=True, key=f"reset_{rid}"):
            settings_repo.reset_settings(review_id)
            st.session_state[mode_key]   = "colour"
            st.session_state[db_list_key] = []
            st.session_state[add_db_flag] = False
            st.success("Reset to defaults.")
            st.rerun()


# ── Export panel ──────────────────────────────────────────────────────────────

def _render_export_panel(review_id, settings, counts):
    st.markdown("#### Export")

    # Use live search counts for databases, same as Diagram and Customise tabs
    live_search_db = st.session_state.get(f"prisma_db_{review_id}", {})
    if isinstance(live_search_db, dict) and live_search_db:
        if "name" in live_search_db:
            live_dbs = [live_search_db]
        else:
            live_dbs = [
                {"name": k, "n": v}
                for k, v in live_search_db.items()
                if k != "duplicates_removed" and isinstance(v, int) and v > 0
            ]
        if live_dbs:
            settings = dict(settings)
            settings["databases"] = live_dbs
            if "duplicates_removed" in live_search_db:
                settings["_duplicates_removed"] = live_search_db["duplicates_removed"]

    def _make_fig(mode):
        return _build_figure(counts, _effective_settings(settings, mode), dark_mode=False)

    for label, mode in [("Colour", "colour"), ("Black & White", "bw")]:
        st.markdown(f"**{label} diagram**")
        fig = _make_fig(mode)
        c1, c2 = st.columns(2)
        with c1:
            try:
                png = fig.to_image(format="png", scale=3, width=900, height=int(fig.layout.height or 650))
                st.download_button(f"📥 PNG ({label})", data=png,
                                   file_name=f"prisma_{mode}_{review_id}.png",
                                   mime="image/png", use_container_width=True)
            except Exception:
                st.info("PNG export: `pip install kaleido`")
        with c2:
            try:
                svg = fig.to_image(format="svg", width=900, height=int(fig.layout.height or 650))
                st.download_button(f"📥 SVG ({label})", data=svg,
                                   file_name=f"prisma_{mode}_{review_id}.svg",
                                   mime="image/svg+xml", use_container_width=True)
            except Exception:
                st.info("SVG export: `pip install kaleido`")
        st.divider()

    identified = counts["total"]
    excluded   = counts["excluded"]
    unsure     = counts["unsure"]
    included   = counts["included"]
    screened   = included + excluded + unsure
    db_rows    = "\n".join(f"Records from {db['name']},{db['n']}"
                           for db in settings.get("databases", []))
    csv = (
        "Stage,Count\n"
        f"Records identified,{identified}\n"
        + (f"{db_rows}\n" if db_rows else "")
        + f"Records screened,{screened}\n"
        f"Records excluded (title/abstract),{excluded}\n"
        f"Full-text assessed,{included+unsure}\n"
        f"Full-text excluded,{unsure}\n"
        f"Studies included,{included}\n"
    )
    st.download_button("📥 PRISMA counts (CSV)", data=csv,
                       file_name=f"prisma_counts_{review_id}.csv", mime="text/csv",
                       use_container_width=True)
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("📥 Settings (JSON)", data=json.dumps(settings, indent=2),
                           file_name=f"prisma_settings_{review_id}.json", mime="application/json")
    with c2:
        up = st.file_uploader("Import settings (JSON)", type=["json"], key=f"prisma_up_{review_id}")
        if up:
            try:
                settings_repo.save_settings(review_id, json.loads(up.read()))
                st.success("Settings imported.")
                st.rerun()
            except Exception as e:
                st.error(f"Import failed: {e}")
    with st.expander("Current settings (JSON)", expanded=False):
        st.json(settings)












