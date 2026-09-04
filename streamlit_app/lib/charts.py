"""One chart style for the whole dashboard.

Every figure on every page goes through :func:`style`, so the pages describe *what* they
plot and this module decides how it looks. That is the same reason the pricing rules live in
one module and the HTTP calls live in ``api_client``: two pages that style their own charts
will disagree, and neither will be wrong on paper.

The palette is the validated default categorical order — blue, orange, aqua, yellow — used in
**fixed slot order and never cycled**. That ordering is the colour-blind-safety mechanism, not
a preference: the slots clear the adjacent-pair separation gates in both light and dark, which
is what stacked bars, grouped bars, lines and areas need. It is only guaranteed for *adjacent*
pairs, so nothing here uses more than three slots in a form where every pair is compared at
once (scatter, bubble) — past that the honest move is a table, and the pages take it.

Dark mode is selected, not flipped: each slot has its own step chosen for the dark surface.
"""

from __future__ import annotations

from typing import Any

import plotly.graph_objects as go
import streamlit as st

#: Categorical slots, in the order they must be assigned. Never sort, never cycle.
SERIES_LIGHT = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100")
SERIES_DARK = ("#3987e5", "#d95926", "#199e70", "#c98500")

#: Single-hue ramp for magnitude (the heatmap). Light → dark, one hue, never a rainbow.
SEQUENTIAL = ("#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b")

#: Reserved for state, never for "series 5". Always shipped with a label beside it.
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}

_INK_LIGHT = ("#0b0b0b", "#52514e", "#e6e5e1")
_INK_DARK = ("#ffffff", "#c3c2b7", "#383835")


def is_dark() -> bool:
    """The viewer's active theme, as Streamlit resolved it (including "system")."""
    try:
        return str(getattr(st.context.theme, "type", "light")).lower() == "dark"
    except Exception:  # bare mode, or an older runtime: light is the safe default
        return False


def series(count: int) -> list[str]:
    """The first ``count`` categorical slots for the current theme, in order."""
    palette = SERIES_DARK if is_dark() else SERIES_LIGHT
    if count > len(palette):
        raise ValueError(
            f"{count} series exceeds the {len(palette)} validated slots; fold the tail into "
            f"'Other', facet the chart, or show a table instead of inventing a hue"
        )
    return list(palette[:count])


def style(fig: go.Figure, *, legend: bool | None = None) -> go.Figure:
    """Apply the shared marks-and-chrome rules to a figure, in place.

    Hairline solid grid one shade off the surface (never dashed — dashing reads as a
    threshold), a transparent plot surface so the card behind it shows through, thin marks,
    and a legend whenever more than one series is on screen.
    """
    primary, secondary, grid = _INK_DARK if is_dark() else _INK_LIGHT
    traces = [t for t in fig.data if getattr(t, "showlegend", None) is not False]
    show_legend = len(traces) > 1 if legend is None else legend

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": secondary, "size": 13},
        title={"font": {"color": primary, "size": 15}},
        margin={"l": 8, "r": 8, "t": 40 if fig.layout.title.text else 12, "b": 8},
        showlegend=show_legend,
        legend={
            "orientation": "h", "yanchor": "bottom", "y": 1.02,
            "xanchor": "left", "x": 0, "title_text": "",
        },
        hovermode="closest",
        bargap=0.25,
    )
    axis = {
        "gridcolor": grid, "griddash": "solid", "gridwidth": 1,
        "zeroline": False, "linecolor": grid, "ticks": "",
        "title_font": {"size": 12}, "automargin": True,
    }
    fig.update_xaxes(**axis)
    fig.update_yaxes(**axis)
    # 2px surface gap between adjacent fills rather than a border drawn around them.
    fig.update_traces(
        selector={"type": "bar"},
        marker_line_width=2,
        marker_line_color="rgba(0,0,0,0)",
    )
    fig.update_traces(selector={"type": "scatter"}, line={"width": 2})
    return fig


def show(fig: go.Figure, **kwargs: Any) -> None:
    """Render a styled figure.

    ``theme=None`` matters: Streamlit's own plotly theme would repaint the traces and undo
    the slot order above, which is the one thing that must not happen to a palette whose
    ordering is what makes it readable.
    """
    st.plotly_chart(style(fig), width="stretch", theme=None, **kwargs)
