"""Generate the AIMternet-Cafe AWS data engineering pipeline diagram.

The diagram is deliberately rendered from code so its labels remain accurate and the
editable SVG and presentation-ready PNG always describe the same architecture.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/aimternet-matplotlib")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp/aimternet-cache")

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.patches import Arc, Circle, Ellipse, FancyArrowPatch, FancyBboxPatch, Polygon

matplotlib.rcParams["svg.fonttype"] = "none"

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "docs"


@dataclass(frozen=True)
class Node:
    """A positioned service or pipeline-stage card."""

    key: str
    x: float
    y: float
    width: float
    height: float
    title: str
    detail: str
    color: str
    logo: str

    @property
    def left(self) -> tuple[float, float]:
        return self.x, self.y + self.height / 2

    @property
    def right(self) -> tuple[float, float]:
        return self.x + self.width, self.y + self.height / 2

    @property
    def top(self) -> tuple[float, float]:
        return self.x + self.width / 2, self.y + self.height


INK = "#182230"
MUTED = "#586579"
LINE = "#66758B"
AWS_ORANGE = "#FF9900"
AWS_DARK = "#232F3E"
S3_GREEN = "#3F8624"
RDS_BLUE = "#3B48CC"
DDB_BLUE = "#2774C7"
REDSHIFT_PURPLE = "#8C4FFF"
AIRFLOW_TEAL = "#017CEE"
PYTHON_BLUE = "#3776AB"
QUALITY_AMBER = "#D97706"
API_GREEN = "#059669"
STREAMLIT_RED = "#FF4B4B"


def add_airflow_logo(ax: plt.Axes, cx: float, cy: float, size: float) -> None:
    """Draw the recognizable four-color Apache Airflow pinwheel."""
    colors = ("#00C7D4", "#017CEE", "#F04E45", "#F5A623")
    angles = (45, 135, 225, 315)
    radius = size * 0.52
    for angle, color in zip(angles, colors, strict=True):
        radians = math.radians(angle)
        left = math.radians(angle - 34)
        right = math.radians(angle + 15)
        blade = Polygon(
            [
                (cx, cy),
                (cx + radius * math.cos(left), cy + radius * math.sin(left)),
                (cx + radius * math.cos(radians), cy + radius * math.sin(radians)),
                (cx + radius * 0.72 * math.cos(right), cy + radius * 0.72 * math.sin(right)),
            ],
            closed=True,
            facecolor=color,
            edgecolor="white",
            linewidth=0.4,
            zorder=5,
        )
        ax.add_patch(blade)
    ax.add_patch(
        Circle(
            (cx, cy),
            size * 0.07,
            facecolor="white",
            edgecolor=AIRFLOW_TEAL,
            linewidth=0.7,
            zorder=6,
        )
    )


def add_logo(ax: plt.Axes, logo: str, cx: float, cy: float, size: float, color: str) -> None:
    """Draw a compact vector service mark instead of a text abbreviation."""
    lw = 1.15
    if logo == "airflow":
        add_airflow_logo(ax, cx, cy, size)
    elif logo == "s3":
        ax.add_patch(
            Ellipse(
                (cx, cy + size * 0.24),
                size * 0.62,
                size * 0.22,
                fill=False,
                edgecolor="white",
                linewidth=lw,
                zorder=5,
            )
        )
        ax.add_patch(
            Arc(
                (cx, cy - size * 0.18),
                size * 0.62,
                size * 0.22,
                theta1=180,
                theta2=360,
                color="white",
                linewidth=lw,
                zorder=5,
            )
        )
        ax.plot(
            [cx - size * 0.31, cx - size * 0.26],
            [cy + size * 0.24, cy - size * 0.18],
            color="white",
            linewidth=lw,
            zorder=5,
        )
        ax.plot(
            [cx + size * 0.31, cx + size * 0.26],
            [cy + size * 0.24, cy - size * 0.18],
            color="white",
            linewidth=lw,
            zorder=5,
        )
    elif logo in {"rds", "redshift"}:
        ax.add_patch(
            Ellipse(
                (cx, cy + size * 0.22),
                size * 0.60,
                size * 0.20,
                fill=False,
                edgecolor="white",
                linewidth=lw,
                zorder=5,
            )
        )
        ax.add_patch(
            Arc(
                (cx, cy - size * 0.22),
                size * 0.60,
                size * 0.20,
                theta1=180,
                theta2=360,
                color="white",
                linewidth=lw,
                zorder=5,
            )
        )
        ax.plot(
            [cx - size * 0.30, cx - size * 0.30],
            [cy + size * 0.22, cy - size * 0.22],
            color="white",
            linewidth=lw,
            zorder=5,
        )
        ax.plot(
            [cx + size * 0.30, cx + size * 0.30],
            [cy + size * 0.22, cy - size * 0.22],
            color="white",
            linewidth=lw,
            zorder=5,
        )
        if logo == "redshift":
            ax.plot(
                [cx - size * 0.12, cx + size * 0.13],
                [cy - size * 0.05, cy + size * 0.09],
                color="white",
                linewidth=lw,
                zorder=5,
            )
            for dx, dy in ((-0.15, -0.07), (0.16, 0.11), (0.04, -0.18)):
                ax.add_patch(
                    Circle(
                        (cx + size * dx, cy + size * dy),
                        size * 0.045,
                        facecolor="white",
                        edgecolor="none",
                        zorder=6,
                    )
                )
    elif logo == "dynamodb":
        for offset, height in ((-0.22, 0.48), (0.0, 0.66), (0.22, 0.40)):
            ax.add_patch(
                FancyBboxPatch(
                    (cx + size * offset - size * 0.07, cy - size * height / 2),
                    size * 0.14,
                    size * height,
                    boxstyle="round,pad=0.01,rounding_size=0.02",
                    facecolor="white",
                    edgecolor="none",
                    zorder=5,
                )
            )
    elif logo == "ec2":
        ax.add_patch(
            FancyBboxPatch(
                (cx - size * 0.27, cy - size * 0.24),
                size * 0.54,
                size * 0.48,
                boxstyle="round,pad=0.01,rounding_size=0.03",
                fill=False,
                edgecolor="white",
                linewidth=lw,
                zorder=5,
            )
        )
        ax.plot(
            [cx - size * 0.15, cx + size * 0.15],
            [cy + size * 0.08, cy + size * 0.08],
            color="white",
            linewidth=lw,
            zorder=5,
        )
        ax.plot(
            [cx - size * 0.15, cx + size * 0.15],
            [cy - size * 0.09, cy - size * 0.09],
            color="white",
            linewidth=lw,
            zorder=5,
        )
    elif logo == "jupyter":
        ax.add_patch(Circle((cx, cy), size * 0.33, facecolor="white", edgecolor="none", zorder=4))
        ax.add_patch(
            Arc(
                (cx, cy),
                size * 0.52,
                size * 0.28,
                theta1=18,
                theta2=162,
                color="#F37726",
                linewidth=1.7,
                zorder=5,
            )
        )
        ax.add_patch(
            Arc(
                (cx, cy),
                size * 0.52,
                size * 0.28,
                theta1=198,
                theta2=342,
                color="#F37726",
                linewidth=1.7,
                zorder=5,
            )
        )
        ax.add_patch(
            Circle(
                (cx - size * 0.25, cy + size * 0.20),
                size * 0.045,
                facecolor="#6B7280",
                edgecolor="none",
                zorder=5,
            )
        )
        ax.add_patch(
            Circle(
                (cx + size * 0.24, cy - size * 0.20),
                size * 0.055,
                facecolor="#9CA3AF",
                edgecolor="none",
                zorder=5,
            )
        )
    elif logo == "streamlit":
        ax.plot(
            [cx - size * 0.27, cx, cx + size * 0.27],
            [cy - size * 0.02, cy + size * 0.25, cy - size * 0.02],
            color="white",
            linewidth=1.5,
            zorder=5,
        )
        ax.plot(
            [cx - size * 0.25, cx, cx + size * 0.25],
            [cy - size * 0.11, cy + size * 0.04, cy - size * 0.11],
            color="white",
            linewidth=1.5,
            zorder=5,
        )
        ax.plot(
            [cx - size * 0.21, cx, cx + size * 0.21],
            [cy - size * 0.21, cy - size * 0.10, cy - size * 0.21],
            color="white",
            linewidth=1.5,
            zorder=5,
        )
    elif logo == "fastapi":
        ax.add_patch(Circle((cx, cy), size * 0.32, facecolor="white", edgecolor="none", zorder=5))
        ax.add_patch(
            Polygon(
                [
                    (cx + size * 0.08, cy + size * 0.28),
                    (cx - size * 0.13, cy + size * 0.02),
                    (cx - size * 0.01, cy + size * 0.02),
                    (cx - size * 0.10, cy - size * 0.28),
                    (cx + size * 0.16, cy + size * 0.05),
                    (cx + size * 0.03, cy + size * 0.05),
                ],
                closed=True,
                facecolor=color,
                edgecolor="none",
                zorder=6,
            )
        )
    elif logo == "quality":
        ax.plot(
            [cx - size * 0.24, cx - size * 0.05, cx + size * 0.27],
            [cy, cy - size * 0.20, cy + size * 0.21],
            color="white",
            linewidth=2.0,
            solid_capstyle="round",
            zorder=5,
        )
    elif logo == "reconcile":
        ax.add_patch(
            Arc(
                (cx, cy),
                size * 0.56,
                size * 0.42,
                theta1=30,
                theta2=190,
                color="white",
                linewidth=1.5,
                zorder=5,
            )
        )
        ax.add_patch(
            Arc(
                (cx, cy),
                size * 0.56,
                size * 0.42,
                theta1=210,
                theta2=350,
                color="white",
                linewidth=1.5,
                zorder=5,
            )
        )
        ax.add_patch(
            Polygon(
                [
                    (cx - size * 0.31, cy + size * 0.02),
                    (cx - size * 0.16, cy + size * 0.03),
                    (cx - size * 0.25, cy + size * 0.16),
                ],
                facecolor="white",
                edgecolor="none",
                zorder=5,
            )
        )
    elif logo == "terraform":
        for dx, dy in ((-0.17, 0.11), (0.03, 0.0), (-0.17, -0.13), (0.23, -0.11)):
            ax.add_patch(
                Polygon(
                    [
                        (cx + size * dx, cy + size * (dy + 0.14)),
                        (cx + size * (dx + 0.14), cy + size * (dy + 0.07)),
                        (cx + size * (dx + 0.14), cy + size * (dy - 0.08)),
                        (cx + size * dx, cy + size * (dy - 0.01)),
                    ],
                    facecolor="#844FBA",
                    edgecolor="none",
                    zorder=5,
                )
            )
    elif logo == "iam":
        ax.add_patch(
            Polygon(
                [
                    (cx, cy + size * 0.32),
                    (cx + size * 0.27, cy + size * 0.20),
                    (cx + size * 0.22, cy - size * 0.16),
                    (cx, cy - size * 0.32),
                    (cx - size * 0.22, cy - size * 0.16),
                    (cx - size * 0.27, cy + size * 0.20),
                ],
                facecolor=AWS_ORANGE,
                edgecolor="none",
                zorder=5,
            )
        )
        ax.add_patch(Circle((cx, cy + size * 0.03), size * 0.07, facecolor="white", zorder=6))
        ax.plot(
            [cx, cx],
            [cy - size * 0.03, cy - size * 0.18],
            color="white",
            linewidth=1.4,
            zorder=6,
        )
        ax.add_patch(
            Polygon(
                [
                    (cx + size * 0.31, cy - size * 0.02),
                    (cx + size * 0.16, cy - size * 0.03),
                    (cx + size * 0.25, cy - size * 0.16),
                ],
                facecolor="white",
                edgecolor="none",
                zorder=5,
            )
        )


def add_card(ax: plt.Axes, node: Node) -> None:
    """Draw a service card with a compact, logo-like badge."""
    shadow = FancyBboxPatch(
        (node.x + 0.06, node.y - 0.06),
        node.width,
        node.height,
        boxstyle="round,pad=0.02,rounding_size=0.09",
        facecolor="#D9E0E8",
        edgecolor="none",
        alpha=0.55,
        zorder=1,
    )
    card = FancyBboxPatch(
        (node.x, node.y),
        node.width,
        node.height,
        boxstyle="round,pad=0.02,rounding_size=0.09",
        facecolor="white",
        edgecolor=node.color,
        linewidth=1.8,
        zorder=2,
    )
    badge = FancyBboxPatch(
        (node.x + 0.14, node.y + node.height - 0.66),
        0.50,
        0.50,
        boxstyle="round,pad=0.02,rounding_size=0.07",
        facecolor=node.color,
        edgecolor="none",
        zorder=3,
    )
    ax.add_patch(shadow)
    ax.add_patch(card)
    ax.add_patch(badge)
    add_logo(
        ax,
        node.logo,
        node.x + 0.39,
        node.y + node.height - 0.41,
        0.50,
        node.color,
    )
    ax.text(
        node.x + 0.76,
        node.y + node.height - 0.28,
        node.title,
        ha="left",
        va="center",
        fontsize=8.7 if len(node.title) > 18 else 9.4,
        fontweight="bold",
        color=INK,
        zorder=4,
    )
    ax.text(
        node.x + 0.18,
        node.y + node.height - 0.81,
        node.detail,
        ha="left",
        va="top",
        fontsize=7.6,
        color=MUTED,
        linespacing=1.32,
        zorder=4,
    )


def add_arrow(
    ax: plt.Axes,
    start: tuple[float, float],
    end: tuple[float, float],
    label: str = "",
    *,
    color: str = LINE,
    rad: float = 0.0,
    dashed: bool = False,
    label_offset: tuple[float, float] = (0.0, 0.0),
    bidirectional: bool = False,
) -> None:
    """Connect two cards and optionally annotate the data movement."""
    arrow = FancyArrowPatch(
        start,
        end,
        arrowstyle="<|-|>" if bidirectional else "-|>",
        mutation_scale=12,
        linewidth=1.45,
        linestyle="--" if dashed else "-",
        color=color,
        connectionstyle=f"arc3,rad={rad}",
        shrinkA=2,
        shrinkB=2,
        zorder=5,
    )
    ax.add_patch(arrow)
    if label:
        midpoint = (
            (start[0] + end[0]) / 2 + label_offset[0],
            (start[1] + end[1]) / 2 + label_offset[1],
        )
        ax.text(
            *midpoint,
            label,
            ha="center",
            va="center",
            fontsize=6.8,
            fontweight="bold",
            color=INK,
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.8, "alpha": 0.94},
            zorder=6,
        )


def add_poly_arrow(
    ax: plt.Axes,
    points: list[tuple[float, float]],
    *,
    color: str = LINE,
    dashed: bool = False,
) -> None:
    """Draw a clean right-angle connector, with an arrowhead on its final segment."""
    if len(points) < 2:
        raise ValueError("A connector needs at least two points")
    xs, ys = zip(*points[:-1], strict=True)
    ax.plot(
        xs,
        ys,
        color=color,
        linewidth=1.45,
        linestyle="--" if dashed else "-",
        solid_capstyle="round",
        zorder=5,
    )
    add_arrow(ax, points[-2], points[-1], color=color, dashed=dashed)


def add_section(
    ax: plt.Axes,
    x: float,
    y: float,
    width: float,
    height: float,
    title: str,
    color: str,
) -> None:
    """Draw a light grouping boundary behind related stages."""
    boundary = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle="round,pad=0.04,rounding_size=0.12",
        facecolor=color,
        edgecolor="none",
        alpha=0.10,
        zorder=0,
    )
    ax.add_patch(boundary)
    ax.text(
        x + 0.18,
        y + height - 0.23,
        title.upper(),
        ha="left",
        va="top",
        fontsize=7.2,
        fontweight="bold",
        color=color,
        zorder=1,
    )


def render() -> tuple[Path, Path]:
    """Render the architecture diagram to SVG and PNG."""
    fig, ax = plt.subplots(figsize=(18, 10.8), dpi=140)
    fig.patch.set_facecolor("white")
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 10.8)
    ax.axis("off")

    # AWS boundary and title.
    cloud = FancyBboxPatch(
        (0.22, 0.25),
        17.56,
        10.28,
        boxstyle="round,pad=0.02,rounding_size=0.10",
        facecolor="#FBFCFE",
        edgecolor=AWS_DARK,
        linewidth=1.7,
        zorder=-2,
    )
    ax.add_patch(cloud)
    ax.add_patch(
        FancyBboxPatch(
            (0.47, 9.78),
            0.63,
            0.43,
            boxstyle="round,pad=0.02,rounding_size=0.06",
            facecolor=AWS_DARK,
            edgecolor="none",
            zorder=3,
        )
    )
    ax.text(
        0.785,
        9.995,
        "aws",
        color="white",
        fontsize=9,
        fontweight="bold",
        ha="center",
        va="center",
    )
    ax.text(1.25, 10.0, "AIMternet-Cafe", fontsize=17, fontweight="bold", color=INK, va="center")
    ax.text(
        4.05,
        10.0,
        "AWS data engineering pipeline",
        fontsize=12,
        color=MUTED,
        va="center",
    )
    ax.text(
        17.40,
        10.0,
        "IMPLEMENTED POC",
        fontsize=7.2,
        fontweight="bold",
        color=AWS_ORANGE,
        ha="right",
        va="center",
    )

    # Airflow orchestrates the complete data path; it is a boundary, not one hop.
    airflow_boundary = FancyBboxPatch(
        (3.18, 2.82),
        14.26,
        6.62,
        boxstyle="round,pad=0.04,rounding_size=0.14",
        facecolor="#F7FBFF",
        edgecolor=AIRFLOW_TEAL,
        linewidth=1.7,
        linestyle=(0, (5, 3)),
        zorder=-1,
    )
    ax.add_patch(airflow_boundary)
    ax.add_patch(
        FancyBboxPatch(
            (3.40, 9.12),
            6.05,
            0.48,
            boxstyle="round,pad=0.02,rounding_size=0.08",
            facecolor="white",
            edgecolor=AIRFLOW_TEAL,
            linewidth=1.2,
            zorder=3,
        )
    )
    add_airflow_logo(ax, 3.75, 9.36, 0.48)
    ax.text(
        4.14,
        9.36,
        "Apache Airflow 3.3 orchestration",
        ha="left",
        va="center",
        fontsize=9.4,
        fontweight="bold",
        color=INK,
        zorder=5,
    )
    ax.text(
        9.15,
        9.36,
        "Python 3.12 • 6 DAGs",
        ha="right",
        va="center",
        fontsize=7.0,
        fontweight="bold",
        color=AIRFLOW_TEAL,
        zorder=5,
    )

    add_section(ax, 0.50, 6.72, 2.35, 2.72, "Read-only source", AWS_ORANGE)
    add_section(ax, 3.40, 6.72, 5.35, 2.30, "Raw data & quality", S3_GREEN)
    add_section(ax, 8.95, 5.12, 8.20, 3.90, "Operational stores", RDS_BLUE)
    add_section(ax, 3.40, 3.02, 13.75, 1.88, "Lakehouse & warehouse", REDSHIFT_PURPLE)
    add_section(ax, 0.50, 0.55, 12.00, 2.17, "Serving layer", API_GREEN)

    nodes = {
        "raw": Node(
            "raw",
            0.68,
            6.95,
            2.08,
            1.56,
            "EC2 raw landing",
            "CSV + JSON source files\nRead-only bootstrap input",
            AWS_ORANGE,
            "ec2",
        ),
        "bronze": Node(
            "bronze",
            3.66,
            6.95,
            2.20,
            1.56,
            "AWS S3 Bronze",
            "boto3 multipart upload\nImmutable, byte-preserved raw",
            S3_GREEN,
            "s3",
        ),
        "validate": Node(
            "validate",
            6.25,
            6.95,
            2.22,
            1.56,
            "Validate & normalize",
            "Pydantic • pandas\nRules, dedupe, lineage",
            QUALITY_AMBER,
            "quality",
        ),
        "quarantine": Node(
            "quarantine",
            6.25,
            5.28,
            2.22,
            1.22,
            "S3 Quarantine",
            "Rejected rows + reasons",
            "#C2413A",
            "s3",
        ),
        "rds": Node(
            "rds",
            9.32,
            6.95,
            2.62,
            1.56,
            "Amazon RDS",
            "PostgreSQL operational store\npsycopg2 • SQLAlchemy",
            RDS_BLUE,
            "rds",
        ),
        "ddb": Node(
            "ddb",
            13.23,
            6.95,
            2.92,
            1.56,
            "Amazon DynamoDB",
            "Events + 6.3M telemetry rows\nboto3 • pay per request",
            DDB_BLUE,
            "dynamodb",
        ),
        "silver": Node(
            "silver",
            3.66,
            3.10,
            2.50,
            1.34,
            "AWS S3 Silver",
            "DuckDB • Parquet/Snappy\nClean data + full snapshots",
            "#6B7280",
            "s3",
        ),
        "gold": Node(
            "gold",
            7.39,
            3.10,
            2.50,
            1.34,
            "AWS S3 Gold",
            "DuckDB dimensional model\n5 dims • 5 facts • aggregate",
            "#B98500",
            "s3",
        ),
        "redshift": Node(
            "redshift",
            10.61,
            3.10,
            2.65,
            1.34,
            "Amazon Redshift",
            "Analytics warehouse (OLAP)\nredshift_connector",
            REDSHIFT_PURPLE,
            "redshift",
        ),
        "reconcile": Node(
            "reconcile",
            14.02,
            3.10,
            2.40,
            1.34,
            "Reconciliation",
            "S3 • RDS • DDB • Redshift\nCross-layer checks every 6h",
            "#475569",
            "reconcile",
        ),
        "pos": Node(
            "pos",
            0.78,
            1.03,
            2.35,
            1.22,
            "Jupyter POS",
            "HTTPX / Requests\nHTTP client only",
            PYTHON_BLUE,
            "jupyter",
        ),
        "streamlit": Node(
            "streamlit",
            3.48,
            1.03,
            2.35,
            1.22,
            "Streamlit + Plotly",
            "3-page dashboard\nHTTP client only",
            STREAMLIT_RED,
            "streamlit",
        ),
        "api": Node(
            "api",
            7.00,
            1.03,
            2.55,
            1.22,
            "FastAPI service",
            "Pydantic • uvicorn\nRDS/DDB live • Redshift history",
            API_GREEN,
            "fastapi",
        ),
    }
    for node in nodes.values():
        add_card(ax, node)

    # Bootstrap path.
    add_arrow(ax, nodes["raw"].right, nodes["bronze"].left, "manual bootstrap")
    add_arrow(ax, nodes["bronze"].right, nodes["validate"].left)
    add_arrow(ax, nodes["validate"].right, nodes["rds"].left)
    add_poly_arrow(
        ax,
        [
            (nodes["validate"].right[0], nodes["validate"].right[1] - 0.29),
            (8.78, 6.64),
            (13.05, 6.64),
            (nodes["ddb"].left[0], nodes["ddb"].left[1] - 0.29),
        ],
    )
    ax.text(
        10.92,
        6.51,
        "events + telemetry",
        ha="center",
        va="center",
        fontsize=6.8,
        fontweight="bold",
        color=INK,
    )
    add_arrow(
        ax,
        (nodes["validate"].x + nodes["validate"].width / 2, nodes["validate"].y),
        nodes["quarantine"].top,
        "failed checks",
        color="#C2413A",
        label_offset=(0.75, 0.0),
    )

    # Curation and warehouse path.
    add_arrow(
        ax,
        (nodes["bronze"].x + nodes["bronze"].width / 2, nodes["bronze"].y),
        nodes["silver"].top,
        "Bronze → Silver",
        color=S3_GREEN,
        label_offset=(0.83, 0.0),
    )
    add_arrow(ax, nodes["silver"].right, nodes["gold"].left, "curate :30")
    add_arrow(ax, nodes["gold"].right, nodes["redshift"].left, "load :45")
    add_arrow(ax, nodes["redshift"].right, nodes["reconcile"].left, "counts", dashed=True)
    # RDS and DynamoDB exports join a bus below the operational-store cards.
    bus_y = 4.98
    bus_start = nodes["silver"].x + 1.90
    ax.plot(
        [bus_start, nodes["ddb"].top[0]],
        [bus_y, bus_y],
        color=LINE,
        linewidth=1.45,
        solid_capstyle="round",
        zorder=5,
    )
    for store in (nodes["rds"], nodes["ddb"]):
        ax.plot(
            [store.top[0], store.top[0]],
            [store.y, bus_y],
            color=LINE,
            linewidth=1.45,
            zorder=5,
        )
    add_arrow(
        ax,
        (bus_start, bus_y),
        (bus_start, nodes["silver"].top[1]),
    )
    ax.text(
        11.53,
        5.10,
        "hourly operational snapshots — RDS :00  •  DynamoDB :15",
        ha="center",
        va="center",
        fontsize=6.8,
        fontweight="bold",
        color=INK,
        bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.8, "alpha": 0.94},
        zorder=6,
    )

    # Both user interfaces are constrained to the HTTP boundary.
    add_poly_arrow(
        ax,
        [
            nodes["pos"].right,
            (3.27, 2.36),
            (6.68, 2.36),
            nodes["api"].left,
        ],
    )
    add_arrow(ax, nodes["streamlit"].right, nodes["api"].left, rad=-0.13)
    ax.text(
        6.40,
        1.80,
        "HTTP only",
        ha="center",
        va="center",
        fontsize=6.8,
        fontweight="bold",
        color=INK,
        bbox={"facecolor": "white", "edgecolor": "none", "pad": 1.8, "alpha": 0.94},
        zorder=6,
    )

    # Infrastructure note is intentionally separate from data flow: Terraform does not own RDS/RS.
    ax.text(
        17.22,
        1.84,
        "INFRASTRUCTURE",
        ha="right",
        va="center",
        fontsize=7.1,
        fontweight="bold",
        color=AWS_ORANGE,
    )
    add_logo(ax, "terraform", 14.65, 1.47, 0.70, "#844FBA")
    add_logo(ax, "iam", 15.20, 1.47, 0.70, AWS_ORANGE)
    ax.text(
        17.22,
        1.48,
        "Terraform + AWS IAM",
        ha="right",
        va="center",
        fontsize=9.0,
        fontweight="bold",
        color=INK,
    )
    ax.text(
        17.22,
        1.08,
        "Manages S3 configuration and\nDynamoDB tables; shared RDS and\n"
        "Redshift stay outside Terraform.",
        ha="right",
        va="center",
        fontsize=7.3,
        color=MUTED,
        linespacing=1.35,
    )

    png_path = OUTPUT_DIR / "aimternet_data_pipeline.png"
    svg_path = OUTPUT_DIR / "aimternet_data_pipeline.svg"
    save_options = {
        "facecolor": fig.get_facecolor(),
        "bbox_inches": "tight",
        "pad_inches": 0.08,
    }
    fig.savefig(png_path, dpi=160, **save_options)
    fig.savefig(svg_path, format="svg", **save_options)
    plt.close(fig)
    return svg_path, png_path


if __name__ == "__main__":
    svg, png = render()
    print(f"Generated {svg.relative_to(ROOT)}")
    print(f"Generated {png.relative_to(ROOT)}")
