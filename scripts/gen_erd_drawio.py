"""Generate docs/aimternet_erd.drawio from the DDL that actually creates the databases.

Written as a generator rather than as a hand-drawn file for the reason F8 keeps teaching:
a second, hand-maintained copy of a schema is a second place to forget a column. The tables
and their columns come from ``db/migrations/*.up.sql`` and ``db/redshift_ddl/schema.sql``;
OLTP relationships come from the REFERENCES clauses in that same SQL.

Only the OLAP relationships are declared here, and they have to be: Redshift enforces no
foreign keys, so the analytical joins exist in ``curate/gold.py`` and in the queries, not in
the DDL. They are checked against the parsed columns before anything is written, so a
renamed column fails the build instead of producing a diagram with a dangling line.

    python scripts/gen_erd_drawio.py            # writes docs/aimternet_erd.drawio
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from xml.sax.saxutils import quoteattr

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "src" / "aimternet" / "db" / "migrations"
REDSHIFT_DDL = ROOT / "src" / "aimternet" / "db" / "redshift_ddl" / "schema.sql"
OUTPUT = ROOT / "docs" / "aimternet_erd.drawio"

ROW_HEIGHT = 26
HEADER_HEIGHT = 34
KEY_WIDTH = 34
TABLE_WIDTH = 340
COLUMN_GAP = 80
ROW_GAP = 60

# Where the type name stops and the constraints begin.
_TYPE_STOPWORDS = {
    "NOT", "NULL", "DEFAULT", "PRIMARY", "REFERENCES", "CHECK", "UNIQUE",
    "GENERATED", "CONSTRAINT", "COLLATE", "ENCODE", "IDENTITY", "SORTKEY", "DISTKEY",
}
# Anchored with a word boundary: without one, a column literally named `checksum` reads as
# the start of a CHECK constraint and vanishes from the diagram -- which it did.
_CONSTRAINT_START = re.compile(r"^(CONSTRAINT|PRIMARY|UNIQUE|FOREIGN|CHECK|EXCLUDE)\b", re.I)


class Column:
    def __init__(self, name: str, type_: str) -> None:
        self.name = name
        self.type = type_
        self.pk = False
        self.fk = False
        self.note = ""

    @property
    def key(self) -> str:
        return "PK" if self.pk and not self.fk else "PK,FK" if self.pk else "FK" if self.fk else ""


class Table:
    def __init__(self, name: str, note: str = "") -> None:
        self.name = name
        self.note = note
        self.columns: list[Column] = []

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)

    @property
    def height(self) -> int:
        return HEADER_HEIGHT + ROW_HEIGHT * len(self.columns)


def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are not inside parentheses."""
    items, depth, current = [], 0, []
    for char in body:
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        current.append(char)
    items.append("".join(current))
    return [i.strip() for i in items if i.strip()]


def _strip_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def parse_tables(sql: str) -> tuple[dict[str, Table], list[tuple[str, str, str, str]]]:
    """Tables keyed by name, plus (child, child_column, parent, parent_column) references."""
    sql = _strip_comments(sql)
    tables: dict[str, Table] = {}
    references: list[tuple[str, str, str, str]] = []

    pattern = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_.\"]+)\s*\(", re.I)
    for match in pattern.finditer(sql):
        name = match.group(1).strip('"').split(".")[-1]
        depth, index = 1, match.end()
        while index < len(sql) and depth:
            depth += {"(": 1, ")": -1}.get(sql[index], 0)
            index += 1
        body = sql[match.end() : index - 1]
        tail = sql[index : sql.find(";", index)]

        table = Table(name, note=_storage_note(tail))
        for item in _split_top_level(body):
            if _CONSTRAINT_START.match(item):
                if (pk := re.match(r"PRIMARY\s+KEY\s*\(([^)]*)\)", item, re.I)) is not None:
                    for column_name in (c.strip() for c in pk.group(1).split(",")):
                        if (column := table.column(column_name)) is not None:
                            column.pk = True
                continue

            tokens = item.split()
            column = Column(tokens[0].strip('"'), _type_of(tokens[1:]))
            if re.search(r"\bPRIMARY\s+KEY\b", item, re.I):
                column.pk = True
            reference = re.search(
                r"REFERENCES\s+([A-Za-z0-9_]+)\s*\(\s*([A-Za-z0-9_]+)", item, re.I
            )
            if reference is not None:
                column.fk = True
                references.append(
                    (name, column.name, reference.group(1), reference.group(2))
                )
            table.columns.append(column)
        tables[name] = table
    return tables, references


def _type_of(tokens: list[str]) -> str:
    parts: list[str] = []
    for token in tokens:
        if token.upper().rstrip(",") in _TYPE_STOPWORDS:
            break
        parts.append(token)
    return " ".join(parts).rstrip(",") or "?"


def _storage_note(tail: str) -> str:
    """DISTSTYLE / DISTKEY / SORTKEY, which say how a Redshift table is physically laid out."""
    bits = []
    if (m := re.search(r"DISTSTYLE\s+(\w+)", tail, re.I)):
        bits.append(f"DISTSTYLE {m.group(1).upper()}")
    if (m := re.search(r"DISTKEY\s*\(([^)]*)\)", tail, re.I)):
        bits.append(f"DISTKEY({m.group(1).strip()})")
    if (m := re.search(r"SORTKEY\s*\(([^)]*)\)", tail, re.I)):
        bits.append(f"SORTKEY({' '.join(m.group(1).split())})")
    return "  ".join(bits)


# ---------------------------------------------------------------- page definitions

#: Created by db/migrate.py rather than by a migration file, so it has no DDL to parse.
SCHEMA_MIGRATIONS = Table("schema_migrations", note="written by db/migrate.py")
for _name, _type in (
    ("version", "TEXT"), ("name", "TEXT"), ("checksum", "TEXT"), ("applied_at", "TIMESTAMPTZ")
):
    _column = Column(_name, _type)
    _column.pk = _name == "version"
    SCHEMA_MIGRATIONS.columns.append(_column)

#: Redshift enforces no foreign keys, so the analytical joins are declared, not parsed.
#: (child, child column, parent, parent column, label)
OLAP_JOINS = [
    ("fact_rental", "member_id", "dim_member", "member_id", ""),
    ("fact_rental", "workstation_id", "dim_workstation", "workstation_id", ""),
    ("fact_rental", "date_key", "dim_date", "date_id", ""),
    ("fact_rental", "time_key", "dim_time", "time_id", ""),
    ("fact_concession_sale", "member_id", "dim_member", "member_id", ""),
    ("fact_concession_sale", "date_key", "dim_date", "date_id", ""),
    ("fact_concession_sale", "time_key", "dim_time", "time_id", ""),
    ("fact_concession_sale", "rental_id", "fact_rental", "rental_id", "NULL = walk-in"),
    ("fact_concession_line_item", "purchase_id", "fact_concession_sale", "purchase_id", ""),
    ("fact_concession_line_item", "item_sku", "dim_concession_item", "item_sku", ""),
    ("fact_concession_line_item", "date_key", "dim_date", "date_id", ""),
    ("fact_points_activity", "member_id", "dim_member", "member_id", ""),
    ("fact_points_activity", "date_key", "dim_date", "date_id", ""),
    ("fact_workstation_event", "workstation_id", "dim_workstation", "workstation_id", ""),
    ("fact_workstation_event", "member_id", "dim_member", "member_id", "optional"),
    ("fact_workstation_event", "date_key", "dim_date", "date_id", ""),
    ("agg_workstation_utilization_hourly", "workstation_id", "dim_workstation",
     "workstation_id", ""),
    ("agg_workstation_utilization_hourly", "date_key", "dim_date", "date_id", ""),
]

#: Columns Gold derives rather than copies, annotated so the diagram says so.
DERIVED = {
    ("dim_member", "member_key"): "row_number() - recomputed each build; merge on member_id",
    ("dim_concession_item", "unit_margin"): "retail - cost",
    ("fact_concession_sale", "is_walk_in"): "rental_id IS NULL",
    ("fact_concession_line_item", "line_margin"): "total - qty * unit_cost",
    ("fact_points_activity", "running_balance"): "sum(points_delta) - F5",
    ("fact_points_activity", "resulting_balance_source"): "carried, not trusted - F5",
    ("agg_workstation_utilization_hourly", "utilization_pct"): "occupied / readings - F6",
    ("rental_transactions", "gross_rental_amount"): "post-discount - F1",
    ("rental_transactions", "session_end_utc"): "NULL while the rental is open",
    ("members", "is_backfilled"): "true for the 840 D2 stubs",
    ("member_points_ledger", "resulting_balance"): "carried, not trusted - F5",
    ("workstations", "status"): "owned by the API, not the source files",
    ("concession_purchases", "rental_id"): "NULL for a walk-in",
}

PALETTE = {
    "oltp": ("#dae8fc", "#6c8ebf"),
    "control": ("#f5f5f5", "#666666"),
    "dim": ("#d5e8d4", "#82b366"),
    "fact": ("#ffe6cc", "#d79b00"),
    "agg": ("#e1d5e7", "#9673a6"),
}


def _kind(page: str, name: str) -> str:
    if page == "control":
        return "control"
    if name.startswith("dim_"):
        return "dim"
    if name.startswith("fact_"):
        return "fact"
    if name.startswith("agg_"):
        return "agg"
    return "oltp"


# ---------------------------------------------------------------- rendering

class Canvas:
    def __init__(self) -> None:
        self.cells: list[str] = []
        self.row_ids: dict[tuple[str, str], str] = {}
        self.table_ids: dict[str, str] = {}
        self._n = 0

    def _id(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}{self._n}"

    def table(self, table: Table, x: int, y: int, kind: str) -> None:
        fill, stroke = PALETTE[kind]
        table_id = self._id("t")
        self.table_ids[table.name] = table_id
        title = table.name + (f"\n{table.note}" if table.note else "")
        style = (
            "shape=table;startSize=" + str(HEADER_HEIGHT) + ";container=1;collapsible=1;"
            "childLayout=tableLayout;fixedRows=1;rowLines=0;fontStyle=1;align=center;"
            f"resizeLast=1;html=1;whiteSpace=wrap;fillColor={fill};strokeColor={stroke};"
        )
        self.cells.append(
            f'<mxCell id="{table_id}" value={quoteattr(title)} style={quoteattr(style)} '
            f'vertex="1" parent="1">'
            f'<mxGeometry x="{x}" y="{y}" width="{TABLE_WIDTH}" height="{table.height}" '
            f'as="geometry"/></mxCell>'
        )
        for index, column in enumerate(table.columns):
            self._row(table, table_id, column, index, stroke)

    def _row(self, table: Table, table_id: str, column: Column, index: int, stroke: str) -> None:
        row_id = self._id("r")
        self.row_ids[(table.name, column.name)] = row_id
        row_style = (
            "shape=tableRow;horizontal=0;startSize=0;swimlaneHead=0;swimlaneBody=0;"
            "fillColor=none;collapsible=0;dropTarget=0;points=[[0,0.5],[1,0.5]];"
            f"portConstraint=eastwest;top=0;left=0;right=0;bottom=0;strokeColor={stroke};"
        )
        self.cells.append(
            f'<mxCell id="{row_id}" value="" style={quoteattr(row_style)} vertex="1" '
            f'parent="{table_id}"><mxGeometry y="{HEADER_HEIGHT + index * ROW_HEIGHT}" '
            f'width="{TABLE_WIDTH}" height="{ROW_HEIGHT}" as="geometry"/></mxCell>'
        )
        cell_style = (
            "shape=partialRectangle;connectable=0;fillColor=none;top=0;left=0;bottom=0;"
            "right=0;overflow=hidden;whiteSpace=wrap;html=1;fontSize=11;"
        )
        label = f"{column.name}  :  {column.type}"
        if column.note:
            label += f"    · {column.note}"
        for offset, width, value, extra in (
            (0, KEY_WIDTH, column.key, "fontStyle=1;fontSize=10;"),
            (KEY_WIDTH, TABLE_WIDTH - KEY_WIDTH, label, "align=left;spacingLeft=6;"),
        ):
            self.cells.append(
                f'<mxCell id="{self._id("c")}" value={quoteattr(value)} '
                f'style={quoteattr(cell_style + extra)} vertex="1" parent="{row_id}">'
                f'<mxGeometry x="{offset}" width="{width}" height="{ROW_HEIGHT}" as="geometry">'
                f'<mxRectangle width="{width}" height="{ROW_HEIGHT}" as="alternateBounds"/>'
                f"</mxGeometry></mxCell>"
            )

    def edge(self, source: str, target: str, label: str = "", dashed: bool = False) -> None:
        style = (
            "edgeStyle=entityRelationEdgeStyle;rounded=0;html=1;fontSize=10;"
            "startArrow=ERzeroToMany;startFill=0;endArrow=ERmandOne;endFill=0;"
            "exitX=0;exitY=0.5;entryX=1;entryY=0.5;"
        ) + ("dashed=1;" if dashed else "")
        self.cells.append(
            f'<mxCell id="{self._id("e")}" value={quoteattr(label)} style={quoteattr(style)} '
            f'edge="1" parent="1" source="{source}" target="{target}">'
            f'<mxGeometry relative="1" as="geometry"/></mxCell>'
        )

    def xml(self) -> str:
        return "\n        ".join(self.cells)


def lay_out(canvas: Canvas, page: str, columns: list[list[Table]]) -> None:
    x = 40
    for column in columns:
        y = 40
        for table in column:
            canvas.table(table, x, y, _kind(page, table.name))
            y += table.height + ROW_GAP
        x += TABLE_WIDTH + COLUMN_GAP


def page(name: str, canvas: Canvas) -> str:
    return (
        f"  <diagram id={quoteattr(name.lower().replace(' ', '-'))} name={quoteattr(name)}>\n"
        f'    <mxGraphModel dx="1200" dy="800" grid="1" gridSize="10" guides="1" tooltips="1" '
        f'connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1654" '
        f'pageHeight="2336" math="0" shadow="0">\n'
        f"      <root>\n"
        f'        <mxCell id="0"/><mxCell id="1" parent="0"/>\n'
        f"        {canvas.xml()}\n"
        f"      </root>\n"
        f"    </mxGraphModel>\n"
        f"  </diagram>\n"
    )


def annotate(tables: dict[str, Table]) -> set[tuple[str, str]]:
    """Attach the DERIVED notes, and report which keys this schema matched.

    The return value is the point: DERIVED spans both schemas, so a miss here is normal --
    but a key that matches *neither* is a note pointing at a column that no longer exists,
    and silently dropping it is how a diagram starts describing a schema it does not have.
    ``build`` unions the two results and fails on the remainder.
    """
    matched: set[tuple[str, str]] = set()
    for (table_name, column_name), note in DERIVED.items():
        table = tables.get(table_name)
        if table is None:
            continue
        if (column := table.column(column_name)) is not None:
            column.note = note
            matched.add((table_name, column_name))
    return matched


def build() -> str:
    oltp_sql = "\n".join(p.read_text(encoding="utf-8") for p in sorted(MIGRATIONS.glob("*.up.sql")))
    oltp, references = parse_tables(oltp_sql)
    olap, _ = parse_tables(REDSHIFT_DDL.read_text(encoding="utf-8"))
    if stale := set(DERIVED) - (annotate(oltp) | annotate(olap)):
        raise SystemExit(f"DERIVED notes for columns that no longer exist: {sorted(stale)}")

    business = ["members", "workstations", "concession_items", "rental_transactions",
                "concession_purchases", "concession_order_items", "member_points_ledger"]
    control = ["load_manifest", "load_checkpoint", "pipeline_watermark", "quarantine_records",
               "reconciliation_results", "api_idempotency"]

    missing = [t for t in business + control if t not in oltp]
    if missing:
        raise SystemExit(f"tables missing from the migrations: {missing}")

    # Mark foreign keys the DDL declares but that are only visible on the child side.
    for child, child_column, parent, parent_column in references:
        if parent not in oltp or oltp[parent].column(parent_column) is None:
            raise SystemExit(f"{child}.{child_column} references unknown {parent}.{parent_column}")

    # Page 1 — the operational business tables.
    oltp_canvas = Canvas()
    lay_out(oltp_canvas, "oltp", [
        [oltp["members"], oltp["member_points_ledger"]],
        [oltp["rental_transactions"]],
        [oltp["workstations"], oltp["concession_purchases"]],
        [oltp["concession_items"], oltp["concession_order_items"]],
    ])
    for child, child_column, parent, parent_column in references:
        if child in business and parent in business:
            oltp_canvas.edge(
                oltp_canvas.row_ids[(child, child_column)],
                oltp_canvas.row_ids[(parent, parent_column)],
                child_column,
            )

    # Page 2 — the control plane. No foreign keys on purpose: pipeline state has to survive
    # a reload of the data it describes.
    control_canvas = Canvas()
    lay_out(control_canvas, "control", [
        [oltp["load_manifest"], oltp["load_checkpoint"]],
        [oltp["quarantine_records"], oltp["pipeline_watermark"]],
        [oltp["reconciliation_results"], oltp["api_idempotency"], SCHEMA_MIGRATIONS],
    ])

    # Page 3 — the warehouse.
    olap_canvas = Canvas()
    lay_out(olap_canvas, "olap", [
        [olap["dim_member"], olap["dim_workstation"]],
        [olap["fact_rental"], olap["dim_date"]],
        [olap["fact_concession_sale"], olap["fact_concession_line_item"], olap["dim_time"]],
        [olap["fact_points_activity"], olap["fact_workstation_event"],
         olap["dim_concession_item"]],
        [olap["agg_workstation_utilization_hourly"]],
    ])
    for child, child_column, parent, parent_column, label in OLAP_JOINS:
        for table_name, column_name in ((child, child_column), (parent, parent_column)):
            if olap[table_name].column(column_name) is None:
                raise SystemExit(
                    f"declared join {child}.{child_column} -> {parent}.{parent_column}: "
                    f"{table_name}.{column_name} is not in the Redshift DDL"
                )
        olap_canvas.edge(
            olap_canvas.row_ids[(child, child_column)],
            olap_canvas.row_ids[(parent, parent_column)],
            label or child_column,
            dashed=True,   # logical join: Redshift enforces nothing
        )

    header = (
        "<!-- Generated by scripts/gen_erd_drawio.py from the DDL in src/aimternet/db/.\n"
        "     Edit the SQL and rerun `make erd`; edits made here are overwritten. -->\n"
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        + header
        + '<mxfile host="app.diagrams.net" type="device" version="24.7.17" '
        'agent="aimternet/scripts/gen_erd_drawio.py">\n'
        + page("OLTP · aimternet_oltp", oltp_canvas)
        + page("Control plane", control_canvas)
        + page("OLAP · aimternet_olap", olap_canvas)
        + "</mxfile>\n"
    )


def main() -> int:
    OUTPUT.write_text(build(), encoding="utf-8")
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
