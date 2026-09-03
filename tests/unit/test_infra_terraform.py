"""The Terraform in `infra/` has to describe the same tables the loader creates.

`make bootstrap` creates the DynamoDB tables through boto3 so the pipeline never depends on
Terraform being installed, and `infra/dynamodb.tf` adopts them with an `import` block. Two
descriptions of the same thing drift; this is the test that notices. It reads the HCL as
text — no terraform binary, no HCL parser, nothing to install — because the whole point is
that it runs in the ordinary `make test`.

It also guards the invariant the operating contract cares about most: nothing in `infra/`
may declare the S3 bucket, the RDS instance or the Redshift cluster as a resource. Terraform
may configure them; it may never be in a position to delete them.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from aimternet.pipeline.loaders.dynamodb import _table_definitions

INFRA = Path(__file__).resolve().parents[2] / "infra"


def _tf_files() -> list[Path]:
    return sorted(INFRA.glob("*.tf"))


def _resource_blocks(text: str) -> list[tuple[str, str, str]]:
    """Return (type, name, body) for every top-level `resource` block."""
    blocks = []
    for match in re.finditer(r'^resource\s+"([^"]+)"\s+"([^"]+)"\s*\{', text, re.MULTILINE):
        depth, i = 0, match.end() - 1
        while i < len(text):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        blocks.append((match.group(1), match.group(2), text[match.end() : i]))
    return blocks


@pytest.fixture(scope="module")
def resources() -> dict[str, str]:
    found = {}
    for path in _tf_files():
        for rtype, name, body in _resource_blocks(path.read_text()):
            found[f"{rtype}.{name}"] = body
    return found


def test_infra_directory_exists() -> None:
    assert _tf_files(), "infra/ has no .tf files"


# --------------------------------------------------------------- what must never be there


FORBIDDEN_RESOURCE_TYPES = {
    "aws_s3_bucket": "the bucket holds 4.5 GB of Bronze/Silver/Gold; configure it, never own it",
    "aws_db_instance": "RDS is pre-existing and shared with simple_oltp and bus_ticketing",
    "aws_rds_cluster": "same",
    "aws_redshift_cluster": "Redshift is pre-existing and shared with krusty_krab_olap",
    "aws_redshiftserverless_namespace": "same",
    "aws_redshiftserverless_workgroup": "same",
    # Not a cluster resource, so the rule above does not catch it -- but it modifies the
    # shared cluster's attached roles, which is exactly what invariant 10 forbids. Absent by
    # omission until now; absent by decision from here. The COPY role is created by Terraform
    # and attached by hand (docs/runbook.md).
    "aws_redshift_cluster_iam_roles": "attaching a role modifies a cluster we do not own",
}


def test_no_resource_can_destroy_shared_infrastructure(resources: dict[str, str]) -> None:
    declared = {key.split(".", 1)[0] for key in resources}
    offending = {t: why for t, why in FORBIDDEN_RESOURCE_TYPES.items() if t in declared}
    assert not offending, (
        f"infra/ declares resources it must not own: {offending}. "
        "A configuration that *could* delete a shared database eventually does."
    )


def test_bucket_is_referenced_as_a_data_source(resources: dict[str, str]) -> None:
    """Configuring the bucket is fine; the reference has to be read-only."""
    text = "".join(p.read_text() for p in _tf_files())
    assert re.search(r'data\s+"aws_s3_bucket"\s+"lake"', text)
    # ...and the config sub-resources must all hang off that data source.
    for name in (
        "aws_s3_bucket_versioning.lake",
        "aws_s3_bucket_public_access_block.lake",
        "aws_s3_bucket_lifecycle_configuration.lake",
    ):
        assert name in resources, f"{name} is missing"
        assert "data.aws_s3_bucket.lake.id" in resources[name]


def test_dynamodb_tables_cannot_be_destroyed(resources: dict[str, str]) -> None:
    for name in ("aws_dynamodb_table.events", "aws_dynamodb_table.telemetry"):
        body = resources[name]
        assert re.search(r"lifecycle\s*\{[^}]*prevent_destroy\s*=\s*true", body, re.S), (
            f"{name} is missing `lifecycle {{ prevent_destroy = true }}`"
        )


def _without_comments(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def test_no_hardcoded_account_or_bucket_names() -> None:
    """Spec §8: never hardcode account ids, bucket names, hosts or credentials."""
    for path in _tf_files():
        code = _without_comments(path.read_text())
        assert not re.search(r"\b\d{12}\b", code), f"{path.name} contains a bare 12-digit id"
        assert "amazonaws.com:" not in code, f"{path.name} looks like it hardcodes a host"
        for secret in ("access_key", "secret_key", "password"):
            assert not re.search(rf"^\s*{secret}\s*=", code, re.MULTILINE), (
                f"{path.name} assigns {secret}; credentials come from the environment"
            )


# ----------------------------------------------------- the schemas have to agree with boto3


def _definition(kind: str) -> dict:
    """The loader's create_table kwargs for one table, by shape rather than by name."""
    definitions = _table_definitions()
    with_gsi = [d for d in definitions if "GlobalSecondaryIndexes" in d]
    without = [d for d in definitions if "GlobalSecondaryIndexes" not in d]
    return (with_gsi if kind == "events" else without)[0]


def _hcl_attributes(body: str) -> set[tuple[str, str]]:
    return {
        (m.group(1), m.group(2))
        for m in re.finditer(
            r'attribute\s*\{\s*name\s*=\s*"([^"]+)"\s*type\s*=\s*"([^"]+)"\s*\}', body
        )
    }


def _boto_attributes(definition: dict) -> set[tuple[str, str]]:
    return {(a["AttributeName"], a["AttributeType"]) for a in definition["AttributeDefinitions"]}


def _boto_keys(key_schema: list[dict]) -> tuple[str, str | None]:
    keys = {k["KeyType"]: k["AttributeName"] for k in key_schema}
    return keys["HASH"], keys.get("RANGE")


@pytest.mark.parametrize("kind", ["events", "telemetry"])
def test_key_schema_matches_the_loader(resources: dict[str, str], kind: str) -> None:
    body = resources[f"aws_dynamodb_table.{kind}"]
    definition = _definition(kind)

    hash_key, range_key = _boto_keys(definition["KeySchema"])
    assert re.search(rf'^\s*hash_key\s*=\s*"{hash_key}"', body, re.MULTILINE)
    assert re.search(rf'^\s*range_key\s*=\s*"{range_key}"', body, re.MULTILINE)

    assert _hcl_attributes(body) == _boto_attributes(definition)
    assert re.search(r'billing_mode\s*=\s*"PAY_PER_REQUEST"', body), (
        "on-demand billing is the documented choice for a one-burst-then-idle load"
    )


def test_global_secondary_indexes_match_the_loader(resources: dict[str, str]) -> None:
    body = resources["aws_dynamodb_table.events"]
    expected = {
        gsi["IndexName"]: (*_boto_keys(gsi["KeySchema"]), gsi["Projection"]["ProjectionType"])
        for gsi in _definition("events")["GlobalSecondaryIndexes"]
    }

    found = {}
    for match in re.finditer(r"global_secondary_index\s*\{(.*?)\n  \}", body, re.S):
        block = match.group(1)
        name = re.search(r'name\s*=\s*"([^"]+)"', block).group(1)
        found[name] = (
            re.search(r'hash_key\s*=\s*"([^"]+)"', block).group(1),
            re.search(r'range_key\s*=\s*"([^"]+)"', block).group(1),
            re.search(r'projection_type\s*=\s*"([^"]+)"', block).group(1),
        )

    assert found == expected


def test_telemetry_ttl_points_at_expires_at(resources: dict[str, str]) -> None:
    """Finding F2: the attribute is always declared; only `enabled` is a decision."""
    body = resources["aws_dynamodb_table.telemetry"]
    ttl = re.search(r"ttl\s*\{(.*?)\}", body, re.S).group(1)
    assert 'attribute_name = "expires_at"' in ttl
    assert "var.ddb_ttl_enabled" in ttl, "TTL must stay a variable, not a hardcoded true"


def test_ttl_defaults_to_disabled() -> None:
    variables = (INFRA / "variables.tf").read_text()
    block = re.search(r'variable\s+"ddb_ttl_enabled"\s*\{(.*?)\n\}', variables, re.S).group(1)
    assert re.search(r"default\s*=\s*false", block), (
        "expires_at is timestamp+7d over a window already in the past (F2); "
        "defaulting TTL on would delete ~61 of 62 days"
    )
