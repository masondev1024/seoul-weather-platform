from __future__ import annotations

import pytest

from common.serving.content_identity import d1_content_hash


COLUMNS = [
    ("product_row_id", "varchar"),
    ("flag", "boolean"),
    ("speed", "double"),
    ("label", "varchar"),
    ("missing", "varchar"),
]
ROWS = [
    {
        "product_row_id": "b",
        "flag": False,
        "speed": 0.1,
        "label": "가",
        "missing": "",
    },
    {
        "product_row_id": "a",
        "flag": True,
        "speed": 1.5,
        "label": "e\u0301",
        "missing": None,
    },
]
GOLDEN_HASH = "debb35f14d1b9edde93420d07317e16c0ea9c9b8230e55307ea85731743ed3a1"


def test_d1_content_hash_has_golden_sqlite_affinity_representation():
    assert (
        d1_content_hash(
            namespace="gold_weather_place_current_outlook",
            columns=COLUMNS,
            rows=ROWS,
            primary_key=("product_row_id",),
        )
        == GOLDEN_HASH
    )


def test_d1_content_hash_is_row_order_invariant_and_unicode_normalized():
    nfc_rows = [
        {**ROWS[1], "label": "é"},
        ROWS[0],
    ]

    assert d1_content_hash(
        namespace="gold_weather_place_current_outlook",
        columns=COLUMNS,
        rows=list(reversed(nfc_rows)),
        primary_key=("product_row_id",),
    ) == d1_content_hash(
        namespace="gold_weather_place_current_outlook",
        columns=COLUMNS,
        rows=nfc_rows,
        primary_key=("product_row_id",),
    )


@pytest.mark.parametrize(
    "columns,rows,namespace",
    [
        ([("product_row_id", "varchar"), ("label", "varchar")], ROWS, "gold_weather_place_current_outlook"),
        (COLUMNS, [{**ROWS[0], "flag": True}, ROWS[1]], "gold_weather_place_current_outlook"),
        (COLUMNS, [{**ROWS[0], "speed": 0.2}, ROWS[1]], "gold_weather_place_current_outlook"),
        (COLUMNS, [{**ROWS[0], "missing": None}, ROWS[1]], "gold_weather_place_current_outlook"),
        (COLUMNS, ROWS, "gold_weather_place_precipitation_window"),
    ],
)
def test_d1_content_hash_changes_on_column_type_value_null_or_namespace_mutation(columns, rows, namespace):
    assert d1_content_hash(
        namespace=namespace,
        columns=columns,
        rows=rows,
        primary_key=("product_row_id",),
    ) != GOLDEN_HASH


@pytest.mark.parametrize(
    "rows,error",
    [
        ([{**ROWS[0], "product_row_id": None}], "primary key"),
        ([ROWS[0], {**ROWS[1], "product_row_id": "b"}], "duplicate primary key"),
        ([{key: value for key, value in ROWS[0].items() if key != "label"}], "absent projected column"),
        ([{**ROWS[0], "label": {"nested": "value"}}], "unsupported"),
        ([{**ROWS[0], "flag": "true"}], "INTEGER"),
    ],
)
def test_d1_content_hash_fails_closed_on_invalid_rows(rows, error):
    with pytest.raises(ValueError, match=error):
        d1_content_hash(
            namespace="gold_weather_place_current_outlook",
            columns=COLUMNS,
            rows=rows,
            primary_key=("product_row_id",),
        )


def test_d1_content_hash_rejects_missing_or_unsafe_primary_key():
    with pytest.raises(ValueError, match="primary_key"):
        d1_content_hash(namespace="x", columns=COLUMNS, rows=ROWS, primary_key=())

    with pytest.raises(ValueError, match="primary_key"):
        d1_content_hash(namespace="x", columns=COLUMNS, rows=ROWS, primary_key=("product_row_id;drop",))
