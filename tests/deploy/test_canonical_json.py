from deployment.canonical_json import canonical_bytes, sha256_hex


def test_canonical_bytes_are_utf8_sorted_and_compact():
    assert canonical_bytes({"b": 2, "a": "한글"}) == (
        b'{"a":"\xed\x95\x9c\xea\xb8\x80","b":2}'
    )


def test_sha256_hex_uses_lowercase_digest():
    assert sha256_hex(b"weather") == (
        "e5e72beb4e3c6926d3dc9e3e2ef7833ba50cd919c2460a782b244fd071e920de"
    )
