#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_crosswalk.py — asac_axes 공용 seed 3종 생성기 (issue #48).

행안부 10자리 행정동 코드를 canonical 키로, 통계청 7/5자리 코드를 alias 로 잇는
서울 행정동 crosswalk 와, 여기에서 파생된 행정동/구 경계 seed 를 생성한다.

소스는 CLI로 주입한다. 공용 package는 consumer domain의 디렉터리를 알지 않는다.

  A) --weather-grid
       place_id(seoul_admd_<행안부10>), place_name, gu, admin_dong,
       latitude, longitude, mapping_method(snapshot 표기), source_admin_code(행안부10)
  B) --dong-boundary
       sigungu, sigungu_code(통계청5), dong, dong_code(통계청7), boundary_wkt
  C) --gu-boundary
       sigungu, sigungu_eng, sigungu_code(통계청5), boundary_wkt

조인 키: (구명, 동명)  =  A.gu / A.admin_dong  ==  B.sigungu / B.dong
  - 동명 표기가 소스마다 달라 정규화가 필요하다:
      * A(행안부): 'N동' 서수를 '제N동' 으로 씀 (예: 고덕제1동)
      * B(통계청): 'N동' 바로 씀 (예: 고덕1동)
      * 숫자 구분자: A 는 '.', B 는 '·' (예: 종로1.2.3.4가동 / 종로1·2·3·4가동)
  - '제' 제거는 A 쪽에만 적용한다. B 의 '홍제1동' 처럼 이름 자체에 '제' 가
    들어간 경우( '홍제' + '1동' )까지 지우면 오매칭되기 때문. A 는 항상 '제N',
    B 는 항상 'N' 이라는 비대칭을 이용한 안전한 정규화다.

출력 (packages/asac_axes/seeds/)
  1) seoul_admin_dong_crosswalk.csv
  2) seoul_admin_dong_boundary.csv   (B 복사 + 행안부 admin_dong_code/gu_code 부가)
  3) seoul_gu_boundary.csv           (C 복사 + 행안부 gu_code 부가)

미매칭 행은 버리지 않고 stderr 리포트로 남긴다(양쪽). 재실행 시 결정적 결과.
"""

import argparse
import csv
import io
import os
import re
import sys

DEFAULT_OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "seeds")
)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Build deterministic asac_axes crosswalk and boundary seeds."
    )
    parser.add_argument("--weather-grid", required=True)
    parser.add_argument("--dong-boundary", required=True)
    parser.add_argument("--gu-boundary", required=True)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    return parser


def read_csv(path):
    # 두 소스 모두 UTF-8. BOM 방어를 위해 utf-8-sig 로 읽는다.
    with io.open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _strip_sep(s):
    s = re.sub(r"\s+", "", (s or "").strip())
    for ch in ("·", "・", "ㆍ", "."):
        s = s.replace(ch, "")
    return s


def norm_gu(s):
    return re.sub(r"\s+", "", (s or "").strip())


def norm_dong_admin(s):
    # 행안부(A): 서수 '제N' -> 'N' 로 낮춘 뒤 구분자 제거
    s = re.sub(r"\s+", "", (s or "").strip())
    s = re.sub(r"제(\d)", r"\1", s)
    return _strip_sep(s)


def norm_dong_stat(s):
    # 통계청(B): 구분자만 제거 (이미 서수 'N' 형태)
    return _strip_sep(s)


def write_csv(path, header, rows):
    with io.open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rows)


def main(argv=None):
    args = build_parser().parse_args(argv)
    seed_out = os.path.abspath(args.output_dir)
    os.makedirs(seed_out, exist_ok=True)

    A = read_csv(args.weather_grid)
    B = read_csv(args.dong_boundary)
    C = read_csv(args.gu_boundary)

    # gu -> 행안부 5자리 (source_admin_code 앞 5). gu 당 유일함을 검증.
    gu_to_hgu = {}
    for r in A:
        g = norm_gu(r["gu"])
        code5 = r["source_admin_code"].strip()[:5]
        gu_to_hgu.setdefault(g, set()).add(code5)
    conflict = {g: v for g, v in gu_to_hgu.items() if len(v) != 1}
    if conflict:
        sys.stderr.write("[FATAL] gu->행안부5 코드 충돌: %r\n" % conflict)
        sys.exit(2)
    gu_to_hgu = {g: sorted(v)[0] for g, v in gu_to_hgu.items()}

    # B 를 (구, 동) 정규화 키로 인덱싱
    Bidx = {}
    for r in B:
        key = (norm_gu(r["sigungu"]), norm_dong_stat(r["dong"]))
        Bidx.setdefault(key, []).append(r)
    dup = {k: len(v) for k, v in Bidx.items() if len(v) > 1}
    if dup:
        sys.stderr.write("[WARN] B(경계) 정규화 후 중복 키: %r\n" % dup)

    # crosswalk 조립
    cross_header = [
        "admin_dong_code",
        "gu_code",
        "stat_dong_code",
        "stat_gu_code",
        "gu",
        "admin_dong",
        "latitude",
        "longitude",
        "snapshot_ref",
    ]
    cross_rows = []
    matched_stat_dong = set()  # 매칭된 B.dong_code
    unmatched_A = []
    for r in A:
        akey = (norm_gu(r["gu"]), norm_dong_admin(r["admin_dong"]))
        adcode = r["source_admin_code"].strip()
        gcode = adcode[:5]
        bmatch = Bidx.get(akey)
        if not bmatch:
            unmatched_A.append((r["gu"], r["admin_dong"], adcode))
            continue
        b = bmatch[0]
        matched_stat_dong.add(b["dong_code"].strip())
        cross_rows.append(
            [
                adcode,
                gcode,
                b["dong_code"].strip(),
                b["sigungu_code"].strip(),
                r["gu"].strip(),
                r["admin_dong"].strip(),
                r["latitude"].strip(),
                r["longitude"].strip(),
                r.get("mapping_method", "").strip(),
            ]
        )

    # B 미매칭
    Akeys = set((norm_gu(a["gu"]), norm_dong_admin(a["admin_dong"])) for a in A)
    unmatched_B = []
    for r in B:
        bkey = (norm_gu(r["sigungu"]), norm_dong_stat(r["dong"]))
        if bkey not in Akeys:
            unmatched_B.append((r["sigungu"], r["dong"], r["dong_code"].strip()))

    cross_rows.sort(key=lambda x: x[0])
    write_csv(
        os.path.join(seed_out, "seoul_admin_dong_crosswalk.csv"),
        cross_header,
        cross_rows,
    )

    # ---- boundary seed: B 복사 + 행안부 admin_dong_code / gu_code 부가 ----
    # admin_dong_code 는 매칭된 경우만, gu_code(행안부5)는 구명으로 항상 부가.
    dong_to_admin = {}  # (구norm, 동norm) -> admin_dong_code
    for row in cross_rows:
        dong_to_admin[(norm_gu(row[4]), norm_dong_stat(row[5]))] = row[0]
    # A 표기 기준으로도 조회 가능해야 하므로 B 정규화 키로 다시 인덱싱
    # (cross_rows[5]=admin_dong 은 A 표기 → norm_dong_admin 로 맞춘다)
    dong_to_admin = {}
    for row in cross_rows:
        dong_to_admin[(norm_gu(row[4]), norm_dong_admin(row[5]))] = row[0]

    b_header = list(B[0].keys()) + ["admin_dong_code", "gu_code"]
    b_rows = []
    for r in B:
        gnorm = norm_gu(r["sigungu"])
        bkey = (gnorm, norm_dong_stat(r["dong"]))
        admin_code = dong_to_admin.get(bkey, "")
        gu_code = gu_to_hgu.get(gnorm, "")
        b_rows.append([r[k] for k in B[0].keys()] + [admin_code, gu_code])
    write_csv(os.path.join(seed_out, "seoul_admin_dong_boundary.csv"), b_header, b_rows)

    # ---- gu boundary seed: C 복사 + 행안부 gu_code 부가 ----
    c_header = list(C[0].keys()) + ["gu_code"]
    c_rows = []
    unmatched_gu = []
    for r in C:
        gnorm = norm_gu(r["sigungu"])
        gu_code = gu_to_hgu.get(gnorm, "")
        if not gu_code:
            unmatched_gu.append(r["sigungu"])
        c_rows.append([r[k] for k in C[0].keys()] + [gu_code])
    write_csv(os.path.join(seed_out, "seoul_gu_boundary.csv"), c_header, c_rows)

    # ---- 리포트 (stderr) ----
    e = sys.stderr
    e.write("=== asac_axes crosswalk build report ===\n")
    e.write("A(weather) rows      : %d\n" % len(A))
    e.write("B(dong boundary)     : %d\n" % len(B))
    e.write("C(gu boundary)       : %d\n" % len(C))
    e.write("crosswalk rows       : %d\n" % len(cross_rows))
    e.write("unmatched A (weather): %d\n" % len(unmatched_A))
    for g, d, c in unmatched_A:
        e.write("    A> %s | %s | %s\n" % (g, d, c))
    e.write("unmatched B (bound.) : %d\n" % len(unmatched_B))
    for g, d, c in unmatched_B:
        e.write("    B> %s | %s | %s\n" % (g, d, c))
    e.write("unmatched gu         : %d %r\n" % (len(unmatched_gu), unmatched_gu))
    e.write(
        "boundary rows w/ admin_dong_code: %d / %d\n"
        % (sum(1 for row in b_rows if row[-2]), len(b_rows))
    )
    e.write(
        "gu boundary rows     : %d (all coded=%s)\n"
        % (len(c_rows), all(row[-1] for row in c_rows))
    )
    e.write("outputs -> %s\n" % seed_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
