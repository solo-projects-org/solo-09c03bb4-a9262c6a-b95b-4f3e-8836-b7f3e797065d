"""备机补数自测：上传/替换、5 秒规则、冲突不可交付、主机与备机交错、查询一致性。"""
from __future__ import annotations

import threading
import time

import pytest
from fastapi.testclient import TestClient

from tests.test_app import make_client, set_anchors, upload

BK_HEADER = "utc_time,temperature\n"


def backup_upload(client: TestClient, start: str, end: str, rows,
                  header: str | None = BK_HEADER, filename="backup.csv"):
    content = (header or "") + "".join(f"{u},{t}\n" for u, t in rows)
    return client.post(
        "/api/backup",
        files={"file": (filename, content, "text/csv")},
        data={"start": start, "end": end},
    )


def wait_merge_idle(client: TestClient, timeout=10.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        info = client.get("/api/merge/status").json()
        calc = client.get("/api/status").json()["calc"]
        if info["status"] != "running" and not info["pending"] \
                and calc["status"] != "running" and not calc["dirty"]:
            return info
        time.sleep(0.05)
    raise AssertionError("合并/重算未在超时内收敛")


def merged(client: TestClient, date: str, version: int | None = None):
    params = {"date": date}
    if version is not None:
        params["version"] = str(version)
    return client.get("/api/merged", params=params)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as c:
        yield c


def _master_ready(client):
    upload(client, [(1, 100, 4.0), (1, 150, 4.5), (1, 200, 5.0)])
    set_anchors(client, 1, 100, "2026-01-01T08:00:00Z", 200, "2026-01-01T08:10:00Z")


# ---------------------------------------------------------------- 上传/替换

def test_backup_upload_and_replace_is_whole_batch(client):
    _master_ready(client)
    r = backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:02:00Z", 3.0),
                       ("2026-01-01T08:20:00Z", 9.9)])  # 区间外
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["file_id"] == 1 and body["rows_in_interval"] == 1
    assert body["rows_outside_interval"] == 1 and body["conflict_count"] == 0
    day = merged(client, "2026-01-01").json()
    assert day["deliverable"] is True
    assert day["count"] == 4
    assert [r["source"] for r in day["readings"]].count("backup") == 1

    # 更换备机文件：整批生效，旧备机点消失
    r = backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:04:00Z", 7.0)])
    assert r.status_code == 200 and r.json()["file_id"] == 2
    day = merged(client, "2026-01-01").json()
    assert day["backup_file_id"] == 2
    used = [(x["utc_time"], x["temperature"]) for x in day["readings"]
            if x["source"] == "backup"]
    assert used == [("2026-01-01T08:04:00.000Z", 7.0)]
    assert day["count"] == 4  # 旧备机点整批被替换


def test_invalid_backup_keeps_original_file(client):
    _master_ready(client)
    assert backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                         [("2026-01-01T08:02:00Z", 3.0)]).status_code == 200
    before = client.get("/api/merge/status").json()["active_file"]
    # 坏文件：温度非法
    r = backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:03:00Z", 1.0), ("2026-01-01T08:04:00Z", "abc")])
    assert r.status_code == 400 and r.json()["code"] == "CSV_INVALID"
    after = client.get("/api/merge/status").json()["active_file"]
    assert after["id"] == before["id"]
    day = merged(client, "2026-01-01").json()
    assert day["deliverable"] is True
    assert day["backup_file_id"] == before["id"]
    assert [x["temperature"] for x in day["readings"] if x["source"] == "backup"] == [3.0]
    # 坏区间
    r = backup_upload(client, "2026-01-01T08:10:00Z", "2026-01-01T08:00:00Z",
                      [("2026-01-01T08:03:00Z", 1.0)])
    assert r.status_code == 400 and r.json()["code"] == "BAD_INTERVAL"
    assert client.get("/api/merge/status").json()["active_file"]["id"] == before["id"]


def test_backup_point_limit(client):
    _master_ready(client)
    rows = [(f"2026-01-01T09:00:{i % 60:02d}Z", 1.0) for i in range(2001)]
    r = backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T10:00:00Z", rows)
    assert r.status_code == 400 and r.json()["code"] == "CSV_INVALID"
    assert any("2000" in e["message"] for e in r.json()["errors"])


# ---------------------------------------------------------------- 合并规则

def test_host_priority_and_five_second_gap(client):
    _master_ready(client)  # 主机点 08:00、08:05、08:10
    backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z", [
        ("2026-01-01T08:00:00Z", 99.0),   # 与主机同 UTC → 主机优先
        ("2026-01-01T08:00:03Z", 98.0),   # 距主机 3 秒 → 跳过
        ("2026-01-01T08:00:05Z", 97.0),   # 恰好 5 秒 → 跳过（须“超过 5 秒”）
        ("2026-01-01T08:00:05.001Z", 96.0),  # 超过 5 秒 → 补入
        ("2026-01-01T08:02:00Z", 95.0),   # 距主机 60 秒 → 补入
    ])
    day = merged(client, "2026-01-01").json()
    by_time = {r["utc_time"]: r for r in day["readings"]}
    assert by_time["2026-01-01T08:00:00.000Z"]["source"] == "master"
    assert by_time["2026-01-01T08:00:00.000Z"]["temperature"] == 4.0
    assert "2026-01-01T08:00:03.000Z" not in by_time
    assert "2026-01-01T08:00:05.000Z" not in by_time
    assert by_time["2026-01-01T08:00:05.001Z"]["source"] == "backup"
    assert by_time["2026-01-01T08:02:00.000Z"]["source"] == "backup"
    assert day["omitted"]["near_master"] == 3  # 08:00:00、08:00:03、08:00:05
    assert day["backup_count"] == 2 and day["master_count"] == 3


def test_conflict_marks_interval_undeliverable(client):
    _master_ready(client)
    r = backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z", [
        ("2026-01-01T08:02:00Z", 3.0),
        ("2026-01-01T08:02:00Z", 3.5),   # 同 UTC 异温 → 冲突
        ("2026-01-01T08:04:00Z", 4.0),
        ("2026-01-01T08:04:00Z", 4.0),   # 同 UTC 同温 → 不算冲突
    ])
    assert r.status_code == 200 and r.json()["conflict_count"] == 1
    day = merged(client, "2026-01-01").json()
    assert day["deliverable"] is False
    assert day["status"] == "undeliverable"
    assert len(day["conflicts"]) == 1
    c = day["conflicts"][0]
    assert c["utc_time"] == "2026-01-01T08:02:00.000Z"
    assert sorted(d["temperature"] for d in c["details"]) == [3.0, 3.5]
    assert sorted(d["row_no"] for d in c["details"]) == [1, 2]
    # 冲突点不进入合并读数；同温重复只保留最小行号
    times = [r["utc_time"] for r in day["readings"] if r["source"] == "backup"]
    assert times == ["2026-01-01T08:04:00.000Z"]
    dup = next(r for r in day["readings"] if r["source"] == "backup")
    assert dup["row_no"] == 3
    assert day["omitted"]["conflict"] == 2 and day["omitted"]["duplicate"] == 1


def test_backup_outside_interval_never_used(client):
    _master_ready(client)
    backup_upload(client, "2026-01-01T08:02:00Z", "2026-01-01T08:04:00Z", [
        ("2026-01-01T08:01:59Z", 1.0),    # 区间前 1 秒
        ("2026-01-01T08:02:00Z", 2.0),    # 闭区间端点，距主机 2 分钟 → 补入
        ("2026-01-01T08:04:00Z", 3.0),    # 闭区间终点
        ("2026-01-01T08:04:00.001Z", 4.0),  # 区间后
    ])
    day = merged(client, "2026-01-01").json()
    backup = [r for r in day["readings"] if r["source"] == "backup"]
    assert [r["temperature"] for r in backup] == [2.0, 3.0]
    assert day["omitted"]["outside_interval"] == 2


def test_daily_query_lists_source_row_and_anchor_version(client):
    _master_ready(client)
    backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                  [("2026-01-01T08:02:00Z", 3.0), ("2026-01-01T08:03:00Z", 3.1)])
    day = merged(client, "2026-01-01").json()
    m = next(r for r in day["readings"] if r["source"] == "master")
    assert m["batch_id"] == 1 and m["row_no"] == 1 and m["anchor_version"] == 1
    b = next(r for r in day["readings"] if r["source"] == "backup")
    assert b["row_no"] == 1 and b["backup_file_id"] == 1 and b["anchor_version"] is None
    # 原按日供数不受影响
    original = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert original["count"] == 3
    assert all("source" not in r or r["source"] == "master" for r in original["readings"])


# ---------------------------------------------------------------- 交错 / 一致性

def test_master_recalc_in_flight_old_merge_readonly(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch, worker="1") as c:
        _master_ready(c)
        backup_upload(c, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:02:00Z", 3.0)])
        wait_merge_idle(c)
        v_merge = merged(c, "2026-01-01").json()
        assert v_merge["deliverable"] is True

        from app import calibration
        orig = calibration.compute_calibration
        release = threading.Event()

        def slow(readings, anchors):
            release.wait(5)
            return orig(readings, anchors)

        monkeypatch.setattr(calibration, "compute_calibration", slow)
        upload(c, [(1, 160, 6.0)])  # 触发主机重算，卡在计算中
        t0 = time.time()
        while time.time() - t0 < 5:
            if c.get("/api/status").json()["calc"]["status"] == "running":
                break
            time.sleep(0.02)
        # 主机未发布新版：旧合并结果只能回看
        old = merged(c, "2026-01-01").json()
        assert old["deliverable"] is False
        assert old["version"] == v_merge["version"]
        assert any("尚未重算" in r for r in old["stale_reasons"])
        assert old["master_calc_version"] == v_merge["master_calc_version"]
        # 旧合并数据完整且未被混入新主机点
        assert old["count"] == v_merge["count"]
        assert "2026-01-01T08:00:06.000Z" not in {r["utc_time"] for r in old["readings"]}

        release.set()
        wait_merge_idle(c)
        new = merged(c, "2026-01-01").json()
        assert new["version"] > v_merge["version"]
        assert new["deliverable"] is True
        assert new["master_calc_version"] == c.get("/api/status").json()["calc"]["version"]
        # 主机新点（本地 160 → 08:06）进入合并结果
        assert "2026-01-01T08:06:00.000Z" in {r["utc_time"] for r in new["readings"]}
        # 历史版本仍可回看
        hist = merged(c, "2026-01-01", version=v_merge["version"]).json()
        assert hist["historical"] is True and hist["deliverable"] is False
        assert hist["count"] == v_merge["count"]


def test_merge_interleaved_with_backup_replace(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch, worker="1") as c:
        _master_ready(c)
        backup_upload(c, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:02:00Z", 1.0)])
        wait_merge_idle(c)
        first = merged(c, "2026-01-01").json()
        assert first["backup_file_id"] == 1

        # 让合并变慢，然后在合并进行中替换备机文件
        import app.merge as merge_mod
        orig_build = merge_mod.compute_merged
        started = threading.Event()
        release = threading.Event()

        def slow_build(*a, **kw):
            result = orig_build(*a, **kw)
            started.set()
            release.wait(5)
            return result

        monkeypatch.setattr(merge_mod, "compute_merged", slow_build)
        backup_upload(c, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:06:00Z", 2.0)])
        assert started.wait(5)
        # 合并进行中替换备机文件
        r = backup_upload(c, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                          [("2026-01-01T08:07:00Z", 3.0)])
        assert r.status_code == 200 and r.json()["file_id"] == 3
        release.set()
        wait_merge_idle(c)

        final = merged(c, "2026-01-01").json()
        assert final["backup_file_id"] == 3
        assert {r["temperature"] for r in final["readings"] if r["source"] == "backup"} == {3.0}
        # 每个合并版本只对应一个备机文件：不存在文件 1/2/3 混用的行
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        conn.row_factory = sqlite3.Row
        versions = conn.execute(
            "SELECT version, backup_file_id, master_calc_version FROM merged_results"
        ).fetchall()
        for v in versions:
            fids = {row[0] for row in conn.execute(
                "SELECT DISTINCT backup_file_id FROM merged_readings"
                " WHERE merged_version = ? AND source = 'backup'", (v["version"],))}
            assert fids <= {v["backup_file_id"]}
            # 主机行记录的校时版本须与合并结果记录一致
            master_versions = {row[0] for row in conn.execute(
                "SELECT DISTINCT calc_version FROM merged_readings"
                " WHERE merged_version = ? AND source = 'master'", (v["version"],))}
            assert master_versions <= {v["master_calc_version"]}
            assert None not in master_versions
        conn.close()


def test_merge_snapshot_never_mixed_under_concurrent_writes(tmp_path, monkeypatch):
    """并发：主机上传 / 备机替换 / 查询同时发生，查询结果始终来自单版本。"""
    with make_client(tmp_path, monkeypatch, worker="1", delay="0.05") as c:
        _master_ready(c)
        backup_upload(c, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:02:00Z", 10.0)])
        wait_merge_idle(c)
        stop = threading.Event()
        errors: list[str] = []

        def reader():
            while not stop.is_set():
                body = merged(c, "2026-01-01").json()
                if not body.get("available"):
                    continue
                # 同一响应内：备机行只来自声明的那个备机文件
                fids = {r["backup_file_id"] for r in body["readings"]
                        if r["source"] == "backup" and r["backup_file_id"]}
                if fids - {body["backup_file_id"]}:
                    errors.append(f"混合备机文件: {fids} vs {body['backup_file_id']}")
                # 同一响应内：主机行只来自声明的那一个主机发布版
                cvs = {r["calc_version"] for r in body["readings"]
                       if r["source"] == "master"}
                if cvs - {body["master_calc_version"]}:
                    errors.append(f"混合主机版本: {cvs} vs {body['master_calc_version']}")

        def writer_backup():
            for i in range(6):
                backup_upload(c, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                              [("2026-01-01T08:02:00Z", float(10 + i)),
                               (f"2026-01-01T08:0{3 + i % 5}:30Z", float(i))])
                time.sleep(0.02)

        def writer_master():
            for i in range(5):
                upload(c, [(1, 160 + i, float(i))])
                time.sleep(0.03)

        threads = [threading.Thread(target=reader),
                   threading.Thread(target=writer_backup),
                   threading.Thread(target=writer_master)]
        for t in threads:
            t.start()
        threads[1].join()
        threads[2].join()
        stop.set()
        threads[0].join()
        wait_merge_idle(c)
        assert not errors, errors[:3]
        final = merged(c, "2026-01-01").json()
        assert final["deliverable"] is True
        assert final["backup_file_id"] == 7  # 初始 1 个 + 并发 6 个
        # 最终合并结果同时包含最新主机点与最新备机文件
        assert final["master_calc_version"] == c.get("/api/status").json()["calc"]["version"]
        times = {r["utc_time"] for r in final["readings"]}
        assert "2026-01-01T08:06:24.000Z" in times  # 最后一个主机点（本地 164）
        backup_rows = {r["utc_time"]: r for r in final["readings"] if r["source"] == "backup"}
        assert backup_rows["2026-01-01T08:02:00.000Z"]["temperature"] == 15.0
        assert backup_rows["2026-01-01T08:03:30.000Z"]["temperature"] == 5.0
        assert all(r["backup_file_id"] == 7 for r in backup_rows.values())


def test_merge_restart_recovery(tmp_path, monkeypatch):
    from app import calibration
    from app.main import create_app

    monkeypatch.setenv("DB_PATH", str(tmp_path / "m.db"))
    monkeypatch.setenv("WORKER_ENABLED", "0")
    orig = calibration.RecalcWorker.run_until_quiet
    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", lambda self: None)
    with TestClient(create_app()) as c1:
        _master_ready(c1)
        backup_upload(c1, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                      [("2026-01-01T08:02:00Z", 3.0)])
        assert c1.get("/api/merge/status").json()["pending"] is True
        assert merged(c1, "2026-01-01").json()["available"] is False
    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", orig)
    monkeypatch.setenv("WORKER_ENABLED", "1")
    with TestClient(create_app()) as c2:
        wait_merge_idle(c2)
        body = merged(c2, "2026-01-01").json()
        assert body["available"] is True and body["deliverable"] is True
        assert body["count"] == 4


# ---------------------------------------------------------------- 页面

def test_backup_pages_render(client):
    _master_ready(client)
    backup_upload(client, "2026-01-01T08:00:00Z", "2026-01-01T08:10:00Z",
                  [("2026-01-01T08:02:00Z", 3.0), ("2026-01-01T08:03:00Z", 3.1)])
    for path in ["/backup", "/merged?date=2026-01-01"]:
        r = client.get(path)
        assert r.status_code == 200 and "text/html" in r.headers["content-type"], path
    html = client.get("/merged?date=2026-01-01").text
    assert "来源" in html and "原行号" in html and "主机锚点版本" in html
    assert "备机" in html
