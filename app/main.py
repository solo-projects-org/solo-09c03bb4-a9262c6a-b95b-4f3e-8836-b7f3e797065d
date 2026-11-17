"""FastAPI 入口：网页（上传 / 校时 / 按日查询 / 处理状态）+ JSON API。"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import merge, services
from .backup import parse_backup_csv
from .calibration import MergeWorker, RecalcWorker
from .config import Settings
from .csvparse import parse_csv_text
from .db import Database
from .services import ServiceError
from .timeutil import parse_utc

logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

MAX_UPLOAD_BYTES = 2_000_000  # 2000 点远小于此，超限直接拒绝


def create_app() -> FastAPI:
    cfg = Settings.from_env()
    db = Database(cfg.db_path)
    merge_worker = MergeWorker(db, delay=cfg.merge_delay)
    worker = RecalcWorker(db, delay=cfg.recalc_delay,
                          on_commit=lambda: merge_worker.trigger())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if cfg.worker_enabled:
            merge_worker.start()
            worker.start()
        worker.trigger()   # 重启恢复：若有未重算的变更，启动后立即补算
        merge_worker.trigger()  # 重启恢复：补齐滞后的合并结果
        yield
        worker.stop()
        merge_worker.stop()
        db.close()

    app = FastAPI(title="冷链记录仪校时平台", lifespan=lifespan)
    app.state.cfg = cfg
    app.state.db = db
    app.state.worker = worker
    app.state.merge_worker = merge_worker

    # ------------------------------------------------------------ 公共

    def render(request: Request, name: str, **ctx):
        return templates.TemplateResponse(request=request, name=name, context=ctx)

    def trigger_recalc():
        """启用后台线程时异步触发；否则同步重算（便于测试与小规模部署）。"""
        if cfg.worker_enabled:
            worker.trigger()
        else:
            worker.run_until_quiet()
            merge_worker.run_until_quiet()

    async def handle_backup_upload(
        file: UploadFile, start_raw: str, end_raw: str
    ) -> dict:
        try:
            start_dt = parse_utc(start_raw)
            end_dt = parse_utc(end_raw)
        except ValueError as exc:
            raise ServiceError(str(exc), code="BAD_INTERVAL")
        if start_dt > end_dt:
            raise ServiceError("断流区间起点不能晚于终点", code="BAD_INTERVAL")
        raw = await file.read()
        if len(raw) > MAX_UPLOAD_BYTES:
            raise ServiceError("文件过大（上限 2MB）", code="FILE_TOO_LARGE")
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise ServiceError("文件编码须为 UTF-8", code="BAD_ENCODING")
        rows, errors = parse_backup_csv(text, cfg.max_points)
        if errors:
            raise ServiceError(
                f"备机 CSV 校验失败（{len(errors)} 处），整批未生效，仍使用原备机文件",
                code="CSV_INVALID", errors=errors,
            )
        try:
            result = merge.replace_backup_file(
                db, file.filename or "", start_dt, end_dt, rows, cfg.max_points
            )
        except ValueError as exc:
            raise ServiceError(str(exc), code="BAD_BACKUP")
        trigger_merge()
        return result

    def trigger_merge():
        """合并跟随主机重算完成；同步模式下由这里显式补齐。"""
        if cfg.worker_enabled:
            merge_worker.trigger()
        else:
            merge_worker.run_until_quiet()

    def error_json(exc: ServiceError) -> JSONResponse:
        body = {"detail": exc.message, "code": exc.code}
        if exc.errors:
            body["errors"] = exc.errors
        if exc.current_version is not None or exc.code == "VERSION_CONFLICT":
            body["current_version"] = exc.current_version
        return JSONResponse(status_code=exc.status, content=body)

    async def handle_upload(file: UploadFile) -> tuple[int, int]:
        raw = await file.read()
        if len(raw) > MAX_UPLOAD_BYTES:
            raise ServiceError("文件过大（上限 2MB）", code="FILE_TOO_LARGE")
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise ServiceError("文件编码须为 UTF-8", code="BAD_ENCODING")
        rows, errors = parse_csv_text(text, cfg.max_points)
        if errors:
            raise ServiceError(f"CSV 校验失败（{len(errors)} 处），整批未入库",
                               code="CSV_INVALID", errors=errors)
        batch_id = services.add_batch(db, cfg, file.filename or "", rows)
        trigger_recalc()
        return batch_id, len(rows)

    def redirect(path: str, msg: str, ok: bool = True) -> RedirectResponse:
        return RedirectResponse(
            url=f"{path}?msg={quote(msg)}&ok={1 if ok else 0}", status_code=303
        )

    # ------------------------------------------------------------ 网页

    @app.get("/", response_class=HTMLResponse)
    def status_page(request: Request, msg: str = "", ok: int = 1):
        st = services.get_status(db, cfg)
        m = merge.merge_info(db)
        return render(request, "status.html", st=st, merge_info=m, msg=msg, ok=ok,
                      refresh=st["calc"]["status"] == "running"
                      or m["status"] == "running")

    @app.post("/recalc")
    def recalc_html():
        trigger_recalc()
        return redirect("/", "已触发重算")

    @app.get("/upload", response_class=HTMLResponse)
    def upload_page(request: Request, msg: str = "", ok: int = 1):
        return render(request, "upload.html", cfg=cfg, msg=msg, ok=ok)

    @app.post("/upload", response_class=HTMLResponse)
    async def upload_do(request: Request, file: UploadFile = File(...)):
        try:
            batch_id, n = await handle_upload(file)
        except ServiceError as exc:
            return render(request, "upload.html", cfg=cfg, error=exc, msg="", ok=1)
        return redirect("/upload", f"批次 {batch_id} 已接纳 {n} 行")

    @app.get("/anchors", response_class=HTMLResponse)
    def anchors_page(request: Request, msg: str = "", ok: int = 1,
                     segment: int | None = None):
        anchors = services.list_anchors(db)
        prefill = None
        if segment is not None:
            prefill = next((a for a in anchors if a["segment"] == segment), None)
        return render(request, "anchors.html", anchors=anchors, prefill=prefill,
                      msg=msg, ok=ok)

    @app.post("/anchors", response_class=HTMLResponse)
    async def anchors_do(request: Request):
        form = await request.form()
        try:
            seg = int(str(form.get("segment", "")).strip())
            local1 = int(str(form.get("local1", "")).strip())
            local2 = int(str(form.get("local2", "")).strip())
            utc1 = parse_utc(form.get("utc1"))
            utc2 = parse_utc(form.get("utc2"))
            v_raw = str(form.get("version", "")).strip()
            version = int(v_raw) if v_raw else None
            new_version = services.set_anchors(db, seg, local1, utc1, local2, utc2, version)
        except ValueError as exc:
            return render(request, "anchors.html", anchors=services.list_anchors(db),
                          prefill=None, error=str(exc), msg="", ok=1)
        except ServiceError as exc:
            return render(request, "anchors.html", anchors=services.list_anchors(db),
                          prefill=None, error=exc.message, msg="", ok=1)
        trigger_recalc()
        return redirect("/anchors", f"段 {seg} 锚点已保存，当前版本 v{new_version}")

    @app.get("/query", response_class=HTMLResponse)
    def query_page(request: Request, date: str = ""):
        result, error = None, None
        if date.strip():
            try:
                result = services.query_by_date(db, date)
            except ServiceError as exc:
                error = exc.message
        pending = services.pending_readings(db)
        return render(request, "query.html", date=date, result=result, error=error,
                      pending=pending[:200], pending_count=len(pending),
                      calc=services.calc_info(db), msg="", ok=1)

    # ------------------------------------------------------------ 备机补数网页

    def parse_version_arg(raw: str | None) -> int | None:
        if raw is None or not str(raw).strip():
            return None
        try:
            return int(str(raw).strip())
        except ValueError:
            raise ServiceError("合并版本须为整数", code="BAD_VERSION")

    def backup_context(**extra) -> dict:
        info = merge.merge_info(db)
        active = info["active_file"]
        latest = info["latest"]
        conflicts = merge.list_conflicts(db, active["id"]) if active else []
        return {
            "info": info, "active": active, "latest": latest,
            "conflicts": conflicts,
            "files": merge.list_backup_files(db),
            "cfg": cfg, **extra,
        }
    @app.get("/backup", response_class=HTMLResponse)
    def backup_page(request: Request, msg: str = "", ok: int = 1):
        return render(request, "backup.html", **backup_context(msg=msg, ok=ok))

    @app.post("/backup", response_class=HTMLResponse)
    async def backup_do(
        request: Request,
        file: UploadFile = File(...),
        start: str = Form(""),
        end: str = Form(""),
    ):
        try:
            result = await handle_backup_upload(file, start, end)
        except ServiceError as exc:
            return render(request, "backup.html",
                          **backup_context(error=exc, msg="", ok=1))
        note = "已整批生效" if result["conflict_count"] == 0 else \
            f"已整批生效，但因 {result['conflict_count']} 个 UTC 存在异温，合并结果不可交付"
        return redirect("/backup", f"备机文件 #{result['file_id']} 已整批生效："
                                   f"区间内 {result['rows_in_interval']} 点，{note}",
                        )

    @app.get("/merged", response_class=HTMLResponse)
    def merged_page(
        request: Request,
        date: str = "",
        version: str = "",
        msg: str = "",
        ok: int = 1,
    ):
        info = merge.merge_info(db)
        result, error = None, None
        try:
            ver = parse_version_arg(version)
        except ServiceError as exc:
            ver, error = None, exc.message
        if not error:
            try:
                result = merge.merged_day(db, date.strip() or "", ver)
            except merge.BadMergeDate as exc:
                error = str(exc)
            except merge.UnknownMergeVersion:
                error = f"合并版本 v{ver} 不存在"
        return render(request, "merged.html", info=info, result=result, error=error,
                      date=date, version=version, msg=msg, ok=ok,
                      history=merge.list_merged_results(db, 20),
                      refresh=info["status"] == "running")

    # ------------------------------------------------------------ JSON API

    @app.get("/api/status")
    def api_status():
        return services.get_status(db, cfg)

    @app.post("/api/recalc")
    def api_recalc():
        trigger_recalc()
        return {"triggered": True, "calc": services.calc_info(db),
                "merge": merge.merge_info(db)}

    @app.post("/api/uploads", status_code=200)
    async def api_upload(file: UploadFile = File(...)):
        try:
            batch_id, n = await handle_upload(file)
        except ServiceError as exc:
            return error_json(exc)
        return {"batch_id": batch_id, "accepted": n}

    @app.post("/api/anchors")
    async def api_anchors(request: Request):
        try:
            body = await request.json()
        except Exception:
            return error_json(ServiceError("请求体须为 JSON", code="BAD_JSON"))
        try:
            seg = int(body["segment"])
            local1 = int(body["local1"])
            local2 = int(body["local2"])
            utc1 = parse_utc(body["utc1"])
            utc2 = parse_utc(body["utc2"])
        except KeyError as exc:
            return error_json(ServiceError(f"缺少字段 {exc}", code="MISSING_FIELD"))
        except ValueError as exc:
            return error_json(ServiceError(str(exc), code="BAD_VALUE"))
        version = body.get("version")
        if version is not None:
            try:
                version = int(version)
            except (TypeError, ValueError):
                return error_json(ServiceError("version 须为整数", code="BAD_VALUE"))
        try:
            new_version = services.set_anchors(db, seg, local1, utc1, local2, utc2, version)
        except ServiceError as exc:
            return error_json(exc)
        trigger_recalc()
        return {"segment": seg, "version": new_version}

    @app.get("/api/readings")
    def api_readings(date: str):
        try:
            return services.query_by_date(db, date)
        except ServiceError as exc:
            return error_json(exc)

    @app.get("/api/pending")
    def api_pending():
        pend = services.pending_readings(db)
        return {"count": len(pend), "pending": pend}

    @app.post("/api/backup")
    async def api_backup(
        file: UploadFile = File(...), start: str = Form(""), end: str = Form("")
    ):
        try:
            return await handle_backup_upload(file, start, end)
        except ServiceError as exc:
            return error_json(exc)

    @app.get("/api/merged")
    def api_merged(date: str = "", version: str = ""):
        try:
            ver = parse_version_arg(version)
        except ServiceError as exc:
            return error_json(exc)
        try:
            return merge.merged_day(db, date.strip(), ver)
        except merge.BadMergeDate as exc:
            return error_json(ServiceError(str(exc), code="BAD_DATE"))
        except merge.UnknownMergeVersion:
            return error_json(ServiceError(f"合并版本 v{ver} 不存在",
                                           status=404, code="MERGE_VERSION_NOT_FOUND"))

    @app.get("/api/merge/status")
    def api_merge_status():
        return merge.merge_info(db)

    return app


app = create_app()
