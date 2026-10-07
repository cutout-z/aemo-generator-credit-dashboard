"""Constraint-table archive downloads retry when nemweb sends no zip (2026-10-07)."""

import io
import zipfile

import pytest

from src import download_constraints as dc


def _zip_bytes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("PUBLIC_ARCHIVE#SPDCONNECTIONPOINTCONSTRAINT#FILE01#202603010000.CSV", "C,x\n")
    return buf.getvalue()


class _Resp:
    def __init__(self, status, content, ctype):
        self.status_code, self.content, self.headers = status, content, {"content-type": ctype}


URL = ("http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2026/MMSDM_2026_03/"
       "MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_ARCHIVE#SPDCONNECTIONPOINTCONSTRAINT#FILE01#202603010000.zip")


@pytest.fixture
def calls(monkeypatch):
    log = {"get": [], "sleep": []}
    monkeypatch.setattr(dc, "_sleep", lambda s: log["sleep"].append(s))
    return log


def _serve(monkeypatch, calls, responses):
    seq = iter(responses)
    def get(url, **kw):
        calls["get"].append(url)
        return next(seq)
    monkeypatch.setattr(dc.requests, "get", get)


def test_a_non_zip_answer_is_retried_with_backoff(tmp_path, monkeypatch, calls):
    _serve(monkeypatch, calls, [_Resp(200, b"<html>busy</html>", "text/html"),
                                _Resp(503, b"", "text/html"), _Resp(200, _zip_bytes(), "application/zip")])
    dc._download_unzip_csv_with_retry(URL, str(tmp_path))
    assert calls["sleep"] == [5, 20] and len(calls["get"]) == 3
    assert "%23" in calls["get"][0]
    assert any(p.suffix == ".CSV" for p in tmp_path.iterdir())


def test_gives_up_after_three_retries(tmp_path, monkeypatch, calls):
    _serve(monkeypatch, calls, [_Resp(200, b"<html/>", "text/html")] * 4)
    with pytest.raises(zipfile.BadZipFile, match="HTTP 200, text/html"):
        dc._download_unzip_csv_with_retry(URL, str(tmp_path))
    assert calls["sleep"] == [5, 20, 60]


def test_a_404_is_retried_once_only(tmp_path, monkeypatch, calls):
    old = URL.replace("2026/MMSDM_2026_03", "2010/MMSDM_2010_03").replace("202603", "201003")
    _serve(monkeypatch, calls, [_Resp(404, b"", "text/html")] * 2)
    with pytest.raises(zipfile.BadZipFile, match="HTTP 404"):
        dc._download_unzip_csv_with_retry(old, str(tmp_path))
    assert calls["sleep"] == [5] and len(calls["get"]) == 2


def test_a_month_that_has_not_happened_is_not_retried(tmp_path, monkeypatch, calls):
    future = URL.replace("2026/MMSDM_2026_03", "2029/MMSDM_2029_11").replace("202603", "202911")
    _serve(monkeypatch, calls, [_Resp(404, b"", "text/html")])
    with pytest.raises(zipfile.BadZipFile):
        dc._download_unzip_csv_with_retry(future, str(tmp_path))
    assert calls["sleep"] == [] and len(calls["get"]) == 1


def test_compiles_route_downloads_through_the_retrying_downloader(monkeypatch):
    seen = {}
    def fake_compile(**kw):
        seen["fn"] = dc.nemosis_downloader.download_unzip_csv
        import pandas as pd
        return pd.DataFrame()
    original = dc.nemosis_downloader.download_unzip_csv
    monkeypatch.setattr(dc, "dynamic_data_compiler", fake_compile)
    dc._compile(table_name="X")
    assert seen["fn"] is dc._download_unzip_csv_with_retry
    assert dc.nemosis_downloader.download_unzip_csv is original
