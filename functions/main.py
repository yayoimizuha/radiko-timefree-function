import re
import tempfile
from datetime import datetime, timedelta, timezone
from os import path
import requests
import yt_dlp
from bs4 import BeautifulSoup
from firebase_functions import https_fn, options
from firebase_admin import initialize_app, storage, firestore
from yt_dlp import DownloadError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
initialize_app()
options.set_global_options(region=options.SupportedRegion.ASIA_NORTHEAST1, memory=options.MemoryOption.MB_512)


class MyLogger:
    def debug(self, msg):
        if msg.startswith('[debug] '):
            logger.debug(msg)
        else:
            self.info(msg)

    def info(self, msg):
        logger.info(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        logger.error(msg)


def print_wrapper(func):
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        print(res)
        return res

    return wrapper


@https_fn.on_request(timeout_sec=240)
@print_wrapper
def download_timefree(req: https_fn.Request) -> https_fn.Response:
    if "ft" in req.args and "channel" in req.args:
        pass
    else:
        return https_fn.Response({
            "status": "error",
            "reason": "ラジオ局のID(channel)と番組の開始時刻(RFC3339による):(ft)が必要です。",
            "code": 400
        }, status=400)
    if ft_datetime := datetime.fromisoformat(req.args.get("ft")):
        # print(ft_datetime)
        ft = ft_datetime
    else:
        return https_fn.Response({
            "status": "error",
            "reason": "ftのフォーマットが違います。RFC3339が必要です。",
            "code": 400
        }, status=400)
    channel = req.args.get("channel")
    firestore_client = firestore.client(database_id="(default)")
    if ((day_exist := firestore_client.collection("hello-radiko-data", "archives", channel)
            .document(ft.strftime("%Y%m%d%H%M%S")).get().exists) or
            firestore_client.collection("hello-radiko-data", "archives", channel)
                    .document((ft - timedelta(days=1)).strftime("%Y%m%d") + f"{ft.hour + 24:02d}" + ft.strftime("%M%S"))
                    .get().exists):
        if day_exist:
            firestore_doc = (firestore_client.collection("hello-radiko-data", "archives", channel)
                             .document(ft.strftime("%Y%m%d%H%M%S")).get().to_dict())
        else:
            firestore_doc = (firestore_client.collection("hello-radiko-data", "archives", channel)
                             .document(
                (ft - timedelta(days=1)).strftime("%Y%m%d") + f"{ft.hour + 24:02d}" + ft.strftime("%M%S")
            ).get().to_dict())
        print({"exist entry": firestore_doc})
        if (firestore_doc["status"] == "success" or
                (firestore_doc["status"] == "error" and firestore_doc["code"] != 404)):
            return https_fn.Response(firestore_doc, status=firestore_doc["code"])

    if (full_xml := requests.get("https://radiko.jp/v3/station/region/full.xml")).status_code != 200:
        return https_fn.Response({
            "status": "error",
            "reason": "https://radiko.jp/v3/station/region/full.xml が取得できません。",
            "code": full_xml.status_code
        }, status=full_xml.status_code)
    full_xml = BeautifulSoup(full_xml.text, "xml")
    if channel in list(map(lambda tag: tag.text, full_xml.find_all("id"))):
        pass
    else:
        return https_fn.Response({
            "status": "error",
            "reason": "存在しない局IDが指定されました。",
            "code": 404
        }, status=404)

    date_json: list = requests.get("https://api.radiko.jp/program/v4/date/{}/station/{}.json".format(
        ft.strftime("%Y%m%d"), channel
    )).json()["stations"][0]["programs"]["program"]
    date_json.extend(requests.get("https://api.radiko.jp/program/v4/date/{}/station/{}.json".format(
        (ft - timedelta(days=1)).strftime("%Y%m%d"), channel
    )).json()["stations"][0]["programs"]["program"])
    program: None | dict[str, str | int | bool] = None
    radiko_datetime_regex = re.compile(
        R"^(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hours>\d{2})(?P<minutes>\d{2})(?P<seconds>\d{2})"
    )

    for item in date_json:
        if program is not None:
            break
        if ((item_ft := radiko_datetime_regex.match(item["ft"])) and
                (item_to := radiko_datetime_regex.match(item["to"]))):
            item_ft = datetime(tzinfo=timezone(offset=timedelta(hours=+9), name="JST"),
                               **{k: int(v) for k, v in item_ft.groupdict().items() if k in ["year", "month", "day"]}) + \
                      timedelta(
                          **{k: int(v) for k, v in item_ft.groupdict().items() if k in ["hours", "minutes", "seconds"]})
            item_to = datetime(tzinfo=timezone(offset=timedelta(hours=+9), name="JST"),
                               **{k: int(v) for k, v in item_to.groupdict().items() if k in ["year", "month", "day"]}) + \
                      timedelta(
                          **{k: int(v) for k, v in item_to.groupdict().items() if k in ["hours", "minutes", "seconds"]})

            if item_ft == ft:
                program = {
                    "ft_string": item["ft"],
                    "to_string": item["to"],
                    "failed_record": bool(item["failed_record"]),
                    "ts_in_ng": item["ts_in_ng"],
                    "ts_out_ng": item["ts_out_ng"],
                    "tsplus_in_ng": item["tsplus_in_ng"],
                    "tsplus_out_ng": item["tsplus_out_ng"],
                    "program_finished": datetime.now(tz=timezone(offset=timedelta(hours=+9), name="JST")) < item_to
                }

    if program is None:
        firestore_doc = {
            "status": "error",
            "reason": "指定された番組が存在しません。",
            "code": 404
        }
    elif program["ts_in_ng"] == 2 or program["ts_in_ng"] == 2:
        firestore_doc = {
            "status": "error",
            "reason": "指定された番組はタイムフリーでアクセスできません。",
            "code": 403
        }
    elif program["program_finished"]:
        firestore_doc = {
            "status": "pending",
            "reason": "まだ番組が終了していません。",
            "code": 404
        }
    else:
        print({"metadata": program})
        with tempfile.TemporaryDirectory() as temp_dir:
            print({"output_directory": temp_dir})
            try:
                with yt_dlp.YoutubeDL(params={"outtmpl": path.join(temp_dir, "program.m4a"), "format": "bestaudio",
                                              "logger": MyLogger(), "verbose": True}) as dl:
                    dl.download([f"https://radiko.jp/#!/ts/{channel}/{program['ft_string']}"])
            except DownloadError as e:
                firestore_doc = {
                    "status": "error",
                    "reason": f"yt-dlpが失敗しました。\n{e}",
                    "code": 500
                }
            else:
                gcs = storage.bucket("hello-radiko.firebasestorage.app")
                blob = gcs.blob(f"{channel}/{program['ft_string']}.m4a")
                blob.upload_from_filename(path.join(temp_dir, "program.m4a"))
                print(blob.public_url)
                firestore_doc = {
                    "status": "success",
                    "url": blob.public_url,
                    "code": 200
                }

    firestore_client.collection("hello-radiko-data", "archives", channel) \
        .document(program["ft_string"]).set(firestore_doc)
    return https_fn.Response(firestore_doc, status=firestore_doc["code"])
