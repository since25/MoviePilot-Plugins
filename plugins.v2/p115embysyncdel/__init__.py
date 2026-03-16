import time
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from app import schemas
from app.core.event import Event, eventmanager
from app.chain.storage import StorageChain
from app.core.config import settings
from app.db.models.transferhistory import TransferHistory
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, NotificationType


class P115EmbySyncDel(_PluginBase):
    plugin_name = "115 Emby 联动删除"
    plugin_desc = "通过神医助手删除 Emby 媒体时，同步删除 115 文件与 MoviePilot 整理记录。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Frontend/refs/heads/v2/src/assets/images/misc/u115.png"
    plugin_version = "0.1.0"
    plugin_author = "Codex"
    author_url = "https://openai.com"
    plugin_config_prefix = "p115embysyncdel_"
    plugin_order = 10
    auth_level = 1

    _enabled = False
    _notify = True
    _delete_transfer_history = True
    _delete_p115_file = True
    _emby_library_path = ""
    _openlist_url_prefix = ""
    _p115_storage = "u115"
    _mediaservers: List[str] = []

    _storagechain: Optional[StorageChain] = None
    _transferhis: Optional[TransferHistoryOper] = None
    _mediaserver_helper: Optional[MediaServerHelper] = None

    def init_plugin(self, config: Optional[dict] = None) -> None:
        """
        初始化插件配置与运行依赖。

        :param config: 插件配置。
        """
        self._storagechain = StorageChain()
        self._transferhis = TransferHistoryOper()
        self._mediaserver_helper = MediaServerHelper()

        if not config:
            return

        self._enabled = bool(config.get("enabled", False))
        self._notify = bool(config.get("notify", True))
        self._delete_transfer_history = bool(
            config.get("delete_transfer_history", True)
        )
        self._delete_p115_file = bool(config.get("delete_p115_file", True))
        self._emby_library_path = (config.get("emby_library_path") or "").strip()
        self._openlist_url_prefix = (config.get("openlist_url_prefix") or "").strip()
        self._p115_storage = (config.get("p115_storage") or "u115").strip() or "u115"
        self._mediaservers = [
            str(server).strip()
            for server in (config.get("mediaservers") or [])
            if str(server).strip()
        ]

        self.update_config(
            {
                "enabled": self._enabled,
                "notify": self._notify,
                "delete_transfer_history": self._delete_transfer_history,
                "delete_p115_file": self._delete_p115_file,
                "emby_library_path": self._emby_library_path,
                "openlist_url_prefix": self._openlist_url_prefix,
                "p115_storage": self._p115_storage,
                "mediaservers": self._mediaservers,
            }
        )

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        返回插件配置页面与默认配置。

        :return: 配置页面与默认配置。
        """
        mediaserver_items = []
        if self._mediaserver_helper:
            mediaserver_items = [
                {"title": config.name, "value": config.name}
                for config in self._mediaserver_helper.get_configs().values()
                if config.type == "emby"
            ]

        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "content": [
                            {
                                "component": "VIcon",
                                "props": {
                                    "icon": "mdi-delete-sync",
                                    "color": "primary",
                                    "class": "mr-2",
                                },
                            },
                            {"component": "span", "text": "基础设置"},
                        ],
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 2},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "enabled",
                                                    "label": "启用插件",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 2},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "notify",
                                                    "label": "发送通知",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "delete_transfer_history",
                                                    "label": "删除整理记录",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "delete_p115_file",
                                                    "label": "删除 115 真实文件",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [
                                            {
                                                "component": "VSelect",
                                                "props": {
                                                    "multiple": True,
                                                    "chips": True,
                                                    "clearable": True,
                                                    "model": "mediaservers",
                                                    "label": "媒体服务器",
                                                    "items": mediaserver_items,
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "p115_storage",
                                                    "label": "115 存储模块名称",
                                                    "placeholder": "u115",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "emby_library_path",
                                                    "label": "Emby 入库 STRM 根路径",
                                                    "placeholder": "/mnt/user/docker1/alist-strm/video/mp302_mv",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "openlist_url_prefix",
                                                    "label": "OpenList URL 前缀",
                                                    "placeholder": "http://192.168.70.138:5244/d",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "density": "compact",
                                    "class": "mt-2",
                                },
                                "content": [
                                    {
                                        "component": "div",
                                        "text": "当前版本只支持电影场景，且要求能在 MoviePilot 转移历史中精确命中整理后的 STRM 目标路径。",
                                    },
                                    {
                                        "component": "div",
                                        "text": "删除链路：Emby 删除路径 -> MoviePilot 转移记录 -> 原始 STRM 文件 -> OpenList URL -> 115 真实文件。",
                                    },
                                ],
                            },
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "density": "compact",
                                    "class": "mt-2",
                                },
                                "content": [
                                    {
                                        "component": "div",
                                        "text": "需要使用神医助手触发 deep.delete 事件，且 MoviePilot 端可访问原始 STRM 文件路径。",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "delete_transfer_history": True,
            "delete_p115_file": True,
            "emby_library_path": "",
            "openlist_url_prefix": "",
            "p115_storage": "u115",
            "mediaservers": [],
        }

    def get_page(self) -> List[dict]:
        """
        返回插件历史页面。

        :return: 页面描述。
        """
        history = self.get_data("history") or []
        if not history:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {"class": "text-center"},
                }
            ]

        history = sorted(history, key=lambda item: item.get("del_time", ""), reverse=True)
        content = []
        for item in history:
            content.append(
                {
                    "component": "VCard",
                    "props": {"variant": "outlined"},
                    "content": [
                        {
                            "component": "VCardText",
                            "text": f"标题：{item.get('title')}",
                        },
                        {
                            "component": "VCardText",
                            "text": f"Emby 路径：{item.get('emby_path')}",
                        },
                        {
                            "component": "VCardText",
                            "text": f"115 路径：{item.get('p115_path')}",
                        },
                        {
                            "component": "VCardText",
                            "text": f"结果：{item.get('result')}",
                        },
                        {
                            "component": "VCardText",
                            "text": f"时间：{item.get('del_time')}",
                        },
                    ],
                }
            )
        return [{"component": "div", "props": {"class": "grid gap-3"}, "content": content}]

    @eventmanager.register(EventType.WebhookMessage)
    def handle_webhook(self, event: Event) -> None:
        """
        处理神医助手删除事件。

        :param event: Webhook 事件对象。
        """
        if not self._enabled:
            return

        event_data = event.event_data
        if not event_data or self._event_value(event_data, "event") != "deep.delete":
            return

        media_server = self._extract_media_server(event_data)
        if self._mediaservers and media_server not in self._mediaservers:
            logger.info("【115 Emby 联动删除】媒体服务器不在配置范围内，跳过：%s", media_server or "未知")
            return

        media_type = self._event_value(event_data, "item_type")
        if media_type not in {"Movie", "MOV"}:
            logger.info("【115 Emby 联动删除】当前版本仅处理电影，跳过类型：%s", media_type)
            return

        media_name = self._event_value(event_data, "item_name")
        emby_path = self._event_value(event_data, "item_path").replace("\\", "/")
        tmdb_id = self._safe_int(self._event_raw_value(event_data, "tmdb_id"))

        if not emby_path:
            logger.warning("【115 Emby 联动删除】删除事件缺少媒体路径，跳过")
            return

        if self._emby_library_path and not self._has_prefix(emby_path, self._emby_library_path):
            logger.info("【115 Emby 联动删除】路径不在目标媒体库内，跳过：%s", emby_path)
            return

        self._handle_movie_delete(
            media_name=media_name,
            emby_path=emby_path,
            tmdb_id=tmdb_id,
        )

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令。

        :return: 命令列表。
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        对外暴露 Webhook 接口，供 Emby Webhooks 调用。

        :return: API 列表。
        """
        return [
            {
                "path": "/webhook",
                "endpoint": self.webhook,
                "methods": ["POST"],
                "summary": "接收 Emby 神医助手深度删除通知",
                "description": "接收 deep.delete 事件并联动删除 115 文件。",
            }
        ]

    async def webhook(
        self,
        apikey: str = "",
        payload: Optional[str] = None,
        request: Any = None,
    ):
        """
        接收 Emby Webhooks 请求。

        :param apikey: API 密钥。
        :param payload: 可选的原始 JSON 字符串。
        :param request: 框架注入的请求对象。
        :return: API 响应。
        """
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")

        event_data = await self._extract_webhook_payload(request=request, payload=payload)
        if not event_data:
            return schemas.Response(success=False, message="未获取到Webhook数据")

        event_name = self._event_value(event_data, "event")
        if event_name != "deep.delete":
            return schemas.Response(success=True, message=f"忽略事件：{event_name or 'unknown'}")

        if not self._enabled:
            return schemas.Response(success=True, message="插件未启用，事件已忽略")

        media_server = self._extract_media_server(event_data)
        if self._mediaservers and media_server not in self._mediaservers:
            return schemas.Response(success=True, message=f"媒体服务器不匹配：{media_server or '未知'}")

        media_type = self._event_value(event_data, "item_type")
        if media_type not in {"Movie", "MOV"}:
            return schemas.Response(success=True, message=f"当前仅处理电影，已忽略：{media_type or 'unknown'}")

        media_name = self._event_value(event_data, "item_name")
        emby_path = self._event_value(event_data, "item_path").replace("\\", "/")
        tmdb_id = self._safe_int(self._event_raw_value(event_data, "tmdb_id"))

        if not emby_path:
            return schemas.Response(success=False, message="缺少 item_path，无法处理")

        if self._emby_library_path and not self._has_prefix(emby_path, self._emby_library_path):
            return schemas.Response(success=True, message="路径不在目标媒体库内，已忽略")

        self._handle_movie_delete(
            media_name=media_name,
            emby_path=emby_path,
            tmdb_id=tmdb_id,
        )
        return schemas.Response(success=True, message="deep.delete 事件处理完成")

    def _handle_movie_delete(
        self,
        media_name: str,
        emby_path: str,
        tmdb_id: Optional[int],
    ) -> None:
        """
        处理电影删除主流程。

        :param media_name: 媒体名称。
        :param emby_path: Emby 上报的整理后 STRM 路径。
        :param tmdb_id: TMDB ID。
        """
        transfer_history = self._get_transfer_record(emby_path=emby_path, tmdb_id=tmdb_id)
        if not transfer_history:
            logger.warning(
                "【115 Emby 联动删除】未找到转移记录，请确认 MP 历史与整理路径一致：%s",
                emby_path,
            )
            self._save_history(
                media_name=media_name or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="未找到转移记录",
            )
            return

        src_path = str(getattr(transfer_history, "src", "") or "").replace("\\", "/")
        if not src_path:
            logger.warning("【115 Emby 联动删除】转移记录缺少源路径，跳过：%s", emby_path)
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="转移记录缺少源路径",
            )
            return

        openlist_url = self._read_strm_target(src_path)
        if not openlist_url:
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="原始 STRM 文件不可读或内容为空",
            )
            return

        p115_path = self._convert_openlist_url_to_pan_path(openlist_url)
        if not p115_path:
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="无法从 STRM 内容还原 115 路径",
            )
            return

        result_parts: List[str] = []
        if self._delete_p115_file:
            if self._delete_p115_file_item(media_name=media_name, p115_path=p115_path):
                result_parts.append("115 文件已删除")
            else:
                result_parts.append("115 文件删除失败")

        if self._delete_transfer_history and (
            not self._delete_p115_file or "115 文件已删除" in result_parts
        ):
            self._transferhis.delete(transfer_history.id)
            result_parts.append("整理记录已删除")
        elif self._delete_transfer_history:
            result_parts.append("整理记录未删除")

        result_text = "，".join(result_parts) if result_parts else "未执行删除动作"
        logger.info("【115 Emby 联动删除】%s -> %s，结果：%s", emby_path, p115_path, result_text)

        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="115 Emby 联动删除完成",
                text=f"{media_name or transfer_history.title or Path(emby_path).stem}\n"
                f"Emby 路径：{emby_path}\n"
                f"115 路径：{p115_path}\n"
                f"结果：{result_text}",
            )

        self._save_history(
            media_name=media_name or transfer_history.title or Path(emby_path).stem,
            emby_path=emby_path,
            p115_path=p115_path,
            result=result_text,
        )

    def _get_transfer_record(
        self,
        emby_path: str,
        tmdb_id: Optional[int],
    ) -> Optional[TransferHistory]:
        """
        查询电影转移记录。

        :param emby_path: Emby 上报的整理后路径。
        :param tmdb_id: TMDB ID。
        :return: 转移记录。
        """
        if tmdb_id:
            histories: List[TransferHistory] = self._transferhis.get_by(
                tmdbid=tmdb_id,
                mtype=MediaType.MOVIE.value,
                dest=emby_path,
            )
            if histories:
                return histories[0]

        return self._transferhis.get_by_dest(emby_path)

    @staticmethod
    def _read_strm_target(src_path: str) -> Optional[str]:
        """
        读取原始 STRM 文件的目标 URL。

        :param src_path: 原始 STRM 路径。
        :return: STRM 中的首个非空目标。
        """
        file_path = Path(src_path)
        if not file_path.exists() or file_path.suffix.lower() != ".strm":
            logger.warning("【115 Emby 联动删除】源路径不是可读取的 STRM 文件：%s", src_path)
            return None

        try:
            for line in file_path.read_text(encoding="utf-8-sig").splitlines():
                target = line.strip()
                if target:
                    return target
        except Exception as err:
            logger.error("【115 Emby 联动删除】读取 STRM 文件失败：%s", err)
        return None

    def _convert_openlist_url_to_pan_path(self, openlist_url: str) -> Optional[str]:
        """
        将 OpenList URL 还原为 115 网盘路径。

        :param openlist_url: STRM 中的 OpenList URL。
        :return: 115 网盘路径。
        """
        prefix = self._openlist_url_prefix.rstrip("/")
        target = openlist_url.strip()
        if not prefix or not target:
            return None

        if target.startswith(prefix):
            raw_path = target[len(prefix) :]
        else:
            target_parsed = urlparse(target)
            prefix_parsed = urlparse(prefix)
            if (
                target_parsed.scheme != prefix_parsed.scheme
                or target_parsed.netloc != prefix_parsed.netloc
                or not target_parsed.path.startswith(prefix_parsed.path)
            ):
                logger.warning(
                    "【115 Emby 联动删除】STRM 内容与配置的 OpenList 前缀不匹配：%s",
                    openlist_url,
                )
                return None
            raw_path = target_parsed.path[len(prefix_parsed.path) :]

        pan_path = unquote(raw_path).strip()
        if not pan_path.startswith("/"):
            pan_path = f"/{pan_path}"
        return pan_path or None

    def _delete_p115_file_item(self, media_name: str, p115_path: str) -> bool:
        """
        删除 115 文件或目录。

        :param media_name: 媒体名称。
        :param p115_path: 115 网盘路径。
        :return: 是否删除成功。
        """
        try:
            fileitem = self._storagechain.get_file_item(
                storage=self._p115_storage,
                path=Path(p115_path),
            )
            if not fileitem:
                logger.warning("【115 Emby 联动删除】未获取到 115 文件项：%s", p115_path)
                return False
            if fileitem.type == "dir":
                self._storagechain.delete_file(fileitem)
            else:
                self._storagechain.delete_media_file(fileitem=fileitem)
            logger.info("【115 Emby 联动删除】%s 删除 115 文件成功：%s", media_name, p115_path)
            return True
        except Exception as err:
            logger.error("【115 Emby 联动删除】删除 115 文件失败：%s", err, exc_info=True)
            return False

    def _save_history(
        self,
        media_name: str,
        emby_path: str,
        p115_path: str,
        result: str,
    ) -> None:
        """
        保存插件处理历史。

        :param media_name: 媒体名称。
        :param emby_path: Emby 路径。
        :param p115_path: 115 路径。
        :param result: 执行结果。
        """
        history = self.get_data("history") or []
        history.append(
            {
                "title": media_name,
                "emby_path": emby_path,
                "p115_path": p115_path,
                "result": result,
                "del_time": time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(time.time()),
                ),
            }
        )
        self.save_data("history", history[-50:])

    def get_state(self) -> bool:
        """
        返回插件启用状态，供前端展示与控制。

        :return: 是否已启用。
        """
        return self._enabled

    def stop_service(self) -> None:
        """
        停止插件服务。
        """
        pass

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """
        将值安全转换为整数。

        :param value: 任意值。
        :return: 整数或空。
        """
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _event_raw_value(event_data: Any, key: str) -> Any:
        """
        兼容对象与字典两种事件载荷访问方式。

        :param event_data: 事件数据。
        :param key: 字段名。
        :return: 原始字段值。
        """
        if isinstance(event_data, dict):
            return event_data.get(key)
        return getattr(event_data, key, None)

    async def _extract_webhook_payload(
        self,
        request: Any = None,
        payload: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        从请求中提取 Webhook 数据，兼容 json 与 form-data。

        :param request: 请求对象。
        :param payload: 原始 JSON 字符串。
        :return: 事件数据字典。
        """
        candidate = self._loads_json(payload)
        if isinstance(candidate, dict):
            return candidate

        if not request:
            return None

        try:
            candidate = await request.json()
            if isinstance(candidate, dict):
                return candidate
        except Exception:
            pass

        try:
            form_data = await request.form()
            normalized = {key: value for key, value in form_data.items()}

            # Emby Webhooks 常把 JSON 包在 payload/data/Message 这类字段中
            for key in ("payload", "data", "message", "Message", "json"):
                nested = self._loads_json(normalized.get(key))
                if isinstance(nested, dict):
                    return nested

            return normalized or None
        except Exception:
            return None

    @staticmethod
    def _loads_json(value: Any) -> Optional[Any]:
        """
        尝试解析 JSON 字符串。

        :param value: 输入值。
        :return: 解析结果。
        """
        if not value or not isinstance(value, str):
            return None
        try:
            return json.loads(value)
        except Exception:
            return None

    @classmethod
    def _event_value(cls, event_data: Any, key: str) -> str:
        """
        读取事件字段并规整为字符串。

        :param event_data: 事件数据。
        :param key: 字段名。
        :return: 字符串值。
        """
        return str(cls._event_raw_value(event_data, key) or "")

    @classmethod
    def _extract_media_server(cls, event_data: Any) -> str:
        """
        从事件中提取媒体服务器名称，兼容常见字段命名。

        :param event_data: 事件数据。
        :return: 媒体服务器名称。
        """
        for key in ("media_server", "mediaserver", "server", "server_name"):
            value = cls._event_value(event_data, key).strip()
            if value:
                return value
        return ""

    @staticmethod
    def _has_prefix(full_path: str, prefix_path: str) -> bool:
        """
        判断路径是否具有指定前缀。

        :param full_path: 完整路径。
        :param prefix_path: 前缀路径。
        :return: 是否匹配。
        """
        full_parts = Path(full_path).parts
        prefix_parts = Path(prefix_path).parts
        return len(prefix_parts) <= len(full_parts) and full_parts[: len(prefix_parts)] == prefix_parts
