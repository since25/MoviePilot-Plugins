import time
import json
from urllib import request as urllib_request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from app import schemas
from fastapi import Request
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
    plugin_version = "0.1.12"
    plugin_author = "Codex"
    author_url = "https://openai.com"
    plugin_config_prefix = "p115embysyncdel_"
    plugin_order = 10
    auth_level = 1

    _enabled = False
    _notify = True
    _delete_transfer_history = True
    _delete_p115_file = True
    _delete_movie_dir = True
    _emby_library_path = ""
    _openlist_url_prefix = ""
    _openlist_api_url = ""
    _openlist_token = ""
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
        self._delete_movie_dir = bool(config.get("delete_movie_dir", True))
        self._emby_library_path = (config.get("emby_library_path") or "").strip()
        self._openlist_url_prefix = (config.get("openlist_url_prefix") or "").strip()
        self._openlist_api_url = (config.get("openlist_api_url") or "").strip()
        self._openlist_token = (config.get("openlist_token") or "").strip()
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
                "delete_movie_dir": self._delete_movie_dir,
                "emby_library_path": self._emby_library_path,
                "openlist_url_prefix": self._openlist_url_prefix,
                "openlist_api_url": self._openlist_api_url,
                "openlist_token": self._openlist_token,
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
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "delete_movie_dir",
                                                    "label": "电影目录联动删除",
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
                                                "component": "VTextarea",
                                                "props": {
                                                    "model": "emby_library_path",
                                                    "rows": 2,
                                                    "label": "Emby 入库 STRM 根路径",
                                                    "placeholder": "/mnt/user/docker1/alist-strm/video/mp302_mv\\n/mnt/user/docker1/alist-strm/video/mp302_tv",
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
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "openlist_api_url",
                                                    "label": "OpenList API 地址",
                                                    "placeholder": "http://192.168.70.138:5244",
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
                                                    "model": "openlist_token",
                                                    "label": "OpenList Token",
                                                    "type": "password",
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
                                    {
                                        "component": "div",
                                        "text": "媒体库根路径支持多行配置，可同时填写电影库和剧集库。",
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
            "delete_movie_dir": True,
            "emby_library_path": "",
            "openlist_url_prefix": "",
            "openlist_api_url": "",
            "openlist_token": "",
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
        request: Request = None,
    ):
        """
        接收 Emby Webhooks 请求。

        :param apikey: API 密钥。
        :param payload: 可选的原始 JSON 字符串。
        :param request: 框架注入的请求对象。
        :return: API 响应。
        """
        if apikey != settings.API_TOKEN:
            logger.warning("【115 Emby 联动删除】Webhook 请求 API 密钥错误")
            return schemas.Response(success=False, message="API密钥错误")

        event_data = await self._extract_webhook_payload(request=request, payload=payload)
        if not event_data:
            logger.warning("【115 Emby 联动删除】Webhook 未获取到有效请求体")
            return schemas.Response(success=False, message="未获取到Webhook数据")

        logger.info("【115 Emby 联动删除】Webhook 原始数据：%s", event_data)
        event_name = self._event_value(event_data, "event")
        logger.info("【115 Emby 联动删除】收到 Webhook 事件：%s", event_name or "unknown")
        if event_name not in {"deep.delete", "library.deleted"}:
            return schemas.Response(success=True, message=f"忽略事件：{event_name or 'unknown'}")

        if not self._enabled:
            logger.info("【115 Emby 联动删除】插件未启用，忽略事件")
            return schemas.Response(success=True, message="插件未启用，事件已忽略")

        media_server = self._extract_media_server(event_data)
        if self._mediaservers and media_server not in self._mediaservers:
            logger.info(
                "【115 Emby 联动删除】媒体服务器不匹配，当前=%s，配置=%s",
                media_server or "未知",
                ",".join(self._mediaservers),
            )
            return schemas.Response(success=True, message=f"媒体服务器不匹配：{media_server or '未知'}")

        media_name = self._extract_media_name(event_data)
        emby_path = self._extract_media_path(event_data).replace("\\", "/")
        if not emby_path:
            logger.warning("【115 Emby 联动删除】Webhook 缺少媒体路径")
            return schemas.Response(success=False, message="缺少 item_path，无法处理")

        media_type = self._extract_media_type(event_data)
        tmdb_id = self._extract_tmdb_id(event_data)

        if self._emby_library_path and not self._matches_emby_library_path(emby_path):
            logger.info(
                "【115 Emby 联动删除】路径不在目标媒体库内，当前=%s，配置前缀=%s",
                emby_path,
                self._emby_library_path,
            )
            return schemas.Response(success=True, message="路径不在目标媒体库内，已忽略")

        logger.info(
            "【115 Emby 联动删除】开始处理删除事件：name=%s, path=%s, tmdb=%s, server=%s",
            media_name or "unknown",
            emby_path,
            tmdb_id or "unknown",
            media_server or "unknown",
        )
        if media_type in {"Movie", "MOV"}:
            self._handle_movie_delete(
                media_name=media_name,
                emby_path=emby_path,
                tmdb_id=tmdb_id,
            )
        elif media_type == "Episode":
            season_num = self._extract_season_num(event_data)
            episode_num = self._extract_episode_num(event_data)
            logger.info(
                "【115 Emby 联动删除】识别到电视剧单集删除：season=%s, episode=%s",
                season_num or "unknown",
                episode_num or "unknown",
            )
            self._handle_tv_delete(
                media_name=media_name,
                emby_path=emby_path,
                tmdb_id=tmdb_id,
                season_num=season_num,
                episode_num=episode_num,
            )
        else:
            logger.info("【115 Emby 联动删除】当前仅处理电影和单集，跳过类型：%s", media_type or "unknown")
            return schemas.Response(success=True, message=f"当前仅处理电影和单集，已忽略：{media_type or 'unknown'}")
        return schemas.Response(success=True, message=f"{event_name} 事件处理完成")

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
        logger.info(
            "【115 Emby 联动删除】查询转移记录：title=%s, dest=%s, tmdb=%s",
            media_name or "unknown",
            emby_path,
            tmdb_id or "unknown",
        )
        try:
            transfer_history = self._get_transfer_record(emby_path=emby_path, tmdb_id=tmdb_id)
        except Exception as err:
            logger.error("【115 Emby 联动删除】查询转移记录异常：%s", err, exc_info=True)
            return

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
        logger.info(
            "【115 Emby 联动删除】命中转移记录：id=%s, src=%s, dest=%s",
            getattr(transfer_history, "id", "unknown"),
            src_path or "unknown",
            str(getattr(transfer_history, "dest", "") or "").replace("\\", "/") or "unknown",
        )
        if not src_path:
            logger.warning("【115 Emby 联动删除】转移记录缺少源路径，跳过：%s", emby_path)
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="转移记录缺少源路径",
            )
            return

        logger.info("【115 Emby 联动删除】开始读取原始 STRM：%s", src_path)
        openlist_url = self._read_strm_target(src_path)
        if not openlist_url:
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="原始 STRM 文件不可读或内容为空",
            )
            return

        logger.info("【115 Emby 联动删除】STRM 目标 URL：%s", openlist_url)
        p115_path = self._convert_openlist_url_to_pan_path(openlist_url)
        if not p115_path:
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="无法从 STRM 内容还原 115 路径",
            )
            return

        logger.info("【115 Emby 联动删除】还原 115 路径：%s", p115_path)
        openlist_api_path = self._convert_openlist_url_to_api_path(openlist_url)
        if openlist_api_path:
            logger.info("【115 Emby 联动删除】还原 OpenList API 路径：%s", openlist_api_path)
        delete_api_path, delete_is_dir = self._resolve_movie_delete_target(
            media_name=media_name or transfer_history.title or Path(emby_path).stem,
            emby_path=emby_path,
            openlist_api_path=openlist_api_path,
        )
        result_parts: List[str] = []
        delete_success = False
        if self._delete_p115_file:
            logger.info(
                "【115 Emby 联动删除】开始删除 115 %s：%s",
                "目录" if delete_is_dir else "文件",
                delete_api_path or p115_path,
            )
            if self._delete_p115_file_item(
                media_name=media_name,
                p115_path=p115_path,
                openlist_api_path=delete_api_path,
                openlist_is_dir=delete_is_dir,
            ):
                delete_success = True
                result_parts.append(f"115 {'目录' if delete_is_dir else '文件'}已删除")
            else:
                result_parts.append(f"115 {'目录' if delete_is_dir else '文件'}删除失败")

        if self._delete_transfer_history and (
            not self._delete_p115_file or delete_success
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

    def _handle_tv_delete(
        self,
        media_name: str,
        emby_path: str,
        tmdb_id: Optional[int],
        season_num: Optional[int],
        episode_num: Optional[int],
    ) -> None:
        """
        处理电视剧单集删除主流程，仅删除单集文件。

        :param media_name: 媒体名称。
        :param emby_path: Emby 上报的整理后 STRM 路径。
        :param tmdb_id: TMDB ID。
        :param season_num: 季号。
        :param episode_num: 集号。
        """
        logger.info(
            "【115 Emby 联动删除】查询电视剧转移记录：title=%s, dest=%s, tmdb=%s, season=%s, episode=%s",
            media_name or "unknown",
            emby_path,
            tmdb_id or "unknown",
            season_num or "unknown",
            episode_num or "unknown",
        )
        try:
            transfer_history = self._get_tv_transfer_record(
                emby_path=emby_path,
                tmdb_id=tmdb_id,
                season_num=season_num,
                episode_num=episode_num,
            )
        except Exception as err:
            logger.error("【115 Emby 联动删除】查询电视剧转移记录异常：%s", err, exc_info=True)
            return

        if not transfer_history:
            logger.warning(
                "【115 Emby 联动删除】未找到电视剧转移记录，请确认 MP 历史与整理路径一致：%s",
                emby_path,
            )
            self._save_history(
                media_name=media_name or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="未找到电视剧转移记录",
            )
            return

        src_path = str(getattr(transfer_history, "src", "") or "").replace("\\", "/")
        logger.info(
            "【115 Emby 联动删除】命中电视剧转移记录：id=%s, src=%s, dest=%s",
            getattr(transfer_history, "id", "unknown"),
            src_path or "unknown",
            str(getattr(transfer_history, "dest", "") or "").replace("\\", "/") or "unknown",
        )
        if not src_path:
            logger.warning("【115 Emby 联动删除】电视剧转移记录缺少源路径，跳过：%s", emby_path)
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="电视剧转移记录缺少源路径",
            )
            return

        logger.info("【115 Emby 联动删除】开始读取电视剧原始 STRM：%s", src_path)
        openlist_url = self._read_strm_target(src_path)
        if not openlist_url:
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="电视剧原始 STRM 文件不可读或内容为空",
            )
            return

        logger.info("【115 Emby 联动删除】电视剧 STRM 目标 URL：%s", openlist_url)
        p115_path = self._convert_openlist_url_to_pan_path(openlist_url)
        if not p115_path:
            self._save_history(
                media_name=media_name or transfer_history.title or Path(emby_path).stem,
                emby_path=emby_path,
                p115_path="",
                result="无法从电视剧 STRM 内容还原 115 路径",
            )
            return

        logger.info("【115 Emby 联动删除】还原电视剧 115 路径：%s", p115_path)
        openlist_api_path = self._convert_openlist_url_to_api_path(openlist_url)
        if openlist_api_path:
            logger.info("【115 Emby 联动删除】还原电视剧 OpenList API 路径：%s", openlist_api_path)

        result_parts: List[str] = []
        delete_success = False
        if self._delete_p115_file:
            logger.info("【115 Emby 联动删除】开始删除电视剧单集文件：%s", openlist_api_path or p115_path)
            if self._delete_p115_file_item(
                media_name=media_name,
                p115_path=p115_path,
                openlist_api_path=openlist_api_path,
                openlist_is_dir=False,
            ):
                delete_success = True
                result_parts.append("115 单集文件已删除")
            else:
                result_parts.append("115 单集文件删除失败")

        if self._delete_transfer_history and (
            not self._delete_p115_file or delete_success
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

    def _resolve_movie_delete_target(
        self,
        media_name: str,
        emby_path: str,
        openlist_api_path: Optional[str],
    ) -> Tuple[Optional[str], bool]:
        """
        解析电影删除目标。优先根据 Emby 电影目录名与 OpenList 上级目录名的关键词匹配决定是否删目录。

        :param media_name: 媒体名称。
        :param emby_path: Emby 文件路径。
        :param openlist_api_path: OpenList 文件路径。
        :return: 删除目标路径与是否为目录。
        """
        if not openlist_api_path:
            return None, False

        if not self._delete_movie_dir:
            logger.info("【115 Emby 联动删除】电影目录联动删除未启用，保持文件删除：%s", openlist_api_path)
            return openlist_api_path, False

        emby_dir_name = Path(emby_path).parent.name
        openlist_dir_path = str(Path(openlist_api_path).parent).replace("\\", "/")
        openlist_dir_name = Path(openlist_dir_path).name
        if self._movie_dir_matches(
            media_name=media_name,
            emby_dir_name=emby_dir_name,
            openlist_dir_name=openlist_dir_name,
        ):
            logger.info(
                "【115 Emby 联动删除】命中电影目录删除规则：emby_dir=%s, openlist_dir=%s",
                emby_dir_name,
                openlist_dir_name,
            )
            return openlist_dir_path, True

        logger.info(
            "【115 Emby 联动删除】未命中电影目录删除规则，保持文件删除：emby_dir=%s, openlist_dir=%s",
            emby_dir_name,
            openlist_dir_name,
        )
        return openlist_api_path, False

    @classmethod
    def _movie_dir_matches(
        cls,
        media_name: str,
        emby_dir_name: str,
        openlist_dir_name: str,
    ) -> bool:
        """
        判断 OpenList 上级目录是否应视为电影目录。

        :param media_name: 媒体名称。
        :param emby_dir_name: Emby 电影目录名。
        :param openlist_dir_name: OpenList 上级目录名。
        :return: 是否匹配为电影目录。
        """
        candidates = {
            cls._normalize_movie_keyword(media_name),
            cls._normalize_movie_keyword(emby_dir_name),
        }
        openlist_key = cls._normalize_movie_keyword(openlist_dir_name)
        candidates = {item for item in candidates if item}
        if not openlist_key or not candidates:
            return False
        return any(item in openlist_key for item in candidates)

    @staticmethod
    def _normalize_movie_keyword(value: str) -> str:
        """
        归一化电影名关键词，用于目录匹配。

        :param value: 原始名称。
        :return: 归一化结果。
        """
        normalized = value.lower().strip()
        for token in (" ", "-", "_", "(", ")", "（", "）", "：", ":", ".", "{", "}", "[", "]"):
            normalized = normalized.replace(token, "")
        return normalized

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

    def _get_tv_transfer_record(
        self,
        emby_path: str,
        tmdb_id: Optional[int],
        season_num: Optional[int],
        episode_num: Optional[int],
    ) -> Optional[TransferHistory]:
        """
        查询电视剧单集转移记录。

        :param emby_path: Emby 上报的整理后路径。
        :param tmdb_id: TMDB ID。
        :param season_num: 季号。
        :param episode_num: 集号。
        :return: 转移记录。
        """
        if tmdb_id and season_num and episode_num:
            histories: List[TransferHistory] = self._transferhis.get_by(
                tmdbid=tmdb_id,
                mtype=MediaType.TV.value,
                season=f"S{season_num:02d}",
                episode=f"E{episode_num:02d}",
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

    @staticmethod
    def _convert_openlist_url_to_api_path(openlist_url: str) -> Optional[str]:
        """
        将 OpenList URL 还原为 OpenList API 路径。

        :param openlist_url: STRM 中的 OpenList URL。
        :return: OpenList API 路径。
        """
        target = openlist_url.strip()
        if not target:
            return None
        parsed = urlparse(target)
        raw_path = unquote(parsed.path or "").strip()
        if not raw_path:
            return None
        if raw_path.startswith("/d/"):
            return raw_path[2:]
        if raw_path == "/d":
            return "/"
        return raw_path

    def _delete_p115_file_item(
        self,
        media_name: str,
        p115_path: str,
        openlist_api_path: Optional[str] = None,
        openlist_is_dir: bool = False,
    ) -> bool:
        """
        删除 115 文件或目录。

        :param media_name: 媒体名称。
        :param p115_path: 115 网盘路径。
        :param openlist_api_path: OpenList API 路径。
        :param openlist_is_dir: OpenList 删除目标是否为目录。
        :return: 是否删除成功。
        """
        if openlist_api_path and self._openlist_api_url and self._openlist_token:
            if self._delete_via_openlist_api(
                media_name=media_name,
                openlist_api_path=openlist_api_path,
                is_dir=openlist_is_dir,
            ):
                return True
            logger.warning("【115 Emby 联动删除】OpenList API 删除失败，回退 StorageChain：%s", openlist_api_path)

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

    def _delete_via_openlist_api(
        self,
        media_name: str,
        openlist_api_path: str,
        is_dir: bool = False,
    ) -> bool:
        """
        通过 OpenList API 删除文件。

        :param media_name: 媒体名称。
        :param openlist_api_path: OpenList API 路径。
        :param is_dir: 是否删除目录。
        :return: 是否删除成功。
        """
        base_url = self._openlist_api_url.rstrip("/")
        target_path = openlist_api_path.rstrip("/")
        parent_dir = str(Path(target_path).parent).replace("\\", "/")
        file_name = Path(target_path).name
        payload = json.dumps({"dir": parent_dir, "names": [file_name]}).encode("utf-8")
        headers = {
            "Authorization": self._openlist_token,
            "Content-Type": "application/json",
        }
        api_url = f"{base_url}/api/fs/remove"
        try:
            req = urllib_request.Request(api_url, data=payload, headers=headers, method="POST")
            with urllib_request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8", errors="ignore")
            logger.info(
                "【115 Emby 联动删除】%s 通过 OpenList API 删除%s成功：%s，响应：%s",
                media_name,
                "目录" if is_dir else "文件",
                target_path,
                body,
            )
            return True
        except Exception as err:
            logger.error("【115 Emby 联动删除】OpenList API 删除失败：%s", err, exc_info=True)
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
            if key in event_data:
                return event_data.get(key)
            lower_key = key.lower()
            for candidate_key, candidate_value in event_data.items():
                if str(candidate_key).lower() == lower_key:
                    return candidate_value
            return None
        return getattr(event_data, key, None)

    async def _extract_webhook_payload(
        self,
        request: Request = None,
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
            pass

        try:
            raw_body = await request.body()
            if raw_body:
                candidate = self._loads_json(raw_body.decode("utf-8", errors="ignore").strip())
                if isinstance(candidate, dict):
                    return candidate
        except Exception:
            pass

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
            value = cls._event_raw_value(event_data, key)
            if isinstance(value, dict):
                server_name = cls._event_value(value, "name").strip()
                if server_name:
                    return server_name
            value = str(value or "").strip()
            if value:
                return value
        return ""

    @classmethod
    def _extract_media_type(cls, event_data: Any) -> str:
        item = cls._event_raw_value(event_data, "item")
        if isinstance(item, dict):
            value = cls._event_value(item, "type").strip()
            if value:
                return value
        return cls._event_value(event_data, "item_type").strip()

    @classmethod
    def _extract_media_name(cls, event_data: Any) -> str:
        item = cls._event_raw_value(event_data, "item")
        if isinstance(item, dict):
            value = cls._event_value(item, "name").strip()
            if value:
                return value
        return cls._event_value(event_data, "item_name").strip()

    @classmethod
    def _extract_media_path(cls, event_data: Any) -> str:
        item = cls._event_raw_value(event_data, "item")
        if isinstance(item, dict):
            value = cls._event_value(item, "path").strip()
            if value:
                return value
        return cls._event_value(event_data, "item_path").strip()

    @classmethod
    def _extract_tmdb_id(cls, event_data: Any) -> Optional[int]:
        item = cls._event_raw_value(event_data, "item")
        if isinstance(item, dict):
            provider_ids = cls._event_raw_value(item, "providerids")
            if isinstance(provider_ids, dict):
                tmdb_id = cls._safe_int(cls._event_raw_value(provider_ids, "tmdb"))
                if tmdb_id:
                    return tmdb_id
        return cls._safe_int(cls._event_raw_value(event_data, "tmdb_id"))

    @classmethod
    def _extract_season_num(cls, event_data: Any) -> Optional[int]:
        """
        提取季号。

        :param event_data: 事件数据。
        :return: 季号。
        """
        item = cls._event_raw_value(event_data, "item")
        if isinstance(item, dict):
            season_num = cls._safe_int(cls._event_raw_value(item, "parentindexnumber"))
            if season_num is not None:
                return season_num
        return cls._safe_int(cls._event_raw_value(event_data, "season_id"))

    @classmethod
    def _extract_episode_num(cls, event_data: Any) -> Optional[int]:
        """
        提取集号。

        :param event_data: 事件数据。
        :return: 集号。
        """
        item = cls._event_raw_value(event_data, "item")
        if isinstance(item, dict):
            episode_num = cls._safe_int(cls._event_raw_value(item, "indexnumber"))
            if episode_num is not None:
                return episode_num
        return cls._safe_int(cls._event_raw_value(event_data, "episode_id"))

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

    def _matches_emby_library_path(self, emby_path: str) -> bool:
        """
        判断媒体路径是否命中任一配置的 Emby 媒体库根路径。

        :param emby_path: Emby 路径。
        :return: 是否命中。
        """
        candidates = [
            item.strip()
            for item in self._emby_library_path.replace(",", "\n").splitlines()
            if item.strip()
        ]
        if not candidates:
            return True
        return any(self._has_prefix(emby_path, candidate) for candidate in candidates)


# 某些 MoviePilot 运行环境会错误地保留抽象方法标记，这里显式清空以确保插件可实例化。
P115EmbySyncDel.__abstractmethods__ = frozenset()
