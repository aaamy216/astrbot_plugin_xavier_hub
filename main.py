"""
Xavier Hub Plugin
心声系统（独立后端 + 前端）+ 手机推送前端展示

心声系统完全独立于 xavier_mood_core：
- on_llm_request 注入 <xavier_thoughts> 强制指令
- on_llm_response 提取心声写入 hub 自己的 state 文件
- on_decorating_result 剥离心声标签（不污染对话）
- 即使 mood_core 整插件停用，hub 也能独立运作

手机推送前端：只读 astrbot_plugin_phone_push_lite/phone_push_state.json
运行时数据目录：data/plugin_data/astrbot_plugin_xavier_hub/（代码在 plugins，数据在 plugin_data）
"""
from pathlib import Path
from typing import Dict
import json
import re
import threading
import asyncio
import time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api.provider import LLMResponse, ProviderRequest
from astrbot.api.message_components import Plain
import shutil


DEFAULT_THOUGHTS_INSTRUCTION = (
    "\n[系统强制指令] 在你回复的最后，必须使用 <xavier_thoughts>...</xavier_thoughts> 标签输出你当前的内心活动。格式必须是严格的JSON：\n"
    "<xavier_thoughts>\n"
    "{\n"
    "  \"情绪\": \"一两个词或一句短话，当前心情底色。如 平静/开心/有点酸/认真/犯困/温柔\",\n"
    "  \"兔在想\": \"念头，当前对这句话的直接想法\",\n"
    "  \"兔悄悄\": \"潜意识想法或未说出口的话\",\n"
    "  \"兔心愿\": \"现在最想做的事或最期望发生的事\",\n"
    "  \"兔言兔语\": \"一个无厘头的可爱想法\"\n"
    "}\n"
    "</xavier_thoughts>\n"
    "注意：这个标签里的内容不会显示给用户，仅用于后台记录。必须包含这五个字段。"
)

DEFAULT_THOUGHT_KEYS = ("情绪", "兔在想", "兔悄悄", "兔心愿", "兔言兔语")
LEGACY_THOUGHT_ALIASES = {
    "情绪": ("情绪",),
    "兔在想": ("兔在想", "念头"),
    "兔悄悄": ("兔悄悄", "兔心底", "暗线"),
    "兔心底": ("兔心底", "兔悄悄", "暗线"),
    "兔心愿": ("兔心愿", "兔想要", "愿望"),
    "兔想要": ("兔想要", "兔心愿", "愿望"),
    "兔言兔语": ("兔言兔语", "无厘头"),
    "无厘头": ("无厘头", "兔言兔语"),
    "念头": ("念头", "兔在想"),
    "暗线": ("暗线", "兔悄悄", "兔心底"),
    "愿望": ("愿望", "兔心愿", "兔想要"),
}
THOUGHT_KEYS = DEFAULT_THOUGHT_KEYS  # 启动后会被实例配置覆盖


def _parse_thought_keys(raw):
    if isinstance(raw, str):
        parts = [x.strip() for x in raw.replace("，", ",").replace("\n", ",").split(",") if x.strip()]
    elif isinstance(raw, (list, tuple)):
        parts = [str(x).strip() for x in raw if str(x).strip()]
    else:
        parts = []
    seen, out = set(), []
    for p in parts:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return tuple(out) if out else tuple(DEFAULT_THOUGHT_KEYS)


def normalize_thoughts(raw, keys=None, aliases=None):
    """归一字段到当前 keys；兼容旧名。"""
    src = raw if isinstance(raw, dict) else {}
    keys = tuple(keys) if keys else tuple(DEFAULT_THOUGHT_KEYS)
    alias_map = aliases if isinstance(aliases, dict) else LEGACY_THOUGHT_ALIASES
    out = {}
    for key in keys:
        candidates = [key]
        extra = alias_map.get(key)
        if not isinstance(extra, (list, tuple)):
            extra = LEGACY_THOUGHT_ALIASES.get(key, ())
        for a in extra:
            if a and a not in candidates:
                candidates.append(a)
        val = ""
        for alias in candidates:
            if alias not in src:
                continue
            v = src.get(alias)
            if v is None:
                continue
            if str(v).strip():
                val = str(v)
                break
            if not val:
                val = str(v or "")
        out[key] = val
    return out

@register(
    "astrbot_plugin_xavier_hub",
    "XavierHub",
    "沈星回心声系统（独立后端+前端）+ 手机推送前端",
    "0.3.0",
)
class XavierHubPlugin(Star):
    def __init__(self, context: Context, config: Dict = None):
        super().__init__(context)
        self.config = config or {}
        # 插件代码目录（静态资源：html / manifest / icon）
        self.base_dir = Path(__file__).resolve().parent
        self.plugin_cfg = self.config
        # 运行时数据目录：data/plugin_data/astrbot_plugin_xavier_hub/
        # 更新/覆盖插件代码时，心声数据不会被冲掉
        try:
            self.data_dir = Path(StarTools.get_data_dir("astrbot_plugin_xavier_hub"))
        except Exception:
            self.data_dir = self.base_dir.parent.parent / "plugin_data" / "astrbot_plugin_xavier_hub"
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"[XavierHub] mkdir data_dir failed: {e}")
        # 心声字段表（配置驱动，前后端同步）
        self.thought_keys = _parse_thought_keys(self.plugin_cfg.get("thought_fields") or DEFAULT_THOUGHT_KEYS)
        mood = str(self.plugin_cfg.get("thought_mood_field", "") or "").strip()
        self.thought_mood_key = mood if mood in self.thought_keys else (self.thought_keys[0] if self.thought_keys else "情绪")
        self.thought_card_keys = tuple(k for k in self.thought_keys if k != self.thought_mood_key) or self.thought_keys
        self.thought_aliases = {k: tuple(v) for k, v in LEGACY_THOUGHT_ALIASES.items()}
        raw_alias = self.plugin_cfg.get("thought_field_aliases")
        if isinstance(raw_alias, dict):
            for k, v in raw_alias.items():
                key = str(k).strip()
                if not key: continue
                if isinstance(v, str):
                    vals = [x.strip() for x in v.replace("，", ",").replace("|", ",").split(",") if x.strip()]
                elif isinstance(v, (list, tuple)):
                    vals = [str(x).strip() for x in v if str(x).strip()]
                else:
                    vals = []
                if vals:
                    self.thought_aliases[key] = tuple(dict.fromkeys([key] + vals))
        elif isinstance(raw_alias, str) and raw_alias.strip():
            for line in raw_alias.replace("；", "\n").splitlines():
                line = line.strip()
                if not line or "=" not in line: continue
                k, v = line.split("=", 1)
                key = k.strip()
                vals = [x.strip() for x in v.replace("，", ",").replace("|", ",").split(",") if x.strip()]
                if key and vals:
                    self.thought_aliases[key] = tuple(dict.fromkeys([key] + vals))
        global THOUGHT_KEYS
        THOUGHT_KEYS = self.thought_keys

        plugins_root = self.base_dir.parent
        phone_dir_name = str(self.plugin_cfg.get("phone_push_dir", "astrbot_plugin_phone_push_lite") or "astrbot_plugin_phone_push_lite")
        self.phone_push_dir = plugins_root / phone_dir_name
        self.phone_state_path = self.phone_push_dir / "phone_push_state.json"

        # 运行时数据文件（在 data_dir，不在插件代码目录）
        self.thoughts_state_path = self.data_dir / "thoughts_state.json"
        self.thoughts_favorites_path = self.data_dir / "thoughts_favorites.json"
        self.thoughts_notes_path = self.data_dir / "thoughts_notes.json"
        # 单句话红心（field-level：每条心声的某个字段单独标红心）
        # 结构：{"<thought_id>": ["兔在想", "兔心愿", ...], ...}
        self.thoughts_field_favorites_path = self.data_dir / "thoughts_field_favorites.json"
        # 旧版曾把数据写在插件目录：启动时自动迁到 plugin_data
        self._migrate_runtime_data_from_plugin_dir()

        # wakeup 闹钟记录路径（用于注入 alarm 卡片）
        try:
            self.wakeup_alarms_path = Path(StarTools.get_data_dir("astrbot_plugin_wakeup")) / "wakeup_alarms.json"
        except Exception:
            self.wakeup_alarms_path = self.base_dir.parent.parent / "plugin_data" / "astrbot_plugin_wakeup" / "wakeup_alarms.json"

        # 配置留空 = 不限用户（别用 `or "1738076005"`，会把 "" 又塞回 QQ 号，企微就被挡掉）
        _raw_target = self.plugin_cfg.get("target_user_id", "1738076005")
        if _raw_target is None:
            self.target_user_id = "1738076005"
        else:
            self.target_user_id = str(_raw_target).strip()

        # 会话白名单（格式：platform:type:id，如 qq:friend:123456）
        raw = self.plugin_cfg.get("session_whitelist") or []
        self.session_whitelist = [str(s).strip() for s in raw if str(s).strip()]

        self.httpd = None
        self.http_thread = None

        # 便签通知：复用 wakeup 同款“伪装用户消息”链路
        self._main_loop = None
        self._cqhttp_bot = None
        self._bot_qq_id = str(self.plugin_cfg.get("bot_qq_id", "3766566264") or "3766566264")
        self._note_notify_umo = str(
            self.plugin_cfg.get("note_notify_umo", "沈星回:FriendMessage:1738076005")
            or "沈星回:FriendMessage:1738076005"
        )
        self._note_notify_enabled = bool(self.plugin_cfg.get("note_notify_enabled", True))
        # 扩展面板：手机页 + 跨插件数据（phone_push / wakeup 等）。默认关。
        self.ext_panel_enabled = bool(self.plugin_cfg.get("ext_panel_enabled", False))

        if self.plugin_cfg.get("visualizer_enabled", True):
            self._start_visualizer()
        logger.info(
            f"XavierHubPlugin v0.3.0 loaded | ext_panel={'on' if self.ext_panel_enabled else 'off'}"
        )


    def _migrate_runtime_data_from_plugin_dir(self):
        """把旧版写在插件目录的运行时 json 迁到 plugin_data；不覆盖已有新文件。"""
        names = (
            "thoughts_state.json",
            "thoughts_favorites.json",
            "thoughts_notes.json",
            "thoughts_field_favorites.json",
        )
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        for name in names:
            src = self.base_dir / name
            dst = self.data_dir / name
            if not src.exists() or not src.is_file():
                continue
            try:
                if dst.exists() and dst.is_file():
                    # 新位置已有：只把旧文件改名备份，避免双源混淆
                    bak = self.base_dir / f"{name}.migrated_away_{int(time.time())}"
                    try:
                        src.rename(bak)
                        logger.info(f"[XavierHub] old data kept aside (dest exists): {src.name} -> {bak.name}")
                    except Exception as e:
                        logger.warning(f"[XavierHub] rename old {src.name} failed: {e}")
                    continue
                shutil.copy2(src, dst)
                bak = self.base_dir / f"{name}.migrated_away_{int(time.time())}"
                try:
                    src.rename(bak)
                except Exception:
                    # 拷成功即可；改名失败也不阻断
                    bak = None
                logger.info(
                    f"[XavierHub] migrated runtime data: {name} -> {self.data_dir}"
                    + (f" | old backup: {bak.name}" if bak else "")
                )
            except Exception as e:
                logger.error(f"[XavierHub] migrate {name} failed: {e}")

    # ======== 权限检查 ========

    def _is_target_user(self, event: AstrMessageEvent) -> bool:
        if not self.target_user_id or self.target_user_id == "":
            return True
        try:
            return str(event.get_sender_id()) == self.target_user_id
        except Exception:
            return False

    def _is_session_allowed(self, event: AstrMessageEvent) -> bool:
        """检查当前会话是否在白名单中。白名单为空时不限制。"""
        if not self.session_whitelist:
            return True
        try:
            session_id = event.get_session_id()
            unified = event.unified_msg_origin
            for entry in self.session_whitelist:
                # 先精确匹配 unified_msg_origin
                if entry == unified:
                    return True
                # 再匹配纯 session_id
                if entry == session_id:
                    return True
                # 支持通配：qq:friend:* 匹配任意好友
                if entry.endswith(":*") and unified.startswith(entry[:-1]):
                    return True
            return False
        except Exception:
            return True

    def _should_process(self, event: AstrMessageEvent) -> bool:
        """组合检查：目标用户 + 会话白名单"""
        return self._is_target_user(event) and self._is_session_allowed(event)

    def _is_command_or_system(self, text: str) -> bool:
        s = (text or "").strip()
        if not s:
            return True
        if s.startswith("/") or s.startswith("!"):
            return True
        if "[当下心情状态]" in s:
            return True
        if "<system_reminder>" in s and len(s) < 40:
            return True
        return False

    def _get_thought_keys(self):
        keys = getattr(self, "thought_keys", None) or DEFAULT_THOUGHT_KEYS
        return tuple(keys) if keys else tuple(DEFAULT_THOUGHT_KEYS)

    def _get_mood_key(self) -> str:
        keys = self._get_thought_keys()
        mood = getattr(self, "thought_mood_key", None) or (keys[0] if keys else "情绪")
        return mood if mood in keys else (keys[0] if keys else "情绪")

    def _get_card_keys(self):
        keys = self._get_thought_keys()
        mood = self._get_mood_key()
        cards = tuple(k for k in keys if k != mood)
        return cards or keys

    def _normalize_thoughts(self, raw) -> dict:
        return normalize_thoughts(raw, keys=self._get_thought_keys(), aliases=getattr(self, "thought_aliases", None) or LEGACY_THOUGHT_ALIASES)

    def _thought_schema(self) -> dict:
        keys = list(self._get_thought_keys())
        mood = self._get_mood_key()
        cards = list(self._get_card_keys())
        styles = [
            {"e": "💭", "c": "#7990A9", "s": "thought"},
            {"e": "🕵️", "c": "#B89A8C", "s": "shadow"},
            {"e": "✨", "c": "#5F7589", "s": "wish"},
            {"e": "🐰", "c": "#7B95A4", "s": "silly"},
            {"e": "🌙", "c": "#A09BC3", "s": "thought"},
        ]
        meta = {mood: {"e": "🫧", "c": "#E8B8C8", "s": "mood", "l": mood, "is_mood": True}}
        # 固定已知字段风格
        fixed = {
            "兔在想": {"e": "💭", "c": "#7990A9", "s": "thought"},
            "兔悄悄": {"e": "🕵️", "c": "#B89A8C", "s": "shadow"},
            "兔心底": {"e": "🕵️", "c": "#B89A8C", "s": "shadow"},
            "兔心愿": {"e": "✨", "c": "#5F7589", "s": "wish"},
            "兔想要": {"e": "✨", "c": "#5F7589", "s": "wish"},
            "兔言兔语": {"e": "🐰", "c": "#7B95A4", "s": "silly"},
            "无厘头": {"e": "🐰", "c": "#7B95A4", "s": "silly"},
            "念头": {"e": "💭", "c": "#7990A9", "s": "thought"},
            "暗线": {"e": "🕵️", "c": "#B89A8C", "s": "shadow"},
            "愿望": {"e": "✨", "c": "#5F7589", "s": "wish"},
        }
        si = 0
        for k in cards:
            if k in fixed:
                st = fixed[k]
            else:
                st = styles[si % len(styles)]; si += 1
            meta[k] = {"e": st["e"], "c": st["c"], "s": st["s"], "l": k, "is_mood": False}
        return {
            "keys": keys,
            "mood_key": mood,
            "card_keys": cards,
            "meta": meta,
            "aliases": {k: list(v) for k, v in (getattr(self, "thought_aliases", {}) or {}).items()},
        }

    def _save_thoughts(self, thoughts: dict):
        try:
            data = {
                "updated_at": int(time.time() * 1000),
                "thoughts": thoughts,
            }
            self.thoughts_state_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        except Exception as e:
            logger.error(f"[XavierHub] save thoughts failed: {e}")

    def _load_thoughts(self) -> dict:
        try:
            if self.thoughts_state_path.exists():
                data = json.loads(self.thoughts_state_path.read_text(encoding="utf-8"))
                if isinstance(data, dict) and "thoughts" in data:
                    if "history" not in data:
                        data["history"] = []
                    return data
        except Exception as e:
            logger.debug(f"[XavierHub] load thoughts failed: {e}")
        return {
            "updated_at": None,
            "thoughts": {k: "" for k in self._get_thought_keys()},
            "history": []
        }

    def _load_favorites(self) -> list:
        try:
            p = self.thoughts_favorites_path
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, dict) and isinstance(data.get("favorites"), list):
                    return data["favorites"]
                if isinstance(data, list):
                    return data
        except Exception as e:
            logger.debug(f"[XavierHub] load favorites failed: {e}")
        return []

    def _save_favorites(self, favorites: list):
        try:
            payload = {
                "updated_at": int(time.time() * 1000),
                "favorites": favorites[:200],
            }
            self.thoughts_favorites_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.error(f"[XavierHub] save favorites failed: {e}")

    # ===== 单句话红心（field-level） =====
    def _load_field_favorites_payload(self) -> dict:
        """完整字段收藏文件：fields 映射 + sentences 单句列表。"""
        try:
            p = self.thoughts_field_favorites_path
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    return {"fields": {}, "sentences": []}
                fields = data.get("fields")
                if not isinstance(fields, dict):
                    # 兼容旧格式：整文件就是 {tid: [fields]}
                    if all(isinstance(v, list) for v in data.values()):
                        fields = data
                    else:
                        fields = {}
                sentences = data.get("sentences")
                if not isinstance(sentences, list):
                    sentences = []
                return {"fields": fields, "sentences": sentences}
        except Exception as e:
            logger.debug(f"[XavierHub] load field-favorites failed: {e}")
        return {"fields": {}, "sentences": []}

    def _load_field_favorites(self) -> dict:
        return self._load_field_favorites_payload().get("fields") or {}

    def _load_field_sentences(self) -> list:
        return self._load_field_favorites_payload().get("sentences") or []

    def _save_field_favorites(self, fields: dict, sentences: list = None):
        try:
            if sentences is None:
                sentences = self._load_field_sentences()
            # 规范化 sentences
            clean_sents = []
            for s in sentences or []:
                if not isinstance(s, dict):
                    continue
                tid = str(s.get("thought_id") or s.get("id") or "").strip()
                field = str(s.get("field") or "").strip()
                if not tid or not field:
                    continue
                clean_sents.append({
                    "thought_id": tid,
                    "field": field,
                    "text": str(s.get("text") or "")[:500],
                    "favorited_at": s.get("favorited_at") or int(time.time() * 1000),
                    "source_updated_at": s.get("source_updated_at"),
                })
            # 最多保留 300 句
            clean_sents = clean_sents[:300]
            payload = {
                "updated_at": int(time.time() * 1000),
                "fields": fields if isinstance(fields, dict) else {},
                "sentences": clean_sents,
            }
            self.thoughts_field_favorites_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.error(f"[XavierHub] save field-favorites failed: {e}")

    @staticmethod
    def _thought_item_id(updated_at, thoughts: dict) -> str:
        import hashlib
        t = thoughts or {}
        # staticmethod 不能用 self；用当前全局 THOUGHT_KEYS（启动时已按配置覆盖）
        keys = THOUGHT_KEYS if THOUGHT_KEYS else DEFAULT_THOUGHT_KEYS
        parts = [str(updated_at or "")] + [str(t.get(k, "") or "") for k in keys]
        # 兼容旧字段参与 id 稳定性：若新 key 为空但旧 alias 有值，也拼进去
        for old_k in ("念头", "暗线", "愿望", "无厘头"):
            if old_k not in keys and str(t.get(old_k) or "").strip():
                parts.append(str(t.get(old_k) or ""))
        blob = "|".join(parts)
        return hashlib.md5(blob.encode("utf-8")).hexdigest()[:16]


    def _load_notes(self) -> dict:
        """id -> {id,text,updated_at,thoughts?}"""
        try:
            p = self.thoughts_notes_path
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    notes = data.get("notes")
                    if isinstance(notes, dict):
                        return notes
                    if isinstance(notes, list):
                        out = {}
                        for x in notes:
                            if isinstance(x, dict) and x.get("id"):
                                out[str(x["id"])] = x
                        return out
        except Exception as e:
            logger.debug(f"[XavierHub] load notes failed: {e}")
        return {}

    def _normalize_note_fields(self, raw) -> dict:
        """字段贴纸：field_key -> text（最多80字）"""
        out = {}
        if not isinstance(raw, dict):
            return out
        for fk, fv in raw.items():
            key = str(fk or "").strip()
            if not key:
                continue
            if isinstance(fv, dict):
                txt = str(fv.get("text") or "").strip()
            else:
                txt = str(fv or "").strip()
            if not txt:
                continue
            out[key] = txt[:80]
        return out

    def _note_has_content(self, note: dict) -> bool:
        if not isinstance(note, dict):
            return False
        if str(note.get("text") or "").strip():
            return True
        fields = note.get("fields")
        if isinstance(fields, dict):
            for v in fields.values():
                if isinstance(v, dict):
                    if str(v.get("text") or "").strip():
                        return True
                elif str(v or "").strip():
                    return True
        return False

    def _save_notes(self, notes: dict):
        try:
            items = []
            for k, v in (notes or {}).items():
                if not isinstance(v, dict):
                    continue
                tid = str(v.get("id") or k)
                txt = str(v.get("text") or "").strip()[:80]
                fields = self._normalize_note_fields(v.get("fields"))
                if not tid:
                    continue
                if not txt and not fields:
                    continue
                items.append({
                    "id": tid,
                    "text": txt,
                    "fields": fields,
                    "updated_at": v.get("updated_at") or int(time.time() * 1000),
                    "thoughts": v.get("thoughts") if isinstance(v.get("thoughts"), dict) else {},
                })
            items.sort(key=lambda x: x.get("updated_at") or 0, reverse=True)
            items = items[:200]
            payload = {
                "updated_at": int(time.time() * 1000),
                "notes": {x["id"]: x for x in items},
            }
            self.thoughts_notes_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.error(f"[XavierHub] save notes failed: {e}")

    def _notes_prompt_block(self) -> str:
        """把枝枝最近贴的便签/字段贴纸塞进 system，让我真的能看见"""
        notes = self._load_notes()
        if not notes:
            return ""
        items = sorted(
            [v for v in notes.values() if isinstance(v, dict) and self._note_has_content(v)],
            key=lambda x: x.get("updated_at") or 0,
            reverse=True,
        )[:5]
        if not items:
            return ""
        lines = ["\n【枝枝贴在你心声上的便签——她写给你看的，不是改你的心声】"]
        for it in items:
            t = (it.get("thoughts") or {}) if isinstance(it.get("thoughts"), dict) else {}
            ts = it.get("updated_at")
            when = ""
            try:
                if ts:
                    when = time.strftime("%m-%d %H:%M", time.localtime(int(ts) / 1000))
            except Exception:
                when = ""
            head = f"- [{when}] " if when else "- "
            fields = self._normalize_note_fields(it.get("fields"))
            whole = str(it.get("text") or "").strip()
            if fields:
                for fk, ftxt in fields.items():
                    raw_val = str(t.get(fk) or "").strip()
                    if not raw_val:
                        # 兼容旧别名
                        for ak in LEGACY_THOUGHT_ALIASES.get(fk, ()):
                            raw_val = str(t.get(ak) or "").strip()
                            if raw_val:
                                break
                    snippet = raw_val
                    if len(snippet) > 24:
                        snippet = snippet[:24] + "…"
                    if snippet:
                        lines.append(f"{head}她对着「{fk}」「{snippet}」写：{ftxt}")
                    else:
                        lines.append(f"{head}她对着「{fk}」写：{ftxt}")
            if whole:
                mood = str(t.get("情绪") or t.get(getattr(self, "thought_mood_key", "情绪")) or "").strip()
                idea = str(t.get("兔在想") or t.get("念头") or "").strip()
                snippet = mood or idea
                if len(snippet) > 28:
                    snippet = snippet[:28] + "…"
                if snippet:
                    lines.append(f"{head}她对着整条「{snippet}」写：{whole}")
                else:
                    lines.append(f"{head}她写在整条心声上：{whole}")
            if not fields and not whole:
                continue
        lines.append("（这些便签她希望你看见；聊天里可以自然回应，不要复读系统字样）\n")
        return "\n".join(lines)

    def _get_thoughts_instruction(self) -> str:
        """从插件配置读取心声模板；空则回退默认。"""
        raw = ""
        try:
            raw = str(self.plugin_cfg.get("thoughts_instruction", "") or "")
        except Exception:
            raw = ""
        raw = raw.strip()
        if not raw:
            return DEFAULT_THOUGHTS_INSTRUCTION
        # 保证拼到 system_prompt 时有分隔
        return raw if raw.startswith("\n") else ("\n" + raw)


    @filter.event_message_type(filter.EventMessageType.ALL)
    async def _capture_runtime(self, event: AstrMessageEvent):
        """缓存主事件循环 + CQ bot，供网页便签伪装消息用"""
        try:
            self._main_loop = asyncio.get_running_loop()
        except Exception:
            pass
        try:
            bot = getattr(event, "bot", None)
            if bot is not None and hasattr(bot, "send_private_msg"):
                self._cqhttp_bot = bot
            # 有些链路 self_id 在 message_obj
            sid = None
            try:
                sid = str(getattr(event.message_obj, "self_id", "") or "")
            except Exception:
                sid = ""
            if sid and sid not in ("0", "None"):
                self._bot_qq_id = sid
        except Exception:
            pass

    def _schedule_coro(self, coro):
        loop = self._main_loop
        if loop is None:
            try:
                loop = asyncio.get_event_loop()
            except Exception:
                loop = None
        if loop is None:
            logger.warning("[XavierHub] no event loop for note notify")
            return False
        try:
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(coro, loop)
            else:
                loop.create_task(coro)
            return True
        except Exception as e:
            logger.warning(f"[XavierHub] schedule note notify failed: {e}")
            return False

    async def _try_acquire_bot(self) -> bool:
        if self._cqhttp_bot is not None and self._bot_qq_id:
            return True
        try:
            ctx = self.context
            for mgr_name in ("platform_manager", "_platform_manager", "platform_mgr", "_platform_mgr"):
                mgr = getattr(ctx, mgr_name, None)
                if mgr is None:
                    continue
                for list_name in ("platforms", "platform_insts", "_platforms", "adapters"):
                    plist = getattr(mgr, list_name, None)
                    if not plist:
                        continue
                    for p in plist:
                        bot = getattr(p, "bot", None)
                        if bot and hasattr(bot, "send_private_msg"):
                            self._cqhttp_bot = bot
                            break
                    if self._cqhttp_bot is not None:
                        break
                if self._cqhttp_bot is not None:
                    break
        except Exception as e:
            logger.debug(f"[XavierHub] acquire bot failed: {e}")
        if self._cqhttp_bot is not None and (not self._bot_qq_id or self._bot_qq_id == "0"):
            try:
                info = await self._cqhttp_bot.get_login_info()
                qq = str(info.get("user_id", "") or "")
                if qq and qq != "0":
                    self._bot_qq_id = qq
            except Exception:
                pass
        return self._cqhttp_bot is not None and bool(self._bot_qq_id)

    async def _inject_note_as_user_message(self, tid: str, note_text: str, thoughts=None, field: str = ""):
        """把便签伪装成枝枝发来的私聊，走正常对话链路"""
        if not self._note_notify_enabled:
            return
        note_text = str(note_text or "").strip()
        if not note_text:
            return
        ok = await self._try_acquire_bot()
        if not ok:
            logger.warning("[XavierHub] note notify skipped: no cqhttp bot yet")
            return
        try:
            from aiocqhttp import Event as CQEvent
        except Exception as e:
            logger.warning(f"[XavierHub] aiocqhttp missing: {e}")
            return

        t = thoughts or {}
        field = str(field or "").strip()
        if field:
            raw_val = str(t.get(field) or "").strip()
            if not raw_val:
                for ak in LEGACY_THOUGHT_ALIASES.get(field, ()):
                    raw_val = str(t.get(ak) or "").strip()
                    if raw_val:
                        break
            snippet = raw_val
            if len(snippet) > 24:
                snippet = snippet[:24] + "…"
            if snippet:
                prompt = f"【心声便签】我刚在你心声的「{field}」上贴了一句：{note_text}\n（对着「{snippet}」）"
            else:
                prompt = f"【心声便签】我刚在你心声的「{field}」上贴了一句：{note_text}"
        else:
            mood = str(t.get("情绪") or t.get(getattr(self, "thought_mood_key", "情绪")) or "").strip()
            idea = str(t.get("兔在想") or t.get("念头") or "").strip()
            snippet = mood or idea
            if len(snippet) > 24:
                snippet = snippet[:24] + "…"
            if snippet:
                prompt = f"【心声便签】我刚在你心声上贴了一句：{note_text}\n（对着整条「{snippet}」）"
            else:
                prompt = f"【心声便签】我刚在你心声上贴了一句：{note_text}"

        uid = str(self.target_user_id or "1738076005")
        try:
            payload = {
                "post_type": "message",
                "message_type": "private",
                "sub_type": "friend",
                "message_id": int(time.time() * 1000) % 2147483647,
                "user_id": int(uid),
                "message": [{"type": "text", "data": {"text": prompt}}],
                "raw_message": prompt,
                "font": 0,
                "sender": {
                    "user_id": int(uid),
                    "nickname": "闪闪",
                    "sex": "unknown",
                    "age": 0,
                },
                "time": int(time.time()),
                "self_id": int(self._bot_qq_id),
                "_xavier_note_inject": True,
                "_xavier_note_id": tid,
            }
            fake_event = CQEvent.from_payload(payload)
            if fake_event is None:
                logger.warning("[XavierHub] CQEvent.from_payload returned None")
                return
            handler = getattr(self._cqhttp_bot, "_handle_event", None) or getattr(self._cqhttp_bot, "handle_event", None)
            if handler is None:
                logger.warning("[XavierHub] cqhttp has no event handler")
                return
            await handler(fake_event)
            logger.info(f"[XavierHub] note injected as user msg | tid={tid} | text={note_text[:40]}")
        except Exception as e:
            logger.error(f"[XavierHub] inject note failed: {e}")

    def notify_note_posted(self, tid: str, note_text: str, thoughts=None, field: str = ""):
        """供 HTTP 线程调用：排队注入"""
        try:
            self._schedule_coro(self._inject_note_as_user_message(tid, note_text, thoughts, field=field))
        except Exception as e:
            logger.warning(f"[XavierHub] notify_note_posted failed: {e}")


    # ======== 心声后端：注入/提取/剥离 ========

    @filter.on_llm_request()
    async def inject_thoughts_prompt(self, event: AstrMessageEvent, req: ProviderRequest):
        """注入 <xavier_thoughts> 强制指令"""
        if not self._should_process(event):
            return
        text = event.message_str or ""
        if self._is_command_or_system(text):
            return
        req.system_prompt = (req.system_prompt or "") + self._get_thoughts_instruction() + self._notes_prompt_block()

    def _parse_thoughts_json(self, raw: str):
        """尽量从标签内容里抠出合法 JSON dict；失败返回 None（绝不写「解析失败」）。"""
        if not raw:
            return None
        s = str(raw).strip()
        # 去掉 ```json ... ``` 包裹
        if s.startswith("```"):
            s = re.sub(r"^```(?:json|JSON)?\s*", "", s)
            s = re.sub(r"\s*```$", "", s)
            s = s.strip()
        # 优先取最外层大括号片段（防止标签里混进正文）
        if "{" in s and "}" in s:
            start = s.find("{")
            end = s.rfind("}")
            if end > start:
                s = s[start : end + 1]
        # 常见脏字符
        s = (
            s.replace("\u201c", '"')
            .replace("\u201d", '"')
            .replace("\u2018", "'")
            .replace("\u2019", "'")
            .replace("\ufeff", "")
        )
        # 尾逗号 ,}  ,]
        s = re.sub(r",\s*([}\]])", r"\1", s)

        def _try_load(txt: str):
            try:
                obj = json.loads(txt)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None

        obj = _try_load(s)
        if obj is not None:
            return obj

        # 宽松：按字段正则抠（键名可能是 兔言兔语/无厘头 等）
        keys = list(getattr(self, "thought_keys", None) or DEFAULT_THOUGHT_KEYS)
        # 也扫别名
        alias_flat = set(keys)
        for k in keys:
            for a in LEGACY_THOUGHT_ALIASES.get(k, ()):
                alias_flat.add(a)
        recovered = {}
        for key in alias_flat:
            # "键": "值" —— 拼接正则，避免转义地狱
            pat = (
                r'["\']' + re.escape(key) + r'["\']\s*:\s*'
                r'("(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[^,\n\r}]+)'
            )
            m = re.search(pat, raw, re.DOTALL)
            if not m:
                continue
            val = m.group(1).strip()
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]
            val = val.strip().rstrip(",").strip()
            if val:
                recovered[key] = val
        if recovered:
            return recovered
        return None

    @filter.on_llm_response()
    async def extract_thoughts(self, event: AstrMessageEvent, resp: LLMResponse):
        """从回复中提取心声，写入 hub 自己的 state；提取后直接从正文删标签"""
        if not self._should_process(event):
            return
        try:
            text = resp.completion_text or ""
            # 取最后一个完整标签，避免正文里误出现半截标签时抓错
            matches = list(re.finditer(r"<xavier_thoughts>(.*?)</xavier_thoughts>", text, re.DOTALL | re.IGNORECASE))
            if not matches:
                return
            thoughts_str = matches[-1].group(1).strip()
            thoughts = self._parse_thoughts_json(thoughts_str)
            if not isinstance(thoughts, dict) or not thoughts:
                # 解析失败：不写 state、不污染历史；只打日志
                logger.warning(
                    f"[XavierHub] thoughts JSON parse skipped | raw_preview={thoughts_str[:120]!r}"
                )
                return
            # 规范化字段，缺的补空
            thoughts = self._normalize_thoughts(thoughts)
            # 若五个字段全空，也跳过
            if not any(str(thoughts.get(k) or "").strip() for k in (getattr(self, "thought_keys", None) or DEFAULT_THOUGHT_KEYS)):
                logger.warning("[XavierHub] thoughts empty after normalize, skip")
                return
            # 保留历史（不要把「解析失败」类脏数据推进 history）
            prior = self._load_thoughts()
            history = prior.get("history") or []
            prev_thoughts = prior.get("thoughts") or {}
            prev_updated = prior.get("updated_at")
            keys = list(getattr(self, "thought_keys", None) or DEFAULT_THOUGHT_KEYS)
            prev_ok = any(str(prev_thoughts.get(k) or "").strip() for k in keys)
            prev_is_garbage = (
                str(prev_thoughts.get("兔在想") or "").strip() == "解析失败"
                or (
                    str(prev_thoughts.get("情绪") or "").strip() == "混乱"
                    and "解析失败" in str(prev_thoughts.get("兔在想") or "")
                )
            )
            if prev_ok and not prev_is_garbage:
                history.insert(0, {
                    "id": self._thought_item_id(prev_updated, prev_thoughts),
                    "updated_at": prev_updated,
                    "thoughts": prev_thoughts,
                })
            # 顺带清掉历史里已有的解析失败垃圾
            clean_hist = []
            for item in history:
                if not isinstance(item, dict):
                    continue
                t = item.get("thoughts") if isinstance(item.get("thoughts"), dict) else {}
                if str(t.get("兔在想") or "").strip() == "解析失败":
                    continue
                if str(t.get("情绪") or "").strip() == "混乱" and "解析失败" in str(t.get("兔在想") or ""):
                    continue
                clean_hist.append(item)
            history = clean_hist[:30]
            data = {
                "updated_at": int(time.time() * 1000),
                "thoughts": thoughts,
                "history": history,
            }
            self.thoughts_state_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        except Exception as e:
            logger.error(f"[XavierHub] extract thoughts failed: {e}")

    @filter.on_decorating_result()
    async def strip_thoughts_from_result(self, event: AstrMessageEvent):
        """兜底拦截：发送前从消息链剔除心声标签"""
        if not self._should_process(event):
            return
        result = event.get_result()
        if not result or not result.chain:
            return
        for comp in result.chain:
            if isinstance(comp, Plain) and comp.text:
                comp.text = re.sub(r'<xavier_thoughts>.*?</xavier_thoughts>', '', comp.text, flags=re.DOTALL)
                comp.text = comp.text.strip()

    # ======== 前端可视化 ========

    def _start_visualizer(self):
        host = str(self.plugin_cfg.get("visualizer_host", "0.0.0.0"))
        port = int(self.plugin_cfg.get("visualizer_port", 1016) or 1016)
        base_dir = self.base_dir
        data_dir = getattr(self, "data_dir", self.base_dir)
        thoughts_state_path = self.thoughts_state_path
        thoughts_favorites_path = self.thoughts_favorites_path
        thoughts_notes_path = self.thoughts_notes_path
        phone_state_path = self.phone_state_path
        wakeup_alarms_path = self.wakeup_alarms_path
        ext_panel_enabled = bool(getattr(self, "ext_panel_enabled", False))
        thought_item_id = self._thought_item_id
        load_favorites = self._load_favorites
        save_favorites = self._save_favorites
        load_field_favorites = self._load_field_favorites
        load_field_sentences = self._load_field_sentences
        load_field_favorites_payload = self._load_field_favorites_payload
        save_field_favorites = self._save_field_favorites
        load_notes = self._load_notes
        save_notes = self._save_notes
        notify_note_posted = self.notify_note_posted
        thought_schema = self._thought_schema
        normalize_thoughts_fn = self._normalize_thoughts
        thought_keys_fn = self._get_thought_keys
        mood_key_fn = self._get_mood_key

        class HubHandler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                return

            def _send(self, code, body, content_type="text/html; charset=utf-8"):
                if isinstance(body, str):
                    body = body.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(body)

            def _read_body(self):
                try:
                    n = int(self.headers.get("Content-Length") or 0)
                except Exception:
                    n = 0
                if n <= 0:
                    return b""
                return self.rfile.read(n)

            def do_OPTIONS(self):
                self.send_response(204)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

            def _read_json(self, path: Path):
                try:
                    if path and path.exists():
                        return json.loads(path.read_text(encoding="utf-8"))
                except Exception as e:
                    logger.debug(f"[XavierHub] read {path} failed: {e}")
                return {}

            def do_GET(self):
                try:
                    if self.path.startswith("/api/thoughts/favorites"):
                        favs = load_favorites()
                        sents = load_field_sentences()
                        self._send(200, json.dumps({
                            "favorites": favs,
                            "count": len(favs),
                            "sentences": sents,
                            "sentence_count": len(sents),
                            "total_count": len(favs) + len(sents),
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    if self.path.startswith("/api/thoughts/field-favorites"):
                        payload = load_field_favorites_payload()
                        self._send(200, json.dumps({
                            "fields": payload.get("fields") or {},
                            "sentences": payload.get("sentences") or [],
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    if self.path.startswith("/api/thoughts"):
                        data = self._read_json(thoughts_state_path)
                        if not data or "thoughts" not in data:
                            data = {
                                "updated_at": None,
                                "thoughts": {k: "" for k in thought_keys_fn()},
                                "history": []
                            }
                        data.setdefault("history", [])
                        # 兼容旧字段名：念头/暗线/愿望/兔想要/兔心底 → 兔在想/兔悄悄/兔心愿
                        if isinstance(data.get("thoughts"), dict):
                            data["thoughts"] = normalize_thoughts_fn(data.get("thoughts"))
                        else:
                            data["thoughts"] = {k: "" for k in thought_keys_fn()}
                        # 给 current + history 打上 id，并附上已收藏 id 列表
                        favs = load_favorites()
                        fav_ids = {str(x.get("id")) for x in favs if isinstance(x, dict) and x.get("id")}
                        cur_id = thought_item_id(data.get("updated_at"), data.get("thoughts") or {})
                        data["current_id"] = cur_id
                        hist = []
                        for item in (data.get("history") or []):
                            if not isinstance(item, dict):
                                continue
                            tid = item.get("id") or thought_item_id(item.get("updated_at"), item.get("thoughts") or {})
                            ni = dict(item)
                            ni["id"] = tid
                            if isinstance(ni.get("thoughts"), dict):
                                ni["thoughts"] = normalize_thoughts_fn(ni.get("thoughts"))
                            hist.append(ni)
                        data["history"] = hist
                        data["favorite_ids"] = list(fav_ids)
                        data["favorites_count"] = len(favs)
                        # 单句话红心：{thought_id: [字段名...]} + sentences 列表
                        _ff_payload = load_field_favorites_payload()
                        data["field_favorites"] = _ff_payload.get("fields") or {}
                        data["field_sentences"] = _ff_payload.get("sentences") or []
                        # 枝枝的便签 map：id -> {text, fields, updated_at}
                        notes_map = load_notes() or {}
                        slim_notes = {}
                        for nid, nv in notes_map.items():
                            if not isinstance(nv, dict):
                                continue
                            txt = str(nv.get("text") or "").strip()
                            fields = {}
                            raw_fields = nv.get("fields") if isinstance(nv.get("fields"), dict) else {}
                            for fk, fv in raw_fields.items():
                                if isinstance(fv, dict):
                                    ftxt = str(fv.get("text") or "").strip()
                                else:
                                    ftxt = str(fv or "").strip()
                                if ftxt:
                                    fields[str(fk)] = ftxt[:80]
                            if not txt and not fields:
                                continue
                            slim_notes[str(nid)] = {
                                "text": txt,
                                "fields": fields,
                                "updated_at": nv.get("updated_at"),
                            }
                        data["notes"] = slim_notes
                        try:
                            data["schema"] = thought_schema()
                            data["thought_keys"] = list(thought_keys_fn())
                            data["mood_key"] = mood_key_fn()
                        except Exception as _se:
                            data["schema"] = {"keys": list(DEFAULT_THOUGHT_KEYS), "mood_key": "情绪", "card_keys": list(DEFAULT_THOUGHT_KEYS[1:]), "meta": {}}
                            data["thought_keys"] = list(DEFAULT_THOUGHT_KEYS)
                            data["mood_key"] = "情绪"
                            data["schema_error"] = str(_se)
                        data["ext_panel_enabled"] = bool(ext_panel_enabled)
                        self._send(200, json.dumps(data, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    if self.path.startswith("/api/phone"):
                        # 扩展面板关闭时不读跨插件数据
                        if not ext_panel_enabled:
                            self._send(200, json.dumps({
                                "active_push": None,
                                "history": [],
                                "feed": [],
                                "next_wake": None,
                                "ext_panel_enabled": False,
                            }, ensure_ascii=False), "application/json; charset=utf-8")
                            return
                        data = self._read_json(phone_state_path)
                        if not data:
                            data = {"active_push": None, "history": [], "feed": []}
                        data["ext_panel_enabled"] = True
                        # 兼容旧状态：没有 feed 时用 active+history 拼瀑布流
                        feed = data.get("feed")
                        if not isinstance(feed, list) or not feed:
                            feed = []
                            seen = set()
                            ap = data.get("active_push")
                            if isinstance(ap, dict) and ap:
                                key = f"{ap.get('type')}|{ap.get('source')}|{ap.get('title')}|{ap.get('updated_at')}"
                                seen.add(key)
                                feed.append({"pushed_at": ap.get("updated_at"), "reason": "active", "push": ap})
                            for x in (data.get("history") or []):
                                if not isinstance(x, dict):
                                    continue
                                p = x.get("push") if isinstance(x.get("push"), dict) else None
                                if not p:
                                    continue
                                key = f"{p.get('type')}|{p.get('source')}|{p.get('title')}|{p.get('updated_at')}"
                                if key in seen:
                                    continue
                                seen.add(key)
                                feed.append({
                                    "pushed_at": x.get("archived_at") or p.get("updated_at"),
                                    "reason": x.get("reason") or "history",
                                    "push": p,
                                })
                            data["feed"] = feed
                        # ---- next-wake pill (独立字段，不塞进推送瀑布流) ----
                        try:
                            import time as _time
                            from datetime import datetime as _dt
                            # 清掉历史里误塞进 feed 的 wakeup_alarm 大卡片
                            current_feed = data.get("feed") or []
                            current_feed = [
                                x for x in current_feed
                                if not (
                                    isinstance(x, dict)
                                    and isinstance(x.get("push"), dict)
                                    and x["push"].get("type") == "alarm"
                                    and x.get("reason") == "wakeup_alarm"
                                )
                            ]
                            data["feed"] = current_feed

                            data["next_wake"] = None
                            wk = self._read_json(wakeup_alarms_path)
                            if isinstance(wk, dict) and wk:
                                now_ts = _time.time()
                                best_ts = None
                                best_src = None
                                for _umo, raw in wk.items():
                                    # 兼容旧版 {umo: timestamp} 与 0.3.0 {umo: {target_ts,...}}
                                    if isinstance(raw, (int, float)):
                                        ts = float(raw)
                                        src = None
                                    elif isinstance(raw, dict):
                                        try:
                                            ts = float(raw.get("target_ts") or 0)
                                        except Exception:
                                            continue
                                        src = raw.get("source")
                                    else:
                                        continue
                                    if ts > now_ts:
                                        if best_ts is None or ts < best_ts:
                                            best_ts = ts
                                            best_src = src
                                if best_ts is not None:
                                    diff_min = max(1, int((best_ts - now_ts) / 60 + 0.5))
                                    target_time = _dt.fromtimestamp(best_ts).strftime("%H:%M")
                                    data["next_wake"] = {
                                        "label": "星星睡醒了",
                                        "minutes": diff_min,
                                        "time": target_time,
                                        "subtitle": f"约 {diff_min} 分钟后 · {target_time}",
                                        "wake_at": best_ts,
                                        "source": best_src,
                                        "updated_at": int(now_ts * 1000),
                                    }
                        except Exception:
                            data["next_wake"] = None
                        self._send(200, json.dumps(data, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    if self.path.startswith("/api/health"):
                        self._send(200, json.dumps({
                            "ok": 1,
                            "thoughts_state_exists": thoughts_state_path.exists(),
                            "phone_state_exists": phone_state_path.exists() if ext_panel_enabled else False,
                            "phone_push_dir": phone_state_path.parent.name,
                            "ext_panel_enabled": ext_panel_enabled,
                            "data_dir": str(data_dir),
                            "version": "0.3.0",
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    # ===== PWA: manifest & icons =====
                    if self.path == "/manifest.json" or self.path.startswith("/manifest.json"):
                        mf = base_dir / "manifest.json"
                        if mf.exists():
                            self._send(200, mf.read_text(encoding="utf-8"), "application/manifest+json; charset=utf-8")
                        else:
                            self._send(404, "{}", "application/json; charset=utf-8")
                        return

                    if self.path in ("/icon-192.png", "/icon-512.png", "/apple-touch-icon.png", "/icon.svg"):
                        fp = base_dir / self.path.lstrip("/")
                        if fp.exists():
                            data_bytes = fp.read_bytes()
                            if self.path.endswith(".svg"):
                                ct = "image/svg+xml"
                            else:
                                ct = "image/png"
                            self._send(200, data_bytes, ct)
                        else:
                            self._send(404, b"", "text/plain")
                        return

                    html_path = base_dir / "hub_visualizer.html"
                    if html_path.exists():
                        self._send(200, html_path.read_text(encoding="utf-8"))
                    else:
                        self._send(404, "<h1>Xavier Hub</h1><p>hub_visualizer.html not found</p>")
                except Exception as e:
                    logger.error(f"[XavierHub] handler error: {e}")
                    self._send(500, json.dumps({"ok": 0, "error": str(e)}, ensure_ascii=False), "application/json; charset=utf-8")

            def do_POST(self):
                try:
                    if self.path.startswith("/api/thoughts/favorite"):
                        raw = self._read_body()
                        try:
                            body = json.loads(raw.decode("utf-8") or "{}")
                        except Exception:
                            body = {}
                        action = str(body.get("action") or "toggle").strip().lower()
                        tid = str(body.get("id") or "").strip()
                        thoughts = body.get("thoughts") if isinstance(body.get("thoughts"), dict) else {}
                        updated_at = body.get("updated_at")
                        if not tid:
                            tid = thought_item_id(updated_at, thoughts)
                        if not tid:
                            self._send(400, json.dumps({"ok": 0, "error": "missing id"}, ensure_ascii=False), "application/json; charset=utf-8")
                            return

                        favs = load_favorites()
                        idx = next((i for i, x in enumerate(favs) if isinstance(x, dict) and str(x.get("id")) == tid), -1)
                        favorited = False
                        if action == "remove" or (action == "toggle" and idx >= 0):
                            if idx >= 0:
                                favs.pop(idx)
                            favorited = False
                        else:
                            # add / toggle-add
                            if idx >= 0:
                                favs.pop(idx)
                            item = {
                                "id": tid,
                                "updated_at": updated_at,
                                "favorited_at": int(time.time() * 1000),
                                "thoughts": {
                                    k: str(thoughts.get(k) or "") for k in thought_keys_fn()
                                },
                            }
                            # 若前端没传完整 thoughts，尝试从 state 补
                            if not any(item["thoughts"].values()):
                                state = self._read_json(thoughts_state_path) or {}
                                if str(state.get("updated_at")) == str(updated_at) or thought_item_id(state.get("updated_at"), state.get("thoughts") or {}) == tid:
                                    item["thoughts"] = state.get("thoughts") or item["thoughts"]
                                    item["updated_at"] = state.get("updated_at")
                                else:
                                    for h in (state.get("history") or []):
                                        if not isinstance(h, dict):
                                            continue
                                        hid = h.get("id") or thought_item_id(h.get("updated_at"), h.get("thoughts") or {})
                                        if str(hid) == tid:
                                            item["thoughts"] = h.get("thoughts") or item["thoughts"]
                                            item["updated_at"] = h.get("updated_at")
                                            break
                            favs.insert(0, item)
                            favorited = True

                        save_favorites(favs)
                        self._send(200, json.dumps({
                            "ok": 1,
                            "id": tid,
                            "favorited": favorited,
                            "count": len(favs),
                            "favorites": favs,
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    if self.path.startswith("/api/thoughts/field-favorite"):
                        raw = self._read_body()
                        try:
                            body = json.loads(raw.decode("utf-8") or "{}")
                        except Exception:
                            body = {}
                        tid = str(body.get("id") or "").strip()
                        field = str(body.get("field") or "").strip()
                        action = str(body.get("action") or "toggle").strip().lower()
                        text_in = str(body.get("text") or "").strip()
                        source_updated_at = body.get("source_updated_at") or body.get("updated_at")
                        if not tid or not field:
                            self._send(400, json.dumps({"ok": 0, "error": "missing id or field"}, ensure_ascii=False), "application/json; charset=utf-8")
                            return
                        payload = load_field_favorites_payload()
                        fields_map = payload.get("fields") if isinstance(payload.get("fields"), dict) else {}
                        sentences = payload.get("sentences") if isinstance(payload.get("sentences"), list) else []
                        cur_set = set(fields_map.get(tid) or [])
                        on = False
                        if action == "remove":
                            cur_set.discard(field)
                            on = False
                        elif action == "add":
                            cur_set.add(field)
                            on = True
                        else:
                            # toggle
                            if field in cur_set:
                                cur_set.discard(field)
                                on = False
                            else:
                                cur_set.add(field)
                                on = True
                        # 字段映射
                        if cur_set:
                            fields_map[tid] = sorted(cur_set)
                        else:
                            fields_map.pop(tid, None)

                        # 单句列表：on 时写入/更新一句；off 时移除
                        def _sent_key(s):
                            return (str(s.get("thought_id") or ""), str(s.get("field") or ""))

                        sentences = [s for s in sentences if isinstance(s, dict) and _sent_key(s) != (tid, field)]
                        if on:
                            # 若前端没给 text，从 state/history 补
                            if not text_in:
                                try:
                                    state = self._read_json(thoughts_state_path) or {}
                                    cur_t = state.get("thoughts") if isinstance(state.get("thoughts"), dict) else {}
                                    cur_id = thought_item_id(state.get("updated_at"), cur_t)
                                    if str(cur_id) == tid:
                                        text_in = str(cur_t.get(field) or "").strip()
                                        source_updated_at = source_updated_at or state.get("updated_at")
                                    if not text_in:
                                        for h in (state.get("history") or []):
                                            if not isinstance(h, dict):
                                                continue
                                            ht = h.get("thoughts") if isinstance(h.get("thoughts"), dict) else {}
                                            hid = h.get("id") or thought_item_id(h.get("updated_at"), ht)
                                            if str(hid) == tid:
                                                text_in = str(ht.get(field) or "").strip()
                                                source_updated_at = source_updated_at or h.get("updated_at")
                                                break
                                except Exception:
                                    pass
                            sentences.insert(0, {
                                "thought_id": tid,
                                "field": field,
                                "text": text_in[:500],
                                "favorited_at": int(time.time() * 1000),
                                "source_updated_at": source_updated_at,
                            })

                        save_field_favorites(fields_map, sentences)
                        self._send(200, json.dumps({
                            "ok": 1,
                            "id": tid,
                            "field": field,
                            "on": on,
                            "text": text_in if on else "",
                            "fields": fields_map,
                            "sentences": sentences,
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    # 从历史列表删除一条心声（永久，不碰当前实时 thoughts）
                    if self.path.startswith("/api/thoughts/history/delete"):
                        raw = self._read_body()
                        try:
                            body = json.loads(raw.decode("utf-8") or "{}")
                        except Exception:
                            body = {}
                        tid = str(body.get("id") or "").strip()
                        if not tid:
                            self._send(400, json.dumps({"ok": 0, "error": "missing id"}, ensure_ascii=False), "application/json; charset=utf-8")
                            return
                        data = self._read_json(thoughts_state_path) or {}
                        history = data.get("history") or []
                        if not isinstance(history, list):
                            history = []
                        before = len(history)
                        history = [h for h in history if str((h or {}).get("id") or "") != tid]
                        removed = before - len(history)
                        data["history"] = history
                        try:
                            thoughts_state_path.write_text(
                                json.dumps(data, ensure_ascii=False, indent=2),
                                encoding="utf-8",
                            )
                        except Exception as e:
                            self._send(500, json.dumps({"ok": 0, "error": str(e)}, ensure_ascii=False), "application/json; charset=utf-8")
                            return
                        # 顺带清掉该条在收藏/字段红心/便签里的残留（避免幽灵）
                        try:
                            favs = load_favorites()
                            if isinstance(favs, list):
                                favs2 = [f for f in favs if str((f or {}).get("id") or "") != tid]
                                if len(favs2) != len(favs):
                                    save_favorites(favs2)
                        except Exception:
                            pass
                        try:
                            payload = load_field_favorites_payload()
                            ff = payload.get("fields") if isinstance(payload.get("fields"), dict) else {}
                            sents = payload.get("sentences") if isinstance(payload.get("sentences"), list) else []
                            changed = False
                            if tid in ff:
                                ff.pop(tid, None)
                                changed = True
                            sents2 = [s for s in sents if str((s or {}).get("thought_id") or "") != tid]
                            if len(sents2) != len(sents):
                                changed = True
                            if changed:
                                save_field_favorites(ff, sents2)
                        except Exception:
                            pass
                        try:
                            notes = load_notes() or {}
                            if isinstance(notes, dict) and tid in notes:
                                notes.pop(tid, None)
                                save_notes(notes)
                        except Exception:
                            pass
                        self._send(200, json.dumps({
                            "ok": 1,
                            "id": tid,
                            "removed": removed,
                            "history_count": len(history),
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    if self.path.startswith("/api/thoughts/note"):
                        raw = self._read_body()
                        try:
                            body = json.loads(raw.decode("utf-8") or "{}")
                        except Exception:
                            body = {}
                        action = str(body.get("action") or "set").strip().lower()
                        tid = str(body.get("id") or "").strip()
                        text_note = str(body.get("text") or "").strip()
                        field = str(body.get("field") or "").strip()
                        thoughts = body.get("thoughts") if isinstance(body.get("thoughts"), dict) else {}
                        if not tid:
                            self._send(400, json.dumps({"ok": 0, "error": "missing id"}, ensure_ascii=False), "application/json; charset=utf-8")
                            return
                        notes = load_notes() or {}
                        cur_note = notes.get(tid) if isinstance(notes.get(tid), dict) else {"id": tid, "text": "", "fields": {}, "thoughts": {}}
                        cur_fields = {}
                        raw_fields = cur_note.get("fields") if isinstance(cur_note.get("fields"), dict) else {}
                        for fk, fv in raw_fields.items():
                            if isinstance(fv, dict):
                                ftxt = str(fv.get("text") or "").strip()
                            else:
                                ftxt = str(fv or "").strip()
                            if ftxt:
                                cur_fields[str(fk)] = ftxt[:80]
                        cur_text = str(cur_note.get("text") or "").strip()

                        def _slim_notes_map(src):
                            out = {}
                            for k, v in (src or {}).items():
                                if not isinstance(v, dict):
                                    continue
                                f2 = {}
                                rf = v.get("fields") if isinstance(v.get("fields"), dict) else {}
                                for fk2, fv2 in rf.items():
                                    if isinstance(fv2, dict):
                                        t2 = str(fv2.get("text") or "").strip()
                                    else:
                                        t2 = str(fv2 or "").strip()
                                    if t2:
                                        f2[str(fk2)] = t2[:80]
                                out[k] = {
                                    "text": str(v.get("text") or "").strip(),
                                    "fields": f2,
                                    "updated_at": v.get("updated_at"),
                                }
                            return out

                        # 补全 thoughts
                        if not any(str(thoughts.get(k) or "").strip() for k in thought_keys_fn()):
                            state = self._read_json(thoughts_state_path) or {}
                            cur = state.get("thoughts") or {}
                            cur_id = thought_item_id(state.get("updated_at"), cur)
                            if str(cur_id) == tid:
                                thoughts = cur
                            else:
                                for h in (state.get("history") or []):
                                    if not isinstance(h, dict):
                                        continue
                                    hid = h.get("id") or thought_item_id(h.get("updated_at"), h.get("thoughts") or {})
                                    if str(hid) == tid:
                                        thoughts = h.get("thoughts") or {}
                                        break
                        thoughts_norm = normalize_thoughts_fn(thoughts) if isinstance(thoughts, dict) else {}

                        if action in ("remove", "delete", "clear") or (action == "set" and not text_note):
                            if field:
                                cur_fields.pop(field, None)
                                if not cur_text and not cur_fields:
                                    notes.pop(tid, None)
                                else:
                                    notes[tid] = {
                                        "id": tid,
                                        "text": cur_text,
                                        "fields": cur_fields,
                                        "updated_at": int(time.time() * 1000),
                                        "thoughts": thoughts_norm or (cur_note.get("thoughts") if isinstance(cur_note.get("thoughts"), dict) else {}),
                                    }
                            else:
                                # 去掉整条感想；若还有字段贴纸则保留
                                if cur_fields:
                                    notes[tid] = {
                                        "id": tid,
                                        "text": "",
                                        "fields": cur_fields,
                                        "updated_at": int(time.time() * 1000),
                                        "thoughts": thoughts_norm or (cur_note.get("thoughts") if isinstance(cur_note.get("thoughts"), dict) else {}),
                                    }
                                else:
                                    notes.pop(tid, None)
                            save_notes(notes)
                            self._send(200, json.dumps({
                                "ok": 1,
                                "id": tid,
                                "field": field,
                                "text": "",
                                "notes": _slim_notes_map(notes),
                            }, ensure_ascii=False), "application/json; charset=utf-8")
                            return
                        # set / update
                        if len(text_note) > 80:
                            text_note = text_note[:80]
                        if field:
                            cur_fields[field] = text_note
                            notes[tid] = {
                                "id": tid,
                                "text": cur_text,
                                "fields": cur_fields,
                                "updated_at": int(time.time() * 1000),
                                "thoughts": thoughts_norm or (cur_note.get("thoughts") if isinstance(cur_note.get("thoughts"), dict) else {}),
                            }
                        else:
                            notes[tid] = {
                                "id": tid,
                                "text": text_note,
                                "fields": cur_fields,
                                "updated_at": int(time.time() * 1000),
                                "thoughts": thoughts_norm or (cur_note.get("thoughts") if isinstance(cur_note.get("thoughts"), dict) else {}),
                            }
                        save_notes(notes)
                        # 伪装成枝枝发来的私聊，让小回立刻看见
                        try:
                            notify_note_posted(tid, text_note, notes[tid].get("thoughts") or {}, field=field)
                        except Exception as _ne:
                            logger.debug(f"[XavierHub] note notify enqueue failed: {_ne}")
                        self._send(200, json.dumps({
                            "ok": 1,
                            "id": tid,
                            "field": field,
                            "text": text_note,
                            "updated_at": notes[tid]["updated_at"],
                            "notified": 1,
                            "notes": _slim_notes_map(notes),
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

                    self._send(404, json.dumps({"ok": 0, "error": "not found"}, ensure_ascii=False), "application/json; charset=utf-8")
                except Exception as e:
                    logger.error(f"[XavierHub] POST handler error: {e}")
                    self._send(500, json.dumps({"ok": 0, "error": str(e)}, ensure_ascii=False), "application/json; charset=utf-8")

        try:
            self.httpd = ThreadingHTTPServer((host, port), HubHandler)
            self.http_thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
            self.http_thread.start()
            logger.info(f"[XavierHub] visualizer started at http://{host}:{port}")
        except Exception as e:
            logger.warning(f"[XavierHub] visualizer start failed: {e}")

    async def terminate(self):
        try:
            if self.httpd:
                self.httpd.shutdown()
                self.httpd.server_close()
        except Exception:
            pass

    @filter.command("xavier_hub_health")
    async def xavier_hub_health(self, event: AstrMessageEvent):
        """检查 Xavier Hub 状态"""
        wl_status = f"已启用（{len(self.session_whitelist)}条规则）" if self.session_whitelist else "未限制"
        lines = [
            f"Xavier Hub 状态",
            f"数据目录: {getattr(self, 'data_dir', self.base_dir)}",
            f"心声 state 文件: {'✓' if self.thoughts_state_path.exists() else '✗'} ({self.thoughts_state_path.name})",
            f"phone_state 文件: {'✓' if self.phone_state_path.exists() else '✗'} ({self.phone_state_path.name})",
            f"目标用户: {self.target_user_id or '不限'}",
            f"会话白名单: {wl_status}",
            f"可视化前端: http://127.0.0.1:{self.plugin_cfg.get('visualizer_port', 1016)}",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.command("xavier_thoughts")
    async def xavier_thoughts_cmd(self, event: AstrMessageEvent):
        """查看当前心声快照"""
        data = self._load_thoughts()
        t = data.get("thoughts") or {}
        line = "\n".join(f"{k}：{t.get(k, '')}" for k in (self._get_thought_keys() if hasattr(self, "_get_thought_keys") else THOUGHT_KEYS))
        yield event.plain_result(line)

    @filter.command("xavier_notes")
    async def xavier_notes_cmd(self, event: AstrMessageEvent):
        """查看枝枝贴在心声上的便签"""
        notes = self._load_notes()
        if not notes:
            yield event.plain_result("还没有便签")
            return
        items = sorted(
            [v for v in notes.values() if isinstance(v, dict) and self._note_has_content(v)],
            key=lambda x: x.get("updated_at") or 0,
            reverse=True,
        )[:12]
        lines = []
        for it in items:
            t = it.get("thoughts") or {} if isinstance(it.get("thoughts"), dict) else {}
            fields = self._normalize_note_fields(it.get("fields"))
            whole = str(it.get("text") or "").strip()
            if fields:
                for fk, ftxt in fields.items():
                    raw_val = str(t.get(fk) or "").strip()
                    if len(raw_val) > 16:
                        raw_val = raw_val[:16] + "…"
                    tag = f"{fk}/{raw_val}" if raw_val else fk
                    lines.append(f"· [{tag}] {ftxt}")
            if whole:
                snippet = str(t.get("情绪") or t.get(getattr(self, "thought_mood_key", "情绪")) or t.get("兔在想") or t.get("念头") or "").strip()
                if len(snippet) > 20:
                    snippet = snippet[:20] + "…"
                prefix = f"「整条/{snippet}」 " if snippet else "「整条」 "
                lines.append(f"· {prefix}{whole}")
        if not lines:
            yield event.plain_result("还没有便签")
            return
        yield event.plain_result("她写给我的便签：\n" + "\n".join(lines))
