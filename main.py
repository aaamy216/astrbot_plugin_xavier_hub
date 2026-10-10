"""
Xavier Hub Plugin
心声系统（独立后端 + 前端）+ 手机推送前端展示

心声在三种模式下的去向：
- 前端网页：任何模式都能看到。模型回复一回来就先解析 <xt> 并写入插件自己的状态文件。
- 对话上下文：none / last_n 不进；all 进。
  AstrBot 4.11 起，助手回复会先放进 run_context.messages，再触发 on_llm_response，
  最后保存历史用的就是 run_context.messages。所以心声的剥离放在 ToolLoopAgentRunner
  取回模型响应的入口处（_RunnerGuard），在上下文消息组装之前完成，全程不改 req.contexts
  和已保存的历史。工具调用那一轮、流式分片都在这里一起处理。
- 系统提示词：只有 last_n 注入，并且只注入「当前会话 + 当前对话」里的最近 N 轮，
  /new、/reset 之后从头计，私聊和群聊互不可见。
- 聊天框：任何模式都看不到（流式分片在拦截处过滤，非流式在 on_decorating_result 清洗）。
"""
from pathlib import Path
from typing import Dict
import json
import re
import threading
import asyncio
import time
import os
import hashlib
import functools
import weakref
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api.provider import LLMResponse, ProviderRequest
from astrbot.api.message_components import Plain
import shutil


DEFAULT_THOUGHTS_INSTRUCTION = (
    "\n[系统强制指令] 在你回复的最后，必须使用 <xt>...</xt> 标签输出你当前的内心活动。格式必须是严格的JSON：\n"
    "<xt>\n"
    "{\n"
    "  \"情绪\": \"一两个词或一句短话，当前心情底色。如 平静/开心/有点酸/认真/犯困/温柔\",\n"
    "  \"兔在想\": \"念头，当前对这句话的直接想法\",\n"
    "  \"兔悄悄\": \"潜意识想法或未说出口的话\",\n"
    "  \"兔心愿\": \"现在最想做的事或最期望发生的事\",\n"
    "  \"兔言兔语\": \"一个无厘头的可爱想法\"\n"
    "}\n"
    "</xt>\n"
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
THOUGHT_KEYS = DEFAULT_THOUGHT_KEYS


PLUGIN_VERSION = "0.4.1"


# ======== 心声标签：识别 / 剥离 / 流式过滤 ========
TURN_EXTRA_KEY = "_xavier_hub_turn"
_XT_OPEN = "<xt>"
_XT_CLOSE = "</xt>"
_XT_OPEN_RE = re.compile(r"<xt>", re.IGNORECASE)
_XT_CLOSE_RE = re.compile(r"</xt>", re.IGNORECASE)
_XT_BLOCK_RE = re.compile(r"<xt>.*?(?:</xt>|$)", re.IGNORECASE | re.DOTALL)
_XT_CAPTURE_RE = re.compile(r"<xt>(.*?)(?:</xt>|$)", re.IGNORECASE | re.DOTALL)


def _has_xt_tag(text) -> bool:
    if not isinstance(text, str) or not text:
        return False
    low = text.lower()
    return _XT_OPEN in low or _XT_CLOSE in low


def strip_xt_text(text):
    """删掉文本里的 <xt> 心声块（没闭合的一直删到结尾），顺手收拾多余空行。
    文本里没有心声标签时原样返回，不做任何改动。"""
    if not isinstance(text, str):
        return ""
    if not _has_xt_tag(text):
        return text
    cleaned = _XT_BLOCK_RE.sub("", text)
    cleaned = _XT_CLOSE_RE.sub("", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _partial_suffix_len(text: str, tag: str) -> int:
    """text 末尾有几个字符可能是 tag 的开头（流式分片把标签切断时用）。"""
    max_k = min(len(tag) - 1, len(text))
    for k in range(max_k, 0, -1):
        if text[-k:].lower() == tag[:k]:
            return k
    return 0


class _XtStreamFilter:
    """按顺序喂入文本片段，吞掉 <xt>...</xt> 之间的内容，标签被切成两半也能接上。"""

    __slots__ = ("pending", "in_xt", "active")

    def __init__(self):
        self.pending = ""
        self.in_xt = False
        self.active = False

    def feed(self, text: str) -> str:
        self.active = True
        data = self.pending + (text or "")
        self.pending = ""
        out = []
        pos = 0
        size = len(data)
        while pos < size:
            if not self.in_xt:
                m = _XT_OPEN_RE.search(data, pos)
                if m is None:
                    keep = _partial_suffix_len(data[pos:], _XT_OPEN)
                    out.append(data[pos:size - keep])
                    self.pending = data[size - keep:] if keep else ""
                    break
                out.append(data[pos:m.start()])
                self.in_xt = True
                pos = m.end()
            else:
                m = _XT_CLOSE_RE.search(data, pos)
                if m is None:
                    keep = _partial_suffix_len(data[pos:], _XT_CLOSE)
                    self.pending = data[size - keep:] if keep else ""
                    break
                self.in_xt = False
                pos = m.end()
        return "".join(out)

    def finish(self) -> str:
        """流结束：没凑成标签的半截文字原样还回去；停在心声里面的内容直接丢掉。"""
        tail = "" if self.in_xt else self.pending
        self.pending = ""
        self.in_xt = False
        self.active = False
        return tail


def _mark(obj, attr: str):
    try:
        setattr(obj, attr, True)
    except Exception:
        pass


def _strip_xt_in_components(comps: list, copy_plain: bool = False) -> list:
    """对消息组件列表剥离心声，返回保留下来的组件。
    copy_plain=True 时换成新的 Plain 对象，不改动原组件（发送前清洗用，避免连带改掉上下文里的原文）。"""
    f = _XtStreamFilter()
    kept, plains = [], []
    for comp in comps:
        if isinstance(comp, Plain):
            new_text = f.feed(comp.text or "")
            if copy_plain:
                comp = Plain(new_text)
            else:
                comp.text = new_text
            plains.append(comp)
            kept.append(comp)
        elif not f.in_xt:
            kept.append(comp)
    tail = f.finish()
    if tail and plains:
        plains[-1].text = (plains[-1].text or "") + tail
    for comp in plains:
        comp.text = re.sub(r"\n{3,}", "\n\n", comp.text or "")
    non_empty = [c for c in plains if c.text.strip()]
    if non_empty:
        non_empty[0].text = non_empty[0].text.lstrip()
        non_empty[-1].text = non_empty[-1].text.rstrip()
    return [c for c in kept if not (isinstance(c, Plain) and not (c.text or "").strip())]


def _components_have_xt(comps: list) -> bool:
    texts = [c.text or "" for c in comps if isinstance(c, Plain)]
    return any(_has_xt_tag(t) for t in texts) or _has_xt_tag("".join(texts))


def _strip_raw_completion(raw):
    """raw_completion（OpenAI 格式）里的正文也洗一下，个别插件会读它。"""
    if not raw:
        return
    try:
        choices = getattr(raw, "choices", None)
        if choices is None and isinstance(raw, dict):
            choices = raw.get("choices")
        for choice in choices or []:
            msg = getattr(choice, "message", None)
            if msg is None and isinstance(choice, dict):
                msg = choice.get("message")
            if msg is None:
                continue
            content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", None)
            if isinstance(content, str) and _has_xt_tag(content):
                if isinstance(msg, dict):
                    msg["content"] = strip_xt_text(content)
                else:
                    msg.content = strip_xt_text(content)
    except Exception as e:
        logger.debug(f"[XavierHub] sanitize raw_completion failed: {e}")


def _strip_xt_from_response(resp) -> None:
    """就地删掉 LLMResponse 里的心声。Runner 之后用这份文本去组装上下文消息。"""
    chain_obj = getattr(resp, "result_chain", None)
    comps = getattr(chain_obj, "chain", None) if chain_obj is not None else None
    if isinstance(comps, list) and any(isinstance(c, Plain) for c in comps):
        if _components_have_xt(comps):
            kept = _strip_xt_in_components(comps)
            if kept:
                chain_obj.chain = kept
            else:
                resp.result_chain = None
                resp.completion_text = ""
    else:
        try:
            text = resp.completion_text
        except Exception:
            text = None
        if isinstance(text, str) and _has_xt_tag(text):
            resp.completion_text = strip_xt_text(text)
    _strip_raw_completion(getattr(resp, "raw_completion", None))


def _num(value) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _thought_has_content(t) -> bool:
    return isinstance(t, dict) and any(str(v or "").strip() for v in t.values())


def _is_garbage_thought(t) -> bool:
    if not isinstance(t, dict):
        return False
    idea = str(t.get("兔在想") or t.get("念头") or "").strip()
    if idea == "解析失败":
        return True
    return str(t.get("情绪") or "").strip() == "混乱" and "解析失败" in idea


def _atomic_write_text(path: Path, text: str):
    """先写临时文件再整体替换，读的一方永远不会读到写了一半的文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    for _ in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            # Windows 上目标文件正被别处打开时会替换失败，稍等再试
            time.sleep(0.05)
    try:
        path.write_text(text, encoding="utf-8")
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


class _TurnMarker:
    """on_llm_request 时挂到事件上的标记：这一轮属于哪个会话、哪段对话、用什么模式。"""

    __slots__ = ("_req_ref", "_req_id", "umo", "cid", "scope", "sess", "mode", "epoch", "recorded_at", "__weakref__")

    def __init__(self, req, umo: str, cid: str, scope: str, sess: str, mode: str):
        try:
            self._req_ref = weakref.ref(req)
        except TypeError:
            self._req_ref = None
        self._req_id = id(req)
        self.umo = umo
        self.cid = cid
        self.scope = scope
        self.sess = sess
        self.mode = mode
        self.epoch = 0
        self.recorded_at = None

    def matches(self, req) -> bool:
        """只认主 Agent 用的那个请求对象，子 Agent、其他插件发起的请求都不算。"""
        if req is None:
            return False
        if self._req_ref is not None:
            return self._req_ref() is req
        return id(req) == self._req_id

    def __repr__(self):
        return f"<XavierHubTurn mode={self.mode} scope={self.scope}>"


_GUARD_HANDLER_ATTR = "_xavier_hub_guard_handler"
_GUARD_REGISTRY_ATTR = "_xavier_hub_guard_wrappers"


class _RunnerGuard:
    """在 ToolLoopAgentRunner 拿到模型响应、还没组装上下文消息的时候处理 <xt>。

    AstrBot 4.11 起，助手回复先被放进 run_context.messages，之后才触发 on_llm_response，
    保存对话历史时存的就是 run_context.messages，所以在 on_llm_response 里改文本已经晚了。
    这里包装的是 Runner 的内部方法；AstrBot 以后改了方法名就会装不上，
    插件会在日志和 /xavier_hub_health 里提示，并退回 on_llm_response 的旧逻辑。
    """

    ITER_CANDIDATES = ("_iter_llm_responses_with_fallback", "_iter_llm_responses")
    FINAL_METHOD = "_complete_with_assistant_response"

    def __init__(self, plugin):
        self._plugin_ref = weakref.ref(plugin)

    def plugin(self):
        return self._plugin_ref()

    @staticmethod
    def _runner_class():
        from astrbot.core.agent.runners.tool_loop_agent_runner import ToolLoopAgentRunner

        return ToolLoopAgentRunner

    @classmethod
    def install(cls, plugin):
        """返回 (已拦截的方法名, 错误说明)。重复安装（插件重载）不会叠加包装。"""
        try:
            runner_cls = cls._runner_class()
        except Exception as e:
            return [], f"找不到 ToolLoopAgentRunner：{e}"
        setattr(runner_cls, _GUARD_HANDLER_ATTR, cls(plugin))
        registry = runner_cls.__dict__.get(_GUARD_REGISTRY_ATTR)
        if not isinstance(registry, dict):
            registry = {}
            setattr(runner_cls, _GUARD_REGISTRY_ATTR, registry)
        installed = []
        iter_name = next((n for n in cls.ITER_CANDIDATES if callable(getattr(runner_cls, n, None))), None)
        if iter_name:
            cls._patch(runner_cls, registry, iter_name, cls._make_iter_wrapper)
            installed.append(iter_name)
        if callable(getattr(runner_cls, cls.FINAL_METHOD, None)):
            cls._patch(runner_cls, registry, cls.FINAL_METHOD, cls._make_final_wrapper)
            installed.append(cls.FINAL_METHOD)
        if not iter_name:
            return installed, "Runner 里没有找到可拦截的响应入口方法"
        return installed, ""

    @staticmethod
    def _patch(runner_cls, registry: dict, name: str, factory):
        current = getattr(runner_cls, name)
        record = registry.get(name)
        if record and runner_cls.__dict__.get(name) is record[0]:
            base = record[1]  # 最外层就是我们上次装的，换成新的包装
        else:
            base = current  # 首次安装，或者外面又被别的插件包了一层：在最外层再包
        wrapper = factory(base)
        setattr(runner_cls, name, wrapper)
        registry[name] = (wrapper, base)

    @classmethod
    def uninstall(cls, plugin):
        try:
            runner_cls = cls._runner_class()
        except Exception:
            return
        handler = getattr(runner_cls, _GUARD_HANDLER_ATTR, None)
        owner = handler.plugin() if handler is not None else None
        if owner is not None and owner is not plugin:
            return  # 已经被新加载的插件实例接管
        setattr(runner_cls, _GUARD_HANDLER_ATTR, None)
        registry = runner_cls.__dict__.get(_GUARD_REGISTRY_ATTR)
        if not isinstance(registry, dict):
            return
        for name, (wrapper, base) in list(registry.items()):
            if runner_cls.__dict__.get(name) is wrapper:
                setattr(runner_cls, name, base)
                registry.pop(name, None)
            # 外面还有别人的包装时不拆，我们这层会因为没有 handler 而直接放行

    @staticmethod
    def _plugin_for(runner):
        handler = getattr(type(runner), _GUARD_HANDLER_ATTR, None)
        return handler.plugin() if handler is not None else None

    @staticmethod
    def _make_iter_wrapper(orig):
        @functools.wraps(orig)
        async def xavier_hub_iter_guard(self, *args, **kwargs):
            plugin = _RunnerGuard._plugin_for(self)
            agen = orig(self, *args, **kwargs)
            try:
                if plugin is not None:
                    plugin._guard_stream_begin(self)
                async for resp in agen:
                    extra = None
                    if plugin is not None:
                        try:
                            extra = await plugin._guard_on_response(self, resp)
                        except Exception as e:
                            logger.warning(f"[XavierHub] 心声拦截出错，本条响应原样放行: {e}")
                    if extra:
                        for item in extra:
                            yield item
                    yield resp
            finally:
                closer = getattr(agen, "aclose", None)
                if closer is not None:
                    try:
                        await closer()
                    except Exception:
                        pass

        return xavier_hub_iter_guard

    @staticmethod
    def _make_final_wrapper(orig):
        @functools.wraps(orig)
        async def xavier_hub_final_guard(self, llm_resp, *args, **kwargs):
            plugin = _RunnerGuard._plugin_for(self)
            if plugin is not None:
                try:
                    await plugin._guard_on_final(self, llm_resp)
                except Exception as e:
                    logger.warning(f"[XavierHub] 心声拦截出错，本条响应原样放行: {e}")
            result = await orig(self, llm_resp, *args, **kwargs)
            if plugin is not None:
                try:
                    await plugin._guard_after_final(self, llm_resp)
                except Exception as e:
                    logger.warning(f"[XavierHub] 清洗已入上下文的响应时出错: {e}")
            return result

        return xavier_hub_final_guard


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
    "沈星回心声系统（独立后端+前端）+ 可选手机推送前端",
    PLUGIN_VERSION,
)
class XavierHubPlugin(Star):
    def __init__(self, context: Context, config: Dict = None):
        super().__init__(context)
        self.config = config or {}
        self.base_dir = Path(__file__).resolve().parent
        self.plugin_cfg = self.config

        try:
            self.data_dir = Path(StarTools.get_data_dir("astrbot_plugin_xavier_hub"))
        except Exception:
            self.data_dir = self.base_dir.parent.parent / "plugin_data" / "astrbot_plugin_xavier_hub"
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"[XavierHub] mkdir data_dir failed: {e}")

        self._io_lock = threading.RLock()
        self._scopes_cache = None
        self._leftover_warned = set()
        self._stream_states = weakref.WeakKeyDictionary()

        # 心声字段配置
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

        # 运行时数据文件
        self.thoughts_state_path = self.data_dir / "thoughts_state.json"
        self.thoughts_favorites_path = self.data_dir / "thoughts_favorites.json"
        self.thoughts_notes_path = self.data_dir / "thoughts_notes.json"
        self.daily_meals_path = self.data_dir / "daily_meals.json"
        self.thoughts_field_favorites_path = self.data_dir / "thoughts_field_favorites.json"
        self.thoughts_scopes_path = self.data_dir / "thoughts_scopes.json"
        self._migrate_runtime_data_from_plugin_dir()

        try:
            self.wakeup_alarms_path = Path(StarTools.get_data_dir("astrbot_plugin_wakeup")) / "wakeup_alarms.json"
        except Exception:
            self.wakeup_alarms_path = self.base_dir.parent.parent / "plugin_data" / "astrbot_plugin_wakeup" / "wakeup_alarms.json"

        _raw_target = self.plugin_cfg.get("target_user_id", "")
        if _raw_target is None or str(_raw_target).strip() == "":
            self.target_user_id = ""
        else:
            self.target_user_id = str(_raw_target).strip()

        raw = self.plugin_cfg.get("session_whitelist") or []
        self.session_whitelist = [str(s).strip() for s in raw if str(s).strip()]

        self.httpd = None
        self.http_thread = None

        try:
            self._main_loop = asyncio.get_running_loop()
        except Exception:
            try:
                self._main_loop = asyncio.get_event_loop()
            except Exception:
                self._main_loop = None
        self._cqhttp_bot = None
        self._bot_qq_id = str(self.plugin_cfg.get("bot_qq_id", "") or "").strip()
        self._note_notify_umo = str(
            self.plugin_cfg.get("note_notify_umo", "") or ""
        ).strip()
        self._note_notify_enabled = bool(self.plugin_cfg.get("note_notify_enabled", True))
        self.ext_panel_enabled = bool(self.plugin_cfg.get("ext_panel_enabled", False))
        self._meals_task = None

        if self.plugin_cfg.get("visualizer_enabled", False):
            self._start_visualizer()
        logger.info(
            f"XavierHubPlugin v{PLUGIN_VERSION} loaded | ext_panel={'on' if self.ext_panel_enabled else 'off'}"
        )

        # 心声拦截：在 Runner 组装上下文之前处理 <xt>
        self._guard_methods, self._guard_error = [], ""
        try:
            self._guard_methods, self._guard_error = _RunnerGuard.install(self)
        except Exception as e:
            self._guard_error = f"安装失败：{e}"
        if self._guard_methods and not self._guard_error:
            logger.info(f"[XavierHub] 心声上下文拦截已启用: {', '.join(self._guard_methods)}")
        else:
            logger.warning(
                f"[XavierHub] 心声上下文拦截没有装上（{self._guard_error or '未知原因'}）。"
                "none/last_n 模式在 AstrBot 4.11 以后将无法阻止心声进入对话历史，"
                "请把 AstrBot 版本和这条日志反馈给插件作者。"
            )

    def _migrate_runtime_data_from_plugin_dir(self):
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
                json.loads(src.read_text(encoding="utf-8"))
            except Exception:
                bak = self.base_dir / f"{name}.invalid_{int(time.time())}"
                try:
                    src.rename(bak)
                except Exception:
                    pass
                logger.warning(f"[XavierHub] 插件目录里的 {name} 不是有效 JSON，未迁移，已改名为 {bak.name}")
                continue
            try:
                if dst.exists() and dst.is_file():
                    bak = self.base_dir / f"{name}.migrated_away_{int(time.time())}"
                    try:
                        src.rename(bak)
                        logger.info(f"[XavierHub] old data kept aside: {src.name} -> {bak.name}")
                    except Exception as e:
                        logger.warning(f"[XavierHub] rename old {src.name} failed: {e}")
                    continue
                shutil.copy2(src, dst)
                bak = self.base_dir / f"{name}.migrated_away_{int(time.time())}"
                try:
                    src.rename(bak)
                except Exception:
                    bak = None
                logger.info(
                    f"[XavierHub] migrated runtime data: {name} -> {self.data_dir}"
                    + (f" | old backup: {bak.name}" if bak else "")
                )
            except Exception as e:
                logger.error(f"[XavierHub] migrate {name} failed: {e}")

    # ======== 权限与过滤 ========
    def _is_target_user(self, event: AstrMessageEvent) -> bool:
        if not self.target_user_id or self.target_user_id == "":
            return True
        try:
            return str(event.get_sender_id()) == self.target_user_id
        except Exception:
            return False

    def _is_session_allowed(self, event: AstrMessageEvent) -> bool:
        if not self.session_whitelist:
            return True
        try:
            session_id = event.get_session_id()
            unified = event.unified_msg_origin
            for entry in self.session_whitelist:
                if entry == unified or entry == session_id:
                    return True
                if entry.endswith(":*") and unified.startswith(entry[:-1]):
                    return True
            return False
        except Exception:
            return True

    def _should_process(self, event: AstrMessageEvent) -> bool:
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

    # ======== 数据文件读写（HTTP 线程和主循环共用，统一加锁 + 原子写） ========
    def _read_json_file(self, path: Path, default=None):
        with self._io_lock:
            try:
                if path.exists():
                    return json.loads(path.read_text(encoding="utf-8"))
            except Exception as e:
                logger.debug(f"[XavierHub] read {path.name} failed: {e}")
        return default

    def _write_json_file(self, path: Path, data):
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        with self._io_lock:
            _atomic_write_text(path, payload)

    def _backup_broken_file(self, path: Path, err):
        try:
            bak = path.with_name(f"{path.name}.broken_{int(time.time())}")
            shutil.copy2(path, bak)
            logger.error(f"[XavierHub] {path.name} 读取失败（{err}），已备份为 {bak.name}，将按空文件继续")
        except Exception as e:
            logger.error(f"[XavierHub] {path.name} 读取失败且备份失败: {err} / {e}")

    def _read_state_for_update(self) -> dict:
        """准备改写心声状态文件时用：文件坏了先备份，避免新数据把能抢救的历史盖掉。"""
        with self._io_lock:
            p = self.thoughts_state_path
            if not p.exists():
                return {}
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception as e:
                self._backup_broken_file(p, e)
                return {}
            return data if isinstance(data, dict) else {}

    def _read_state_for_read(self) -> dict:
        data = self._read_json_file(self.thoughts_state_path, {})
        return data if isinstance(data, dict) else {}

    def _save_thoughts(self, thoughts: dict):
        """保留给外部调用的旧接口：只改最新一条，不动历史。"""
        try:
            with self._io_lock:
                data = self._read_state_for_update()
                data["updated_at"] = int(time.time() * 1000)
                data["thoughts"] = thoughts
                data.setdefault("history", [])
                data.pop("scope", None)
                data.pop("sess", None)
                self._write_json_file(self.thoughts_state_path, data)
        except Exception as e:
            logger.error(f"[XavierHub] save thoughts failed: {e}")

    def _load_thoughts(self) -> dict:
        data = self._read_state_for_read()
        if "thoughts" in data:
            data.setdefault("history", [])
            return data
        return {
            "updated_at": None,
            "thoughts": {k: "" for k in self._get_thought_keys()},
            "history": []
        }

    def _load_favorites(self) -> list:
        try:
            p = self.thoughts_favorites_path
            data = self._read_json_file(p)
            if data is not None:
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
            self._write_json_file(self.thoughts_favorites_path, payload)
        except Exception as e:
            logger.error(f"[XavierHub] save favorites failed: {e}")

    def _load_field_favorites_payload(self) -> dict:
        try:
            p = self.thoughts_field_favorites_path
            data = self._read_json_file(p)
            if data is not None:
                if not isinstance(data, dict):
                    return {"fields": {}, "sentences": []}
                fields = data.get("fields")
                if not isinstance(fields, dict):
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
            clean_sents = clean_sents[:300]
            payload = {
                "updated_at": int(time.time() * 1000),
                "fields": fields if isinstance(fields, dict) else {},
                "sentences": clean_sents,
            }
            self._write_json_file(self.thoughts_field_favorites_path, payload)
        except Exception as e:
            logger.error(f"[XavierHub] save field-favorites failed: {e}")

    @staticmethod
    def _thought_item_id(updated_at, thoughts: dict) -> str:
        import hashlib
        t = thoughts or {}
        keys = THOUGHT_KEYS if THOUGHT_KEYS else DEFAULT_THOUGHT_KEYS
        parts = [str(updated_at or "")] + [str(t.get(k, "") or "") for k in keys]
        for old_k in ("念头", "暗线", "愿望", "无厘头"):
            if old_k not in keys and str(t.get(old_k) or "").strip():
                parts.append(str(t.get(old_k) or ""))
        blob = "|".join(parts)
        return hashlib.md5(blob.encode("utf-8")).hexdigest()[:16]

    def _load_notes(self) -> dict:
        try:
            p = self.thoughts_notes_path
            data = self._read_json_file(p)
            if data is not None:
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
                row = {
                    "id": tid,
                    "text": txt,
                    "fields": fields,
                    "updated_at": v.get("updated_at") or int(time.time() * 1000),
                    "thoughts": v.get("thoughts") if isinstance(v.get("thoughts"), dict) else {},
                }
                if v.get("notified_at"):
                    row["notified_at"] = v.get("notified_at")
                items.append(row)
            items.sort(key=lambda x: x.get("updated_at") or 0, reverse=True)
            items = items[:200]
            payload = {
                "updated_at": int(time.time() * 1000),
                "notes": {x["id"]: x for x in items},
            }
            self._write_json_file(self.thoughts_notes_path, payload)
        except Exception as e:
            logger.error(f"[XavierHub] save notes failed: {e}")

    def _thought_sess_map(self) -> dict:
        """心声 id -> 所属会话哈希（旧数据没有会话信息，不在表里）"""
        data = self._read_state_for_read()
        out = {}
        if data.get("sess") and isinstance(data.get("thoughts"), dict):
            out[self._thought_item_id(data.get("updated_at"), data.get("thoughts"))] = data.get("sess")
        for item in data.get("history") or []:
            if not isinstance(item, dict) or not item.get("sess"):
                continue
            tid = item.get("id") or self._thought_item_id(item.get("updated_at"), item.get("thoughts") or {})
            out[str(tid)] = item.get("sess")
        return out

    def _notes_prompt_block(self, sess: str = None) -> str:
        notes = self._load_notes()
        if not notes:
            return ""
        pending = any(
            isinstance(v, dict) and self._note_has_content(v) and not v.get("notified_at")
            for v in notes.values()
        )
        if not pending:
            return ""
        owners = self._thought_sess_map() if sess else {}

        def _visible(key, note):
            owner = owners.get(str(note.get("id") or key))
            return (not sess) or (not owner) or owner == sess

        items = sorted(
            [
                v for k, v in notes.items()
                if isinstance(v, dict)
                and self._note_has_content(v)
                and not v.get("notified_at")
                and _visible(k, v)
            ],
            key=lambda x: x.get("updated_at") or 0,
            reverse=True,
        )[:5]
        if not items:
            return ""
        lines = ["\n【有人贴在你心声上的便签——对方写给你看的，不是改你的心声】"]
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
        lines.append("（这些便签她希望你看见；聊天里可以自然回应，不要复读系统字样）\n")
        return "\n".join(lines)

    def _get_thoughts_instruction(self) -> str:
        raw = ""
        try:
            raw = str(self.plugin_cfg.get("thoughts_instruction", "") or "")
        except Exception:
            raw = ""
        raw = raw.strip()
        if not raw:
            return DEFAULT_THOUGHTS_INSTRUCTION
        return raw if raw.startswith("\n") else ("\n" + raw)

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def _capture_runtime(self, event: AstrMessageEvent):
        try:
            self._main_loop = asyncio.get_running_loop()
        except Exception:
            pass
        try:
            bot = getattr(event, "bot", None)
            if bot is not None and hasattr(bot, "send_private_msg"):
                self._cqhttp_bot = bot
            sid = None
            try:
                sid = str(getattr(event.message_obj, "self_id", "") or "")
            except Exception:
                sid = ""
            if sid and sid not in ("0", "None"):
                self._bot_qq_id = sid
        except Exception:
            pass
        try:
            self._ensure_meals_task()
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

        # ==========================================================
        # 核心提取：自动从「回复目标QQ」或「UMO」中获取你的真实 QQ 号
        # ==========================================================
        raw_uid = ""
        # 1. 尝试读取网页配置项里的回复目标 QQ 号字段
        for k in (
            "note_notify_user_id",
            "note_reply_target_user_id",
            "note_target_user_id",
            "note_notify_target_user_id",
            "reply_target_user_id",
            "note_target_id",
            "note_notify_target_id",
        ):
            v = self.plugin_cfg.get(k)
            if v and str(v).strip() and str(v).strip() != "0":
                raw_uid = str(v).strip()
                break

        # 2. 如果配置项键名未匹配上，直接从 UMO (沈星回:FriendMessage:1738076005) 提取末尾 QQ 号
        if not raw_uid:
            umo = str(self._note_notify_umo or self.plugin_cfg.get("note_notify_umo", "") or "").strip()
            if umo and ":" in umo:
                last_part = umo.split(":")[-1].strip()
                if last_part.isdigit() and last_part != "0":
                    raw_uid = last_part

        # 3. 最后回退 target_user_id
        if not raw_uid:
            raw_uid = self.target_user_id or ""

        uid = str(raw_uid).strip()
        if not uid or uid == "0":
            logger.warning("[XavierHub] note notify skipped: 便签注入目标ID为空，无法确定发送给谁")
            return

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
                    "nickname": str(self.plugin_cfg.get("note_notify_nickname", "") or "用户"),
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
            await asyncio.to_thread(self._mark_note_notified, tid)
        except Exception as e:
            logger.error(f"[XavierHub] inject note failed: {e}")

    def _mark_note_notified(self, tid: str):
        tid = str(tid or "").strip()
        if not tid:
            return
        try:
            with self._io_lock:
                data = self._read_json_file(self.thoughts_notes_path)
                if not isinstance(data, dict):
                    return
                notes = data.get("notes")
                if not isinstance(notes, dict):
                    return
                item = notes.get(tid)
                if not isinstance(item, dict):
                    return
                item["notified_at"] = int(time.time() * 1000)
                notes[tid] = item
                data["notes"] = notes
                self._write_json_file(self.thoughts_notes_path, data)
            logger.debug(f"[XavierHub] note marked notified | tid={tid}")
        except Exception as e:
            logger.warning(f"[XavierHub] mark note notified failed: {e}")

    def notify_note_posted(self, tid: str, note_text: str, thoughts=None, field: str = ""):
        try:
            self._schedule_coro(self._inject_note_as_user_message(tid, note_text, thoughts, field=field))
        except Exception as e:
            logger.warning(f"[XavierHub] notify_note_posted failed: {e}")

    # ==========================================
    # 模式识别 (last_n / none / all)
    # ==========================================
    def _get_history_mode(self) -> str:
        """规范化获取上下文模式，兼容 lastn、last_n、none、all"""
        raw = str(self.plugin_cfg.get("thoughts_history_mode", "last_n") or "last_n").lower().strip()
        norm = raw.replace("-", "").replace("_", "")
        if norm in ("lastn", "last"):
            return "last_n"
        if norm in ("none", "no", "off"):
            return "none"
        if norm in ("all", "full"):
            return "all"
        return "last_n"

    def _get_history_n(self) -> int:
        """last_n 模式注入几轮。配置项是 thoughts_history_n；旧键名 thoughts_history_keep_count 兼容读取。"""
        raw = self.plugin_cfg.get("thoughts_history_n", None)
        if raw is None or str(raw).strip() == "":
            raw = self.plugin_cfg.get("thoughts_history_keep_count", 3)
        try:
            n = int(raw)
        except Exception:
            n = 3
        return max(0, min(n, 20))

    # ==========================================
    # 会话 / 对话范围（隔离心声注入用）
    # ==========================================
    @staticmethod
    def _hash_key(*parts) -> str:
        raw = "\x1f".join(str(p or "") for p in parts)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    def _scope_key(self, umo: str, cid: str) -> str:
        """会话 + 对话 ID。/new、/reset 会换对话 ID，自然就是新的范围。
        存哈希而不存原文，因为前端接口默认对外开放，原文里有 QQ 号 / 群号。"""
        return self._hash_key("scope", umo, cid)

    def _sess_key(self, umo: str) -> str:
        return self._hash_key("sess", umo)

    async def _resolve_cid(self, umo: str, req) -> str:
        conv = getattr(req, "conversation", None)
        cid = str(getattr(conv, "cid", "") or "") if conv is not None else ""
        if cid:
            return cid
        try:
            mgr = getattr(self.context, "conversation_manager", None)
            if mgr is not None:
                return str(await mgr.get_curr_conversation_id(umo) or "")
        except Exception as e:
            logger.debug(f"[XavierHub] get current conversation id failed: {e}")
        return ""

    @staticmethod
    def _conversation_is_fresh(req) -> bool:
        """这段对话在数据库里还没有任何正式消息（新对话，或旧版本 /reset 清空了历史）。"""
        conv = getattr(req, "conversation", None)
        raw = getattr(conv, "history", None) if conv is not None else None
        items = None
        if isinstance(raw, str):
            try:
                items = json.loads(raw) if raw.strip() else []
            except Exception:
                items = None
        elif isinstance(raw, list):
            items = raw
        if items is None:
            items = [
                m for m in (getattr(req, "contexts", None) or [])
                if not (isinstance(m, dict) and m.get("_no_save"))
            ]
        # 只要历史消息中还没有任何一条 assistant (AI) 的正式回复，就判定为新会话第一轮
        for m in (items or []):
            role = m.get("role") if isinstance(m, dict) else getattr(m, "role", None)
            if role == "assistant":
                return False
        return True

    def _load_scopes(self) -> dict:
        data = self._read_json_file(self.thoughts_scopes_path, {}) or {}
        scopes = data.get("scopes") if isinstance(data, dict) else None
        return scopes if isinstance(scopes, dict) else {}

    def _touch_scope_sync(self, scope: str, umo: str, cid: str, fresh: bool) -> int:
        """登记对话范围并返回它的起点时间。fresh=True 时起点改成现在，之前的心声不再注入。"""
        with self._io_lock:
            if self._scopes_cache is None:
                self._scopes_cache = self._load_scopes()
            scopes = self._scopes_cache
            now = int(time.time() * 1000)
            entry = scopes.get(scope)
            changed = False
            # 起点要晚于这个范围里已有的所有心声，同一毫秒内发生的也不会被算进新对话
            new_epoch = max(now, self._latest_scope_ts(scope) + 1) if fresh else 0
            if not isinstance(entry, dict):
                entry = {"umo": umo, "cid": cid, "epoch": new_epoch, "created_at": now}
                scopes[scope] = entry
                changed = True
            elif fresh:
                entry["epoch"] = new_epoch
                changed = True
            if changed:
                entry["touched_at"] = now
                if len(scopes) > 500:
                    ordered = sorted(
                        scopes.items(),
                        key=lambda kv: (kv[1] or {}).get("touched_at") or 0,
                        reverse=True,
                    )
                    self._scopes_cache = dict(ordered[:400])
                self._write_json_file(
                    self.thoughts_scopes_path,
                    {"version": 1, "scopes": self._scopes_cache},
                )
            return _num(entry.get("epoch"))

    def _latest_scope_ts(self, scope: str) -> int:
        """这个范围里最新一条心声的时间（历史按新到旧排列，找到第一条就是最大的）。"""
        data = self._read_state_for_read()
        if data.get("scope") == scope:
            return _num(data.get("updated_at"))
        for item in data.get("history") or []:
            if isinstance(item, dict) and item.get("scope") == scope:
                return _num(item.get("updated_at"))
        return 0

    def _warn_leftover_xt(self, req, scope: str):
        """none/last_n 模式下，历史里如果还有旧的 <xt>，只提醒，不动历史。"""
        if scope in self._leftover_warned:
            return
        for m in getattr(req, "contexts", None) or []:
            if not isinstance(m, dict) or m.get("role") != "assistant":
                continue
            content = m.get("content")
            texts = []
            if isinstance(content, str):
                texts.append(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        texts.append(part["text"])
            if any(_has_xt_tag(t) for t in texts):
                self._leftover_warned.add(scope)
                logger.warning(
                    "[XavierHub] 这段对话的历史里还留着以前的 <xt>（all 模式或旧版本写进去的）。"
                    "插件不会改动历史记录，想要干净的上下文，请在这个会话里发一次 /new。"
                )
                return

    # ==========================================
    # 心声读取 / 记录
    # ==========================================
    def _collect_scope_thoughts(self, scope: str, epoch: int, limit: int) -> list:
        data = self._read_state_for_read()
        out = []
        latest = data.get("thoughts")
        if (
            data.get("scope") == scope
            and _num(data.get("updated_at")) >= epoch
            and _thought_has_content(latest)
        ):
            out.append(latest)
        for item in data.get("history") or []:
            if len(out) >= limit:
                break
            if not isinstance(item, dict) or item.get("scope") != scope:
                continue
            if _num(item.get("updated_at")) < epoch:
                continue
            t = item.get("thoughts")
            if _thought_has_content(t):
                out.append(t)
        return out[:limit]

    def _build_recent_thoughts_prompt(self, scope: str, epoch: int, keep_count: int) -> str:
        """只取当前会话、当前对话里的最近 N 轮心声，拼成系统提示词（仅 last_n 模式）。"""
        if keep_count <= 0:
            return ""
        recent = self._collect_scope_thoughts(scope, epoch, keep_count)
        if not recent:
            return ""
        lines = ["\n【你的近期内心活动记忆】（仅作前情回顾，帮助你保持情绪连贯，无需在本次回复中复述）："]
        total = len(recent)
        # 旧的在前、新的在后
        for i, t in enumerate(reversed(recent)):
            norm = self._normalize_thoughts(t)
            lines.append(f"倒数第 {total - i} 轮心声: <xt>{json.dumps(norm, ensure_ascii=False)}</xt>")
        return "\n".join(lines)

    def _latest_thought_for_sess(self, sess: str):
        data = self._read_state_for_read()
        if data.get("sess") == sess and _thought_has_content(data.get("thoughts")):
            return data.get("thoughts")
        for item in data.get("history") or []:
            if isinstance(item, dict) and item.get("sess") == sess and _thought_has_content(item.get("thoughts")):
                return item.get("thoughts")
        return None

    def _record_thoughts_sync(self, thoughts: dict, turn: "_TurnMarker"):
        """写入前端用的心声状态文件：新的一条放最前，上一条挪进历史（全量保留）。
        同一轮里模型输出了两次心声（比如工具调用前后），只保留最后一次。"""
        with self._io_lock:
            prior = self._read_state_for_update()
            now = max(int(time.time() * 1000), _num(turn.epoch))
            history = [h for h in (prior.get("history") or []) if isinstance(h, dict)]
            prev_thoughts = prior.get("thoughts") if isinstance(prior.get("thoughts"), dict) else {}
            prev_updated = prior.get("updated_at")
            same_turn = (
                turn.recorded_at is not None
                and prev_updated == turn.recorded_at
                and prior.get("scope") == turn.scope
            )
            if not same_turn and _thought_has_content(prev_thoughts) and not _is_garbage_thought(prev_thoughts):
                item = {
                    "id": self._thought_item_id(prev_updated, prev_thoughts),
                    "updated_at": prev_updated,
                    "thoughts": prev_thoughts,
                }
                for k in ("scope", "sess"):
                    if prior.get(k):
                        item[k] = prior[k]
                history.insert(0, item)
            history = [
                h for h in history
                if not _is_garbage_thought(h.get("thoughts") if isinstance(h.get("thoughts"), dict) else {})
            ]
            if len(history) > 300:
                history = history[:300]
            data = dict(prior)
            data.update({
                "updated_at": now,
                "thoughts": thoughts,
                "scope": turn.scope,
                "sess": turn.sess,
                "history": history,
            })
            self._write_json_file(self.thoughts_state_path, data)
            turn.recorded_at = now

    # ======== 心声解析 ========

    def _parse_thoughts_json(self, raw_str: str) -> dict:
        """从标签内容里尽量抠出心声字典：去代码块、容忍中文引号和尾逗号，实在不行按字段名逐个找。"""
        if not raw_str:
            return {}
        raw = str(raw_str)
        s = raw.strip()
        if s.startswith("```"):
            s = re.sub(r"^```(?:json|JSON)?\s*", "", s)
            s = re.sub(r"\s*```$", "", s).strip()
        if "{" in s and "}" in s:
            start, end = s.find("{"), s.rfind("}")
            if end > start:
                s = s[start:end + 1]

        def _try_load(txt: str):
            try:
                obj = json.loads(txt)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None

        obj = _try_load(s)
        if obj is not None:
            return obj
        s2 = (
            s.replace("\u201c", '"').replace("\u201d", '"')
            .replace("\u2018", "'").replace("\u2019", "'")
            .replace("\ufeff", "")
        )
        s2 = re.sub(r",\s*([}\]])", r"\1", s2)
        obj = _try_load(s2)
        if obj is not None:
            return obj

        # 宽松兜底：按字段名（含旧别名）逐个抠
        names = set(self._get_thought_keys())
        alias_map = getattr(self, "thought_aliases", None) or LEGACY_THOUGHT_ALIASES
        for k in list(names):
            for a in alias_map.get(k, ()) or ():
                names.add(a)
        recovered = {}
        for key in names:
            pat = (
                r'["\'\u201c\u201d]' + re.escape(key) + r'["\'\u201c\u201d]\s*[:\uff1a]\s*'
                r'("(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|\u201c[^\u201d]*\u201d|[^,\n\r}]+)'
            )
            m = re.search(pat, raw, re.DOTALL)
            if not m:
                continue
            val = m.group(1).strip()
            if val.startswith('"') and val.endswith('"') and len(val) >= 2:
                try:
                    val = json.loads(val)
                except Exception:
                    val = val[1:-1]
            elif len(val) >= 2 and val[0] in "'\u201c" and val[-1] in "'\u201d":
                val = val[1:-1]
            val = str(val).strip().rstrip(",").strip()
            if val:
                recovered[key] = val
        return recovered

    def _extract_thoughts(self, text: str) -> dict:
        """取最后一个能解析出内容的 <xt> 块。"""
        for m in reversed(list(_XT_CAPTURE_RE.finditer(text or ""))):
            body = (m.group(1) or "").strip()
            if not body:
                continue
            parsed = self._parse_thoughts_json(body)
            if not parsed:
                continue
            normalized = self._normalize_thoughts(parsed)
            if _thought_has_content(normalized) and not _is_garbage_thought(normalized):
                return normalized
        return {}

    # ======== 心声后端：注入 ========

    @filter.on_llm_request()
    async def inject_thoughts_instruction(self, event: AstrMessageEvent, req: ProviderRequest):
        """注入心声指令、三餐/便签；last_n 模式再注入本会话本对话的最近 N 轮心声。
        不改 req.contexts。"""
        if not self._should_process(event):
            return
        mode = self._get_history_mode()
        umo = str(getattr(event, "unified_msg_origin", "") or "")
        cid = await self._resolve_cid(umo, req)
        scope = self._scope_key(umo, cid)
        sess = self._sess_key(umo)
        turn = _TurnMarker(req, umo, cid, scope, sess, mode)
        try:
            event.set_extra(TURN_EXTRA_KEY, turn)
        except Exception as e:
            logger.warning(f"[XavierHub] 无法给事件打标记，本轮心声不会记录: {e}")

        has_conv = getattr(req, "conversation", None) is not None
        fresh = has_conv and self._conversation_is_fresh(req)
        try:
            turn.epoch = await asyncio.to_thread(self._touch_scope_sync, scope, umo, cid, fresh)
        except Exception as e:
            logger.warning(f"[XavierHub] 记录对话范围失败: {e}")
            turn.epoch = int(time.time() * 1000) if fresh else 0

        if mode in ("none", "last_n") and has_conv and not fresh:
            self._warn_leftover_xt(req, scope)

        # 1. 心声输出指令
        req.system_prompt = (req.system_prompt or "") + self._get_thoughts_instruction()

        # 2. 今日三餐灵感（若启用）
        meals_block = self._meals_prompt_block()
        if meals_block:
            req.system_prompt += meals_block

        # 3. 便签提醒（只带本会话心声上的便签）
        try:
            notes_block = await asyncio.to_thread(self._notes_prompt_block, sess)
        except Exception as e:
            logger.debug(f"[XavierHub] build notes block failed: {e}")
            notes_block = ""
        if notes_block:
            req.system_prompt += notes_block

        # 4. 只有 last_n 注入近期心声；none 和 all 都不注入
        if mode == "last_n":
            keep_count = self._get_history_n()
            if keep_count > 0:
                try:
                    recent_prompt = await asyncio.to_thread(
                        self._build_recent_thoughts_prompt, scope, turn.epoch, keep_count
                    )
                except Exception as e:
                    logger.warning(f"[XavierHub] 读取近期心声失败: {e}")
                    recent_prompt = ""
                if recent_prompt:
                    req.system_prompt += recent_prompt

    # ======== 心声后端：Runner 拦截回调 ========

    def _runner_event_turn(self, runner):
        rc = getattr(runner, "run_context", None)
        agent_ctx = getattr(rc, "context", None)
        event = getattr(agent_ctx, "event", None)
        if event is None:
            return None, None
        turn = None
        try:
            marker = event.get_extra(TURN_EXTRA_KEY)
            if isinstance(marker, _TurnMarker) and marker.matches(getattr(runner, "req", None)):
                turn = marker
        except Exception:
            turn = None
        return event, turn

    def _guard_wants(self, event, turn) -> bool:
        if turn is not None:
            return True
        # 没打标记的请求（非目标用户、子 Agent、其他插件发起的等）：
        # 在允许的会话里照样清洗，不记录心声
        try:
            return self._is_session_allowed(event)
        except Exception:
            return False

    def _stream_state(self, runner, create: bool):
        try:
            st = self._stream_states.get(runner)
            if st is None and create:
                st = _XtStreamFilter()
                self._stream_states[runner] = st
            return st
        except TypeError:
            return None

    def _guard_stream_begin(self, runner):
        try:
            self._stream_states.pop(runner, None)
        except TypeError:
            pass

    def _filter_stream_chunk(self, runner, resp):
        if getattr(resp, "_xavier_hub_chunk_done", False):
            return
        _mark(resp, "_xavier_hub_chunk_done")
        st = self._stream_state(runner, create=True)
        if st is None:
            return
        chain_obj = getattr(resp, "result_chain", None)
        comps = getattr(chain_obj, "chain", None) if chain_obj is not None else None
        if isinstance(comps, list):
            kept = []
            for comp in comps:
                if isinstance(comp, Plain):
                    text = st.feed(comp.text or "")
                    if text:
                        comp.text = text
                        kept.append(comp)
                elif not st.in_xt:
                    kept.append(comp)
            if kept:
                chain_obj.chain = kept
            else:
                resp.result_chain = None
                resp.completion_text = ""
            return
        if chain_obj is None:
            text = resp.completion_text
            if isinstance(text, str) and text:
                resp.completion_text = st.feed(text)

    def _finish_stream(self, runner):
        st = self._stream_state(runner, create=False)
        if st is None or not st.active:
            return None
        tail = st.finish()
        if not tail:
            return None
        try:
            chunk = LLMResponse("assistant", is_chunk=True)
            chunk.result_chain = MessageChain(chain=[Plain(tail)])
            _mark(chunk, "_xavier_hub_chunk_done")
            return [chunk]
        except Exception as e:
            logger.debug(f"[XavierHub] flush stream tail failed: {e}")
            return None

    async def _guard_on_response(self, runner, resp):
        event, turn = self._runner_event_turn(runner)
        if event is None or not self._guard_wants(event, turn):
            return None
        if getattr(resp, "is_chunk", False):
            self._filter_stream_chunk(runner, resp)
            return None
        extra = self._finish_stream(runner)
        await self._process_final_response(resp, event, turn)
        return extra

    async def _guard_on_final(self, runner, resp):
        event, turn = self._runner_event_turn(runner)
        if event is None or not self._guard_wants(event, turn):
            return
        await self._process_final_response(resp, event, turn)

    async def _guard_after_final(self, runner, resp):
        """助手消息已经进了上下文，再把响应对象上的心声清掉，之后发往聊天框的内容就是干净的。
        none/last_n 在进上下文之前就清过了，这一步主要照顾 all 模式：上下文里的原文不受影响，
        其他插件的 on_llm_response 也仍然能看到完整原文（它们在这之前执行）。"""
        if resp is None or getattr(resp, "_xavier_hub_display_clean", False):
            return
        _mark(resp, "_xavier_hub_display_clean")
        event, turn = self._runner_event_turn(runner)
        if event is None or not self._guard_wants(event, turn):
            return
        if getattr(resp, "role", None) != "assistant":
            return
        try:
            text = resp.completion_text or ""
        except Exception:
            return
        if not _has_xt_tag(text) or not strip_xt_text(text).strip():
            # 只有心声、没有正文时保持原样，免得这一轮被当成空回复而不保存；聊天框由发送前清洗兜住
            return
        _strip_xt_from_response(resp)

    async def _process_final_response(self, resp, event, turn):
        """先把心声存下来（前端任何模式都有数据），再按模式决定要不要从响应里拿掉。"""
        if resp is None or getattr(resp, "_xavier_hub_done", False):
            return
        _mark(resp, "_xavier_hub_done")
        if getattr(resp, "role", None) != "assistant":
            return
        try:
            text = resp.completion_text or ""
        except Exception:
            return
        if not _has_xt_tag(text):
            return
        mode = turn.mode if turn is not None else self._get_history_mode()
        if turn is not None:
            thoughts = self._extract_thoughts(text)
            if thoughts:
                try:
                    await asyncio.to_thread(self._record_thoughts_sync, thoughts, turn)
                except Exception as e:
                    logger.error(f"[XavierHub] 保存心声失败: {e}")
            else:
                m = _XT_CAPTURE_RE.search(text)
                preview = (m.group(1) if m else text)[:120]
                logger.warning(f"[XavierHub] 心声没能解析，本轮不记录 | {preview!r}")
        if mode in ("none", "last_n"):
            _strip_xt_from_response(resp)

    # ======== 心声后端：兜底提取 / 发送前清洗 ========

    @filter.on_llm_response(priority=10)
    async def extract_thoughts(self, event: AstrMessageEvent, resp: LLMResponse):
        """兜底。正常情况下 Runner 拦截已经处理过这条响应，这里直接跳过。
        拦截没装上时才在这里记录心声并按模式剥离 completion_text：
        这在 AstrBot 4.10 及以前能挡住上下文；4.11 以后只能保证前端有数据和聊天框干净。"""
        if resp is None or getattr(resp, "_xavier_hub_done", False):
            return
        turn = None
        try:
            marker = event.get_extra(TURN_EXTRA_KEY)
            if isinstance(marker, _TurnMarker):
                turn = marker
        except Exception:
            turn = None
        if turn is None and not self._should_process(event):
            return
        try:
            await self._process_final_response(resp, event, turn)
        except Exception as e:
            logger.error(f"[XavierHub] extract thoughts failed: {e}")

    def _strip_thoughts_text(self, text: str) -> str:
        """剥除 <xt> 标签及其内容（保留给旧代码调用）"""
        return strip_xt_text(text or "")

    @filter.on_decorating_result(priority=50)
    async def strip_thoughts_from_result(self, event: AstrMessageEvent):
        """发送前清洗，三种模式都做，聊天框里不出现心声。
        换成新的 Plain 对象，不去改动 Runner 里的原文（all 模式要靠原文留在上下文里）。
        priority=50：要早于 active_function 的 reply/recall/poke（15/12/11/10）。"""
        result = event.get_result()
        comps = getattr(result, "chain", None) if result is not None else None
        if isinstance(comps, list) and comps and _components_have_xt(comps):
            result.chain = _strip_xt_in_components(comps, copy_plain=True)

        for key in ("_active_func_original_text", "_recall_full_text"):
            raw = event.get_extra(key)
            if isinstance(raw, str) and _has_xt_tag(raw):
                event.set_extra(key, strip_xt_text(raw))

        # 监听 /reset 执行成功（官方标记），彻底切断该会话的旧心声
        if event.get_extra("_clean_group_context_session"):
            try:
                umo = str(getattr(event, "unified_msg_origin", "") or "")
                cid = await self.context.conversation_manager.get_curr_conversation_id(umo)
                scope = self._scope_key(umo, cid)
                await asyncio.to_thread(self._touch_scope_sync, scope, umo, cid, True)
                logger.info(f"[Xavier_Hub] 检测到 /reset 执行成功，已重置会话 {scope} 的心声起点。")
            except Exception as e:
                logger.warning(f"[Xavier_Hub] 重置心声起点失败: {e}")

    # ======== 一日三餐 ========
    DEFAULT_MEALS_PROMPT = (
        "你要为「沈星回」这个角色安排今天的一日三餐。有人希望他每天都吃得好、吃得香。\n"
        "要求：\n"
        "1. 三餐必须顿顿是热的、有滋味的、看着就馋的。家常可以，但一定要好吃。\n"
        "2. 优先安排他爱吃的：各种肉（羊排、红烧肉、牛腩、猪肋排、炸鸡、牛排、火锅）、有汤有水的面食（番茄鸡蛋面、老火汤面、萝卜牛肉汤、小面）、香口的主食（生煎包、肉夹馍、芝士焗饭、煲仔饭），可以有甜品甜的（蛋挞、可可面包、芝士奶黄包、冰激凌、草莓、曲奇）。有时候会有他自己做的创新料理，以前做过辣椒炒蓝莓，酸奶油青瓜片，仅供参考不要照抄。\n"
        "3. 早餐要顶饱、要热，不能只有一杯咖啡；午餐要有肉有菜；晚餐最丰盛，可以有汤。\n"
        "4. 结合季节安排，天冷偏炖煮暖身，天热偏清爽不油，但都要好吃。不要写天气解说。\n"
        "5. 用简短好懂的中文菜名，不写做法步骤，不写营养分析，不写精致摆盘和菜谱术语。\n"
        "6. 每餐 2-3 样，搭配合理，别把三顿写成一样的。\n"
        "note 写一句很短的、让人觉得这顿饭很香很舒服的实感，比如「汤炖了一下午，肉一咬就脱骨」，可留空。\n"
        "只输出 JSON，不要任何解释、不要代码块：\n"
        "{\"breakfast\":{\"dishes\":[\"...\"],\"note\":\"\"},\"lunch\":{\"dishes\":[\"...\"],\"note\":\"\"},\"dinner\":{\"dishes\":[\"...\"],\"note\":\"\"}}"
    )

    DEFAULT_MEALS_INJECT = (
        "\n"
        "【今天给你准备的三餐——只作灵感参考】\n"
        "{meals}\n"
        "（有人希望你今天吃得好、吃得香。这是个方向，不是必须照着念的菜单：被问到就可以这么说，也可以换成别的好吃的、加一样、不必完全按照这个吃。不要主动报菜名，不必把它当成每天固定食谱，不要复读这段系统文字。）\n"
        ""
    )

    def _meals_enabled(self) -> bool:
        return bool(self.plugin_cfg.get("meals_enabled", False))

    def _meals_gen_time(self) -> str:
        raw = str(self.plugin_cfg.get("meals_gen_time", "06:30") or "06:30").strip()
        if not re.match(r"^\d{1,2}:\d{2}$", raw):
            return "06:30"
        try:
            hh, mm = raw.split(":")
            hh, mm = int(hh), int(mm)
            if not (0 <= hh <= 23 and 0 <= mm <= 59):
                return "06:30"
            return f"{hh:02d}:{mm:02d}"
        except Exception:
            return "06:30"

    def _load_meals(self) -> dict:
        try:
            data = self._read_json_file(self.daily_meals_path)
            if isinstance(data, dict):
                return data
        except Exception as e:
            logger.debug(f"[XavierHub] load meals failed: {e}")
        return {}

    def _save_meals(self, payload: dict):
        with self._io_lock:
            self._save_meals_locked(payload)

    def _save_meals_locked(self, payload: dict):
        try:
            old = self._load_meals()
            recent: list = old.get("recent_dishes") or []
            old_meals = old.get("meals")
            if isinstance(old_meals, dict):
                prev_names = []
                for slot in old_meals.values():
                    if isinstance(slot, dict):
                        for d in (slot.get("dishes") or []):
                            t = str(d).strip()
                            if t and t not in prev_names:
                                prev_names.append(t)
                if prev_names:
                    recent.insert(0, prev_names)
            recent = recent[:3]
            payload["recent_dishes"] = recent
            self._write_json_file(self.daily_meals_path, payload)
        except Exception as e:
            logger.error(f"[XavierHub] save meals failed: {e}")

    @staticmethod
    def _today_ymd() -> str:
        return time.strftime("%Y-%m-%d", time.localtime())

    def _meals_is_today(self, data=None) -> bool:
        data = data if data is not None else self._load_meals()
        if not isinstance(data, dict):
            return False
        return str(data.get("date") or "") == self._today_ymd() and bool(data.get("meals"))

    def _clean_meal_slot(self, raw) -> dict:
        dishes, note = [], ""
        if isinstance(raw, dict):
            rd = raw.get("dishes")
            if isinstance(rd, list):
                for x in rd:
                    t = str(x or "").strip()
                    if t:
                        dishes.append(t[:18])
            elif isinstance(rd, str):
                for t in re.split(r"[、,，/|]", rd):
                    t = t.strip()
                    if t:
                        dishes.append(t[:18])
            note = str(raw.get("note") or "").strip()[:40]
        elif isinstance(raw, list):
            for x in raw:
                t = str(x or "").strip()
                if t:
                    dishes.append(t[:18])
        elif isinstance(raw, str):
            for t in re.split(r"[、,，/|]", raw):
                t = t.strip()
                if t:
                    dishes.append(t[:18])
        return {"dishes": dishes[:3], "note": note}

    async def _generate_daily_meals(self, force: bool = False) -> bool:
        if not self._meals_enabled():
            return False
        if not force and self._meals_is_today():
            return False
        provider = None
        try:
            umo = self._note_notify_umo or None
            if umo:
                try:
                    provider = self.context.get_using_provider(umo=umo)
                except TypeError:
                    provider = self.context.get_using_provider()
            if provider is None:
                provider = self.context.get_using_provider()
        except Exception as e:
            logger.warning(f"[XavierHub] meals: get provider failed: {e}")
            return False
        if provider is None:
            logger.warning("[XavierHub] meals: no LLM provider available")
            return False

        sys_prompt = str(self.plugin_cfg.get("meals_prompt", "") or "").strip() or self.DEFAULT_MEALS_PROMPT
        wk = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][time.localtime().tm_wday]
        avoid_dishes: list[str] = []
        try:
            old_data = self._load_meals()
            cur_meals = old_data.get("meals")
            if isinstance(cur_meals, dict):
                for slot in cur_meals.values():
                    if isinstance(slot, dict):
                        for d in (slot.get("dishes") or []):
                            t = str(d).strip()
                            if t and t not in avoid_dishes:
                                avoid_dishes.append(t)
            for batch in (old_data.get("recent_dishes") or []):
                if isinstance(batch, list):
                    for d in batch:
                        t = str(d).strip()
                        if t and t not in avoid_dishes:
                            avoid_dishes.append(t)
        except Exception:
            pass
        avoid_hint = ""
        if avoid_dishes:
            avoid_hint = "\n以下菜最近刚吃过，这次全部换掉，一个都不要再出现：" + "、".join(avoid_dishes[:18])
        user_prompt = f"今天是 {self._today_ymd()} {wk}。生成今天的三餐 JSON。{avoid_hint}"

        raw = ""
        try:
            resp = await provider.text_chat(system_prompt=sys_prompt, prompt=user_prompt)
            raw = (getattr(resp, "completion_text", "") or "").strip()
        except Exception as e:
            logger.warning(f"[XavierHub] meals: text_chat failed: {e}")
            return False
        if not raw:
            return False

        obj = None
        try:
            txt = re.sub(r"^```[a-zA-Z]*|```$", "", raw.strip(), flags=re.M).strip()
            m = re.search(r"\{.*\}", txt, re.S)
            if m:
                obj = json.loads(m.group(0))
        except Exception as e:
            logger.warning(f"[XavierHub] meals: parse failed: {e} | raw={raw[:160]}")
            return False
        if not isinstance(obj, dict):
            logger.warning(f"[XavierHub] meals: bad json | raw={raw[:160]}")
            return False

        meals = {}
        alias = {
            "breakfast": ("breakfast", "早餐", "早饭"),
            "lunch": ("lunch", "午餐", "中饭", "午饭"),
            "dinner": ("dinner", "晚餐", "晚饭"),
        }
        for key, names in alias.items():
            for n in names:
                if n in obj:
                    slot = self._clean_meal_slot(obj.get(n))
                    if slot["dishes"]:
                        meals[key] = slot
                    break
        if not meals:
            logger.warning("[XavierHub] meals: nothing usable after clean")
            return False

        payload = {
            "date": self._today_ymd(),
            "generated_at": int(time.time() * 1000),
            "meals": meals,
        }
        self._save_meals(payload)
        logger.info(
            "[XavierHub] daily meals generated | "
            + " | ".join(f"{k}:{'/'.join(v['dishes'])}" for k, v in meals.items())
        )
        return True

    def _meals_prompt_block(self) -> str:
        if not self._meals_enabled():
            return ""
        data = self._load_meals()
        if not self._meals_is_today(data):
            return ""
        meals = data.get("meals") or {}
        label = {"breakfast": "早", "lunch": "午", "dinner": "晚"}
        rows = []
        for k in ("breakfast", "lunch", "dinner"):
            v = meals.get(k)
            if not isinstance(v, dict):
                continue
            dishes = [str(x).strip() for x in (v.get("dishes") or []) if str(x).strip()]
            if not dishes:
                continue
            note = str(v.get("note") or "").strip()
            line = f"{label[k]}：{'、'.join(dishes)}"
            if note:
                line += f"（{note}）"
            rows.append(line)
        if not rows:
            return ""
        body = "  ".join(rows)
        tpl = str(self.plugin_cfg.get("meals_inject_template", "") or "").strip()
        if not tpl:
            return self.DEFAULT_MEALS_INJECT.replace("{meals}", body)
        if "{meals}" in tpl:
            out = tpl.replace("{meals}", body)
        else:
            out = tpl + "\n" + body
        if not out.startswith("\n"):
            out = "\n" + out
        if not out.endswith("\n"):
            out = out + "\n"
        return out

    async def _meals_scheduler(self):
        logger.info(f"[XavierHub] meals scheduler started | at {self._meals_gen_time()}")
        while True:
            try:
                await asyncio.sleep(60)
                if not self._meals_enabled():
                    continue
                now = time.strftime("%H:%M", time.localtime())
                if now != self._meals_gen_time():
                    continue
                if self._meals_is_today():
                    continue
                await self._generate_daily_meals()
            except asyncio.CancelledError:
                logger.info("[XavierHub] meals scheduler stopped")
                return
            except Exception as e:
                logger.warning(f"[XavierHub] meals scheduler error: {e}")

    def _ensure_meals_task(self):
        if not self._meals_enabled():
            return
        t = getattr(self, "_meals_task", None)
        if t is not None and not t.done():
            return
        loop = self._main_loop
        if loop is None:
            return
        try:
            self._meals_task = loop.create_task(self._meals_scheduler())
            logger.info("[XavierHub] meals task created")
        except Exception as e:
            logger.warning(f"[XavierHub] create meals task failed: {e}")

    @filter.command("小回吃什么")
    async def cmd_meals_show(self, event: AstrMessageEvent):
        if not self._meals_enabled():
            yield event.plain_result("三餐功能没开（插件配置里 meals_enabled）")
            return
        data = self._load_meals()
        if not self._meals_is_today(data):
            yield event.plain_result("今天还没生成，正在想…")
            ok = await self._generate_daily_meals(force=True)
            if not ok:
                yield event.plain_result("生成失败，看下日志里 [XavierHub] meals")
                return
            data = self._load_meals()
        meals = data.get("meals") or {}
        label = {"breakfast": "早餐", "lunch": "午餐", "dinner": "晚餐"}
        lines = [f"{data.get('date','')} 三餐"]
        for k in ("breakfast", "lunch", "dinner"):
            v = meals.get(k)
            if not isinstance(v, dict):
                continue
            dishes = [str(x).strip() for x in (v.get("dishes") or []) if str(x).strip()]
            if not dishes:
                continue
            row = f"{label[k]}：{'、'.join(dishes)}"
            note = str(v.get("note") or "").strip()
            if note:
                row += f"（{note}）"
            lines.append(row)
        yield event.plain_result("\n".join(lines))

    @filter.command("小回重新吃")
    async def cmd_meals_regen(self, event: AstrMessageEvent):
        if not self._meals_enabled():
            yield event.plain_result("三餐功能没开（插件配置里 meals_enabled）")
            return
        ok = await self._generate_daily_meals(force=True)
        yield event.plain_result("重新想好了，用 /小回吃什么 看" if ok else "生成失败，看日志")

    # ======== 前端可视化 ========

    def _start_visualizer(self):
        host = str(self.plugin_cfg.get("visualizer_host", "0.0.0.0"))
        port = int(self.plugin_cfg.get("visualizer_port", 8765) or 8765)
        base_dir = self.base_dir
        data_dir = getattr(self, "data_dir", self.base_dir)
        thoughts_state_path = self.thoughts_state_path
        thoughts_favorites_path = self.thoughts_favorites_path
        thoughts_notes_path = self.thoughts_notes_path
        daily_meals_path = self.daily_meals_path
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
        io_lock = self._io_lock
        write_json_file = self._write_json_file

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

            def _run_api_locked(self, fn):
                """数据接口在锁里执行，响应先攒着，出锁后再发，慢客户端不会一直占着锁。"""
                captured = []

                def _capture(code, body, content_type="text/html; charset=utf-8"):
                    if not captured:
                        captured.append((code, body, content_type))

                self._send = _capture
                try:
                    with io_lock:
                        fn()
                finally:
                    try:
                        del self._send
                    except AttributeError:
                        pass
                for code, body, content_type in captured:
                    self._send(code, body, content_type)

            def _read_json(self, path: Path):
                try:
                    if path and path.exists():
                        return json.loads(path.read_text(encoding="utf-8"))
                except Exception as e:
                    logger.debug(f"[XavierHub] read {path} failed: {e}")
                return {}

            def do_GET(self):
                if self.path.startswith("/api/"):
                    self._run_api_locked(self._do_GET)
                else:
                    self._do_GET()

            def _do_GET(self):
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
                        data.pop("scope", None)
                        data.pop("sess", None)
                        if isinstance(data.get("thoughts"), dict):
                            data["thoughts"] = normalize_thoughts_fn(data.get("thoughts"))
                        else:
                            data["thoughts"] = {k: "" for k in thought_keys_fn()}
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
                            ni.pop("scope", None)
                            ni.pop("sess", None)
                            ni["id"] = tid
                            if isinstance(ni.get("thoughts"), dict):
                                ni["thoughts"] = normalize_thoughts_fn(ni.get("thoughts"))
                            hist.append(ni)
                        
                        data["history"] = hist[:20]
                        data["total_history_count"] = len(hist)
                        data["favorite_ids"] = list(fav_ids)
                        data["favorites_count"] = len(favs)
                        _ff_payload = load_field_favorites_payload()
                        data["field_favorites"] = _ff_payload.get("fields") or {}
                        data["field_sentences"] = _ff_payload.get("sentences") or []
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
                        try:
                            import time as _time
                            from datetime import datetime as _dt
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
                        try:
                            dm = self._read_json(daily_meals_path)
                            data["daily_meals"] = dm if isinstance(dm, dict) else None
                        except Exception:
                            data["daily_meals"] = None
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
                            "version": PLUGIN_VERSION,
                        }, ensure_ascii=False), "application/json; charset=utf-8")
                        return

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
                            ct = "image/svg+xml" if self.path.endswith(".svg") else "image/png"
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
                self._run_api_locked(self._do_POST)

            def _do_POST(self):
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
                            if field in cur_set:
                                cur_set.discard(field)
                                on = False
                            else:
                                cur_set.add(field)
                                on = True
                        if cur_set:
                            fields_map[tid] = sorted(cur_set)
                        else:
                            fields_map.pop(tid, None)

                        def _sent_key(s):
                            return (str(s.get("thought_id") or ""), str(s.get("field") or ""))

                        sentences = [s for s in sentences if isinstance(s, dict) and _sent_key(s) != (tid, field)]
                        if on:
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
                        data = {}
                        if thoughts_state_path.exists():
                            try:
                                data = json.loads(thoughts_state_path.read_text(encoding="utf-8"))
                            except Exception as e:
                                self._send(500, json.dumps({"ok": 0, "error": f"心声文件读取失败，未做修改: {e}"}, ensure_ascii=False), "application/json; charset=utf-8")
                                return
                        if not isinstance(data, dict):
                            data = {}
                        history = data.get("history") or []
                        if not isinstance(history, list):
                            history = []
                        before = len(history)
                        history = [h for h in history if str((h or {}).get("id") or "") != tid]
                        removed = before - len(history)
                        data["history"] = history
                        try:
                            write_json_file(thoughts_state_path, data)
                        except Exception as e:
                            self._send(500, json.dumps({"ok": 0, "error": str(e)}, ensure_ascii=False), "application/json; charset=utf-8")
                            return
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
            _RunnerGuard.uninstall(self)
        except Exception as e:
            logger.warning(f"[XavierHub] uninstall guard failed: {e}")
        try:
            t = getattr(self, "_meals_task", None)
            if t is not None and not t.done():
                t.cancel()
        except Exception:
            pass
        try:
            if self.httpd:
                self.httpd.shutdown()
                self.httpd.server_close()
        except Exception:
            pass

    @filter.command("xavier_hub_health")
    async def xavier_hub_health(self, event: AstrMessageEvent):
        wl_status = f"已启用（{len(self.session_whitelist)}条规则）" if self.session_whitelist else "未限制"
        try:
            from astrbot.core.config.default import VERSION as ASTRBOT_VERSION
        except Exception:
            ASTRBOT_VERSION = "未知"
        mode = self._get_history_mode()
        mode_text = {
            "last_n": f"last_n（每轮注入本对话最近 {self._get_history_n()} 轮）",
            "none": "none（不进上下文，不注入）",
            "all": "all（进上下文，不注入）",
        }.get(mode, mode)
        if self._guard_methods and not self._guard_error:
            guard_text = "已生效（" + "、".join(self._guard_methods) + "）"
        else:
            guard_text = f"未生效：{self._guard_error or '未知原因'}"
        lines = [
            f"Xavier Hub v{PLUGIN_VERSION} 状态",
            f"AstrBot 版本: {ASTRBOT_VERSION}",
            f"心声模式: {mode_text}",
            f"上下文拦截: {guard_text}",
            f"数据目录: {getattr(self, 'data_dir', self.base_dir)}",
            f"心声 state 文件: {'✓' if self.thoughts_state_path.exists() else '✗'} ({self.thoughts_state_path.name})",
            f"phone_state 文件: {'✓' if self.phone_state_path.exists() else '✗'} ({self.phone_state_path.name})",
            f"目标用户: {self.target_user_id or '不限'}",
            f"会话白名单: {wl_status}",
            f"可视化前端: http://127.0.0.1:{self.plugin_cfg.get('visualizer_port', 8765)}",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.command("xt")
    async def xt_cmd(self, event: AstrMessageEvent):
        """查看当前会话最近的一条心声"""
        sess = self._sess_key(str(getattr(event, "unified_msg_origin", "") or ""))
        t = await asyncio.to_thread(self._latest_thought_for_sess, sess)
        if not t:
            yield event.plain_result("这个会话里还没有心声记录")
            return
        t = self._normalize_thoughts(t)
        line = "\n".join(f"{k}：{t.get(k, '')}" for k in self._get_thought_keys())
        yield event.plain_result(line)

    @filter.command("xavier_notes")
    async def xavier_notes_cmd(self, event: AstrMessageEvent):
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