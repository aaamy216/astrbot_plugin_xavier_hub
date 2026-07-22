# Xavier Hub

沈星回心声可视化 + 独立前端服务。模型在回复末尾输出 `<xavier_thoughts>` JSON，本插件解析后写入状态，并在网页展示、收藏、便签。

## 安装

1. 把本目录放到 AstrBot 的 `data/plugins/` 下  
2. 仪表盘 → 插件 → 启用 / 重载 **Xavier Hub**  
3. 浏览器打开：`http://服务器IP:端口`（端口在配置 `visualizer_port` 里设置，默认 `8765`）

## 必配

| 配置项 | 说明 |
|--------|------|
| `target_user_id` | 只对这个用户启用心声（填对方用户 ID；留空=不限制，请谨慎） |
| `visualizer_port` | 前端端口，默认 `8765`（请改成自己的端口） |
| `visualizer_host` | 默认 `0.0.0.0` 允许外网；仅本机可改 `127.0.0.1` |
| `thoughts_instruction` | 注入给模型的心声模板，**字段名要和下面一致** |
| `thought_fields` | 心声字段，逗号分隔，默认：`情绪,兔在想,兔悄悄,兔心愿,兔言兔语` |

改配置后 **重载插件** 生效。

## 网页怎么用

- **实时**：当前心声卡片  
- **历史**：往期心声；右上角 × 可删（点两次确认）  
- **小心心**：整条收藏 → 进「悄悄喜欢」  
- **短按 emoji**：给该字段打红心标记  
- **长按 emoji**：贴纸  
- **便签**：贴在某条心声上；可配置是否伪装成私聊通知机器人  

## 可选

- `session_whitelist`：只在指定会话生效；留空=不限制会话（仍受 `target_user_id` 约束）  
- `ext_panel_enabled`：开启后才显示「手机」页，并读取手机推送 / 闹钟等跨插件数据（默认关）  
- `note_notify_enabled` / `note_notify_umo` / `bot_qq_id`：便签通知相关（**请填你自己的会话与机器人 ID，不要提交到公开仓库**）  

## 命令

- `/xavier_thoughts` — 查看当前心声（若已注册）  
- `/xavier_hub_health` — 健康检查  

## 数据

运行时数据在插件数据目录（`thoughts_state`、收藏、便签等）。**不要把个人数据、真实 QQ/会话号提交到公开仓库。** 推 Git 时只保留代码与配置模板即可。

## 开发备注

- 前端：`hub_visualizer.html`  
- 后端：`main.py` 内嵌 HTTP 服务  
- Logo：`logo.png` / `icon.svg`  
