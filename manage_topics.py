"""
主题管理命令行工具
==================
用于方便地添加、删除、查看、启用/禁用推荐主题，
无需手动编辑 config.yaml 的 YAML 语法。

使用方法：
  python manage_topics.py list                     # 查看所有主题
  python manage_topics.py add                      # 交互式添加主题
  python manage_topics.py disable ai_general       # 禁用某个主题
  python manage_topics.py enable  ai_general       # 启用某个主题
  python manage_topics.py delete  ai_general       # 删除某个主题
  python manage_topics.py edit    ai_general       # 交互式修改主题
"""

import sys
import yaml

CONFIG_FILE = "config.yaml"


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False,
                  sort_keys=False, indent=2)
    print(f"✅ 已保存到 {CONFIG_FILE}")
    print("   提醒：修改后需要 git add config.yaml && git commit && git push 才能生效")


def cmd_list(cfg):
    """列出所有主题"""
    topics = cfg.get("topics", [])
    if not topics:
        print("暂无主题配置")
        return

    print(f"\n{'ID':<20} {'名称':<15} {'状态':<8} {'频率':<8} {'推送数量'}")
    print("-" * 65)
    for t in topics:
        status = "✅ 启用" if t.get("enabled", True) else "⏸ 停用"
        schedule = "每天" if t.get("schedule") == "daily" else "每周"
        print(f"{t['id']:<20} {t['name']:<15} {status:<10} {schedule:<8} {t.get('top_n', 8)} 篇")
    print()


def ask(prompt, default=None):
    """带默认值的输入提示"""
    suffix = f" [{default}]" if default is not None else ""
    val = input(f"{prompt}{suffix}: ").strip()
    return val if val else default


def cmd_add(cfg):
    """交互式添加新主题"""
    print("\n── 添加新主题 ──────────────────────────────")
    print("（直接回车使用默认值）\n")

    topic_id = ask("主题 ID（英文，如 rl_papers）").replace(" ", "_").lower()
    if any(t["id"] == topic_id for t in cfg.get("topics", [])):
        print(f"❌ 主题 ID '{topic_id}' 已存在，请用其他名称")
        return

    name = ask("主题中文名称（如 强化学习）")
    schedule = ask("推送频率 daily/weekly", "daily")
    top_n = int(ask("每次推送几篇", "8"))
    context = ask(f"研究方向描述（给 AI 的提示，如 {name} 相关论文）", name)

    # arXiv 分类
    print("\n常用 arXiv 分类参考：")
    print("  cs.AI cs.CV cs.LG cs.CL cs.RO cs.NE cs.IR")
    print("  physics.optics physics.bio-ph q-bio.QM eess.SP stat.ML")
    cats_input = ask("arXiv 分类（空格分隔，不需要则留空）", "")
    categories = cats_input.split() if cats_input else []

    # 关键词
    kws_input = ask("关键词（逗号分隔，用于 PubMed 和关键词过滤）", "")
    keywords = [k.strip() for k in kws_input.split(",") if k.strip()]

    # 是否需要 AI 粗筛
    use_coarse = ask("是否启用 AI 粗筛（量多时建议 yes，量少时 no）", "yes")
    coarse_threshold = float(ask("粗筛相关性阈值（0-1）", "0.65")) if use_coarse.lower() == "yes" else None

    # 期刊白名单（可选）
    journals_input = ask("PubMed 期刊白名单（逗号分隔，不需要则留空）", "")
    journals = [j.strip() for j in journals_input.split(",") if j.strip()]

    # 构建主题配置字典
    new_topic = {
        "id": topic_id,
        "name": name,
        "enabled": True,
        "schedule": schedule,
        "top_n": top_n,
        "context": context,
        "sources": {
            "arxiv": {
                "enabled": bool(categories),
                "categories": categories,
                "max_results": 100,
            },
            "semantic_scholar": {
                "enabled": False,
                "venues": [],
            },
            "pubmed": {
                "enabled": bool(journals or keywords),
                "journals": journals,
                "keywords": keywords,
                "lookback_days": 7,
            },
        },
        "keywords": keywords,
        "deep_model": "doubao-pro-32k",
        "max_deep_analysis": 15,
    }

    if coarse_threshold is not None:
        new_topic["coarse_model"] = "doubao-lite-4k"
        new_topic["coarse_threshold"] = coarse_threshold

    # 添加到 config
    if "topics" not in cfg:
        cfg["topics"] = []
    cfg["topics"].append(new_topic)

    print(f"\n📋 新主题预览：")
    print(yaml.dump(new_topic, allow_unicode=True, default_flow_style=False, indent=2))

    confirm = ask("确认保存？(yes/no)", "yes")
    if confirm.lower() in ("yes", "y"):
        save_config(cfg)
        print(f"✅ 主题「{name}」已添加（ID: {topic_id}）")
    else:
        print("已取消")


def cmd_toggle(cfg, topic_id: str, enabled: bool):
    """启用或禁用主题"""
    for t in cfg.get("topics", []):
        if t["id"] == topic_id:
            t["enabled"] = enabled
            save_config(cfg)
            state = "启用" if enabled else "禁用"
            print(f"✅ 主题「{t['name']}」已{state}")
            return
    print(f"❌ 找不到主题 ID：{topic_id}")


def cmd_delete(cfg, topic_id: str):
    """删除主题"""
    topics = cfg.get("topics", [])
    match = next((t for t in topics if t["id"] == topic_id), None)
    if not match:
        print(f"❌ 找不到主题 ID：{topic_id}")
        return

    confirm = input(f"确认删除主题「{match['name']}」？(yes/no): ").strip()
    if confirm.lower() in ("yes", "y"):
        cfg["topics"] = [t for t in topics if t["id"] != topic_id]
        save_config(cfg)
        print(f"✅ 主题「{match['name']}」已删除")
    else:
        print("已取消")


def cmd_edit(cfg, topic_id: str):
    """交互式修改主题的关键字段"""
    match = next((t for t in cfg.get("topics", []) if t["id"] == topic_id), None)
    if not match:
        print(f"❌ 找不到主题 ID：{topic_id}")
        return

    print(f"\n── 修改主题「{match['name']}」────────────────")
    print("（直接回车保持原值不变）\n")

    name = ask("主题名称", match["name"])
    schedule = ask("推送频率 daily/weekly", match.get("schedule", "daily"))
    top_n = ask("每次推送几篇", str(match.get("top_n", 8)))
    context = ask("研究方向描述", match.get("context", name))

    kws_input = ask("关键词（逗号分隔）",
                    ", ".join(match.get("keywords", [])))
    keywords = [k.strip() for k in kws_input.split(",") if k.strip()]

    match["name"] = name
    match["schedule"] = schedule
    match["top_n"] = int(top_n)
    match["context"] = context
    match["keywords"] = keywords

    save_config(cfg)
    print(f"✅ 主题「{name}」已更新")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]
    cfg = load_config()

    if cmd == "list":
        cmd_list(cfg)
    elif cmd == "add":
        cmd_add(cfg)
    elif cmd == "disable" and len(sys.argv) >= 3:
        cmd_toggle(cfg, sys.argv[2], enabled=False)
    elif cmd == "enable" and len(sys.argv) >= 3:
        cmd_toggle(cfg, sys.argv[2], enabled=True)
    elif cmd == "delete" and len(sys.argv) >= 3:
        cmd_delete(cfg, sys.argv[2])
    elif cmd == "edit" and len(sys.argv) >= 3:
        cmd_edit(cfg, sys.argv[2])
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
