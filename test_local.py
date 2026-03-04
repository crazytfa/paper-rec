"""
本地测试脚本
============
在本地快速验证系统各模块是否正常工作，无需真正运行完整流水线。

使用方法：
  python test_local.py                     # 运行所有测试
  python test_local.py --only arxiv        # 只测试 arXiv 采集
  python test_local.py --only pubmed       # 只测试 PubMed 采集
  python test_local.py --only doubao       # 只测试 Doubao API
  python test_local.py --only feishu       # 只测试飞书推送
  python test_local.py --only email        # 只测试邮件推送
  python test_local.py --only db           # 只测试数据库连接

运行前需要先设置环境变量（或在这里直接填写，注意不要提交到 Git）：
  export ARK_API_KEY="your_key"
  export SUPABASE_URL="https://xxxx.supabase.co"
  export SUPABASE_KEY="eyJ..."
  export RESEND_API_KEY="re_..."
  export FEISHU_WEBHOOK="https://open.feishu.cn/..."
"""

import os
import sys
import time
import argparse

# ── 颜色输出 ─────────────────────────────────────────────────────

def ok(msg):  print(f"  ✅ {msg}")
def fail(msg): print(f"  ❌ {msg}")
def info(msg): print(f"  ℹ️  {msg}")
def title(msg): print(f"\n{'─'*50}\n🧪 {msg}\n{'─'*50}")


# ── 测试：arXiv 采集 ──────────────────────────────────────────────

def test_arxiv():
    title("arXiv 采集测试")
    try:
        import arxiv
        search = arxiv.Search(query="cat:cs.CV", max_results=3,
                               sort_by=arxiv.SortCriterion.SubmittedDate)
        results = list(search.results())
        if results:
            ok(f"arXiv 连接正常，采集到 {len(results)} 篇")
            for r in results:
                info(f"  · {r.title[:60]}...")
        else:
            fail("arXiv 未返回结果")
    except Exception as e:
        fail(f"arXiv 采集失败: {e}")


# ── 测试：PubMed 采集 ─────────────────────────────────────────────

def test_pubmed():
    title("PubMed 采集测试")
    try:
        import requests
        resp = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params={
                "db": "pubmed",
                "term": '"Nature methods"[Journal] AND "fluorescence"[Title/Abstract]',
                "retmax": 3, "retmode": "json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        pmids = resp.json().get("esearchresult", {}).get("idlist", [])
        if pmids:
            ok(f"PubMed 连接正常，找到 {len(pmids)} 篇（PMID: {', '.join(pmids)}）")
        else:
            info("PubMed 连接正常，但本次查询无结果（时间范围内可能无新论文）")
    except Exception as e:
        fail(f"PubMed 连接失败: {e}")


# ── 测试：Semantic Scholar ────────────────────────────────────────

def test_semantic_scholar():
    title("Semantic Scholar 采集测试")
    try:
        import requests
        resp = requests.get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            params={"query": "CVPR", "fields": "title,year", "limit": 2},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if data:
            ok(f"Semantic Scholar 连接正常，返回 {len(data)} 条结果")
            for item in data:
                info(f"  · {item.get('title', '')[:60]}... ({item.get('year', '')})")
        else:
            info("Semantic Scholar 连接正常，但本次查询无结果")
    except Exception as e:
        fail(f"Semantic Scholar 连接失败: {e}")


# ── 测试：Doubao API ──────────────────────────────────────────────

def test_doubao():
    title("火山方舟 Doubao API 测试")
    api_key = os.environ.get("ARK_API_KEY", "")
    if not api_key:
        fail("ARK_API_KEY 环境变量未设置")
        return

    # 测试 lite 模型（粗筛用）
    try:
        from volcenginesdkarkruntime import Ark
        client = Ark(api_key=api_key)
        resp = client.chat.completions.create(
            model="doubao-lite-4k",
            messages=[{"role": "user",
                       "content": '请给以下论文打相关性分数，只返回JSON: {"scores": [{"index": 0, "score": 0.9}]}\n[0] 标题: Deep learning for fluorescence microscopy'}],
            max_tokens=100,
        )
        content = resp.choices[0].message.content
        ok(f"doubao-lite-4k 正常，响应: {content[:80]}")
    except Exception as e:
        fail(f"doubao-lite-4k 失败: {e}")

    time.sleep(1)

    # 测试 pro 模型（精析用）
    try:
        resp = client.chat.completions.create(
            model="doubao-pro-32k",
            messages=[{"role": "user",
                       "content": '用一句中文概括：STED microscopy achieves super-resolution by depleting fluorophores.'}],
            max_tokens=100,
        )
        content = resp.choices[0].message.content
        ok(f"doubao-pro-32k 正常，响应: {content[:80]}")
    except Exception as e:
        fail(f"doubao-pro-32k 失败: {e}")


# ── 测试：Supabase 数据库 ─────────────────────────────────────────

def test_db():
    title("Supabase 数据库测试")
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        fail("SUPABASE_URL 或 SUPABASE_KEY 未设置")
        return

    try:
        from supabase import create_client
        db = create_client(url, key)
        # 测试读取（sent_papers 表应该已经建好）
        result = db.table("sent_papers").select("paper_id").limit(1).execute()
        ok(f"Supabase 连接正常，sent_papers 表可访问")
        info(f"  当前记录数（抽样）: {len(result.data)} 条")
    except Exception as e:
        fail(f"Supabase 连接失败: {e}")
        info("请确认已在 Supabase 执行了 setup_database.sql")


# ── 测试：飞书推送 ────────────────────────────────────────────────

def test_feishu():
    title("飞书推送测试")
    webhook = os.environ.get("FEISHU_WEBHOOK", "")
    if not webhook:
        fail("FEISHU_WEBHOOK 环境变量未设置")
        return

    import requests
    test_card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "🧪 PaperBot 测试消息"},
                "template": "blue",
            },
            "elements": [{
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "✅ **飞书推送测试成功！**\n\n"
                        "如果你看到这条消息，说明飞书机器人配置正确。\n"
                        "下次 GitHub Actions 运行时就会自动推送论文了。"
                    )
                }
            }]
        }
    }
    try:
        resp = requests.post(webhook, json=test_card, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == 0:
            ok("飞书推送成功！请检查飞书群是否收到测试消息")
        else:
            fail(f"飞书返回错误: {data}")
    except Exception as e:
        fail(f"飞书推送失败: {e}")


# ── 测试：邮件推送 ────────────────────────────────────────────────

def test_email():
    title("邮件推送测试（Resend）")
    api_key = os.environ.get("RESEND_API_KEY", "")
    if not api_key:
        fail("RESEND_API_KEY 环境变量未设置")
        return

    try:
        import resend
        import yaml
        resend.api_key = api_key

        with open("config.yaml") as f:
            cfg = yaml.safe_load(f)

        to_addr = cfg["push"]["email"]["to"]
        from_addr = cfg["push"]["email"]["from"]

        if isinstance(to_addr, str):
            to_addr = [to_addr]

        resend.Emails.send({
            "from": from_addr,
            "to": to_addr,
            "subject": "[PaperBot] 邮件测试 - 配置正常 ✅",
            "html": """<div style="font-family:sans-serif;padding:30px;">
              <h2>✅ PaperBot 邮件测试成功</h2>
              <p>如果你收到这封邮件，说明邮件推送配置正确。</p>
              <p>收件人：{to}</p>
              <p style="color:#666;font-size:13px;">这是一封自动测试邮件，请忽略。</p>
            </div>""".format(to=", ".join(to_addr)),
        })
        ok(f"邮件发送成功 → {', '.join(to_addr)}")
        info("请检查邮箱（含垃圾邮件箱）是否收到测试邮件")
    except Exception as e:
        fail(f"邮件发送失败: {e}")


# ── 入口 ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PaperBot 本地测试工具")
    parser.add_argument("--only", choices=["arxiv", "pubmed", "s2", "doubao",
                                           "db", "feishu", "email"],
                        help="只运行指定测试")
    args = parser.parse_args()

    test_map = {
        "arxiv": test_arxiv,
        "pubmed": test_pubmed,
        "s2": test_semantic_scholar,
        "doubao": test_doubao,
        "db": test_db,
        "feishu": test_feishu,
        "email": test_email,
    }

    if args.only:
        test_map[args.only]()
    else:
        print("🚀 PaperBot 全量测试开始...")
        for name, fn in test_map.items():
            fn()

    print("\n测试完成。如有 ❌ 请根据提示检查对应配置。\n")


if __name__ == "__main__":
    main()
