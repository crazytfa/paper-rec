"""
PaperBot 主流水线
================
每日/每周自动从 arXiv、PubMed、Semantic Scholar 采集论文，
用火山方舟 Doubao 模型分析，推送到邮件和飞书群。

运行方式：
  python pipeline.py                    # 运行所有到期的主题
  python pipeline.py --topic ai_general # 只运行指定主题
  python pipeline.py --topic all        # 强制运行所有主题（忽略日期判断）
  python pipeline.py --dry-run          # 测试模式：打印结果但不推送、不写数据库
  python pipeline.py --dry-run --topic ai_general  # 组合使用
"""

import os
import sys
import json
import time
import hashlib
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional

import yaml
import arxiv
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import resend
from volcenginesdkarkruntime import Ark

# ══════════════════════════════════════════════════════════════════
# 初始化：加载配置、连接外部服务
# ══════════════════════════════════════════════════════════════════

def load_config() -> dict:
    """加载 config.yaml，自动替换 ${ENV_VAR} 占位符为环境变量"""
    with open("config.yaml", "r", encoding="utf-8") as f:
        raw = f.read()
    for key, val in os.environ.items():
        raw = raw.replace(f"${{{key}}}", val)
    return yaml.safe_load(raw)


CFG = load_config()

# 火山方舟客户端（Doubao）
ARK = Ark(api_key=os.environ["ARK_API_KEY"])

# Supabase REST API 配置（用 requests 直接调用，避免 SDK 版本冲突）
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

# Resend 邮件客户端
resend.api_key = os.environ.get("RESEND_API_KEY", "")


# ══════════════════════════════════════════════════════════════════
# 数据结构
# ══════════════════════════════════════════════════════════════════

class Paper:
    """
    单篇论文的数据容器。
    采集阶段填充基础字段，AI 分析阶段填充 summary/innovation 等。
    """
    def __init__(self, title: str, abstract: str, authors: list,
                 url: str, doi: str, source: str,
                 published_date: str, venue: str = ""):
        self.title = title
        self.abstract = abstract
        self.authors = authors[:4]       # 最多保留前4位作者
        self.url = url
        self.doi = doi
        self.source = source             # "arxiv" / "pubmed" / "semantic_scholar"
        self.venue = venue               # 期刊/会议名称
        self.published_date = published_date
        self.paper_id = self._make_id()

        # 以下字段由 AI 分析后填充
        self.relevance_score: float = 0.0
        self.summary_zh: str = ""        # 中文摘要
        self.innovation: str = ""        # 核心创新点
        self.recommendation_reason: str = ""  # 推荐理由
        self.conference_tag: str = ""    # 顶会标签（如 "✨ CVPR 2024"），从摘要/comment 中提取

    def _make_id(self) -> str:
        """
        用 DOI（首选）或标题哈希生成唯一 ID，用于数据库去重。
        DOI 是最稳定的学术标识符。
        """
        key = self.doi.strip() if self.doi else self.title.lower().strip()
        return hashlib.md5(key.encode()).hexdigest()[:16]

    def __repr__(self):
        return f"<Paper [{self.source}] {self.title[:55]}...>"


# ══════════════════════════════════════════════════════════════════
# 数据库操作
# ══════════════════════════════════════════════════════════════════

def is_already_sent(paper_id: str, cooldown_days: int) -> bool:
    """
    查询数据库：这篇论文是否在冷却期内已经推送过？
    防止同一篇论文反复出现在推荐列表中。
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=cooldown_days)).isoformat()
    try:
        # Supabase REST API：GET /rest/v1/sent_papers?paper_id=eq.xxx&sent_at=gte.xxx
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/sent_papers",
            headers={**SUPABASE_HEADERS, "Prefer": "return=representation"},
            params={
                "paper_id": f"eq.{paper_id}",
                "sent_at": f"gte.{cutoff}",
                "select": "paper_id",
            },
            timeout=10,
        )
        resp.raise_for_status()
        return len(resp.json()) > 0
    except Exception as e:
        logger.warning(f"去重查询失败（网络问题？），默认不去重: {e}")
        return False


def mark_as_sent(papers: list, topic_id: str):
    """将已推送的论文写入数据库，供下次去重使用"""
    rows = [{
        "paper_id": p.paper_id,
        "title": p.title[:500],
        "doi": p.doi or "",
        "topic_id": topic_id,
        "relevance_score": round(p.relevance_score, 3),
        "sent_at": datetime.now(timezone.utc).isoformat(),
    } for p in papers]
    try:
        # Supabase REST API：POST /rest/v1/sent_papers（upsert）
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/sent_papers",
            headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"},
            json=rows,
            timeout=15,
        )
        resp.raise_for_status()
        logger.info(f"已记录 {len(rows)} 篇到数据库（topic: {topic_id}）")
    except Exception as e:
        logger.error(f"写入数据库失败: {e}")


def save_topic_to_db(topic: dict):
    """
    将主题配置保存到数据库（供飞书机器人动态修改主题时使用）。
    如果你只用 config.yaml 管理主题，这个函数不会被调用。
    """
    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/user_topics",
            headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"},
            json={
                "topic_id": topic["id"],
                "name": topic["name"],
                "config_json": json.dumps(topic, ensure_ascii=False),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"保存主题到数据库失败: {e}")


# ══════════════════════════════════════════════════════════════════
# 数据采集：arXiv
# ══════════════════════════════════════════════════════════════════

def fetch_arxiv(categories: list, max_results: int = 150,
                lookback_days: int = 1) -> list:
    """
    从 arXiv 拉取最近 lookback_days 天的新论文。

    arXiv 是开放的预印本平台，顶会论文（CVPR/ICCV/NeurIPS 等）
    作者几乎都会同步上传，所以 cs.CV 分类基本覆盖了主流 CV 会议。
    """
    papers = []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days + 1)).date()

    for cat in categories:
        logger.info(f"  采集 arXiv [{cat}]...")
        try:
            search = arxiv.Search(
                query=f"cat:{cat}",
                max_results=max_results,
                sort_by=arxiv.SortCriterion.SubmittedDate,
                sort_order=arxiv.SortOrder.Descending,
            )
            for result in search.results():
                pub_date = result.published.date()
                # 超出时间窗口就停止（结果按时间降序排列）
                if pub_date < cutoff:
                    break
                p = Paper(
                    title=result.title.strip(),
                    abstract=result.summary.strip(),
                    authors=[a.name for a in result.authors],
                    url=result.entry_id,
                    doi=result.doi or "",
                    source="arxiv",
                    published_date=str(pub_date),
                    venue=cat,
                )
                # arXiv comment 字段里作者经常写 "Accepted at CVPR 2024"
                # 把 comment 拼到摘要末尾，供后续顶会标签提取使用
                if getattr(result, "comment", None):
                    p.abstract = p.abstract + f" [arXiv comment: {result.comment}]"
                papers.append(p)
            time.sleep(1)  # 礼貌延迟，避免被限流
        except Exception as e:
            logger.error(f"  arXiv [{cat}] 采集失败: {e}")

    logger.info(f"  arXiv 共采集 {len(papers)} 篇")
    return papers


# ══════════════════════════════════════════════════════════════════
# 数据采集：Semantic Scholar
# ══════════════════════════════════════════════════════════════════

def _s2_request_one(venue: str, min_year: int) -> list:
    """
    对单个 venue 发起一次 Semantic Scholar 请求，内置重试和限速处理。

    429 处理策略：
    - 遇到 429 时等待 retry-after 响应头指定的秒数（通常 30-60s）
    - 若无该响应头，则指数退避：第1次等30s，第2次60s，第3次120s
    - 最多重试 3 次，全部失败才放弃这个 venue
    """
    # 如果配置了 API Key，请求速率可提升到 10 req/s
    api_key = os.environ.get("S2_API_KEY", "")
    headers = {"x-api-key": api_key} if api_key else {}

    params = {
        "query": venue,
        "fields": "title,abstract,authors,year,externalIds,venue,openAccessPdf",
        "limit": 40,
        "publicationTypes": "JournalArticle,Conference",
    }

    wait_times = [30, 60, 120]  # 三次重试的等待秒数
    for attempt, wait_sec in enumerate(wait_times, 1):
        try:
            resp = requests.get(
                "https://api.semanticscholar.org/graph/v1/paper/search",
                headers=headers,
                params=params,
                timeout=20,
            )

            if resp.status_code == 429:
                # 优先用服务器返回的 retry-after 时间
                retry_after = int(resp.headers.get("retry-after", wait_sec))
                logger.warning(
                    f"  S2 [{venue[:25]}] 限速（第{attempt}次），"
                    f"等待 {retry_after} 秒后重试..."
                )
                time.sleep(retry_after)
                continue  # 重试

            resp.raise_for_status()

            results = []
            for item in resp.json().get("data", []):
                if not item.get("abstract"):
                    continue
                if (item.get("year") or 0) < min_year:
                    continue
                ext_ids = item.get("externalIds", {})
                doi = ext_ids.get("DOI", "")
                arxiv_id = ext_ids.get("ArXiv", "")
                url = (f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id
                       else f"https://doi.org/{doi}" if doi else "")
                results.append(Paper(
                    title=item["title"].strip(),
                    abstract=item["abstract"].strip(),
                    authors=[a["name"] for a in item.get("authors", [])],
                    url=url,
                    doi=doi,
                    source="semantic_scholar",
                    published_date=str(item.get("year", "")),
                    venue=venue,
                ))
            return results  # 成功，返回结果

        except requests.exceptions.HTTPError as e:
            if attempt < len(wait_times):
                logger.warning(f"  S2 [{venue[:25]}] HTTP错误（第{attempt}次）: {e}，等待重试")
                time.sleep(wait_sec)
            else:
                logger.error(f"  S2 [{venue[:25]}] 失败（已重试{len(wait_times)}次）: {e}")
        except Exception as e:
            logger.error(f"  S2 [{venue[:25]}] 异常: {e}")
            break  # 非 HTTP 错误不重试

    return []


def fetch_semantic_scholar(venues: list) -> list:
    """
    从 Semantic Scholar 批量采集指定顶会/期刊的论文。

    限速策略：
    - 无 API Key：每次请求后固定等待 5 秒（Semantic Scholar 免费限速约 1 req/s，
      保守取 5s 留出余量，12 个 venue 约需 60 秒）
    - 有 API Key（在 GitHub Secrets 中配置 S2_API_KEY）：等待 1 秒即可，
      申请地址：https://www.semanticscholar.org/product/api

    429 时会自动等待后重试（见 _s2_request_one）。
    """
    papers = []
    min_year = datetime.now().year - 1
    has_api_key = bool(os.environ.get("S2_API_KEY", ""))
    delay = 1.0 if has_api_key else 5.0  # 有 Key 用 1s，无 Key 用 5s

    for i, venue in enumerate(venues):
        logger.info(f"  采集 S2 [{venue[:30]}] ({i+1}/{len(venues)})...")
        results = _s2_request_one(venue, min_year)
        papers.extend(results)
        logger.info(f"    → {len(results)} 篇")

        # 最后一个 venue 不需要等待
        if i < len(venues) - 1:
            time.sleep(delay)

    logger.info(f"  Semantic Scholar 共采集 {len(papers)} 篇")
    return papers


# ══════════════════════════════════════════════════════════════════
# 数据采集：PubMed
# ══════════════════════════════════════════════════════════════════

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=8))
def fetch_pubmed(journals: list, keywords: list, days_back: int = 7) -> list:
    """
    从 PubMed E-utilities API 采集论文。

    PubMed 是美国国立医学图书馆维护的数据库，收录了几乎所有
    Nature 系列、Science、Cell、Optica 等顶刊的摘要和 DOI，完全免费。

    检索逻辑：(期刊白名单中的任意一个) AND (关键词列表中的任意一个)
    """
    papers = []
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    api_key = os.environ.get("PUBMED_API_KEY", "")  # 可选，有则速率更高

    # 构建检索式
    date_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y/%m/%d")
    journal_q = " OR ".join([f'"{j}"[Journal]' for j in journals])
    keyword_q = " OR ".join([f'"{k}"[Title/Abstract]' for k in keywords])
    query = (f"({journal_q}) AND ({keyword_q}) AND "
             f'("{date_from}"[Date - Publication] : "2099/12/31"[Date - Publication])')

    logger.info(f"  PubMed 查询：近{days_back}天，{len(journals)}个期刊，{len(keywords)}个关键词")

    try:
        # 第一步：搜索，获取 PMID 列表
        search = requests.get(f"{base_url}/esearch.fcgi", params={
            "db": "pubmed", "term": query,
            "retmax": 100, "retmode": "json",
            "api_key": api_key,
        }, timeout=20)
        search.raise_for_status()
        pmids = search.json().get("esearchresult", {}).get("idlist", [])
        logger.info(f"  PubMed 检索到 {len(pmids)} 篇")

        if not pmids:
            return []

        # 第二步：获取摘要 XML（包含完整摘要文本）
        abstract_resp = requests.get(f"{base_url}/efetch.fcgi", params={
            "db": "pubmed", "id": ",".join(pmids),
            "rettype": "xml", "retmode": "xml",
            "api_key": api_key,
        }, timeout=30)

        # 解析摘要 XML，建立 pmid -> abstract 的映射
        abstracts = {}
        try:
            root = ET.fromstring(abstract_resp.content)
            for article in root.findall(".//PubmedArticle"):
                pmid_el = article.find(".//PMID")
                abs_texts = article.findall(".//AbstractText")
                if pmid_el is not None and abs_texts:
                    full_abstract = " ".join(
                        (el.text or "") for el in abs_texts if el.text
                    )
                    abstracts[pmid_el.text] = full_abstract
        except ET.ParseError as e:
            logger.warning(f"  PubMed XML 解析失败: {e}")

        # 第三步：获取元数据摘要（标题、作者、DOI、期刊名）
        summary_resp = requests.get(f"{base_url}/esummary.fcgi", params={
            "db": "pubmed", "id": ",".join(pmids),
            "retmode": "json", "api_key": api_key,
        }, timeout=30)
        summary_resp.raise_for_status()
        summaries = summary_resp.json().get("result", {})

        # 第四步：整合数据
        for pmid in pmids:
            s = summaries.get(pmid, {})
            title = s.get("title", "").strip()
            if not title:
                continue

            # 提取 DOI
            doi = ""
            for id_item in s.get("articleids", []):
                if id_item.get("idtype") == "doi":
                    doi = id_item.get("value", "")
                    break

            url = f"https://doi.org/{doi}" if doi else f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
            authors = [a.get("name", "") for a in s.get("authors", [])[:4]]

            papers.append(Paper(
                title=title,
                abstract=abstracts.get(pmid, "摘要暂时无法获取"),
                authors=authors,
                url=url,
                doi=doi,
                source="pubmed",
                published_date=s.get("pubdate", ""),
                venue=s.get("source", ""),
            ))

        time.sleep(1)

    except Exception as e:
        logger.error(f"  PubMed 采集失败: {e}")

    logger.info(f"  PubMed 共采集 {len(papers)} 篇（含摘要）")
    return papers


# ══════════════════════════════════════════════════════════════════
# 去重处理
# ══════════════════════════════════════════════════════════════════

def dedup(papers: list, cooldown_days: int) -> list:
    """
    三层去重：
    1. 数据库去重：近 cooldown_days 天内推送过的不再推荐
    2. DOI 去重：同一篇论文可能同时出现在 arXiv 和 PubMed
    3. 标题去重：没有 DOI 时用标题前50字符去重
    """
    seen_dois = set()
    seen_title_keys = set()
    result = []

    for p in papers:
        # 数据库去重
        if is_already_sent(p.paper_id, cooldown_days):
            continue
        # DOI 去重
        if p.doi and p.doi in seen_dois:
            continue
        # 标题去重
        title_key = p.title.lower().strip()[:50]
        if title_key in seen_title_keys:
            continue

        seen_dois.add(p.doi)
        seen_title_keys.add(title_key)
        result.append(p)

    logger.info(f"去重：{len(papers)} → {len(result)} 篇")
    return result


# ══════════════════════════════════════════════════════════════════
# AI 分析：粗筛（Doubao-lite，批量，便宜）
# ══════════════════════════════════════════════════════════════════

def coarse_filter(papers: list, context: str, model: str,
                  threshold: float = 0.65) -> list:
    """
    用 Doubao-lite 批量评分，过滤相关性低的论文。

    策略：每批 20 篇打包成一次 API 调用（节省 token），
    返回 0-1 的相关性分数，低于阈值的直接丢弃。

    为什么用 lite 不用 pro？
    - 粗筛只需判断"相不相关"，lite 够用
    - lite 比 pro 便宜约 16 倍
    - 节省下来的 token 用在精析上更有价值
    """
    if not papers:
        return []

    BATCH = 20
    scored = []

    for i in range(0, len(papers), BATCH):
        batch = papers[i: i + BATCH]

        # 把这批论文格式化成一个大 prompt
        items_text = "\n\n".join([
            f"[{j}] 标题: {p.title}\n摘要（前300字）: {p.abstract[:300]}"
            for j, p in enumerate(batch)
        ])

        prompt = f"""你是科研助手。请评估以下每篇论文与研究方向"{context}"的相关程度。

{items_text}

请严格只返回 JSON，格式如下（不要有任何其他文字）：
{{"scores": [{{"index": 0, "score": 0.85}}, {{"index": 1, "score": 0.2}}, ...]}}

评分标准：1.0=高度相关，0.5=有一定关联，0.0=完全无关"""

        try:
            resp = ARK.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
            )
            raw = resp.choices[0].message.content.strip()
            # 清理可能的 markdown 代码块包裹
            if "```" in raw:
                raw = raw.split("```")[1].lstrip("json").strip()
            result = json.loads(raw)

            for item in result.get("scores", []):
                idx, score = item.get("index", -1), item.get("score", 0)
                if 0 <= idx < len(batch):
                    batch[idx].relevance_score = float(score)
                    if float(score) >= threshold:
                        scored.append(batch[idx])

        except Exception as e:
            logger.warning(f"粗筛批次 {i//BATCH+1} 失败，保留全部: {e}")
            scored.extend(batch)  # 失败时保守处理：全部保留

        time.sleep(0.5)

    scored.sort(key=lambda p: p.relevance_score, reverse=True)
    logger.info(f"粗筛：{len(papers)} → {len(scored)} 篇（阈值={threshold}）")
    return scored


# ══════════════════════════════════════════════════════════════════
# AI 分析：深度精析（Doubao-pro，单篇，高质量）
# ══════════════════════════════════════════════════════════════════

DEEP_PROMPT = """请深度分析以下论文，严格按 JSON 返回，不要有任何其他内容：

标题：{title}
作者与单位：{authors}
来源期刊/会议：{venue}（{source}）
发表日期：{date}
摘要：{abstract}

知名机构参考列表（来自这些机构的论文适当加分）：
{institutions}

返回格式：
{{
  "summary_zh": "2-3句中文，概括这篇论文解决了什么问题、用了什么方法、取得了什么结果（≤150字）",
  "innovation": "核心创新点是什么，一句话（≤80字）",
  "method": "主要技术路线或方法，一句话",
  "institution_note": "作者单位简评：是否来自知名机构/实验室/公司，一句话（若无法判断则填"未知"）",
  "recommendation_reason": "为什么值得读，结合研究方向「{context}」和作者单位说明（≤100字）",
  "relevance_score": 0到1之间的浮点数，评分规则：相关性占70%权重 + 机构知名度占30%权重（顶校/顶级公司+0.1，普通机构不加减）
}}"""


def deep_analyze(paper: Paper, model: str, context: str) -> Paper:
    """
    用 Doubao-pro 对单篇论文做深度分析，生成结构化的中文摘要。
    只对粗筛后的精选论文调用，控制成本。
    """
    # 构建机构列表字符串，供 AI 判断作者来源
    inst_cfg = CFG.get("prestigious_institutions", {})
    all_insts = (
        inst_cfg.get("universities", []) +
        inst_cfg.get("companies", []) +
        inst_cfg.get("labs", [])
    )
    institutions_str = "、".join(all_insts[:30])  # 取前30个防止 token 过多

    prompt = DEEP_PROMPT.format(
        title=paper.title,
        authors=", ".join(paper.authors),
        venue=paper.venue,
        source=paper.source,
        date=paper.published_date,
        abstract=paper.abstract[:2500],
        context=context,
        institutions=institutions_str,
    )

    try:
        resp = ARK.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800,
        )
        raw = resp.choices[0].message.content.strip()
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()

        result = json.loads(raw)
        paper.summary_zh = result.get("summary_zh", "")
        paper.innovation = result.get("innovation", "")
        paper.recommendation_reason = result.get("recommendation_reason", "")

        # 提取顶会标签
        conf_tag = result.get("conference_tag", "").strip()
        if conf_tag:
            paper.conference_tag = f"✨ {conf_tag}"

        # institution_note 附加到推荐理由末尾（如果有意义的话）
        inst_note = result.get("institution_note", "")
        if inst_note and inst_note != "未知" and inst_note not in paper.recommendation_reason:
            paper.recommendation_reason = f"{paper.recommendation_reason}（{inst_note}）"
        if "relevance_score" in result:
            paper.relevance_score = max(paper.relevance_score,
                                        float(result["relevance_score"]))
    except Exception as e:
        logger.error(f"精析失败 [{paper.title[:40]}]: {e}")
        # 失败时降级处理：直接截取原摘要
        paper.summary_zh = paper.abstract[:200] + "..."
        paper.innovation = "（AI 分析暂时失败）"

    return paper


# ══════════════════════════════════════════════════════════════════
# 推送：飞书
# ══════════════════════════════════════════════════════════════════

def push_feishu(papers: list, topic: dict, date_str: str, dry_run: bool):
    """
    通过飞书自定义机器人 Webhook 发送消息卡片。
    每篇论文显示：标题、作者、来源、创新点、摘要、推荐理由、原文链接。
    """
    webhook = os.environ.get("FEISHU_WEBHOOK", "")
    if not webhook or not CFG["push"]["feishu"]["enabled"]:
        logger.info("飞书推送未启用，跳过")
        return

    # 根据主题 schedule 决定标题样式
    is_weekly = topic.get("schedule") == "weekly"
    header_color = "wathet" if not is_weekly else "green"
    title_text = f"{'📅 本周' if is_weekly else '📄 今日'}{topic['name']}论文 · {date_str}"

    # 构建每篇论文的飞书卡片 Block
    paper_blocks = []
    for i, p in enumerate(papers, 1):
        source_badge = {"arxiv": "arXiv", "pubmed": "PubMed",
                        "semantic_scholar": "S2"}.get(p.source, p.source)
        venue_info = f" · {p.venue}" if p.venue else ""

        paper_blocks.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**{i}. {p.title}**\n"
                    f"*{', '.join(p.authors[:3])}{'等' if len(p.authors) >= 3 else ''}*"
                    f"　`{source_badge}`{venue_info}"
                    + (f"　**{p.conference_tag}**" if p.conference_tag else "") +
                    f"\n\n"
                    f"💡 **创新点：** {p.innovation}\n\n"
                    f"📝 {p.summary_zh}\n\n"
                    f"⭐ **推荐理由：** {p.recommendation_reason}\n\n"
                    f"🔗 [查看原文]({p.url})"
                    + (f"　｜　DOI: `{p.doi}`" if p.doi else "")
                )
            }
        })
        if i < len(papers):
            paper_blocks.append({"tag": "hr"})

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title_text},
                "template": header_color,
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md",
                 "content": f"共 **{len(papers)}** 篇 · AI 自动筛选分析 · 回复 `/帮助` 查看可用命令"}},
                {"tag": "hr"},
                *paper_blocks,
            ],
        }
    }

    if dry_run:
        logger.info(f"[DRY RUN] 飞书消息已构建（{len(papers)} 篇），未实际发送")
        return

    try:
        resp = requests.post(webhook, json=card_payload, timeout=10)
        resp.raise_for_status()
        logger.success(f"飞书推送成功：{len(papers)} 篇论文")
    except Exception as e:
        logger.error(f"飞书推送失败: {e}")


# ══════════════════════════════════════════════════════════════════
# 推送：邮件（HTML 格式）
# ══════════════════════════════════════════════════════════════════

def push_email(papers: list, topic: dict, date_str: str, dry_run: bool):
    """
    通过 Resend 发送 HTML 格式邮件。
    每篇论文有独立的卡片样式，包含标题、摘要、推荐理由和跳转按钮。
    """
    if not CFG["push"]["email"]["enabled"]:
        logger.info("邮件推送未启用，跳过")
        return

    is_weekly = topic.get("schedule") == "weekly"
    accent = "#2563EB" if not is_weekly else "#059669"
    period = "本周" if is_weekly else "今日"

    # 生成论文卡片 HTML（循环拼接）
    cards = ""
    for i, p in enumerate(papers, 1):
        source_label = {"arxiv": "arXiv 预印本", "pubmed": "PubMed",
                        "semantic_scholar": "Semantic Scholar"}.get(p.source, p.source)
        cards += f"""
        <div style="background:#fff;border:1px solid #E2E8F0;border-radius:10px;
                    padding:22px;margin-bottom:18px;border-left:4px solid {accent};">
          <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:12px;">
            <span style="background:{accent};color:#fff;border-radius:50%;
                         min-width:26px;height:26px;display:flex;align-items:center;
                         justify-content:center;font-size:13px;font-weight:700;
                         flex-shrink:0;">{i}</span>
            <a href="{p.url}" style="font-size:16px;font-weight:700;color:#1E293B;
                                     text-decoration:none;line-height:1.4;">{p.title}</a>
          </div>
          <div style="color:#64748B;font-size:12px;margin-bottom:14px;">
            {", ".join(p.authors[:3])}{"等" if len(p.authors) >= 3 else ""}
            &nbsp;·&nbsp;
            <span style="background:{accent}18;color:{accent};padding:1px 7px;
                          border-radius:10px;font-size:11px;">{source_label}</span>
            {f'&nbsp;·&nbsp;<span style="color:#94A3B8;font-size:11px;">{p.venue}</span>' if p.venue else ""}
            {f'&nbsp;·&nbsp;<span style="background:#FEF3C7;color:#D97706;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;">{p.conference_tag}</span>' if p.conference_tag else ""}
          </div>
          <div style="margin-bottom:10px;">
            <span style="font-size:11px;font-weight:700;color:{accent};
                         text-transform:uppercase;letter-spacing:0.05em;">💡 核心创新</span>
            <p style="margin:5px 0 0;color:#374151;font-size:13px;line-height:1.6;">
              {p.innovation}
            </p>
          </div>
          <div style="margin-bottom:10px;">
            <span style="font-size:11px;font-weight:700;color:#6B7280;">📝 摘要</span>
            <p style="margin:5px 0 0;color:#374151;font-size:13px;line-height:1.7;">
              {p.summary_zh}
            </p>
          </div>
          <div style="margin-bottom:16px;">
            <span style="font-size:11px;font-weight:700;color:#D97706;">⭐ 推荐理由</span>
            <p style="margin:5px 0 0;color:#374151;font-size:13px;line-height:1.6;">
              {p.recommendation_reason}
            </p>
          </div>
          <a href="{p.url}" style="display:inline-block;background:{accent};color:#fff;
                                    padding:8px 18px;border-radius:6px;font-size:13px;
                                    text-decoration:none;font-weight:600;">
            查看原文 →
          </a>
          {f'<span style="margin-left:12px;color:#94A3B8;font-size:11px;">DOI: {p.doi}</span>' if p.doi else ""}
        </div>"""

    html = f"""<!DOCTYPE html><html lang="zh"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="background:#F8FAFC;font-family:-apple-system,BlinkMacSystemFont,
             'Segoe UI','PingFang SC',sans-serif;margin:0;padding:24px;">
  <div style="max-width:680px;margin:0 auto;">
    <div style="background:{accent};border-radius:12px 12px 0 0;padding:28px 32px;">
      <h1 style="margin:0;font-size:22px;color:#fff;font-weight:800;">
        {period}{topic['name']}论文精选
      </h1>
      <p style="margin:8px 0 0;opacity:0.85;font-size:14px;color:#fff;">
        {date_str} · 共 {len(papers)} 篇
      </p>
    </div>
    <div style="background:#EFF6FF;padding:14px 32px;border:1px solid #BFDBFE;
                border-top:none;font-size:12px;color:#3B82F6;">
      由 PaperBot 自动从 arXiv / PubMed / Semantic Scholar 筛选并分析 ·
      相关性评分 ≥ {topic.get('coarse_threshold', 0.65)}
    </div>
    <div style="padding:20px 0;">
      {cards}
    </div>
    <div style="text-align:center;color:#94A3B8;font-size:11px;padding:20px;">
      PaperBot 自动生成 · 如需调整推荐主题，请修改 config.yaml 后重新部署
    </div>
  </div>
</body></html>"""

    if dry_run:
        # 保存预览文件，方便本地查看效果
        preview_path = f"preview_{topic['id']}_{date_str}.html"
        with open(preview_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"[DRY RUN] 邮件预览已保存：{preview_path}（用浏览器打开查看）")
        return

    try:
        to_addr = CFG["push"]["email"]["to"]
        if isinstance(to_addr, str):
            to_addr = [to_addr]
        resend.Emails.send({
            "from": CFG["push"]["email"]["from"],
            "to": to_addr,
            "subject": f"[PaperBot] {period}{topic['name']}论文 · {date_str}（{len(papers)} 篇）",
            "html": html,
        })
        logger.success(f"邮件发送成功 → {', '.join(to_addr)}")
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")


# ══════════════════════════════════════════════════════════════════
# 判断主题今天是否应该运行
# ══════════════════════════════════════════════════════════════════

def should_run_today(topic: dict) -> bool:
    """
    根据 schedule 字段判断今天是否应该运行这个主题：
    - daily：每天运行
    - weekly：只在 config 指定的星期几运行
    """
    schedule = topic.get("schedule", "daily")
    if schedule == "daily":
        return True
    if schedule == "weekly":
        today_weekday = datetime.now().isoweekday()  # 1=周一 … 7=周日
        target_weekday = CFG["schedule"].get("weekly_weekday", 1)
        return today_weekday == target_weekday
    return False


# ══════════════════════════════════════════════════════════════════
# 单个主题的完整流水线
# ══════════════════════════════════════════════════════════════════

def run_topic(topic: dict, dry_run: bool = False):
    """
    对单个主题执行完整的采集→去重→筛选→分析→推送流程。

    流程：
    1. 根据 sources 配置分别采集 arXiv / Semantic Scholar / PubMed
    2. 合并去重
    3. 如果配置了 coarse_model，用 lite 模型粗筛（降低后续成本）
    4. 用 pro 模型精析 top N 篇
    5. 推送到飞书 + 邮件
    6. 写入数据库（dry_run 时跳过）
    """
    logger.info(f"\n{'='*55}")
    logger.info(f"▶ 主题：{topic['name']}  （{topic.get('schedule','daily')}）")
    logger.info(f"{'='*55}")

    today = datetime.now().strftime("%Y-%m-%d")
    cooldown = CFG.get("dedup_cooldown_days", 90)
    context = topic.get("context", topic["name"])
    sources = topic.get("sources", {})

    # ── 步骤1：采集 ──────────────────────────────────────────────
    papers = []
    lookback = sources.get("pubmed", {}).get("lookback_days", 7) \
               if topic.get("schedule") == "weekly" else 1

    arxiv_cfg = sources.get("arxiv", {})
    if arxiv_cfg.get("enabled"):
        papers += fetch_arxiv(
            arxiv_cfg.get("categories", []),
            arxiv_cfg.get("max_results", 100),
            lookback_days=lookback,
        )

    ss_cfg = sources.get("semantic_scholar", {})
    if ss_cfg.get("enabled"):
        papers += fetch_semantic_scholar(ss_cfg.get("venues", []))

    pubmed_cfg = sources.get("pubmed", {})
    if pubmed_cfg.get("enabled"):
        # PubMed 关键词：主题级 keywords 或 pubmed 子配置的 keywords
        kws = topic.get("keywords") or pubmed_cfg.get("keywords", [])
        papers += fetch_pubmed(
            pubmed_cfg.get("journals", []),
            kws,
            days_back=lookback,
        )

    if not papers:
        logger.warning(f"主题「{topic['name']}」未采集到任何论文，跳过")
        return

    # ── 步骤2：去重 ──────────────────────────────────────────────
    papers = dedup(papers, cooldown)

    # 关键词前置过滤：
    # - 有 coarse_model 的主题（AI综合）：收集所有子方向关键词做粗过滤，去掉完全不相关的
    # - 无 coarse_model 的主题（生物光学）：直接用 keywords 字段过滤
    subtopics = topic.get("subtopics", [])
    direct_kws = topic.get("keywords", [])

    if subtopics:
        # 把所有子方向关键词合并，做一次宽松过滤（命中任意一个子方向即保留）
        all_sub_kws = []
        for st in subtopics:
            all_sub_kws.extend(st.get("keywords", []))
        kw_lower = [k.lower() for k in all_sub_kws]
        before = len(papers)
        papers = [p for p in papers
                  if any(kw in p.title.lower() or kw in p.abstract.lower()
                         for kw in kw_lower)]
        logger.info(f"子方向关键词前置过滤：{before} → {len(papers)} 篇")
    elif direct_kws and not topic.get("coarse_model"):
        kw_lower = [k.lower() for k in direct_kws]
        before = len(papers)
        papers = [p for p in papers
                  if any(kw in p.title.lower() or kw in p.abstract.lower()
                         for kw in kw_lower)]
        logger.info(f"关键词过滤：{before} → {len(papers)} 篇")

    # ── 步骤3：粗筛（可选，有 coarse_model 才启用）──────────────
    if topic.get("coarse_model") and papers:
        papers = coarse_filter(
            papers,
            context=context,
            model=topic["coarse_model"],
            threshold=topic.get("coarse_threshold", 0.65),
        )

    if not papers:
        logger.warning(f"主题「{topic['name']}」过滤后无结果，跳过")
        return

    # ── 步骤4：精析 ──────────────────────────────────────────────
    max_analyze = topic.get("max_deep_analysis", 15)
    to_analyze = papers[:max_analyze]
    logger.info(f"开始精析 {len(to_analyze)} 篇...")

    analyzed = []
    for i, p in enumerate(to_analyze):
        logger.info(f"  精析 [{i+1}/{len(to_analyze)}] {p.title[:50]}...")
        analyzed.append(deep_analyze(p, topic["deep_model"], context))
        time.sleep(0.3)  # 避免 API 限流

    # ── 步骤5：排序，取 top_n ────────────────────────────────────
    analyzed.sort(key=lambda p: p.relevance_score, reverse=True)
    final = analyzed[:topic.get("top_n", 8)]
    logger.info(f"最终推荐：{len(final)} 篇")

    # ── 步骤6：推送 ──────────────────────────────────────────────
    push_feishu(final, topic, today, dry_run)
    push_email(final, topic, today, dry_run)

    # ── 步骤7：写数据库 ──────────────────────────────────────────
    if not dry_run:
        mark_as_sent(final, topic["id"])
    else:
        logger.info("[DRY RUN] 数据库写入已跳过")

    logger.success(f"✅ 主题「{topic['name']}」完成，推送 {len(final)} 篇")


# ══════════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="PaperBot - 每日论文推荐系统")
    parser.add_argument(
        "--topic", default=None,
        help="运行指定主题 ID（如 ai_general），或 'all' 强制运行所有主题"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="测试模式：执行全流程但不实际推送、不写数据库，会生成邮件预览 HTML"
    )
    args = parser.parse_args()

    # 配置日志：控制台 + 文件
    logger.remove()
    logger.add(sys.stdout,
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
    logger.add("pipeline.log", rotation="7 days", retention="30 days",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")

    if args.dry_run:
        logger.warning("⚠️  DRY RUN 模式：不会实际推送，数据库不写入")

    # 从 config.yaml 读取所有启用的主题
    all_topics = [t for t in CFG.get("topics", []) if t.get("enabled", True)]
    logger.info(f"共加载 {len(all_topics)} 个启用的主题")

    if args.topic == "all":
        # 强制运行所有主题
        topics_to_run = all_topics
    elif args.topic:
        # 运行指定主题
        topics_to_run = [t for t in all_topics if t["id"] == args.topic]
        if not topics_to_run:
            logger.error(f"找不到主题 ID：{args.topic}，可用 ID：{[t['id'] for t in all_topics]}")
            sys.exit(1)
    else:
        # 默认：根据 schedule 判断今天应该运行哪些
        topics_to_run = [t for t in all_topics if should_run_today(t)]
        logger.info(f"今天应运行 {len(topics_to_run)} 个主题："
                    f"{[t['name'] for t in topics_to_run]}")

    if not topics_to_run:
        logger.info("今天没有需要运行的主题（可能是非推送日），退出")
        return

    for topic in topics_to_run:
        try:
            run_topic(topic, dry_run=args.dry_run)
        except Exception as e:
            # 单个主题失败不影响其他主题
            logger.error(f"主题「{topic['name']}」发生未捕获异常: {e}")
            import traceback
            logger.debug(traceback.format_exc())

    logger.success("🎉 所有主题运行完毕")


if __name__ == "__main__":
    main()