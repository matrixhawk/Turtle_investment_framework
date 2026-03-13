#!/usr/bin/env python3
"""Generate all video chart assets for 龟龟投资策略 v1.0 video."""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

# ── Font config ──────────────────────────────────────────────────────────
plt.rcParams["font.sans-serif"] = ["Heiti SC", "Hiragino Sans GB", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)

# ── Color palette (professional, dark-bg friendly) ───────────────────────
BG_COLOR = "#1a1a2e"
CARD_COLOR = "#16213e"
ACCENT_BLUE = "#0f3460"
ACCENT_CYAN = "#00d2ff"
ACCENT_ORANGE = "#ff6b35"
ACCENT_GREEN = "#00c896"
ACCENT_RED = "#ff4757"
ACCENT_YELLOW = "#ffd93d"
ACCENT_PURPLE = "#a855f7"
TEXT_WHITE = "#e8e8e8"
TEXT_DIM = "#8899aa"
FUNNEL_COLORS = ["#3b82f6", "#2563eb", "#1d4ed8", "#1e40af",
                 "#1e3a8a", "#312e81", "#4c1d95", "#5b21b6", "#6d28d9"]


def save_fig(fig, name):
    path = os.path.join(ASSETS_DIR, name)
    fig.savefig(path, dpi=200, bbox_inches="tight", facecolor=fig.get_facecolor(),
                edgecolor="none", pad_inches=0.3)
    plt.close(fig)
    print(f"  -> {path}")


# ═══════════════════════════════════════════════════════════════════════════
# 1. Tier 1 Funnel Chart (漏斗图)
# ═══════════════════════════════════════════════════════════════════════════
def chart_01_tier1_funnel():
    print("Generating 01_tier1_funnel...")
    stages = [
        ("全市场 A 股", "~5,300", ""),
        ("剔除 ST / 退市", "~4,300", "- ST、*ST、退市整理"),
        ("剔除银行", "~4,250", "- 财务结构特殊"),
        ("上市 >= 3 年", "~3,900", "- 数据充分性"),
        ("市值 >= 5 亿", "~2,400", "- 流动性门槛"),
        ("换手率 >= 0.1%", "~2,300", "- 排除僵尸股"),
        ("PB <= 10", "~1,400", "- 排除极端泡沫"),
        ("PE 双通道分流", "", "主通道 ~700 | 观察通道 ~50"),
        ("综合评分排序", "200", "主 150 + 观察 50 → Tier 2"),
    ]

    fig, ax = plt.subplots(figsize=(14, 10), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(-1, 1)
    ax.set_ylim(-0.5, len(stages) + 0.5)
    ax.axis("off")

    # Title
    ax.text(0, len(stages) + 0.3, "Tier 1 粗筛漏斗", fontsize=24, fontweight="bold",
            color=TEXT_WHITE, ha="center", va="bottom")

    for i, (label, count, note) in enumerate(stages):
        y = len(stages) - 1 - i
        # Width narrows as we go down
        w = 0.95 - i * 0.065
        color = FUNNEL_COLORS[i] if i < len(FUNNEL_COLORS) else FUNNEL_COLORS[-1]

        # Draw trapezoid
        if i < len(stages) - 1:
            w_next = 0.95 - (i + 1) * 0.065
        else:
            w_next = w - 0.065

        verts = [(-w, y + 0.45), (w, y + 0.45), (w_next, y - 0.45), (-w_next, y - 0.45)]
        polygon = plt.Polygon(verts, facecolor=color, edgecolor="#ffffff22", linewidth=1)
        ax.add_patch(polygon)

        # Main label
        ax.text(0, y + 0.05, label, fontsize=13, fontweight="bold",
                color=TEXT_WHITE, ha="center", va="center")
        # Count on left
        if count:
            ax.text(-w - 0.05, y + 0.05, count, fontsize=11, color=ACCENT_CYAN,
                    ha="right", va="center", fontweight="bold")
        # Note on right
        if note:
            ax.text(w + 0.05, y + 0.05, note, fontsize=9, color=TEXT_DIM,
                    ha="left", va="center")

    # APIs badge
    ax.text(0, -0.4, "仅调用 2 个 API: stock_basic + daily_basic  |  耗时 ~5 秒",
            fontsize=11, color=ACCENT_ORANGE, ha="center", va="center",
            bbox=dict(boxstyle="round,pad=0.4", facecolor=CARD_COLOR, edgecolor=ACCENT_ORANGE,
                      linewidth=1.5))

    save_fig(fig, "01_tier1_funnel.png")


# ═══════════════════════════════════════════════════════════════════════════
# 2. Tier 2 Pipeline Flowchart (流水线流程图)
# ═══════════════════════════════════════════════════════════════════════════
def chart_02_tier2_pipeline():
    print("Generating 02_tier2_pipeline...")
    fig, ax = plt.subplots(figsize=(16, 9), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 9)
    ax.axis("off")

    ax.text(8, 8.5, "Tier 2 逐股深度分析流水线", fontsize=22, fontweight="bold",
            color=TEXT_WHITE, ha="center")
    ax.text(8, 8.0, "每只股票 ~5-10 秒  |  8 个 API 调用", fontsize=12,
            color=TEXT_DIM, ha="center")

    steps = [
        (2.0, 6.0, "硬否决\nHard Veto", ACCENT_RED, "1",
         ["质押率 > 70% → 淘汰", "审计非标准 → 淘汰"],
         ["pledge_stat", "fina_audit"]),
        (6.0, 6.0, "财务质量门\nQuality Gate", ACCENT_ORANGE, "2",
         ["ROE ≥ 8%", "毛利率 ≥ 15%", "负债率 ≤ 70%"],
         ["fina_indicator"]),
        (10.0, 6.0, "穿透回报率\nPenetration R", ACCENT_CYAN, "3",
         ["Owner Earnings", "分红比例 M", "R = AA×M / 市值"],
         ["income", "cashflow", "dividend"]),
        (14.0, 6.0, "估值指标\nValuation", ACCENT_PURPLE, "4",
         ["EV/EBITDA", "FCF 收益率", "现金调整 PE"],
         ["balancesheet"]),
        (4.0, 2.5, "地板价\n5-Method Floor", ACCENT_GREEN, "5",
         ["净流动资产", "BVPS", "10 年最低价", "股息隐含价", "悲观 FCF"],
         ["weekly"]),
        (10.0, 2.5, "五维综合评分\nComposite Score", ACCENT_YELLOW, "6",
         ["ROE 20%", "FCF 20%", "穿透R 25%", "EV/EBITDA 15%", "地板价 20%"],
         []),
    ]

    for x, y, title, color, num, details, apis in steps:
        # Box
        box_w, box_h = 3.2, 2.8
        rect = FancyBboxPatch((x - box_w/2, y - box_h/2), box_w, box_h,
                               boxstyle="round,pad=0.15", facecolor=CARD_COLOR,
                               edgecolor=color, linewidth=2)
        ax.add_patch(rect)

        # Step number badge
        circle = plt.Circle((x - box_w/2 + 0.3, y + box_h/2 - 0.3), 0.22,
                           facecolor=color, edgecolor="none")
        ax.add_patch(circle)
        ax.text(x - box_w/2 + 0.3, y + box_h/2 - 0.3, num, fontsize=10,
                color="white", ha="center", va="center", fontweight="bold")

        # Title
        ax.text(x, y + box_h/2 - 0.55, title, fontsize=11, fontweight="bold",
                color=color, ha="center", va="top", linespacing=1.3)

        # Details
        detail_y = y + box_h/2 - 1.3
        for d in details[:4]:
            ax.text(x, detail_y, f"  {d}", fontsize=8, color=TEXT_DIM,
                    ha="center", va="top")
            detail_y -= 0.28

        # API tags
        if apis:
            api_str = " | ".join(apis)
            ax.text(x, y - box_h/2 + 0.15, api_str, fontsize=7,
                    color=TEXT_DIM, ha="center", va="bottom",
                    style="italic")

    # Arrows between steps (top row)
    arrow_style = "Simple,tail_width=0.5,head_width=4,head_length=3"
    for (x1, x2) in [(3.6, 4.4), (7.6, 8.4), (11.6, 12.4)]:
        ax.annotate("", xy=(x2, 6.0), xytext=(x1, 6.0),
                    arrowprops=dict(arrowstyle="->", color=TEXT_DIM, lw=1.5))

    # Arrows from top row down to bottom row
    ax.annotate("", xy=(5.5, 3.9), xytext=(14.0, 4.6),
                arrowprops=dict(arrowstyle="->", color=TEXT_DIM, lw=1.5,
                               connectionstyle="arc3,rad=0.2"))
    # Arrow from floor price to composite
    ax.annotate("", xy=(8.4, 2.5), xytext=(5.6, 2.5),
                arrowprops=dict(arrowstyle="->", color=TEXT_DIM, lw=1.5))

    # Veto exit arrows
    ax.annotate("淘汰", xy=(2.0, 4.2), xytext=(2.0, 4.6),
                fontsize=9, color=ACCENT_RED, ha="center",
                arrowprops=dict(arrowstyle="->", color=ACCENT_RED, lw=1.5))

    save_fig(fig, "02_tier2_pipeline.png")


# ═══════════════════════════════════════════════════════════════════════════
# 3. Five-Dimension Scoring Radar Chart (5 维雷达图)
# ═══════════════════════════════════════════════════════════════════════════
def chart_03_scoring_radar():
    print("Generating 03_scoring_radar...")
    fig = plt.figure(figsize=(12, 6), facecolor=BG_COLOR)

    # Left: Pie chart for weights
    ax_pie = fig.add_subplot(121)
    ax_pie.set_facecolor(BG_COLOR)

    labels = ["ROE\n20%", "FCF 收益率\n20%", "穿透回报率\n25%", "EV/EBITDA\n15%", "地板价溢价\n20%"]
    sizes = [20, 20, 25, 15, 20]
    colors = [ACCENT_CYAN, ACCENT_GREEN, ACCENT_ORANGE, ACCENT_PURPLE, ACCENT_YELLOW]
    explode = (0, 0, 0.08, 0, 0)  # Highlight penetration return

    wedges, texts = ax_pie.pie(
        sizes, explode=explode, labels=labels, colors=colors,
        startangle=90,
        textprops={"color": TEXT_WHITE, "fontsize": 10},
        wedgeprops={"edgecolor": BG_COLOR, "linewidth": 2}
    )
    ax_pie.set_title("Tier 2 五维评分权重", fontsize=16, fontweight="bold",
                     color=TEXT_WHITE, pad=20)

    # Right: Radar chart (example stock)
    ax_radar = fig.add_subplot(122, polar=True, facecolor=BG_COLOR)

    categories = ["ROE\n质量", "FCF\n强度", "穿透\n回报率", "估值\n效率", "安全\n边际"]
    N = len(categories)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
    angles += angles[:1]

    # Example: a good stock
    values_good = [0.82, 0.75, 0.90, 0.68, 0.85]
    values_good += values_good[:1]

    # Example: a mediocre stock
    values_mid = [0.55, 0.40, 0.60, 0.45, 0.35]
    values_mid += values_mid[:1]

    ax_radar.plot(angles, values_good, "o-", color=ACCENT_CYAN, linewidth=2, label="优质标的")
    ax_radar.fill(angles, values_good, alpha=0.15, color=ACCENT_CYAN)
    ax_radar.plot(angles, values_mid, "o--", color=ACCENT_RED, linewidth=2, label="一般标的")
    ax_radar.fill(angles, values_mid, alpha=0.08, color=ACCENT_RED)

    ax_radar.set_xticks(angles[:-1])
    ax_radar.set_xticklabels(categories, fontsize=10, color=TEXT_WHITE)
    ax_radar.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax_radar.set_yticklabels(["20", "40", "60", "80", "100"],
                             fontsize=8, color=TEXT_DIM)
    ax_radar.set_ylim(0, 1.0)
    ax_radar.set_rlabel_position(30)
    ax_radar.spines["polar"].set_color(TEXT_DIM)
    ax_radar.grid(color=TEXT_DIM, alpha=0.3)
    ax_radar.tick_params(colors=TEXT_DIM)

    ax_radar.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1),
                    fontsize=10, facecolor=CARD_COLOR, edgecolor=TEXT_DIM,
                    labelcolor=TEXT_WHITE)
    ax_radar.set_title("百分位排名对比", fontsize=16, fontweight="bold",
                       color=TEXT_WHITE, pad=30)

    fig.tight_layout(pad=3)
    save_fig(fig, "03_scoring_radar.png")


# ═══════════════════════════════════════════════════════════════════════════
# 4. Three Challenges Diagram (三个挑战示意图)
# ═══════════════════════════════════════════════════════════════════════════
def chart_04_three_challenges():
    print("Generating 04_three_challenges...")
    fig, ax = plt.subplots(figsize=(16, 7), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 7)
    ax.axis("off")

    ax.text(8, 6.5, "为什么不能直接让 LLM 分析股票？", fontsize=22,
            fontweight="bold", color=TEXT_WHITE, ha="center")

    challenges = [
        (2.8, 3.3, "数据幻觉", ACCENT_RED, "LLM 虚构数据",
         ["毛利率 35% vs 实际 32%", "3% 差异 = 致命误判", "财务数据必须来自 API"],
         "HALLUCINATION"),
        (8.0, 3.3, "上下文溢出", ACCENT_ORANGE, "信息超出窗口",
         ["5 年财报 + 年报附注", "= 10 万+ tokens", "分析到一半丢信息"],
         "CONTEXT OVERFLOW"),
        (13.2, 3.3, "不可复现", ACCENT_PURPLE, "结果随机波动",
         ["相同 prompt 不同结果", "ROE 计算不应有随机性", "确定性计算需确定性工具"],
         "NON-REPRODUCIBLE"),
    ]

    for x, y, title, color, subtitle, bullets, en_label in challenges:
        # Card
        w, h = 4.0, 4.5
        rect = FancyBboxPatch((x - w/2, y - h/2), w, h,
                               boxstyle="round,pad=0.2", facecolor=CARD_COLOR,
                               edgecolor=color, linewidth=2.5)
        ax.add_patch(rect)

        # Warning icon (triangle)
        tri_y = y + h/2 - 0.6
        triangle = plt.Polygon([(x - 0.3, tri_y - 0.3), (x + 0.3, tri_y - 0.3),
                                (x, tri_y + 0.25)],
                               facecolor=color, edgecolor="none")
        ax.add_patch(triangle)
        ax.text(x, tri_y - 0.08, "!", fontsize=14, color="white",
                ha="center", va="center", fontweight="bold")

        # Title
        ax.text(x, y + h/2 - 1.1, title, fontsize=16, fontweight="bold",
                color=color, ha="center")
        ax.text(x, y + h/2 - 1.55, subtitle, fontsize=10, color=TEXT_DIM, ha="center")

        # Bullets
        by = y + h/2 - 2.1
        for b in bullets:
            ax.text(x, by, f"  {b}", fontsize=10, color=TEXT_WHITE, ha="center")
            by -= 0.45

        # English label
        ax.text(x, y - h/2 + 0.25, en_label, fontsize=8, color=TEXT_DIM,
                ha="center", style="italic")

    # Solution bar at bottom
    ax.text(8, 0.5, "解决方案: 混合架构 — Python 算确定性的, LLM 判断定性的",
            fontsize=14, fontweight="bold", color=ACCENT_GREEN, ha="center",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=CARD_COLOR,
                      edgecolor=ACCENT_GREEN, linewidth=2))

    save_fig(fig, "04_three_challenges.png")


# ═══════════════════════════════════════════════════════════════════════════
# 5. Six-Phase Pipeline Flowchart (六阶段流水线)
# ═══════════════════════════════════════════════════════════════════════════
def chart_05_six_phases():
    print("Generating 05_six_phases...")
    fig, ax = plt.subplots(figsize=(18, 10), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 10)
    ax.axis("off")

    ax.text(9, 9.5, "六阶段分析流水线", fontsize=24, fontweight="bold",
            color=TEXT_WHITE, ha="center")
    ax.text(9, 9.0, "coordinator.md 总调度", fontsize=12,
            color=TEXT_DIM, ha="center")

    # Phase boxes: (x, y, label, sublabel, color, engine_type)
    phases = [
        # Phase 0 - top left
        (2.5, 7.0, "Phase 0", "PDF 下载", TEXT_DIM, "条件触发",
         "snowball-report-\ndownloader"),

        # Phase 1A - Python
        (6.5, 7.0, "Phase 1A", "Tushare 数据采集", ACCENT_CYAN, "Python",
         "15 个 API\n→ data_pack_market.md"),

        # Phase 2A - Python
        (6.5, 4.0, "Phase 2A", "PDF 预处理", ACCENT_CYAN, "Python",
         "关键词匹配提取 7 段\n→ pdf_sections.json"),

        # Phase 1B - LLM
        (11.5, 7.0, "Phase 1B", "WebSearch 补充", ACCENT_ORANGE, "LLM",
         "管理层、竞争格局\n行业动态"),

        # Phase 2B - LLM
        (11.5, 4.0, "Phase 2B", "PDF 精析", ACCENT_ORANGE, "LLM",
         "粗文本 → 结构化表格"),

        # Phase 3 - LLM
        (15.5, 5.5, "Phase 3", "四因子分析", ACCENT_GREEN, "LLM",
         "因子 1→2→3→4\n→ 分析报告.md"),
    ]

    for x, y, phase, subtitle, color, engine, detail in phases:
        w, h = 3.5, 2.2
        rect = FancyBboxPatch((x - w/2, y - h/2), w, h,
                               boxstyle="round,pad=0.15", facecolor=CARD_COLOR,
                               edgecolor=color, linewidth=2)
        ax.add_patch(rect)

        # Engine badge
        badge_color = ACCENT_CYAN if engine == "Python" else (ACCENT_ORANGE if engine == "LLM" else TEXT_DIM)
        ax.text(x + w/2 - 0.15, y + h/2 - 0.15, engine, fontsize=8,
                color="white", ha="right", va="top",
                bbox=dict(boxstyle="round,pad=0.15", facecolor=badge_color,
                          edgecolor="none"))

        # Phase label
        ax.text(x, y + 0.45, phase, fontsize=14, fontweight="bold",
                color=color, ha="center")
        ax.text(x, y + 0.05, subtitle, fontsize=11, color=TEXT_WHITE, ha="center")
        ax.text(x, y - 0.55, detail, fontsize=8, color=TEXT_DIM, ha="center",
                linespacing=1.4)

    # Arrows: Phase 0 → 1A
    ax.annotate("", xy=(4.75, 7.0), xytext=(4.25, 7.0),
                arrowprops=dict(arrowstyle="->", color=TEXT_DIM, lw=1.5))

    # Phase 1A → 1B
    ax.annotate("", xy=(9.75, 7.0), xytext=(8.25, 7.0),
                arrowprops=dict(arrowstyle="->", color=ACCENT_CYAN, lw=2))

    # Phase 2A → 2B
    ax.annotate("", xy=(9.75, 4.0), xytext=(8.25, 4.0),
                arrowprops=dict(arrowstyle="->", color=ACCENT_CYAN, lw=2))

    # Phase 0 → 2A (down)
    ax.annotate("", xy=(4.75, 4.0), xytext=(2.5, 5.9),
                arrowprops=dict(arrowstyle="->", color=TEXT_DIM, lw=1.5,
                               connectionstyle="arc3,rad=-0.3"))

    # 1A ↔ 2A parallel indicator
    ax.annotate("", xy=(6.5, 5.9), xytext=(6.5, 5.1),
                arrowprops=dict(arrowstyle="<->", color=ACCENT_YELLOW, lw=1.5,
                               linestyle="dashed"))
    ax.text(7.3, 5.5, "可并行", fontsize=9, color=ACCENT_YELLOW, ha="left")

    # Phase 1B → 3
    ax.annotate("", xy=(13.75, 6.2), xytext=(13.25, 7.0),
                arrowprops=dict(arrowstyle="->", color=ACCENT_ORANGE, lw=2,
                               connectionstyle="arc3,rad=0.2"))

    # Phase 2B → 3
    ax.annotate("", xy=(13.75, 4.8), xytext=(13.25, 4.0),
                arrowprops=dict(arrowstyle="->", color=ACCENT_ORANGE, lw=2,
                               connectionstyle="arc3,rad=-0.2"))

    # Legend
    legend_items = [
        (ACCENT_CYAN, "Python 脚本 (确定性计算)"),
        (ACCENT_ORANGE, "LLM Agent (定性判断)"),
        (TEXT_DIM, "条件触发"),
    ]
    for i, (c, label) in enumerate(legend_items):
        lx = 1.5
        ly = 2.0 - i * 0.5
        ax.add_patch(FancyBboxPatch((lx - 0.15, ly - 0.12), 0.3, 0.24,
                                     boxstyle="round,pad=0.05", facecolor=c,
                                     edgecolor="none"))
        ax.text(lx + 0.35, ly, label, fontsize=10, color=TEXT_WHITE, va="center")

    # Input/Output labels
    ax.text(0.8, 7.0, "输入:\n股票代码", fontsize=10, color=TEXT_DIM, ha="center",
            linespacing=1.4)
    ax.annotate("", xy=(0.75 + 0.6, 7.0), xytext=(0.75 + 0.1, 7.0),
                arrowprops=dict(arrowstyle="->", color=TEXT_DIM, lw=1))

    ax.text(17.3, 5.5, "输出:\n分析报告", fontsize=10, color=ACCENT_GREEN, ha="center",
            linespacing=1.4, fontweight="bold")

    save_fig(fig, "05_six_phases.png")


# ═══════════════════════════════════════════════════════════════════════════
# 6. Four-Factor Progression Diagram (四因子递进关系图)
# ═══════════════════════════════════════════════════════════════════════════
def chart_06_four_factors():
    print("Generating 06_four_factors...")
    fig, ax = plt.subplots(figsize=(18, 9), facecolor=BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 18)
    ax.set_ylim(0, 9)
    ax.axis("off")

    ax.text(9, 8.5, "四因子递进分析体系", fontsize=24, fontweight="bold",
            color=TEXT_WHITE, ha="center")
    ax.text(9, 8.0, "闯关制: 前一关不通过 → 后面不用看", fontsize=13,
            color=ACCENT_RED, ha="center")

    factors = [
        (2.5, 4.5, "因子 1", "资产质量与\n商业模式", ACCENT_CYAN,
         ["5 分钟快筛 (6 项否决)", "审计意见 | 造假历史", "商业模式 | 经济周期",
          "─ ─ ─ ─ ─ ─ ─ ─ ─",
          "深度定性 (9 模块)", "资本密集度 | 现金周期",
          "竞争护城河 | 管理层"],
         "LLM 定性判断",
         "通过 → 进入因子 2"),

        (7.0, 4.5, "因子 2", "穿透回报率\n粗算", ACCENT_GREEN,
         ["Top-Down 快速估算", "Owner Earnings (OE)",
          "= 净利润 + 折旧 - 维持 Capex",
          "R = OE × M / 市值",
          "─ ─ ─ ─ ─ ─ ─ ─ ─",
          "门槛 II = max(Rf+3%, 5%)",
          "R < II/2 → 直接否决"],
         "Python + LLM",
         "R ≥ II/2 → 因子 3"),

        (11.5, 4.5, "因子 3", "穿透回报率\n精算", ACCENT_ORANGE,
         ["Bottom-Up 逐年追踪", "从现金流量表出发",
          "11 步精算流程",
          "年报附注交叉验证",
          "─ ─ ─ ─ ─ ─ ─ ─ ─",
          "GG = 2 年均值基准",
          "核心指标: 真实可支配现金"],
         "Python 计算 + LLM 调整",
         "完成 → 因子 4"),

        (16.0, 4.5, "因子 4", "估值与\n安全边际", ACCENT_YELLOW,
         ["安全边际 = GG - II", "正 → 有超额回报",
          "5 项价值陷阱排查:",
          "现金流恶化?", "护城河收窄?",
          "─ ─ ─ ─ ─ ─ ─ ─ ─",
          "目标买入价 + 历史分位"],
         "Python + LLM",
         ""),
    ]

    for x, y, label, title, color, bullets, engine, outcome in factors:
        w, h = 3.8, 6.5
        rect = FancyBboxPatch((x - w/2, y - h/2), w, h,
                               boxstyle="round,pad=0.2", facecolor=CARD_COLOR,
                               edgecolor=color, linewidth=2.5)
        ax.add_patch(rect)

        # Factor label badge
        ax.text(x, y + h/2 - 0.35, label, fontsize=13, fontweight="bold",
                color="white", ha="center",
                bbox=dict(boxstyle="round,pad=0.2", facecolor=color, edgecolor="none"))

        # Title
        ax.text(x, y + h/2 - 1.1, title, fontsize=14, fontweight="bold",
                color=color, ha="center", linespacing=1.3)

        # Engine badge
        ax.text(x, y + h/2 - 2.0, engine, fontsize=8, color=TEXT_DIM,
                ha="center", style="italic")

        # Bullets
        by = y + h/2 - 2.5
        for b in bullets:
            if b.startswith("─"):
                ax.plot([x - w/2 + 0.3, x + w/2 - 0.3], [by, by],
                       color=TEXT_DIM, alpha=0.3, linewidth=0.5)
                by -= 0.3
            else:
                ax.text(x, by, b, fontsize=9, color=TEXT_WHITE, ha="center")
                by -= 0.38

        # Outcome arrow text
        if outcome:
            ax.text(x + w/2 + 0.25, y + 0.3, outcome, fontsize=8,
                    color=ACCENT_GREEN, ha="left", rotation=0)

    # Progressive arrows between factors
    for i in range(3):
        x1 = [2.5, 7.0, 11.5][i] + 1.9
        x2 = [7.0, 11.5, 16.0][i] - 1.9
        color = ACCENT_GREEN
        ax.annotate("", xy=(x2, 4.5), xytext=(x1, 4.5),
                    arrowprops=dict(arrowstyle="-|>", color=color, lw=2.5,
                                   mutation_scale=15))

    # Fail exit arrows (downward)
    for i, x in enumerate([2.5, 7.0]):
        ax.annotate("", xy=(x, 1.0), xytext=(x, 1.25),
                    arrowprops=dict(arrowstyle="-|>", color=ACCENT_RED, lw=2))
        ax.text(x, 0.7, "排除", fontsize=11, color=ACCENT_RED,
                ha="center", fontweight="bold")

    # Final outcome
    ax.text(16.0, 0.7, "结论", fontsize=13, fontweight="bold",
            color=TEXT_WHITE, ha="center")

    outcomes = [
        (14.8, 0.25, "买入", ACCENT_GREEN),
        (16.0, 0.25, "观察", ACCENT_YELLOW),
        (17.2, 0.25, "排除", ACCENT_RED),
    ]
    for ox, oy, olabel, ocolor in outcomes:
        ax.text(ox, oy, olabel, fontsize=12, color=ocolor, ha="center",
                fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.25", facecolor=CARD_COLOR,
                          edgecolor=ocolor, linewidth=1.5))

    save_fig(fig, "06_four_factors.png")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Generating video assets for 龟龟投资策略 v1.0...\n")
    chart_01_tier1_funnel()
    chart_02_tier2_pipeline()
    chart_03_scoring_radar()
    chart_04_three_challenges()
    chart_05_six_phases()
    chart_06_four_factors()
    print(f"\nDone! All assets saved to {ASSETS_DIR}/")
