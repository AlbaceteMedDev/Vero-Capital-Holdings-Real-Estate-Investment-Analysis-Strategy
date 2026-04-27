#!/usr/bin/env python3
"""Generate a self-contained static HTML dashboard for Vero Capital Holdings."""

import json
import os
import glob
import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def safe_json(obj):
    """Convert to JSON-safe values, handling NaN/None."""
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def df_to_records(df):
    """Convert DataFrame to list of dicts with JSON-safe values."""
    records = df.to_dict(orient="records")
    cleaned = []
    for row in records:
        cleaned.append({k: safe_json(v) for k, v in row.items()})
    return cleaned


def main():
    # ── Load data ──────────────────────────────────────────────────────
    scored = pd.read_parquet(os.path.join(ROOT, "data/processed/scored_markets.parquet"))
    scored = scored.sort_values("market_rank")

    strategies = pd.read_csv(os.path.join(ROOT, "outputs/strategies/strategy_comparison.csv"))
    capital = pd.read_csv(os.path.join(ROOT, "outputs/strategies/capital_sensitivity.csv"))
    corr = pd.read_csv(os.path.join(ROOT, "outputs/strategies/price_correlation_matrix.csv"), index_col=0)

    memo_files = sorted(glob.glob(os.path.join(ROOT, "outputs/memos/*.md")))
    memo_text = ""
    if memo_files:
        with open(memo_files[-1], "r") as f:
            memo_text = f.read()

    # ── Prepare JSON payloads ──────────────────────────────────────────
    markets_json = json.dumps(df_to_records(scored), default=safe_json)
    strategies_json = json.dumps(df_to_records(strategies), default=safe_json)
    capital_json = json.dumps(df_to_records(capital), default=safe_json)

    corr_labels = list(corr.columns)
    corr_values = corr.values.tolist()
    corr_json = json.dumps({"labels": corr_labels, "values": corr_values}, default=safe_json)

    memo_escaped = json.dumps(memo_text)

    # ── Investment narratives per market ──────────────────────────────
    narratives = {
        "Abilene, TX Metro Area": "Abilene is anchored by Dyess Air Force Base (7th Bomb Wing, B-1B Lancers), which injects $1B+ annually into the local economy and provides a guaranteed tenant pipeline of military families. Three universities (Abilene Christian, Hardin-Simmons, McMurry) add 10,000+ students and steady rental demand. Texas has the most landlord-friendly laws in the country — 21-day evictions, no rent control, no security deposit limits. Rents have surged 33% YoY as new construction hasn't kept pace with base expansion. At $204K median price, you're buying into a recession-resistant market with government-backed demand.",
        "Decatur, IL Metro Area": "Decatur is the ADM (Archer Daniels Midland) company town — the global agricultural processing giant headquartered here. It's also home to Caterpillar and Tate & Lyle production facilities, creating a blue-collar workforce that rents. At $119K entry price, this is the cheapest viable market in the US with the highest cap rate (5.9%). Prices are appreciating 8.3% YoY as remote workers from Chicago (3 hours away) discover they can buy a house for less than a car. The risk: Illinois is a tenant-friendly state. The reward: you're buying at the absolute floor of US housing costs with institutional employers guaranteeing jobs.",
        "Lawton, OK Metro Area": "Lawton exists because of Fort Sill — one of the Army's most critical posts, home to the Fires Center of Excellence and basic combat training. The base employs 25,000+ military and civilian workers and generates $3.5B in economic impact. Military housing allowances (BAH) effectively set a rent floor — soldiers receive $1,100-1,400/mo for off-base housing. Oklahoma landlord laws are fast (16-day eviction) and fully preempted on rent control. Lawton has the highest migration score in our dataset (100/100) because every Army training cycle brings new tenants. You're not betting on the local economy — you're betting on the Department of Defense budget.",
        "Beaumont-Port Arthur, TX Metro Area": "Beaumont-Port Arthur sits on the Golden Triangle — one of America's largest petrochemical refining corridors. ExxonMobil, Total, Valero, and Motiva operate massive refineries here employing 50,000+ workers with $80K+ average salaries. These are blue-collar millionaires who rent because refinery jobs are contract-based and workers rotate between facilities. The $178K median price is a fraction of what you'd pay near similar industrial hubs in Houston or the Gulf Coast. Texas landlord laws give you maximum protection. The risk is oil price dependency, but these refineries process fuel regardless of crude prices — they run in all market conditions.",
        "Wichita Falls, TX Metro Area": "Wichita Falls is a dual-anchor military market: Sheppard Air Force Base (NATO's largest pilot training program, 80+ countries) and the 82nd Civil Affairs Battalion. Sheppard trains 60,000 students annually, creating constant turnover and rental demand. The city has invested $200M+ in downtown revitalization and Midwestern State University (6,000 students) adds another tenant base. At $170K entry with $1,258 rents, the numbers work in Texas's 9/10 landlord-friendly environment. Rents are growing 5.1% YoY while the military keeps vacancy near zero.",
        "Lumberton, NC Micro Area": "Lumberton sits at the intersection of I-95 and I-74 — the main corridor from New York to Florida. It's a logistics and distribution hub benefiting from the reshoring boom. Amazon, FedEx, and regional distributors are expanding warehouse operations along the I-95 corridor, bringing thousands of jobs to workers who can't afford Raleigh or Fayetteville prices (both within 1.5 hours). At $128K, it's one of the cheapest entry points on the East Coast. North Carolina gives landlords an 8/10 score with preempted rent control and 21-day evictions. Rent growth is 8.2% YoY as the logistics workforce expands.",
        "Bay City, MI Metro Area": "Bay City is a Rust Belt comeback story. The Saginaw Bay region is seeing reinvestment from Michigan's EV manufacturing push — GM's Ultium battery plants and Hemlock Semiconductor (the world's largest polysilicon producer) are within commuting distance. Dow Chemical's massive Midland headquarters is 15 minutes away. At $176K median price with 6.7% annual appreciation, you're buying into Michigan's industrial renaissance at pre-boom prices. Rent control is preempted statewide. The risk: Michigan winters thin the tenant pool. The reward: you're positioned before the EV supply chain investment wave hits housing.",
        "Jackson, MS Metro Area": "Jackson is Mississippi's capital and largest metro (616K people) — it's the state government, healthcare, and education hub. University of Mississippi Medical Center (the state's only Level 1 trauma center), Jackson State University, and dozens of state agencies provide recession-proof employment. Mississippi has the fastest eviction process in the country (14 days) and rents are preempted from local control. At $208K entry, you're buying the state capital at half the price of comparable Sun Belt metros. The risk is population stagnation, but the institutional employment base means rental demand persists regardless.",
        "Brownsville-Harlingen, TX Metro Area": "Brownsville is ground zero for Elon Musk's SpaceX Starship program. Boca Chica is 20 minutes from downtown — SpaceX is building the world's most advanced launch facility and has attracted 3,000+ aerospace engineers and contractors who need housing NOW. The University of Texas Rio Grande Valley (30,000 students) and the Port of Brownsville (largest LNG export facility in the US) add demand layers. At $203K you're buying before the full SpaceX build-out completes. Texas landlord laws protect you fully. This is the 'Tesla Gigafactory effect' — when a transformative employer arrives in a low-cost market, early investors capture outsized appreciation.",
        "Goldsboro, NC Metro Area": "Goldsboro is home to Seymour Johnson Air Force Base — the 4th Fighter Wing (F-15E Strike Eagles) and the 916th Air Refueling Wing. The base employs 10,000+ and drives most of the local economy. Military BAH rates set a rent floor around $1,200-1,400/mo. North Carolina's landlord-friendly laws (8/10) and preempted rent control make operations simple. At $181K entry, you're buying military-backed rental demand with government-guaranteed tenant income in a state that protects property owners.",
        "Tuscaloosa, AL Metro Area": "Tuscaloosa is a university powerhouse — the University of Alabama (38,000 students) drives the entire rental market. Student housing demand is insatiable and recession-proof: enrollment grows regardless of economic conditions. Mercedes-Benz's massive assembly plant (4,000+ workers) adds blue-collar rental demand. Alabama scores 8/10 on landlord friendliness with 14-day evictions. At $217K entry, you're buying into a market where demand (students + autoworkers) structurally exceeds supply every August. The risk: summer vacancy between semesters. The reward: 9 months of near-guaranteed occupancy at premium student rents.",
        "Paducah, KY-IL Metro Area": "Paducah sits at the confluence of the Ohio and Tennessee Rivers — it's a logistics and energy hub. The Paducah Gaseous Diffusion Plant (uranium enrichment) and multiple river terminal operations provide stable, high-wage employment. Murray State University (20 minutes south) adds student rental demand. Kentucky has preempted rent control and offers reasonable 25-day evictions. At $170K entry with 9.2% rent growth, Paducah is an overlooked Midwest value play where infrastructure and energy sector jobs create durable demand.",
        "Houma-Bayou Cane-Thibodaux, LA Metro Area": "Houma is the service hub for Gulf of Mexico offshore oil and gas operations. When rigs need crew changes, equipment, or repairs, Houma is where it happens. Companies like Schlumberger, Halliburton, and Edison Chouest have major facilities here. The offshore workforce works 14-on/14-off rotations but needs local housing year-round. At $175K entry, you're buying into energy-sector rental demand with Louisiana's landlord-friendly laws (7/10, 21-day eviction). The risk: hurricane exposure. The reward: oil service workers earning $100K+ who need a place to call home between hitches.",
        "Shreveport-Bossier City, LA Metro Area": "Shreveport-Bossier is a dual-anchor market: Barksdale Air Force Base (B-52 Stratofortress wing, Global Strike Command) and a growing casino/entertainment economy. Barksdale's 8,000+ personnel create guaranteed rental demand with BAH-backed rents. The Cyber Innovation Center, adjacent to the base, is building a tech corridor that's attracting defense contractors. At $174K entry with Louisiana's fast eviction process, you're buying military-backed demand. The challenge: Shreveport's population has been flat. The advantage: the tenants who ARE here are government-funded.",
        "Pottsville, PA Micro Area": "Pottsville is Schuylkill County's seat — a small Appalachian market that's quietly benefiting from Philadelphia and Allentown spillover. Workers priced out of the Lehigh Valley ($350K+ homes) are commuting from Pottsville ($157K homes) on I-78. D.G. Yuengling & Son (America's oldest brewery) is the anchor employer. Healthcare and distribution center jobs are growing. At $157K entry with 4.2% rent growth, Pottsville is a commuter arbitrage play — you're buying housing that costs half of what the same worker's alternative costs 60 miles east.",
        "Beckley, WV Metro Area": "Beckley is the gateway to New River Gorge — America's newest National Park (designated 2020). Adventure tourism is exploding: rock climbing, whitewater rafting, zip-lining, and mountain biking draw visitors year-round. This is creating a short-term rental and hospitality worker housing market that didn't exist 5 years ago. At $142K, it's the cheapest entry in our dataset. Rent growth is 7.6% YoY as the tourism economy builds. The play: buy at rock-bottom prices before the National Park effect fully reprices the market, the way Gatlinburg TN repriced after Smoky Mountains tourism boomed.",
        "Charleston, WV Metro Area": "Charleston is West Virginia's capital and largest city — state government, healthcare (CAMC hospital system), and chemical manufacturing (Dow, DuPont legacy) provide the employment base. As the state capital, government jobs are recession-proof. At $142K entry with $1,031 rents and 6.5% rent growth, the cash flow math is approaching breakeven. The real thesis: West Virginia passed the most aggressive tax reform in the country, eliminating personal income tax to attract remote workers. If that migration materializes, Charleston — as the most livable WV city — captures the upside.",
        "Danville, VA Micro Area": "Danville was a dying tobacco town until the casino arrived. Caesars Virginia (opened 2024) is a $650M resort casino that created 2,000+ permanent jobs overnight in a city of 40,000. That's a 5% employment shock. The workforce needs housing NOW and Danville's $160K entry point means landlords can buy 2-3 properties for the price of one in Richmond (1.5 hours north). Virginia's landlord laws are moderate (7/10). Rent growth is 8.2% YoY — the fastest in our dataset — as casino workers flood a market with zero new construction. This is the classic 'major employer arrives in a small town' play.",
    }
    narratives_json = json.dumps(narratives)

    # ── Build HTML ─────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Vero Capital">
<meta name="theme-color" content="#1d1d1f">
<title>Vero Capital Holdings - Real Estate Investment Dashboard</title>
<link rel="icon" type="image/png" href="vero-capital-icon-light.png">
<link rel="apple-touch-icon" href="vero-stacked-dark.png">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
:root{{
  --bg:#1d1d1f;--card:#2a2a2c;--border:#3a3a3c;
  --primary:#C9A96E;--accent:#C9A96E;
  --text:#f5f5f7;--muted:#a1a1a6;
  --sidebar-w:240px;
}}
body{{
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:var(--bg);color:var(--text);display:flex;min-height:100vh;
  padding:env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);
  -webkit-text-size-adjust:100%;
}}
a{{color:var(--accent)}}

/* Sidebar */
.sidebar{{
  width:var(--sidebar-w);min-height:100vh;background:var(--card);
  border-right:1px solid var(--border);position:fixed;top:0;left:0;
  display:flex;flex-direction:column;z-index:100;
}}
.sidebar .brand{{padding:24px 20px;border-bottom:1px solid var(--border);text-align:center}}
.sidebar .brand img{{max-width:150px;height:auto;margin:0 auto;display:block}}
.sidebar nav{{padding:12px 0;flex:1}}
.sidebar nav button{{
  display:block;width:100%;text-align:left;padding:12px 20px;
  background:none;border:none;color:var(--muted);font-size:13px;
  cursor:pointer;transition:all .15s;border-left:3px solid transparent;
}}
.sidebar nav button:hover{{color:var(--text);background:rgba(201,169,110,.08)}}
.sidebar nav button.active{{color:var(--primary);border-left-color:var(--primary);background:rgba(201,169,110,.1)}}

/* Main */
.main{{margin-left:var(--sidebar-w);flex:1;padding:32px;max-width:1400px}}
.tab{{display:none}}.tab.active{{display:block}}

/* Cards */
.cards{{display:grid;gap:16px;margin-bottom:24px}}
.cards-4{{grid-template-columns:repeat(4,1fr)}}
.cards-3{{grid-template-columns:repeat(3,1fr)}}
.cards-2{{grid-template-columns:repeat(2,1fr)}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:20px}}
.card .label{{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}}
.card .value{{font-size:28px;font-weight:700;margin-top:6px;color:var(--text)}}
.card .sub{{font-size:12px;color:var(--muted);margin-top:4px}}
.card.strategy-card .value{{font-size:20px}}
.card.strategy-card{{border-top:3px solid var(--primary)}}

/* Table */
.table-wrap{{overflow-x:auto;margin-bottom:24px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:var(--card);position:sticky;top:0;cursor:pointer;padding:10px 12px;
    text-align:left;color:var(--muted);font-weight:600;border-bottom:2px solid var(--border);
    user-select:none;white-space:nowrap}}
th:hover{{color:var(--primary)}}
th .sort-arrow{{margin-left:4px;font-size:10px}}
td{{padding:10px 12px;border-bottom:1px solid var(--border);white-space:nowrap}}
tr:hover td{{background:rgba(201,169,110,.05)}}

/* Chart containers */
.chart-box{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:24px}}
.chart-row{{display:grid;gap:16px;margin-bottom:24px}}
.chart-row-2{{grid-template-columns:1fr 1fr}}

/* Dropdown */
select{{background:var(--card);color:var(--text);border:1px solid var(--border);
  padding:10px 14px;border-radius:8px;font-size:16px;margin-bottom:20px;width:100%;max-width:400px}}
select:focus{{outline:none;border-color:var(--primary)}}

/* Memo */
.memo-content{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:32px;line-height:1.7;font-size:14px}}
.memo-content h1,.memo-content h2,.memo-content h3{{color:var(--primary);margin:24px 0 12px}}
.memo-content h1{{font-size:22px}}.memo-content h2{{font-size:18px}}.memo-content h3{{font-size:15px}}
.memo-content table{{margin:16px 0}}.memo-content th,.memo-content td{{padding:6px 12px}}
.memo-content hr{{border:none;border-top:1px solid var(--border);margin:20px 0}}
.memo-content ul,.memo-content ol{{padding-left:24px;margin:8px 0}}
.memo-content strong{{color:var(--accent)}}
.memo-content code{{background:var(--bg);padding:2px 6px;border-radius:4px;font-size:13px}}
.memo-content blockquote{{border-left:3px solid var(--primary);padding-left:16px;color:var(--muted);margin:12px 0}}

.btn{{background:var(--primary);color:#fff;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600}}
.btn:hover{{opacity:.9}}

h2.section-title{{font-size:18px;color:var(--text);margin-bottom:16px;padding-bottom:8px;border-bottom:1px solid var(--border)}}

/* Risk tag */
.risk-tag{{display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;margin:2px}}
.risk-tag.green{{background:rgba(13,148,136,.2);color:#34D399}}
.risk-tag.yellow{{background:rgba(251,191,36,.2);color:#FBBF24}}
.risk-tag.red{{background:rgba(244,114,182,.2);color:#F472B6}}

/* Deep dive panels */
.dd-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}}
.comp-list{{list-style:none;padding:0}}.comp-list li{{padding:6px 0;border-bottom:1px solid var(--border);font-size:13px}}

/* Tablet */
@media(max-width:1100px){{
  .cards-4{{grid-template-columns:repeat(2,1fr)}}
  .cards-3{{grid-template-columns:1fr}}
  .chart-row-2{{grid-template-columns:1fr}}
  .dd-grid{{grid-template-columns:1fr}}
}}

/* Mobile */
@media(max-width:768px){{
  :root{{--sidebar-w:0px}}
  body{{flex-direction:column}}

  /* Sidebar becomes top nav bar */
  .sidebar{{
    width:100%;height:auto;position:sticky;top:0;
    flex-direction:column;min-height:auto;
    border-right:none;border-bottom:1px solid var(--border);
  }}
  .sidebar .brand{{
    padding:12px 16px;display:flex;align-items:center;justify-content:center;
    border-bottom:1px solid var(--border);
  }}
  .sidebar .brand img{{max-width:80px}}
  .sidebar nav{{
    display:flex;padding:0;overflow-x:auto;-webkit-overflow-scrolling:touch;
    scrollbar-width:none;
  }}
  .sidebar nav::-webkit-scrollbar{{display:none}}
  .sidebar nav button{{
    white-space:nowrap;border-left:none;flex-shrink:0;
    border-bottom:3px solid transparent;
    padding:12px 16px;font-size:12px;min-width:auto;
  }}
  .sidebar nav button.active{{border-bottom-color:var(--primary);border-left-color:transparent}}

  /* Main content */
  .main{{margin-left:0;padding:16px;max-width:100vw;overflow-x:hidden}}

  /* Cards stack vertically */
  .cards-4,.cards-3,.cards-2{{grid-template-columns:1fr}}
  .card .value{{font-size:22px}}
  .card.strategy-card .value{{font-size:16px}}
  .card{{padding:16px}}

  /* Tables scroll */
  .table-wrap{{-webkit-overflow-scrolling:touch}}
  table{{font-size:12px}}
  th,td{{padding:8px 10px}}

  /* Charts full width and shorter */
  .chart-row-2{{grid-template-columns:1fr}}
  .chart-box{{padding:8px;overflow:hidden}}
  .chart-box>div{{height:250px !important}}

  /* Dropdowns full width, 16px prevents iOS zoom */
  select{{width:100%;max-width:100%;font-size:16px}}

  /* Memo */
  .memo-content{{padding:14px;font-size:13px;overflow-x:auto}}
  .memo-content table{{font-size:11px;display:block;overflow-x:auto}}

  /* Section titles */
  h2.section-title{{font-size:16px;margin-bottom:12px}}

  /* Strategy cards */
  .card.strategy-card{{padding:14px}}
  .strategy-row{{font-size:12px;gap:4px}}
  .strategy-row span:first-child{{min-width:0}}
  .strategy-row span{{word-break:break-word}}

  /* Deep dive */
  .dd-grid{{grid-template-columns:1fr}}
  .comp-list li{{font-size:12px;word-break:break-word}}

  /* Buttons */
  .btn{{width:100%;text-align:center;padding:14px 20px;font-size:14px;border-radius:10px}}

  /* Risk tags wrap */
  .risk-tag{{font-size:10px;margin:2px 2px}}

  /* Plotly modebar hidden on mobile */
  .modebar{{display:none !important}}
}}

/* Small phones (iPhone SE / mini) */
@media(max-width:375px){{
  .sidebar .brand img{{max-width:55px}}
  .sidebar nav button{{padding:10px 10px;font-size:10px}}
  .card .value{{font-size:18px}}
  .card{{padding:12px}}
  .main{{padding:10px}}
  .chart-box>div{{height:220px !important}}
  .memo-content{{padding:10px;font-size:12px}}
  h2.section-title{{font-size:14px}}
}}
</style>
</head>
<body>

<div class="sidebar">
  <div class="brand">
    <img src="vero-stacked-dark.png" alt="Vero Capital Holdings">
  </div>
  <nav>
    <button class="active" onclick="showTab('summary',this)">Executive Summary</button>
    <button onclick="showTab('rankings',this)">Market Rankings</button>
    <button onclick="showTab('deepdive',this)">Market Deep Dive</button>
    <button onclick="showTab('portfolio',this)">Portfolio Optimization</button>
    <button onclick="showTab('memo',this)">Investment Memo</button>
  </nav>
</div>

<div class="main">

<!-- TAB 1: Executive Summary -->
<div id="tab-summary" class="tab active">
  <h2 class="section-title">Executive Summary</h2>
  <div id="summary-cards" class="cards cards-4"></div>
  <div id="strategy-cards" class="cards cards-3"></div>
  <div class="chart-box">
    <div id="capital-chart" style="height:400px"></div>
  </div>
</div>

<!-- TAB 2: Market Rankings -->
<div id="tab-rankings" class="tab">
  <h2 class="section-title">Market Rankings</h2>
  <div class="table-wrap">
    <table id="rankings-table">
      <thead><tr id="rankings-head"></tr></thead>
      <tbody id="rankings-body"></tbody>
    </table>
  </div>
</div>

<!-- TAB 3: Market Deep Dive -->
<div id="tab-deepdive" class="tab">
  <h2 class="section-title">Market Deep Dive</h2>
  <select id="market-select" onchange="renderDeepDive()"></select>
  <div id="dd-cards" class="cards cards-4"></div>
  <div class="chart-row chart-row-2">
    <div class="chart-box"><div id="dd-cashflow" style="height:300px"></div></div>
    <div class="chart-box"><div id="dd-irr" style="height:300px"></div></div>
  </div>
  <div class="chart-row chart-row-2">
    <div class="chart-box"><div id="dd-cagr" style="height:300px"></div></div>
    <div class="chart-box" id="dd-comps-box">
      <h3 style="font-size:14px;color:var(--muted);margin-bottom:12px">Comparable Markets</h3>
      <div id="dd-comps"></div>
    </div>
  </div>
  <div class="chart-box" id="dd-thesis-box" style="border-left:3px solid var(--primary);margin-top:16px">
    <h3 style="font-size:14px;color:var(--primary);margin-bottom:10px">Investment Thesis</h3>
    <div id="dd-thesis" style="color:var(--text);font-size:14px;line-height:1.8"></div>
  </div>
  <div class="card" id="dd-risk" style="margin-top:8px"></div>
</div>

<!-- TAB 4: Portfolio Optimization -->
<div id="tab-portfolio" class="tab">
  <h2 class="section-title">Portfolio Optimization</h2>
  <div class="chart-box"><div id="corr-heatmap" style="height:550px"></div></div>
  <div class="chart-row chart-row-2">
    <div class="chart-box"><div id="risk-return" style="height:380px"></div></div>
    <div class="chart-box"><div id="div-benefit" style="height:380px"></div></div>
  </div>
</div>

<!-- TAB 5: Investment Memo -->
<div id="tab-memo" class="tab">
  <h2 class="section-title">Investment Memo</h2>
  <button class="btn" onclick="downloadMemo()" style="margin-bottom:16px">Download Memo (.md)</button>
  <div id="memo-rendered" class="memo-content"></div>
</div>

</div><!-- /main -->

<script>
// ── Embedded Data ──
const DATA_MARKETS = {markets_json};
const DATA_STRATEGIES = {strategies_json};
const DATA_CAPITAL = {capital_json};
const DATA_CORR = {corr_json};
const DATA_MEMO = {memo_escaped};
const NARRATIVES = {narratives_json};

const PALETTE = ["#C9A96E","#E8D5B0","#8B7D6B","#D4A849","#A89060","#BFA76F","#F0DBA8","#9C8B6E"];
const PLOTLY_LAYOUT = {{
  paper_bgcolor:'transparent', plot_bgcolor:'transparent',
  font:{{color:'#F8FAFC',size:12}},
  margin:{{t:40,b:50,l:60,r:20}},
  xaxis:{{gridcolor:'#3a3a3c',zerolinecolor:'#3a3a3c'}},
  yaxis:{{gridcolor:'#3a3a3c',zerolinecolor:'#3a3a3c'}},
}};
const PLOTLY_CFG = {{displayModeBar:false,responsive:true}};

// ── Helpers ──
function fmtDollar(v){{ if(v==null||isNaN(v)) return '—'; return '$'+Number(v).toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}}) }}
function fmtPct(v){{ if(v==null||isNaN(v)) return '—'; return (v*100).toFixed(1)+'%' }}
function fmtNum(v){{ if(v==null||isNaN(v)) return '—'; return Number(v).toLocaleString('en-US') }}
function fmtPctRaw(v){{ if(v==null||isNaN(v)) return '—'; return Number(v).toFixed(1)+'%' }}
function shortName(s){{ if(!s) return '—'; return s.replace(/ Metro Area| Micro Area/g,'') }}

// ── Tab switching ──
function showTab(id, btn){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('tab-'+id).classList.add('active');
  document.querySelectorAll('.sidebar nav button').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  if(id==='portfolio') renderPortfolio();
  if(id==='deepdive') renderDeepDive();
  window.dispatchEvent(new Event('resize'));
}}

// ══════════════════════════════════════════════════════════════════
// TAB 1: Executive Summary
// ══════════════════════════════════════════════════════════════════
function renderSummary(){{
  const top = DATA_MARKETS[0];
  const avgCap = DATA_MARKETS.reduce((s,m)=>s+(m.cap_rate||0),0)/DATA_MARKETS.length;
  const avgIRR = DATA_MARKETS.reduce((s,m)=>s+(m.irr_5yr||0),0)/DATA_MARKETS.length;

  document.getElementById('summary-cards').innerHTML = `
    <div class="card"><div class="label">Top Ranked Market</div><div class="value" style="font-size:20px;color:var(--primary)">${{shortName(top.cbsa_title)}}</div><div class="sub">Score: ${{(top.composite_score||0).toFixed(1)}}</div></div>
    <div class="card"><div class="label">Avg Cap Rate</div><div class="value">${{fmtPct(avgCap)}}</div><div class="sub">Across ${{DATA_MARKETS.length}} markets</div></div>
    <div class="card"><div class="label">Avg 5-Year IRR</div><div class="value">${{fmtPct(avgIRR)}}</div><div class="sub">Projected</div></div>
    <div class="card"><div class="label">Markets Analyzed</div><div class="value">${{DATA_MARKETS.length}}</div><div class="sub">Passed screening</div></div>
  `;

  // Strategy cards
  let sh = '';
  DATA_STRATEGIES.forEach((s,i)=>{{
    const col = PALETTE[i%PALETTE.length];
    sh += `<div class="card strategy-card" style="border-top-color:${{col}}">
      <div class="label">${{s.strategy||s.name||'Strategy '+(i+1)}}</div>
      <div class="value">${{s.total_properties||'—'}} Properties</div>
      <div class="sub">Cash Flow: ${{fmtDollar(s.annual_cash_flow)}} &middot; IRR: ${{fmtPct(s.portfolio_irr_5yr)}}</div>
      <div class="sub">Sharpe: ${{s.sharpe_ratio!=null?s.sharpe_ratio.toFixed(3):'—'}} &middot; Diversification: ${{s.diversification_ratio!=null?(s.diversification_ratio*100).toFixed(1)+'%':'—'}}</div>
    </div>`;
  }});
  document.getElementById('strategy-cards').innerHTML = sh;

  // Capital sensitivity chart
  Plotly.newPlot('capital-chart', [
    {{x:DATA_CAPITAL.map(r=>r.capital), y:DATA_CAPITAL.map(r=>r.total_properties), name:'Properties', type:'scatter', mode:'lines+markers', marker:{{color:PALETTE[0],size:8}}, yaxis:'y'}},
    {{x:DATA_CAPITAL.map(r=>r.capital), y:DATA_CAPITAL.map(r=>r.annual_cash_flow), name:'Annual Cash Flow', type:'scatter', mode:'lines+markers', marker:{{color:PALETTE[1],size:8}}, yaxis:'y2'}},
  ], {{
    ...PLOTLY_LAYOUT,
    title:{{text:'Capital Sensitivity Analysis',font:{{size:15,color:'#F8FAFC'}}}},
    xaxis:{{...PLOTLY_LAYOUT.xaxis, title:'Capital ($)', tickformat:'$,.0f'}},
    yaxis:{{...PLOTLY_LAYOUT.yaxis, title:'Properties', side:'left'}},
    yaxis2:{{...PLOTLY_LAYOUT.yaxis, title:'Cash Flow ($)', overlaying:'y', side:'right', tickformat:'$,.0f'}},
    legend:{{x:0.01,y:1.15,orientation:'h',font:{{size:11}}}},
  }}, PLOTLY_CFG);
}}

// ══════════════════════════════════════════════════════════════════
// TAB 2: Market Rankings
// ══════════════════════════════════════════════════════════════════
const RANK_COLS = [
  {{key:'market_rank',label:'Rank',fmt:v=>v}},
  {{key:'cbsa_title',label:'Market',fmt:shortName}},
  {{key:'composite_score',label:'Score',fmt:v=>v!=null?v.toFixed(1):'—'}},
  {{key:'cap_rate',label:'Cap Rate',fmt:fmtPct}},
  {{key:'cash_on_cash_return',label:'Cash-on-Cash',fmt:fmtPct}},
  {{key:'irr_5yr',label:'IRR 5yr',fmt:fmtPct}},
  {{key:'dscr',label:'DSCR',fmt:v=>v!=null?v.toFixed(3):'—'}},
  {{key:'median_home_price',label:'Price',fmt:fmtDollar}},
  {{key:'median_rent',label:'Rent',fmt:fmtDollar}},
  {{key:'population',label:'Population',fmt:fmtNum}},
  {{key:'landlord_friendliness_score',label:'LL Score',fmt:v=>v!=null?v+'/10':'—'}},
];

let rankSort = {{col:'market_rank', asc:true}};

function renderRankings(){{
  let head = '';
  RANK_COLS.forEach(c=>{{
    const arrow = rankSort.col===c.key ? (rankSort.asc?'&#9650;':'&#9660;') : '';
    head += `<th onclick="sortRankings('${{c.key}}')">${{c.label}}<span class="sort-arrow">${{arrow}}</span></th>`;
  }});
  document.getElementById('rankings-head').innerHTML = head;

  const sorted = [...DATA_MARKETS].sort((a,b)=>{{
    let va=a[rankSort.col], vb=b[rankSort.col];
    if(va==null) return 1; if(vb==null) return -1;
    if(typeof va==='string') return rankSort.asc?va.localeCompare(vb):vb.localeCompare(va);
    return rankSort.asc?va-vb:vb-va;
  }});

  let body = '';
  sorted.forEach(m=>{{
    body += '<tr>';
    RANK_COLS.forEach(c=>{{ body += `<td>${{c.fmt(m[c.key])}}</td>`; }});
    body += '</tr>';
  }});
  document.getElementById('rankings-body').innerHTML = body;
}}

function sortRankings(col){{
  if(rankSort.col===col) rankSort.asc=!rankSort.asc;
  else {{ rankSort.col=col; rankSort.asc=true; }}
  renderRankings();
}}

// ══════════════════════════════════════════════════════════════════
// TAB 3: Market Deep Dive
// ══════════════════════════════════════════════════════════════════
function initDeepDive(){{
  const sel = document.getElementById('market-select');
  DATA_MARKETS.forEach((m,i)=>{{
    const opt = document.createElement('option');
    opt.value = i;
    opt.textContent = `#${{m.market_rank}} — ${{m.cbsa_title}}`;
    sel.appendChild(opt);
  }});
}}

function renderDeepDive(){{
  const m = DATA_MARKETS[document.getElementById('market-select').value];
  if(!m) return;

  // Metric cards
  document.getElementById('dd-cards').innerHTML = `
    <div class="card"><div class="label">Cap Rate</div><div class="value">${{fmtPct(m.cap_rate)}}</div></div>
    <div class="card"><div class="label">Cash-on-Cash</div><div class="value">${{fmtPct(m.cash_on_cash_return)}}</div></div>
    <div class="card"><div class="label">DSCR</div><div class="value">${{m.dscr!=null?m.dscr.toFixed(3):'—'}}</div></div>
    <div class="card"><div class="label">5-Year IRR</div><div class="value">${{fmtPct(m.irr_5yr)}}</div></div>
  `;

  // Cash flow bar chart
  const cfLabels = ['Annual NOI','Debt Service','Cash Flow'];
  const cfVals = [m.annual_noi, -(m.annual_debt_service||0), m.annual_cash_flow];
  Plotly.newPlot('dd-cashflow', [{{
    x:cfLabels, y:cfVals, type:'bar',
    marker:{{color:[PALETTE[0],PALETTE[4],cfVals[2]>=0?PALETTE[5]:PALETTE[4]]}},
  }}], {{
    ...PLOTLY_LAYOUT,
    title:{{text:'Annual Cash Flow Breakdown',font:{{size:14,color:'#F8FAFC'}}}},
    yaxis:{{...PLOTLY_LAYOUT.yaxis,tickformat:'$,.0f'}},
  }}, PLOTLY_CFG);

  // IRR bars
  const irrLabels = ['5-Year','7-Year','10-Year'];
  const irrVals = [m.irr_5yr, m.irr_7yr, m.irr_10yr];
  Plotly.newPlot('dd-irr', [{{
    x:irrLabels, y:irrVals.map(v=>v!=null?v*100:0), type:'bar',
    marker:{{color:[PALETTE[0],PALETTE[1],PALETTE[2]]}},
    text:irrVals.map(v=>v!=null?(v*100).toFixed(1)+'%':'—'), textposition:'outside',
    textfont:{{color:'#F8FAFC'}},
  }}], {{
    ...PLOTLY_LAYOUT,
    title:{{text:'IRR by Horizon',font:{{size:14,color:'#F8FAFC'}}}},
    yaxis:{{...PLOTLY_LAYOUT.yaxis,title:'IRR (%)',ticksuffix:'%'}},
  }}, PLOTLY_CFG);

  // CAGR bars
  const cagrLabels = ['3-Year','5-Year','10-Year'];
  const cagrVals = [m.cagr_3yr, m.cagr_5yr, m.cagr_10yr];
  Plotly.newPlot('dd-cagr', [{{
    x:cagrLabels, y:cagrVals.map(v=>v!=null?v*100:0), type:'bar',
    marker:{{color:[PALETTE[5],PALETTE[6],PALETTE[7]]}},
    text:cagrVals.map(v=>v!=null?(v*100).toFixed(1)+'%':'—'), textposition:'outside',
    textfont:{{color:'#F8FAFC'}},
  }}], {{
    ...PLOTLY_LAYOUT,
    title:{{text:'CAGR by Horizon',font:{{size:14,color:'#F8FAFC'}}}},
    yaxis:{{...PLOTLY_LAYOUT.yaxis,title:'CAGR (%)',ticksuffix:'%'}},
  }}, PLOTLY_CFG);

  // Comparables
  let comps = m.comparable_markets;
  if(typeof comps === 'string'){{
    try{{ comps = JSON.parse(comps.replace(/'/g,'"')); }}catch(e){{ comps=[]; }}
  }}
  let compHtml = '';
  if(comps && comps.length){{
    compHtml += `<ul class="comp-list">`;
    comps.forEach(c=>{{ compHtml += `<li>${{c}}</li>`; }});
    compHtml += `</ul>`;
    compHtml += `<div style="margin-top:12px;font-size:12px;color:var(--muted)">`;
    compHtml += `Avg Price: ${{fmtDollar(m.comp_avg_price)}} &middot; Avg Rent: ${{fmtDollar(m.comp_avg_rent)}} &middot; Avg Growth: ${{m.comp_avg_price_growth!=null?m.comp_avg_price_growth.toFixed(1)+'%':'—'}}`;
    compHtml += `</div>`;
  }} else {{
    compHtml = '<p style="color:var(--muted)">No comparable markets data</p>';
  }}
  document.getElementById('dd-comps').innerHTML = compHtml;

  // Risk factors
  const dscr_class = m.dscr>=1.25?'green':m.dscr>=1.0?'yellow':'red';
  const evict_class = (m.eviction_timeline_days||0)<=30?'green':(m.eviction_timeline_days||0)<=60?'yellow':'red';
  const rc = m.rent_control_status||'unknown';
  const rc_class = rc==='preempted'?'green':rc==='none'?'green':'yellow';
  document.getElementById('dd-risk').innerHTML = `
    <h3 style="font-size:14px;color:var(--muted);margin-bottom:12px">Risk Factors</h3>
    <div>
      <span class="risk-tag ${{dscr_class}}">DSCR ${{m.dscr!=null?m.dscr.toFixed(2):'—'}}</span>
      <span class="risk-tag ${{evict_class}}">Eviction ${{m.eviction_timeline_days||'—'}} days</span>
      <span class="risk-tag ${{rc_class}}">Rent Control: ${{rc}}</span>
      <span class="risk-tag ${{(m.landlord_friendliness_score||0)>=7?'green':(m.landlord_friendliness_score||0)>=4?'yellow':'red'}}">LL Score ${{m.landlord_friendliness_score||'—'}}/10</span>
      <span class="risk-tag ${{(m.break_even_occupancy||1)<=0.85?'green':(m.break_even_occupancy||1)<=0.95?'yellow':'red'}}">Break-Even Occ ${{m.break_even_occupancy!=null?(m.break_even_occupancy*100).toFixed(1)+'%':'—'}}</span>
    </div>
    <div style="margin-top:12px;font-size:12px;color:var(--muted)">
      State: ${{m.state_abbrev||'—'}} &middot; Population: ${{fmtNum(m.population)}} &middot;
      Median HH Income: ${{fmtDollar(m.median_household_income)}} &middot;
      Migration Score: ${{m.migration_score!=null?m.migration_score.toFixed(1):'—'}} &middot;
      Diversification Score: ${{m.diversification_score!=null?m.diversification_score.toFixed(1):'—'}} &middot;
      Max Props @$200k: ${{m.max_properties_200k||'—'}} &middot; @$500k: ${{m.max_properties_500k||'—'}} &middot;
      Mortgage Rate: ${{m.mortgage_rate_30yr!=null?m.mortgage_rate_30yr.toFixed(2)+'%':'—'}}
    </div>
  `;

  // Investment thesis narrative
  const thesis = NARRATIVES[m.cbsa_title] || '';
  document.getElementById('dd-thesis').innerHTML = thesis
    ? `<p>${{thesis}}</p>`
    : '<p style="color:var(--muted)">No investment thesis available for this market.</p>';
}}

// ══════════════════════════════════════════════════════════════════
// TAB 4: Portfolio Optimization
// ══════════════════════════════════════════════════════════════════
let portfolioRendered = false;
function renderPortfolio(){{
  if(portfolioRendered) return;
  portfolioRendered = true;

  // Correlation heatmap
  const labels = DATA_CORR.labels.map(shortName);
  Plotly.newPlot('corr-heatmap', [{{
    z:DATA_CORR.values, x:labels, y:labels,
    type:'heatmap', colorscale:[[0,'#1d1d1f'],[0.5,'#8B7D6B'],[1,'#C9A96E']],
    zmin:0, zmax:1,
  }}], {{
    ...PLOTLY_LAYOUT,
    title:{{text:'Price Correlation Matrix',font:{{size:15,color:'#F8FAFC'}}}},
    margin:{{t:40,b:120,l:140,r:20}},
    xaxis:{{...PLOTLY_LAYOUT.xaxis,tickangle:-45}},
  }}, PLOTLY_CFG);

  // Risk-return scatter
  Plotly.newPlot('risk-return', [{{
    x:DATA_STRATEGIES.map(s=>s.portfolio_volatility!=null?s.portfolio_volatility*100:0),
    y:DATA_STRATEGIES.map(s=>s.portfolio_return!=null?s.portfolio_return*100:0),
    text:DATA_STRATEGIES.map(s=>s.strategy||s.name),
    mode:'markers+text', type:'scatter',
    marker:{{size:16,color:PALETTE.slice(0,DATA_STRATEGIES.length)}},
    textposition:'top center', textfont:{{color:'#F8FAFC',size:12}},
  }}], {{
    ...PLOTLY_LAYOUT,
    title:{{text:'Risk vs Return by Strategy',font:{{size:14,color:'#F8FAFC'}}}},
    xaxis:{{...PLOTLY_LAYOUT.xaxis,title:'Volatility (%)',ticksuffix:'%'}},
    yaxis:{{...PLOTLY_LAYOUT.yaxis,title:'Return (%)',ticksuffix:'%'}},
  }}, PLOTLY_CFG);

  // Diversification benefit bars
  Plotly.newPlot('div-benefit', [{{
    x:DATA_STRATEGIES.map(s=>s.strategy||s.name),
    y:DATA_STRATEGIES.map(s=>s.diversification_benefit!=null?s.diversification_benefit*100:0),
    type:'bar',
    marker:{{color:PALETTE.slice(0,DATA_STRATEGIES.length)}},
    text:DATA_STRATEGIES.map(s=>s.diversification_benefit!=null?(s.diversification_benefit*100).toFixed(1)+'%':'—'),
    textposition:'outside', textfont:{{color:'#F8FAFC'}},
  }}], {{
    ...PLOTLY_LAYOUT,
    title:{{text:'Diversification Benefit',font:{{size:14,color:'#F8FAFC'}}}},
    yaxis:{{...PLOTLY_LAYOUT.yaxis,title:'Benefit (%)',ticksuffix:'%'}},
  }}, PLOTLY_CFG);
}}

// ══════════════════════════════════════════════════════════════════
// TAB 5: Investment Memo
// ══════════════════════════════════════════════════════════════════
function renderMemo(){{
  document.getElementById('memo-rendered').innerHTML = marked.parse(DATA_MEMO);
}}

function downloadMemo(){{
  const blob = new Blob([DATA_MEMO], {{type:'text/markdown'}});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'vero_capital_investment_memo.md';
  a.click();
}}

// ── Init ──
renderSummary();
renderRankings();
initDeepDive();
renderMemo();
</script>
</body>
</html>"""

    # ── Write output ───────────────────────────────────────────────────
    out_path = os.path.join(ROOT, "docs", "index.html")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        f.write(html)

    size = os.path.getsize(out_path)
    print(f"Generated {out_path}")
    print(f"File size: {size:,} bytes ({size/1024:.1f} KB)")


if __name__ == "__main__":
    main()
