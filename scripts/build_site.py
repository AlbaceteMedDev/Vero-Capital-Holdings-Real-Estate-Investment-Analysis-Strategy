"""Build a static HTML dashboard from pipeline output data."""
import json, math, pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def clean(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean(i) for i in obj]
    return obj

def load_data():
    scored = pd.read_parquet(ROOT / 'data/processed/scored_markets.parquet')
    cols = ['cbsa_fips','cbsa_title','state_abbrev','composite_score','market_rank',
            'population','median_household_income','median_home_price','median_rent',
            'cap_rate','cash_on_cash_return','dscr','irr_5yr','irr_7yr','irr_10yr',
            'monthly_rent_to_price_pct','landlord_friendliness_score','eviction_timeline_days',
            'rent_control_status','price_growth_pct','rent_growth_pct','annual_noi',
            'annual_debt_service','annual_cash_flow','total_cash_per_property',
            'max_properties_200k','max_properties_500k','break_even_occupancy',
            'cagr_3yr','cagr_5yr','cagr_10yr','migration_score','diversification_score',
            'hhi_index','rent_growth_vs_national','comparable_markets',
            'comp_avg_price','comp_avg_rent','comp_avg_price_growth','mortgage_rate_30yr']
    avail = [c for c in cols if c in scored.columns]
    strat = pd.read_csv(ROOT / 'outputs/strategies/strategy_comparison.csv')
    sens = pd.read_csv(ROOT / 'outputs/strategies/capital_sensitivity.csv')
    corr = pd.read_csv(ROOT / 'outputs/strategies/price_correlation_matrix.csv', index_col=0)
    corr_data = {'labels': [l.split(',')[0].split('-')[0].strip()[:18] for l in corr.index],
                 'values': clean(corr.values.tolist())}
    memos = sorted((ROOT / 'outputs/memos').glob('*.md'), reverse=True)
    memo = memos[0].read_text() if memos else ''
    return json.dumps(clean({
        'scored': scored[avail].to_dict(orient='records'),
        'strategies': strat.to_dict(orient='records'),
        'sensitivity': sens.to_dict(orient='records'),
        'correlation': corr_data,
        'memo': memo,
    }))

if __name__ == '__main__':
    data_json = load_data()
    html_template = (ROOT / 'scripts' / 'template.html').read_text()
    html = html_template.replace('__DATA_BUNDLE__', data_json)
    out = ROOT / 'docs' / 'index.html'
    out.parent.mkdir(exist_ok=True)
    out.write_text(html)
    print(f'Built: {out} ({len(html)} bytes)')
