"""Daily job search over the public ats-scrapers / stapply.ai hosted dataset.

The dataset is a free, no-auth Parquet snapshot of ~5M live jobs published at
https://storage.stapply.ai/jobhive/v1/manifest.json (see github.com/kalil0321/ats-scrapers).

This script does NOT use `ats_scrapers.search()` because that helper loads the
entire multi-GB snapshot into memory. Instead it streams the Parquet file with
pyarrow, pushing the title filter down and projecting away the huge
`description` / `raw` columns, so it runs comfortably in ~1 GB of RAM.

For each profile (--profiles; a profile is just a set of title keywords) it runs
two searches:
  1. Berlin         - title contains one of the keywords AND location mentions
                      the city. Every row is tagged remote / hybrid / onsite /
                      unknown so you can see the work arrangement at a glance.
  2. Germany-remote - title contains one of the keywords AND the role looks
                      remote AND it is tied to Germany (country_iso == DE, or the
                      location text names Germany or a German city).

Outputs (in results/):
  jobs_<date>.csv    - flat table of every match
  jobs_<date>.html   - browsable page with clickable job links
  latest.html        - copy of today's page
"""

from __future__ import annotations

import argparse
import hashlib
import html
import re
import shutil
import sys
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq

MANIFEST_URL = "https://storage.stapply.ai/jobhive/v1/manifest.json"
PROJECT_DIR = Path(__file__).resolve().parent
CACHE_DIR = PROJECT_DIR / ".cache"
RESULTS_DIR = PROJECT_DIR / "results"

# Search profiles: same locations, different title keywords. Pick with --profiles.
PROFILES: dict[str, list[str]] = {
    "engineer": ["engineer", "tech lead"],
    "product": ["product manager", "product management", "product owner"],
}
DEFAULT_PROFILES = ["engineer", "product"]

# Columns we keep. `description` and `raw` are dropped - together they are the
# bulk of the file and we never filter or display them.
KEEP_COLUMNS = [
    "url", "title", "company", "ats_type", "location", "country_iso", "region",
    "language", "is_remote", "salary_min", "salary_max", "salary_currency",
    "salary_period", "salary_summary", "employment_type", "department",
    "posted_at", "apply_url", "commitment",
]

# Lower-case German city names used to attribute a free-form location to Germany
# when country_iso is missing (the dataset leaves it null for many sources).
GERMAN_CITIES = [
    "berlin", "munich", "münchen", "muenchen", "hamburg", "frankfurt", "cologne",
    "köln", "koeln", "stuttgart", "düsseldorf", "duesseldorf", "dusseldorf",
    "leipzig", "dortmund", "essen", "bremen", "dresden", "hannover", "hanover",
    "nuremberg", "nürnberg", "nuernberg", "karlsruhe", "mannheim", "bonn",
    "münster", "muenster", "wiesbaden", "augsburg", "aachen", "potsdam",
    "heidelberg", "freiburg", "mainz", "kiel", "braunschweig", "chemnitz",
    "halle", "magdeburg", "erfurt", "kaiserslautern", "regensburg", "ingolstadt",
    "walldorf", "böblingen", "tübingen", "jena", "ulm", "darmstadt", "kassel",
]

# Kept deliberately tight: bare " de " matches "Rio de Janeiro", so only the
# dataset's structured ", <region>, de" tail and explicit country names count.
GERMANY_TEXT = ["germany", "deutschland", ", de", "(de)", "d-a-ch", "dach region"]
EU_REMOTE_TEXT = ["europe", "emea", "european union", " eu ", "eu-remote", "remote eu", "remote - eu", "cet", "cest"]

# Signals that a location covers more than just Germany. Matched as whole words
# (regex \b...\b) against the lower-cased location, so short codes like "eu" / "uk"
# are safe. Used to drop multi-country / region-wide postings when the caller
# wants Germany-only roles. Tuned for --country DE.
OTHER_COUNTRY_TERMS = [
    "netherlands", "nederland", "austria", "österreich", "oesterreich", "switzerland",
    "schweiz", "suisse", "france", "spain", "españa", "espana", "portugal", "italy",
    "italia", "poland", "polska", "czech", "czechia", "sweden", "denmark", "norway",
    "finland", "ireland", "belgium", "belgië", "luxembourg", "united kingdom",
    "great britain", "england", "scotland", "wales", "uk", "u.k.", "romania",
    "bulgaria", "greece", "hungary", "croatia", "serbia", "slovakia", "slovenia",
    "estonia", "latvia", "lithuania", "ukraine", "moldova", "turkey", "türkiye",
    "cyprus", "malta", "iceland", "israel", "united states", "u.s.", "u.s.a.", "usa",
    "canada", "brazil", "brasil", "mexico", "méxico", "argentina", "chile", "colombia",
    "us", "u.s", "united states of america", "america",
    "india", "singapore", "australia", "new zealand", "japan", "china", "hong kong",
    "philippines", "vietnam", "indonesia", "malaysia", "thailand", "andorra", "albania",
    "morocco", "egypt", "nigeria", "kenya", "south africa", "uae",
    "united arab emirates",
]
MULTI_REGION_TERMS = [
    "europe", "european", "emea", "apac", "americas", "latam", "mena", "worldwide",
    "world wide", "global", "international", "european union", "dach", "d-a-ch",
    "d/a/ch", "benelux", "nordics", "scandinavia", "eu", "eea", "cee",
]
NON_DE_CITIES = [
    "london", "manchester", "paris", "lyon", "amsterdam", "rotterdam", "eindhoven",
    "madrid", "barcelona", "valencia", "lisbon", "lisboa", "porto", "dublin",
    "vienna", "wien", "graz", "linz", "salzburg", "zurich", "zürich", "zuerich",
    "geneva", "geneve", "basel", "bern", "lausanne", "zug", "lugano", "warsaw",
    "warszawa", "krakow", "kraków", "wroclaw", "wrocław", "gdansk", "poznan",
    "prague", "praha", "brno", "bratislava", "stockholm", "gothenburg", "malmö",
    "copenhagen", "københavn", "aarhus", "oslo", "bergen", "helsinki", "tallinn",
    "riga", "vilnius", "brussels", "bruxelles", "antwerp", "milan", "milano",
    "rome", "roma", "turin", "bologna", "athens", "thessaloniki", "bucharest",
    "cluj", "sofia", "budapest", "zagreb", "ljubljana", "belgrade", "beograd",
    "kyiv", "kiev", "lviv", "istanbul", "ankara", "tel aviv", "new york",
    "san francisco", "bay area", "boston", "austin", "seattle", "chicago",
    "los angeles", "denver", "atlanta", "miami", "toronto", "montreal",
    "vancouver", "são paulo", "sao paulo", "rio de janeiro", "buenos aires",
    "mexico city", "bogota", "bangalore", "bengaluru", "mumbai", "new delhi",
    "hyderabad", "pune", "chennai", "gurgaon", "noida", "singapore", "sydney",
    "melbourne", "brisbane", "auckland", "tokyo", "osaka", "seoul", "shanghai",
    "beijing", "shenzhen", "dubai", "abu dhabi", "cairo", "nairobi", "lagos",
    "cape town", "johannesburg",
]

WORKPLACE_ORDER = {"remote": 0, "hybrid": 1, "unknown": 2, "onsite": 3}


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def fetch_manifest() -> dict:
    log(f"Fetching manifest: {MANIFEST_URL}")
    r = httpx.get(MANIFEST_URL, timeout=60, follow_redirects=True)
    r.raise_for_status()
    return r.json()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_parquet(entry: dict, dest: Path, *, refresh: bool) -> Path:
    """Download entry['parquet'] to dest, reusing the cache when sha256 matches."""
    url = entry["parquet"]
    expected_sha = entry.get("parquet_sha256")
    size = entry.get("parquet_size_bytes", 0)

    if dest.exists() and not refresh:
        if expected_sha is None:
            log(f"Using cached {dest.name} (no checksum in manifest to verify)")
            return dest
        log(f"Verifying cached {dest.name} ...")
        if sha256_file(dest) == expected_sha:
            log(f"Cache hit: {dest.name}")
            return dest
        log("Checksum mismatch - re-downloading")

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log(f"Downloading {url}  (~{size / 1e9:.2f} GB)")
    tty = sys.stderr.isatty()
    done = last_pct = 0
    with httpx.stream("GET", url, timeout=None, follow_redirects=True) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in resp.iter_bytes(1 << 20):
                fh.write(chunk)
                done += len(chunk)
                if not size:
                    continue
                pct = int(done / size * 100)
                if tty:
                    print(f"\r  {done / 1e9:5.2f} / {size / 1e9:.2f} GB ({pct:3d}%)",
                          end="", file=sys.stderr, flush=True)
                elif pct >= last_pct + 10:  # log file: one line per 10%
                    last_pct = pct
                    print(f"  {pct}% ({done / 1e9:.2f} GB)", file=sys.stderr, flush=True)
    if tty:
        print("", file=sys.stderr)
    if expected_sha and sha256_file(tmp) != expected_sha:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded {url} failed checksum verification")
    tmp.replace(dest)
    return dest


def scan_parquet(path: Path, keywords: list[str]) -> pd.DataFrame:
    """Stream the Parquet file, keep rows whose title contains ANY keyword."""
    available = set(pq.ParquetFile(path).schema_arrow.names)
    columns = [c for c in KEEP_COLUMNS if c in available]
    dataset = pa_ds.dataset(path, format="parquet")

    title_filter = None
    for kw in keywords:
        expr = pc.match_substring(pc.field("title"), kw, ignore_case=True)
        title_filter = expr if title_filter is None else (title_filter | expr)

    table = dataset.to_table(columns=columns, filter=title_filter)
    df = table.to_pandas(types_mapper=None)
    for col in KEEP_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    df["matched"] = matched_keywords(df["title"], keywords)
    return df


def matched_keywords(title: pd.Series, keywords: list[str]) -> list[str]:
    lowered = [k.lower() for k in keywords]
    return [", ".join(k for k, kl in zip(keywords, lowered) if kl in t)
            for t in title.fillna("").str.lower()]


def title_has_any(title: pd.Series, keywords: list[str]) -> pd.Series:
    lowered = title.fillna("").str.lower()
    mask = pd.Series(False, index=title.index)
    for kw in keywords:
        mask |= lowered.str.contains(kw.lower(), regex=False)
    return mask


def _text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.lower()


def classify_workplace(df: pd.DataFrame) -> pd.Series:
    loc = _text(df["location"])
    commit = _text(df["commitment"])
    title = _text(df["title"])
    flag_remote = df["is_remote"].astype("object").map(lambda v: v is True)

    blob = loc + " || " + commit + " || " + title
    is_remote = flag_remote | blob.str.contains("remote", regex=False) | blob.str.contains("work from home", regex=False)
    is_hybrid = blob.str.contains("hybrid", regex=False)

    out = pd.Series("onsite", index=df.index, dtype="object")
    out[loc.eq("") & commit.eq("") & ~flag_remote] = "unknown"
    out[is_hybrid] = "hybrid"
    out[is_remote] = "remote"  # remote wins over hybrid when both appear
    return out


def _contains_any(series: pd.Series, needles: list[str]) -> pd.Series:
    text = _text(series)
    mask = pd.Series(False, index=series.index)
    for n in needles:
        mask |= text.str.contains(n, regex=False)
    return mask


_OTHER_PLACE_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(t) for t in
                        OTHER_COUNTRY_TERMS + MULTI_REGION_TERMS + NON_DE_CITIES) + r")\b",
    re.IGNORECASE,
)


def names_other_country(series: pd.Series) -> pd.Series:
    """True where the location text references a country / region / city that is
    not Germany (i.e. the role is multi-country or elsewhere, not Germany-only)."""
    return _text(series).str.contains(_OTHER_PLACE_RE)


def filter_single_country(df: pd.DataFrame, country: str) -> tuple[pd.DataFrame, int]:
    """Keep only rows whose location names Germany and no other country/region."""
    if country.upper() != "DE":
        log(f"--country {country} is not DE; skipping the Germany-only location filter")
        return df, 0
    drop = names_other_country(df["location"])
    # Structured signal: a non-DE country code is authoritative (catches e.g.
    # "New Berlin, Wisconsin" tagged country_iso=US).
    iso = _text(df["country_iso"])
    drop |= iso.ne("") & iso.ne(country.lower())
    return df[~drop].reset_index(drop=True), int(drop.sum())


def berlin_slice(df: pd.DataFrame, city: str) -> pd.DataFrame:
    hit = _text(df["location"]).str.contains(city.lower(), regex=False)
    out = df[hit].copy()
    out["workplace"] = classify_workplace(out)
    out["bucket"] = "berlin"
    return out


def germany_remote_slice(df: pd.DataFrame, country: str, include_eu: bool) -> pd.DataFrame:
    workplace = classify_workplace(df)
    remote = workplace.eq("remote")

    in_germany = _text(df["country_iso"]).eq(country.lower())
    in_germany |= _contains_any(df["location"], GERMANY_TEXT + GERMAN_CITIES)

    if include_eu:
        eu_remote = _contains_any(df["location"], EU_REMOTE_TEXT) | _text(df["region"]).eq("europe")
        in_scope = in_germany | eu_remote
    else:
        in_scope = in_germany

    out = df[remote & in_scope].copy()
    out["workplace"] = "remote"
    out["bucket"] = "germany-remote"
    return out


def combine(berlin: pd.DataFrame, germany: pd.DataFrame) -> pd.DataFrame:
    both_urls = set(berlin["url"]) & set(germany["url"])
    df = pd.concat([berlin, germany], ignore_index=True)
    df.loc[df["url"].isin(both_urls), "bucket"] = "both"
    df = df.drop_duplicates(subset="url", keep="first").reset_index(drop=True)
    df["posted_at_dt"] = pd.to_datetime(df["posted_at"], errors="coerce", utc=True)
    df = df.sort_values("posted_at_dt", ascending=False, na_position="last").reset_index(drop=True)
    return df


def apply_age_filter(df: pd.DataFrame, max_age_days: int | None,
                     *, require_date: bool) -> pd.DataFrame:
    if not max_age_days:
        return df
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    recent = df["posted_at_dt"] >= cutoff
    # Many ATS sources in this dataset expose no posted_at at all. By default we
    # keep undated rows (can't prove they're stale); --require-date drops them so
    # "max N days old" means exactly that.
    keep = recent if require_date else (df["posted_at_dt"].isna() | recent)
    return df[keep].reset_index(drop=True)


def berlin_view(combined: pd.DataFrame) -> pd.DataFrame:
    v = combined[combined["bucket"].isin(["berlin", "both"])].copy()
    v["_o"] = v["workplace"].map(WORKPLACE_ORDER).fillna(9)
    return v.sort_values(["_o", "posted_at_dt"], ascending=[True, False], na_position="last")


def germany_view(combined: pd.DataFrame) -> pd.DataFrame:
    return combined[combined["bucket"].isin(["germany-remote", "both"])].copy()


def process_profile(raw: pd.DataFrame, keywords: list[str], args: argparse.Namespace) -> dict:
    """Run the Berlin + Germany-remote pipeline for one profile's keywords."""
    sub = raw[title_has_any(raw["title"], keywords)].copy()
    sub["matched"] = matched_keywords(sub["title"], keywords)

    berlin = berlin_slice(sub, args.city)
    if args.berlin_workplace:
        berlin = berlin[berlin["workplace"].isin(args.berlin_workplace)].copy()
    germany = germany_remote_slice(sub, args.country, args.include_eu_remote)

    combined = combine(berlin, germany)
    combined = apply_age_filter(combined, args.max_age_days, require_date=args.require_date)
    dropped = 0
    if not args.allow_multi_country:
        combined, dropped = filter_single_country(combined, args.country)

    return {
        "berlin": berlin_view(combined),
        "germany": germany_view(combined),
        "combined": combined,
        "dropped_multi_country": dropped,
    }


def show(df: pd.DataFrame, title: str, n: int) -> None:
    print(f"\n{'=' * 78}\n{title}  ({len(df)} roles)\n{'=' * 78}")
    if df.empty:
        print("  (none)")
        return
    cols = ["posted_at_dt", "workplace", "title", "company", "location"]
    view = df[cols].head(n).copy()
    view["posted_at_dt"] = view["posted_at_dt"].dt.strftime("%Y-%m-%d").fillna("?")
    view["title"] = view["title"].str.slice(0, 45)
    view["company"] = view["company"].fillna("?").str.slice(0, 22)
    view["location"] = view["location"].fillna("?").str.slice(0, 30)
    with pd.option_context("display.max_rows", None, "display.width", 200,
                           "display.max_colwidth", 50):
        print(view.to_string(index=False,
                             header=["posted", "work", "title", "company", "location"]))
    if len(df) > n:
        print(f"  ... {len(df) - n} more in the CSV / HTML")


# --------------------------------------------------------------------------- HTML

HTML_CSS = """
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { font: 15px/1.5 system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
       margin: 0; padding: 1.5rem clamp(1rem, 4vw, 3rem); background: Canvas; color: CanvasText; }
h1 { margin: 0 0 .25rem; font-size: 1.4rem; }
h2 { margin: 2.5rem 0 .5rem; font-size: 1.25rem; border-bottom: 2px solid CanvasText; padding-bottom: .2rem; }
h3 { margin: 1.4rem 0 .4rem; font-size: 1rem; }
.meta { color: GrayText; font-size: .85rem; margin-bottom: 1rem; }
.nav { display: flex; gap: .6rem; flex-wrap: wrap; }
.nav a { color: LinkText; text-decoration: none; font-size: .85rem; }
.nav a:hover { text-decoration: underline; }
.controls { position: sticky; top: 0; background: Canvas; padding: .6rem 0; z-index: 2;
            display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; border-bottom: 1px solid GrayText; }
#q { flex: 1 1 240px; padding: .45rem .6rem; font: inherit; border: 1px solid GrayText;
     border-radius: 6px; background: Field; color: FieldText; }
.wp { padding: .35rem .6rem; border: 1px solid GrayText; border-radius: 999px;
      background: transparent; color: inherit; cursor: pointer; font: inherit; }
.wp.active { background: CanvasText; color: Canvas; }
table { border-collapse: collapse; width: 100%; margin-bottom: 1rem; font-size: .9rem; }
th, td { text-align: left; padding: .4rem .55rem; border-bottom: 1px solid color-mix(in srgb, GrayText 35%, transparent); vertical-align: top; }
th { cursor: pointer; user-select: none; white-space: nowrap; position: sticky; top: 3.1rem; background: Canvas; }
th:hover { color: LinkText; }
tr:hover td { background: color-mix(in srgb, GrayText 12%, transparent); }
td.title a { font-weight: 600; text-decoration: none; color: LinkText; }
td.title a:hover { text-decoration: underline; }
.apply { font-size: .8rem; color: GrayText; text-decoration: none; margin-left: .4rem; }
.apply:hover { color: LinkText; }
.tag { display: inline-block; padding: .05rem .4rem; border-radius: 999px; font-size: .75rem;
       border: 1px solid GrayText; }
.wp-remote { background: color-mix(in srgb, green 22%, transparent); }
.wp-hybrid { background: color-mix(in srgb, orange 25%, transparent); }
.wp-onsite { background: transparent; color: GrayText; }
.wp-unknown { background: transparent; color: GrayText; }
.count { color: GrayText; font-weight: normal; font-size: .9rem; }
.empty { color: GrayText; font-style: italic; }
"""

HTML_JS = """
const q = document.getElementById('q');
q.addEventListener('input', () => {
  const t = q.value.trim().toLowerCase();
  document.querySelectorAll('tbody tr').forEach(tr => {
    tr.dataset.hideQ = t && !tr.textContent.toLowerCase().includes(t) ? '1' : '';
    applyRow(tr);
  });
});
function applyRow(tr){ tr.style.display = (tr.dataset.hideQ || tr.dataset.hideWp) ? 'none' : ''; }
document.querySelectorAll('.wp').forEach(btn => btn.addEventListener('click', () => {
  btn.parentElement.querySelectorAll('.wp').forEach(b => b.classList.toggle('active', b === btn));
  const want = btn.dataset.wp;
  const table = document.querySelector(btn.dataset.target);
  table.querySelectorAll('tbody tr').forEach(tr => {
    tr.dataset.hideWp = (want === 'all' || tr.dataset.wp === want) ? '' : '1';
    applyRow(tr);
  });
}));
document.querySelectorAll('th[data-k]').forEach(th => th.addEventListener('click', () => {
  const table = th.closest('table');
  const idx = [...th.parentElement.children].indexOf(th);
  const asc = !(th.dataset.asc === '1'); th.dataset.asc = asc ? '1' : '0';
  const rows = [...table.querySelectorAll('tbody tr')];
  rows.sort((a, b) => {
    const x = a.children[idx].dataset.v ?? a.children[idx].textContent;
    const y = b.children[idx].dataset.v ?? b.children[idx].textContent;
    return (x < y ? -1 : x > y ? 1 : 0) * (asc ? 1 : -1);
  });
  const tb = table.querySelector('tbody'); rows.forEach(r => tb.appendChild(r));
}));
"""


def _cell(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NA:
        return ""
    return html.escape(str(value))


def _row_html(r: pd.Series) -> str:
    posted = r["posted_at_dt"]
    posted_txt = posted.strftime("%Y-%m-%d") if pd.notna(posted) else "–"
    posted_v = posted.strftime("%Y-%m-%d") if pd.notna(posted) else "0000"
    wp = str(r["workplace"])
    url = "" if pd.isna(r["url"]) else str(r["url"])
    apply_url = "" if pd.isna(r["apply_url"]) else str(r["apply_url"])
    title = _cell(r["title"]) or "(untitled)"
    title_html = f'<a href="{html.escape(url)}" target="_blank" rel="noopener">{title}</a>' if url else title
    if apply_url and apply_url != url:
        title_html += f'<a class="apply" href="{html.escape(apply_url)}" target="_blank" rel="noopener">apply&nearr;</a>'
    salary = _cell(r["salary_summary"])
    return (
        f'<tr data-wp="{html.escape(wp)}">'
        f'<td data-v="{posted_v}">{posted_txt}</td>'
        f'<td data-v="{WORKPLACE_ORDER.get(wp, 9)}"><span class="tag wp-{html.escape(wp)}">{html.escape(wp)}</span></td>'
        f'<td class="title">{title_html}</td>'
        f'<td>{_cell(r["company"])}</td>'
        f'<td>{_cell(r["location"])}</td>'
        f'<td>{_cell(r["employment_type"])}</td>'
        f'<td>{salary}</td>'
        f'<td>{_cell(r["matched"])}</td>'
        f'<td>{_cell(r["ats_type"])}</td>'
        f'</tr>'
    )


def _table_html(df: pd.DataFrame, table_id: str, *, workplace_filter: bool) -> str:
    if df.empty:
        return '<p class="empty">No matching roles.</p>'
    heads = ["Posted", "Work", "Title", "Company", "Location", "Type", "Salary", "Keyword", "Source"]
    ths = "".join(f'<th data-k>{h}</th>' for h in heads)
    body = "".join(_row_html(r) for _, r in df.iterrows())
    buttons = ""
    if workplace_filter:
        opts = ["all", "remote", "hybrid", "onsite", "unknown"]
        buttons = '<div class="controls">' + "".join(
            f'<button class="wp{" active" if o == "all" else ""}" data-wp="{o}" '
            f'data-target="#{table_id}">{o}</button>' for o in opts
        ) + '</div>'
    return f'{buttons}<table id="{table_id}"><thead><tr>{ths}</tr></thead><tbody>{body}</tbody></table>'


def _section_html(name: str, berlin: pd.DataFrame, germany: pd.DataFrame, keywords: list[str]) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    kw = ", ".join(html.escape(k) for k in keywords)
    return f"""
<section id="p-{slug}">
<h2>{html.escape(name.title())} <span class="count">&mdash; titles: {kw}</span></h2>
<h3>Berlin <span class="count">&mdash; {len(berlin)} roles (remote/hybrid first)</span></h3>
{_table_html(berlin, f"berlin-{slug}", workplace_filter=True)}
<h3>Germany-wide remote <span class="count">&mdash; {len(germany)} roles</span></h3>
{_table_html(germany, f"germany-{slug}", workplace_filter=False)}
</section>
"""


def write_html(path: Path, sections: list[dict], meta: dict) -> None:
    filt = (
        f'city: {html.escape(meta["city"])} &nbsp;|&nbsp; country: {html.escape(meta["country"])}'
    )
    if meta.get("max_age_days"):
        filt += f' &nbsp;|&nbsp; max age: {meta["max_age_days"]}d'
        if meta.get("require_date"):
            filt += " (dated only)"
    if meta.get("include_eu_remote"):
        filt += " &nbsp;|&nbsp; +EU-wide remote"
    if meta.get("single_country_only"):
        filt += " &nbsp;|&nbsp; Germany-only locations"

    nav, body, totals = [], [], []
    for s in sections:
        slug = re.sub(r"[^a-z0-9]+", "-", s["name"].lower()).strip("-")
        nav.append(f'<a href="#p-{slug}">{html.escape(s["name"].title())}</a>')
        totals.append(f'{html.escape(s["name"].title())}: {len(s["berlin"])} Berlin / {len(s["germany"])} remote')
        body.append(_section_html(s["name"], s["berlin"], s["germany"], s["keywords"]))

    doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Job search - {meta["date"]}</title>
<style>{HTML_CSS}</style></head><body>
<h1>Job search &mdash; {meta["date"]}</h1>
<div class="meta">
  Generated {meta["generated"]} &nbsp;&middot;&nbsp; dataset snapshot {meta["snapshot"]}<br>
  {filt}<br>
  {" &nbsp;&middot;&nbsp; ".join(totals)}
</div>
<div class="controls">
  <input id="q" type="search" placeholder="filter all tables (title, company, location, source&hellip;)">
  <span class="nav">{" ".join(nav)}</span>
</div>
{"".join(body)}
<script>{HTML_JS}</script>
</body></html>
"""
    path.write_text(doc, encoding="utf-8")


# --------------------------------------------------------------------------- CLI

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profiles", nargs="+", default=DEFAULT_PROFILES, choices=list(PROFILES),
                   help=f"which search profiles to run (default: {DEFAULT_PROFILES}). "
                        f"Each is a title-keyword set: "
                        + "; ".join(f"{k} = {', '.join(v)}" for k, v in PROFILES.items()))
    p.add_argument("--keywords", nargs="+", metavar="KW",
                   help="ad-hoc title keywords; overrides --profiles with a single 'custom' profile")
    p.add_argument("--city", default="Berlin", help="city for the location search (default: Berlin)")
    p.add_argument("--country", default="DE", help="ISO country code for the remote search (default: DE)")
    p.add_argument("--sources", nargs="+", metavar="ATS",
                   help="restrict to these ATS slices (e.g. greenhouse lever ashby). "
                        "Downloads only those slices instead of the full 2.3 GB snapshot.")
    p.add_argument("--berlin-workplace", nargs="+",
                   choices=["remote", "hybrid", "onsite", "unknown"],
                   help="keep only these work arrangements in the Berlin results "
                        "(default: keep all, sorted with remote/hybrid first)")
    p.add_argument("--include-eu-remote", action="store_true",
                   help="also include Europe/EMEA-wide remote roles in the Germany-remote search")
    p.add_argument("--allow-multi-country", action="store_true",
                   help="keep roles whose location also names another country / region / "
                        "foreign city (by default only Germany-only locations are kept)")
    p.add_argument("--max-age-days", type=int, help="drop roles whose posted_at is older than this")
    p.add_argument("--require-date", action="store_true",
                   help="with --max-age-days, also drop roles that have no posted_at "
                        "(most sources in this dataset don't publish one)")
    p.add_argument("--refresh", action="store_true", help="ignore the cache and re-download")
    p.add_argument("--open", action="store_true", help="open the generated HTML page in a browser")
    p.add_argument("--display", type=int, default=25, help="rows to print per bucket (default: 25)")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    manifest = fetch_manifest()
    stats = manifest.get("stats", {})
    snapshot = manifest.get("generated_at", "?")
    log(f"Dataset generated {snapshot}  "
        f"({stats.get('total_jobs'):,} jobs, {stats.get('ats_count')} sources)")

    if args.keywords:
        profiles = {"custom": args.keywords}
    else:
        profiles = {name: PROFILES[name] for name in args.profiles}
    all_keywords = list(dict.fromkeys(kw for kws in profiles.values() for kw in kws))
    kw_repr = ", ".join(repr(k) for k in all_keywords)

    if args.sources:
        unknown = [s for s in args.sources if s not in manifest["by_ats"]]
        if unknown:
            log(f"Unknown sources: {unknown}")
            log(f"Available: {', '.join(sorted(manifest['by_ats']))}")
            return 2
        frames = []
        for src in args.sources:
            path = ensure_parquet(manifest["by_ats"][src], CACHE_DIR / f"{src}.parquet",
                                  refresh=args.refresh)
            log(f"Scanning {src} for titles containing any of [{kw_repr}] ...")
            frames.append(scan_parquet(path, all_keywords))
        raw = pd.concat(frames, ignore_index=True)
    else:
        path = ensure_parquet(manifest["all"], CACHE_DIR / "all.parquet", refresh=args.refresh)
        log(f"Scanning full snapshot for titles containing any of [{kw_repr}] ...")
        raw = scan_parquet(path, all_keywords)

    log(f"{len(raw):,} roles matching [{kw_repr}] in the title")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    export_cols = ["profile", "bucket", "workplace", "posted_at", "title", "company", "location",
                   "country_iso", "employment_type", "salary_summary", "matched",
                   "ats_type", "url", "apply_url"]

    sections, all_rows = [], []
    for name, keywords in profiles.items():
        res = process_profile(raw, keywords, args)
        if res["dropped_multi_country"]:
            log(f"[{name}] dropped {res['dropped_multi_country']} roles naming another country/region")
        show(res["berlin"], f"[{name}] BERLIN roles ({args.city})", args.display)
        show(res["germany"], f"[{name}] GERMANY-WIDE REMOTE roles", args.display)
        sections.append({"name": name, "keywords": keywords,
                         "berlin": res["berlin"], "germany": res["germany"]})
        rows = res["combined"].copy()
        rows.insert(0, "profile", name)
        all_rows.append(rows)

    combined_all = pd.concat(all_rows, ignore_index=True)
    csv_path = RESULTS_DIR / f"jobs_{stamp}.csv"
    combined_all[export_cols].to_csv(csv_path, index=False)

    meta = {
        "date": stamp,
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "snapshot": snapshot,
        "city": args.city,
        "country": args.country,
        "max_age_days": args.max_age_days,
        "require_date": args.require_date,
        "include_eu_remote": args.include_eu_remote,
        "single_country_only": not args.allow_multi_country,
    }
    html_path = RESULTS_DIR / f"jobs_{stamp}.html"
    write_html(html_path, sections, meta)
    latest = RESULTS_DIR / "latest.html"
    shutil.copyfile(html_path, latest)

    print(f"\nWrote {len(combined_all)} rows:")
    print(f"  {csv_path}")
    print(f"  {html_path}")
    print(f"  {latest}")
    for s in sections:
        print(f"  [{s['name']}]  Berlin: {len(s['berlin'])}   Germany-remote: {len(s['germany'])}")

    if args.open:
        webbrowser.open(html_path.resolve().as_uri())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
