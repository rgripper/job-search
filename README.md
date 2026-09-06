# job-search

Search the free, no-auth [stapply.ai / ats-scrapers](https://github.com/kalil0321/ats-scrapers)
job dataset (~5M live jobs, 65 ATS sources) for engineering roles, and produce a
browsable HTML page you can skim each morning.

## What it does

`job_search.py` streams the hosted Parquet snapshot with pyarrow (title filter
pushed down, the huge `description`/`raw` columns skipped, so it runs in ~1 GB
RAM). For each **profile** (a named set of title keywords) it produces two lists:

1. **Berlin** – title contains one of the profile's keywords **and** the location
   mentions Berlin. Each row is tagged `remote` / `hybrid` / `onsite` / `unknown`
   from the free-form location + commitment text and the dataset's `is_remote` flag.
2. **Germany-wide remote** – title contains one of the keywords, the role looks
   remote, and it is tied to Germany (`country_iso == DE`, or the location text
   names Germany or a German city).

### Profiles

| profile | title keywords |
|---|---|
| `engineer` | engineer, tech lead |
| `product` | product manager, product management, product owner |

Edit `PROFILES` in `job_search.py` to add or tune them. `--profiles engineer product`
(default runs both); `--keywords <words…>` overrides with an ad-hoc `custom` profile.
The snapshot is scanned once for all selected profiles' keywords combined, then
partitioned per profile. Each profile gets its own section (with its own tables)
in the HTML.

By default both lists **drop roles whose location also names another country,
a multi-country region (Europe/EMEA/DACH/…), or a well-known foreign city** –
only Germany-only locations are kept. Multi-*city* German locations (e.g.
"Hamburg, Berlin, München") are fine. Pass `--allow-multi-country` to disable.

Outputs to `results/`:

| file | what |
|---|---|
| `jobs_<date>.csv` | flat table of every match (has a `profile` column) |
| `jobs_<date>.html` | one section per profile; clickable job + apply links, text filter, sortable columns, remote/hybrid/onsite toggle |
| `latest.html` | copy of today's page |

## Published page

A GitHub Actions workflow (`.github/workflows/daily.yml`) runs the search every
day at **15:00 UTC** (17:00 Berlin) and deploys the result to GitHub Pages:

**https://rgripper.github.io/job-search/**

15:00 UTC is deliberate: the stapply snapshot publishes at 14:30 UTC, so each run
picks up that day's data. Trigger it by hand from the Actions tab
(`Run workflow`) or with `gh workflow run "Daily job search"`.

The published site holds the current run's `index.html` (same as `latest.html`),
the dated `jobs_<date>.html`, and the `jobs_<date>.csv` (linked from the page
header). It is replaced on each deploy - there is no archive of past days.

> GitHub disables scheduled workflows on repos with no activity for 60 days;
> push a commit or re-enable it in the Actions tab if that happens.

## Daily use (local)

```bash
./daily.sh                 # engineer + product profiles, Berlin + DE-remote, posted in the last 7 days, opens latest.html
```

`daily.sh` uses `--max-age-days 7 --require-date`, so it only shows roles with a
real posting date inside the last 7 days (a small, sharp list — typically a few
dozen). Remove `--require-date` from the script for a much larger list that also
keeps roles with no posting date.

Or via cron:

```cron
0 8 * * *  cd /home/vladimir/github/job-search && ./daily.sh >> daily.log 2>&1
```

## Direct usage

```bash
uv run job_search.py                                    # engineer + product profiles, full snapshot
uv run job_search.py --profiles product                 # just the product profile
uv run job_search.py --keywords "data scientist" "ml engineer"   # ad-hoc custom profile
uv run job_search.py --city Munich --country DE
uv run job_search.py --berlin-workplace remote hybrid   # Berlin: drop onsite-only
uv run job_search.py --include-eu-remote                # also Europe/EMEA-wide remote roles
uv run job_search.py --allow-multi-country              # keep roles that also list another country
uv run job_search.py --max-age-days 30                  # last 30 days; keeps undated roles
uv run job_search.py --max-age-days 5 --require-date    # last 5 days, and must have a real posted_at
uv run job_search.py --sources greenhouse lever ashby   # scan only these ATS slices (smaller download)
uv run job_search.py --refresh                          # re-download the snapshot
uv run job_search.py --open                             # open the HTML page when done
```

## Notes / caveats

- The dataset is a **periodic snapshot**, not real-time. The page shows its
  `generated_at` timestamp. For a specific company's freshest postings, use the
  `ats_scrapers` per-company scrapers directly.
- `posted_at` is **sparse** – only a minority of sources publish it. `--max-age-days`
  keeps undated roles by default; add `--require-date` to drop them.
- No dedicated "hybrid" field exists, so `hybrid` is inferred from text and
  undercounts. Many Berlin roles tagged `onsite` here are hybrid in practice.
- `country_iso` is null for many sources; Germany attribution falls back to a
  city-name list in `job_search.py` (`GERMAN_CITIES`), extend it as needed.
- First run downloads `all.parquet` (~2.3 GB) to `.cache/` and verifies its
  SHA-256 against the manifest. Subsequent runs reuse it (~5 s scan).
