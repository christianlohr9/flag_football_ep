# Phase M3-3: Explosiveness & Efficiency - Research

**Researched:** 2026-09-03
**Domain:** Football analytics metric design (explosive-play definition, success-rate/efficiency
reproduction, small-sample handling) for a 5v5, 50-yard-field flag-football corpus
**Confidence:** MEDIUM (literature/critique side is HIGH -- multiple independent sources agree;
the flag-football-specific calibration is HIGH because it is computed directly on our own corpus;
the HC's workbook `Efficiency` semantics are LOW -- reverse-engineering it from down/distance/
yards did not converge, see Open Questions)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **EXP-D01 Research before definition:** no metric is coded before `docs/explosiveness-recherche.md` (German) exists with sources; the HC's definition is reproduced FIRST as the baseline row so every alternative is compared against his number on the same plays.
- **EXP-D02 Flag-football specifics matter:** 5v5, 50-yard field (`yardline_50` ∈ [0,50]), 4-down series to midfield/goal, no rushing across the LOS by the QB (rules), pass-heavy — NFL yard thresholds are not transferable 1:1; the proposal must be calibrated on OUR distribution (2023–2026 corpus incl. HC rows once M3-2 unlocks them; until then our Hudl/IFAF plays).
- **EXP-D03 Prefer smooth/relative over cliff thresholds:** e.g. explosiveness score = probability mass above a down-&-distance-conditional quantile, or EPA-per-play z-score; keep ONE simple headline number for coaches ("Explosive %" stays as a name if the definition is defensible) plus the continuous version.
- **EXP-D04 Efficiency:** document exactly how the HC computes `Efficiency` in his workbook (formula cells, data_only=False) and reproduce it; then relate it to success rate (EPA > 0) so the two vocabularies are reconciled, not replaced.
- **EXP-D05 Honest small-sample handling:** per-player rates carry n; shrinkage/minimum-attempt rules proposed (ties into the Timo Riske questions, BL-05).
- PII: player names only via roster mapping; docs show ids/initials or aggregated tables.

### Claude's Discretion
- Exact metric formulas, quantile levels, module layout (`features/` vs `reports/aggregate.py`), plotting.

### Deferred Ideas (OUT OF SCOPE)
- Win-driver analysis (BL-04) — uses these metrics later.

### Phase Boundary (from CONTEXT.md, for reference)
Not this phase: the report product (M3-4 renders the metrics), EP/WP retraining (M3-2), any CV
file. The user's stated concern driving this whole phase: "So krasse Thresholds sind in meinen
Augen immer ein Problem" ("hard thresholds are always a problem in my view") — "was ist, wenn eine
Spielerin nur 11 Yards erzielt?" ("what if a player only gains 11 yards?"). The deliverable must
make the 11-vs-12-yard cliff visibly disappear.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| HC-04 | Recherchierte, begründete Explosiveness-Definition (NFL-/PFF-Konventionen, Success Rate, EPA-basiert) statt der harten ">12 Yards und/oder positive EPA"-Schwelle; auf unseren Daten berechnet und gegen die HC-Werte gestellt; Efficiency-Definition des HC dokumentiert und reproduziert. | Literature review (Summary, State of the Art, Sources) identifies the efficiency/explosiveness dichotomy and three candidate replacement definitions (Architecture Patterns 1-3). HC's literal workbook formulas for `ExpPlays`/`Explosive %` and `Efficiency` are pinned verbatim (HC's Own Formulas / Pattern 1, Pitfall 1-2) as the required baseline row. Our-corpus distribution (Summary, Common Pitfalls) supplies the calibration data EXP-D02 requires. `docs/explosiveness-recherche.md` is the required German research artifact (EXP-D01), written this session. |

</phase_requirements>

## Summary

Two conceptually separate questions hide inside the HC's single "Explosive %" rule
(">12 yards and/or positive EPA"): **efficiency** ("was this play good enough, yes/no,
context-adjusted") and **explosiveness** ("given it was good, how big was it"). NFL and college
analytics literature (Bill Connelly's Success Rate / IsoPPP split, nflverse's `EPA > 0` success
definition, PFF's own explosive-play data study) treat these as two different metrics with two
different statistical properties: efficiency/success rate is stable and should drive most of a
composite score; explosiveness/IsoPPP is inherently noisy and should never be reported without its
sample size. Computed on our own corpus, this distinction turns out to matter concretely: the
HC's own verbally-stated "and/or EPA" rule is 52.8% satisfied almost entirely by the EPA clause
(success rate alone is 52.2%) -- the yardage clause adds only 89 plays (0.6%) on top. His rule is,
in practice, already measuring efficiency, mislabeled as explosiveness. Separately, his workbook's
actual `Explosive %` formula (verified from the formula cells) checks ONLY `yards_gained > 12` --
no EPA condition is implemented there at all, contradicting the verbal description he gave the
user. Both discrepancies are open questions for the HC, documented below, not assumptions we
resolved silently.

On our corpus (15,006 run/pass scrimmage plays, downs 1-4), the "cliff zone" the user worried
about (10-12 yards, immediately around the HC's cutoff) holds 11.5% of all plays -- not a rare
edge case, a dense part of the distribution. This is the quantitative version of the user's "what
about 11 yards" objection.

**Primary recommendation:** Reproduce the HC's workbook formula literally as the baseline row
(yards-only, per EXP-D01), then propose an EPA-magnitude-on-successful-plays metric (IsoPPP-style,
Candidate B below) as the threshold-free explosiveness replacement, paired with `epa > 0` as the
efficiency/success-rate metric (nflverse convention) -- both computed from the `epa` column that
`features/mutations.py::add_ep_variables` already produces, no new EP-model work required. Present
a continuous/smooth score (Candidate C) as a supporting chart, not the headline number.

## Architectural Responsibility Map

This project has no browser/API/CDN tiers -- it is a local Python batch pipeline (ingest ->
canonical plays -> features/reports -> rendered HTML). Tiers below are this project's actual
layers.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Explosiveness/Efficiency formula (the metric itself) | Feature/Metrics Layer (`features/` or `reports/aggregate.py`) | -- | Pure computation on canonical `epa`/`yards_gained` columns, no I/O; mirrors `estimate_fourth_down_rates`/`estimate_pat_baselines` pattern in `features/mutations.py` |
| HC-baseline reproduction (workbook formula on our data) | Feature/Metrics Layer | Data Layer (`data/raw/hc_files/`, gitignored) | Needs the workbook's literal formula (read once, hard-coded as a documented constant) applied to canonical plays, not a live Excel read per run |
| Small-sample handling (shrinkage/muting) | Feature/Metrics Layer | -- | Extends the existing `MUTED_MIN_N`/Clopper-Pearson convention in `reports/aggregate.py`, does not replace it |
| Rendered coach-facing output (tables/charts) | Report Rendering Layer (`reports/own_team.py`, `reports/render.py`) | -- | Explicitly out of scope for M3-3 (deferred to M3-4 per CONTEXT "Not this phase") -- this phase produces the metrics module + tests only |
| Canonical `epa`/`yards_gained`/`down`/`yardline_50` source columns | Data Layer (`data/processed/plays_scored.parquet`, `features/mutations.py::add_ep_variables`) | -- | Already exists (M1/M3-2 output); this phase consumes it, does not modify it |

## Standard Stack

No new external packages. Every technique below (quantiles, Clopper-Pearson CI, logistic/smooth
scoring) is implementable with the project's existing dependencies.

### Core (already project dependencies -- no install needed)
| Library | Version (installed, verified via `pip show`) | Purpose | Why Standard |
|---------|---------|---------|--------------|
| polars | 1.5.0 `[VERIFIED: installed in project .venv]` | Quantile/group-by computation for the distribution calibration and the metric itself | Already the project's sole dataframe library; `pl.Expr.quantile()` is the native, non-hand-rolled percentile primitive |
| scipy | 1.14.1 `[VERIFIED: installed in project .venv]` | `scipy.stats.binomtest(...).proportion_ci()` for rate confidence intervals (same call `reports/aggregate.py::rate_table` already uses) | Reuses the project's existing small-sample-honesty convention instead of inventing a second one |
| openpyxl | 3.1.5 `[VERIFIED: installed in project .venv]` | One-time read of the HC workbook's formula cells to pin the baseline formula as a documented constant | Already used by `ingest/hc_workbook.py`; `data_only=False` for formula text (this phase), `data_only=True` for resolved values (ingest phase) |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| scipy.stats (norm/logistic) | 1.14.1 (same install) | Optional smooth/continuous scoring (Candidate C) via `scipy.stats.logistic.cdf` or a hand-rolled sigmoid on a standardized EPA/yards z-score | Only if Candidate C's continuous score is implemented; a two-line `1 / (1 + exp(-z))` is also acceptable and avoids the extra scipy import -- Claude's discretion per CONTEXT |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Empirical quantile (Candidate A) | Fixed NFL-style thresholds (20+ pass / 10+ run) | NFL thresholds are not transferable to a 50-yard 5v5 field with a different scoring geometry (EXP-D02 locks this out) -- rejected, not proposed as a candidate |
| EPA-magnitude on successes (Candidate B, recommended) | IsoPPP computed from a hand-built equivalent-points table (college-style, independent of our own EP model) | Our `epa` column already IS an EP-model-based equivalent-points delta -- building a parallel points table would duplicate M1/M3-2 work for no benefit |
| Empirical-Bayes/beta-binomial shrinkage for small samples | Plain Clopper-Pearson CI + `MUTED_MIN_N` suppression (current project convention) | Shrinkage is the more "correct" small-sample answer per the literature, but the project has an established, tested, coach-legible convention already (`reports/aggregate.py`) -- recommend extending it, not replacing it, unless the planner decides otherwise (Claude's discretion) |

**Installation:** none required -- all three core libraries are already pinned in `pyproject.toml`
(`polars>=1.5.0`, `scipy>=1.14.1`, and openpyxl per the `hc_workbook.py` ingest module's existing
dependency).

**Version verification:** confirmed via `./.venv/bin/python3 -c "import polars, scipy, openpyxl; print(...)"`
against the project's own `.venv` (not a fresh install) -- see Environment Availability.

## Package Legitimacy Audit

**Not applicable.** This phase introduces zero new external packages -- every computation
(quantiles, Clopper-Pearson intervals, optional logistic scoring) uses polars/scipy, both already
installed project dependencies verified present in `.venv` (see Standard Stack). The Package
Legitimacy Gate protocol is skipped per its own trigger condition ("whenever this phase installs
external packages").

## Architecture Patterns

### System Architecture Diagram

```
canonical plays_scored.parquet (epa, yards_gained, down, yardline_50, play_type)
        |
        v
[NEW] features/explosiveness.py  (or reports/aggregate.py -- planner's discretion, CONTEXT)
  - baseline_hc_explosive_rate(df)       -> reproduces workbook formula literally (yards>12 only)
  - success_rate(df)                      -> share of epa > 0                    (efficiency)
  - explosive_rate_epa_magnitude(df)      -> share of (epa>0 AND epa>=q80|epa>0) (Candidate B)
  - explosive_score_continuous(df)        -> per-play smooth score in [0,1]      (Candidate C, optional)
  - with sample-size-aware output (n, muted flag, Clopper-Pearson CI) per group
        |
        v
per-QB / per-team rollup tables (mirrors _epa_rollup_by in reports/own_team.py)
        |
        v
[OUT OF SCOPE for M3-3] reports/own_team.py or a new report section (M3-4 renders it)
```

Entry point: canonical `plays_scored.parquet` (already produced upstream by M1/M3-2). Processing:
one new pure-function module computing 2-3 candidate metrics plus the literal HC baseline, each
paired with its sample size. Decision point: which candidate becomes the coach-facing headline
number is a CONTEXT/discuss-phase decision informed by this research, not something this research
document decides unilaterally. Output: a metrics table ready for M3-4's report to consume -- this
phase does not render HTML (CONTEXT: "Not this phase: the report product").

### Recommended Project Structure
```
src/flag_football_ep/
├── features/
│   └── mutations.py          # existing: epa/ep already computed here (add_ep_variables)
│   └── explosiveness.py      # NEW (or fold into reports/aggregate.py -- discretion):
│                              #   baseline_hc_explosive_rate, success_rate,
│                              #   explosive_rate_epa_magnitude, HC-workbook-formula constant
tests/
└── test_features_explosiveness.py   # NEW, mirrors tests/test_features_mutations.py conventions
```

### Pattern 1: Reproduce the baseline literally before proposing alternatives (EXP-D01)
**What:** Hard-code the HC workbook's exact formula (`yards_gained > 12`, no EPA term -- verified
from the formula cells, see HC's Own Formulas below) as a named constant/function, and compute it
on OUR canonical corpus before writing any alternative candidate.
**When to use:** Always, per the locked CONTEXT decision -- every alternative must be compared
against the HC's number on the same plays, not against a re-interpretation of what the HC meant.
**Example:**
```python
# Source: reverse-engineered from Player Analysis All Camps!S2 formula cell (this research)
# =COUNTIFS(Data!$P$2:$P$19562,$A2, Data!$J$2:$J$19562, ">12")  ->  ExpPlays / Attempts
def baseline_hc_explosive_rate(plays: pl.DataFrame) -> pl.DataFrame:
    """Literal reproduction of the workbook's Explosive % formula: share of pass attempts
    with yards_gained > 12. NOTE: the workbook formula has no EPA term, despite the HC's
    verbal description to the user -- this is the workbook's literal rule, not the verbal one
    (see Open Questions)."""
    attempts = plays.filter(pl.col("play_type") == "pass")
    return attempts.group_by("qb").agg(
        exp_plays=(pl.col("yards_gained") > 12).sum(),
        attempts=pl.len(),
    ).with_columns(explosive_pct=pl.col("exp_plays") / pl.col("attempts"))
```

### Pattern 2: Success rate as `epa > 0` (nflverse convention)
**What:** A play is a "success" when `epa > 0` -- no down/distance bucket needed, the EP model
already encodes situational context.
**When to use:** As the efficiency/reliability headline metric (Connelly: this should carry most
of any composite weight, ~86% by his S&P+ analogy).
**Example:**
```python
# Source: nflverse convention (EPA > 0 == success), reproduced with the project's existing
# binomtest/Clopper-Pearson pattern from reports/aggregate.py::rate_table
def success_rate(plays: pl.DataFrame) -> pl.DataFrame:
    scored = plays.filter(pl.col("epa").is_not_null())
    # reuse reports.aggregate.rate_table(scored, [...], pl.col("epa") > 0) rather than
    # hand-rolling a second binomtest call site
```

### Pattern 3: Explosiveness as EPA-magnitude on successful plays (IsoPPP-style, Candidate B)
**What:** Among plays that already succeeded (`epa > 0`), flag the subset with `epa` at or above
a data-derived quantile (our corpus: q80 of successful-play EPA ≈ +2.3) as "explosive". No yard
threshold at all -- the cliff moves from a yard boundary (which the user objects to) to an EPA
boundary that already accounts for down, distance, field position and score context.
**Example:**
```python
# Source: Connelly's IsoPPP concept (mean equivalent-points on successes only), adapted to our
# own epa column and calibrated on our own corpus's q80(epa | epa>0) instead of a borrowed
# college value
def explosive_rate_epa_magnitude(plays: pl.DataFrame, *, epa_quantile: float = 0.80) -> pl.DataFrame:
    scored = plays.filter(pl.col("epa").is_not_null())
    threshold = scored.filter(pl.col("epa") > 0)["epa"].quantile(epa_quantile)
    return scored.with_columns(explosive=(pl.col("epa") > 0) & (pl.col("epa") >= threshold))
```

### Anti-Patterns to Avoid
- **Re-deriving the HC's `Efficiency` column (Data!O) from down/distance/yards:** three plausible
  formulas (literal conversion, half-distance success rule, `yards > 0`) were tested against the
  actual charted values and none exceeded 80% agreement (see Common Pitfalls #2). Treat this
  column as manually charted and opaque until the HC clarifies it -- do not ship a guessed formula
  as if it were verified.
- **Blending run and rush plays into one "Explosive %" without checking the workbook's own scope:**
  the workbook's `Attempts` denominator (`Comps+Incs+Sacks`) is pass-only per QB row; a metric that
  silently includes rush attempts in the same rate is not comparable to the HC's number (Open
  Question #3).
- **Reporting IsoPPP/explosive-magnitude without its `n`:** Connelly's own finding is that this
  metric is "dramatically unstable" season to season -- never report it as a bare percentage; pair
  it with `n` and a Clopper-Pearson-style interval, same as every other rate this project renders.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Rate confidence interval for a small-sample explosive/success rate | A hand-rolled normal approximation (`p ± 1.96*sqrt(p(1-p)/n)`) | `scipy.stats.binomtest(...).proportion_ci()` (Clopper-Pearson), exactly as `reports/aggregate.py::rate_table`/`features/mutations.py::estimate_pat_baselines` already do | This corpus's per-player counts are small (tens-to-low-hundreds); a normal approximation is known to break down at exactly this scale, and the project already has a working, tested Clopper-Pearson call site to reuse |
| Percentile/quantile of a yards or EPA distribution | Manual sort + index arithmetic | `pl.Expr.quantile()` (polars native) | Native, vectorized, already the project's dataframe library |
| Smooth/continuous explosiveness score (Candidate C) | A hand-tuned exponential curve with magic constants | A standardized-score sigmoid (`z = (x - median) / IQR`, then `1/(1+exp(-z))`) or `scipy.stats.logistic.cdf` | Both are one-line, well-understood, and avoid inventing an unverifiable curve shape; if this candidate is implemented, its exact scale (which `z` denominator) is Claude's discretion at plan time, not something this research prescribes further |
| Small-sample shrinkage for per-player explosive/efficiency rates (EXP-D05) | An ad hoc "if n < X, show grand mean instead" rule invented fresh for this phase | Extend the existing `MUTED_MIN_N`/Clopper-Pearson muting convention (`reports/aggregate.py`) first; only reach for a beta-binomial/empirical-Bayes shrinkage estimator (see Sources) if the planner decides muting alone is insufficient | The project has one working, tested small-sample-honesty convention already; a second, different one for this phase's metrics would fragment the coach-facing vocabulary the CONTEXT already worked to keep unified ("Explosive %" stays as a name if the definition is defensible") |

**Key insight:** Nothing here needs new infrastructure. The hard part of this phase is not
implementation, it is *deciding which of Connelly's two axes (efficiency vs. explosiveness) each
proposed number measures*, and being honest that the HC's current single number conflates them
(see Summary).

## Common Pitfalls

### Pitfall 1: Assuming the HC's verbal rule matches his spreadsheet formula
**What goes wrong:** Reproducing "yards>12 OR positive EPA" as the baseline, because that is what
the HC told the user, when the actual workbook formula (`Player Analysis All Camps!S2`) checks
`Data!J > 12` (yards only) with no EPA term anywhere in the formula chain.
**Why it happens:** The verbal description and the implemented formula diverge -- likely an
earlier mental model the HC has not yet encoded into the spreadsheet.
**How to avoid:** Reproduce the workbook's literal formula (verified via `data_only=False` cell
read, this research) as the EXP-D01 baseline row; separately report the verbal "and/or EPA" rule's
result (52.8% vs. 16.7%-yards-only) as a labeled comparison, and raise the discrepancy as an open
question rather than silently picking one.
**Warning signs:** A reproduced "Explosive %" that doesn't match the HC's own workbook output on
the same plays -- check which of the two rules (literal formula vs. verbal description) was used.

### Pitfall 2: Trying to reverse-engineer `Data!Efficiency` (column O) as a pure formula
**What goes wrong:** Assuming this manually-charted 0/1 column follows a clean down/distance/yards
rule (e.g., "gained >= 50% of distance on 1st down"), then shipping a formula that quietly
disagrees with the HC's real numbers on 20% of plays.
**Why it happens:** The column name ("Efficiency") suggests a formula, but this research tested
three plausible rules against the actual charted 0/1 values and none reached even 80% agreement
(see docs/explosiveness-recherche.md's HC formulas section) -- the values likely include manual
charting judgment (e.g. drop attribution, QB decision quality) beyond raw down/distance/yards.
**How to avoid:** Reproduce the workbook's literal `Efficiency` formula
(`SUMIF(Data!O by QB) / (Attempts+Drops)`) treating column O as an opaque per-play input, not
something to re-derive; flag the semantics as an open question for the HC (EXP-D04 requires
"document exactly how the HC computes Efficiency", not "guess and verify").
**Warning signs:** A "reproduced" Efficiency number that requires guessing what counts as
efficient -- if the formula can't be pinned to the actual charted values, don't guess.

### Pitfall 3: Denominator mismatch between the HC's per-QB "Explosive %" and a team-wide metric
**What goes wrong:** Computing a combined offense-wide explosive rate (run + pass) and comparing
it directly to the HC's per-QB `Explosive %`, which only counts pass attempts in its denominator
(`Attempts = Comps+Incs+Sacks`).
**Why it happens:** The workbook's per-QB table structure hides that rushing plays are tracked
elsewhere (`Carries`/`Rush Yards` columns 23-24) and never enter this QB-row's Explosive %.
**How to avoid:** State the denominator explicitly in every reproduced/proposed metric (pass-only
vs. all-scrimmage-plays); our own corpus shows run and pass have different explosive-play rates
(12.6% vs. 15.4% at yards>12) and different medians, so blending them silently changes the number.
**Warning signs:** A team-level rate that doesn't reconcile with any single QB row's rate even
after aggregation.

### Pitfall 4: Mistaking a field-position tendency for a distributional artifact
**What goes wrong:** Building down/field-zone-conditional percentile thresholds (Candidate A) and
being surprised that the "Eigene Hälfte" (own-half) zone's share of plays > 12 yards is only 1.2%,
concluding the field geometry caps gains there.
**Why it happens:** It looks like a ceiling effect, but `yardline_50` in the own-half zone (37-50)
is still 37-50 yards from the opponent goal line -- there is no geometric cap on a 20+ yard gain
from there. The near-zero explosive share is a real offensive tendency (conservative play-calling
deep in own territory to avoid a turnover-worthy mistake), not a data artifact.
**How to avoid:** If Candidate A (field-zone-conditional percentiles) is implemented, note that
zone-level thresholds will also encode play-calling conservatism, not just "what's achievable" --
document this rather than silently normalizing it away.
**Warning signs:** A field-zone threshold table where one zone's numbers look implausibly low/high
relative to the others without an explanation.

### Pitfall 5: Down `0` (PAT) rows polluting a distribution or rate calculation
**What goes wrong:** Computing "share of plays > 12 yards" or `epa > 0` over the full corpus
without filtering `down > 0` first; PAT attempts (`down == 0`, n=1,458 in our corpus) have
`yards_gained == 0` by construction and would silently deflate any rate.
**Why it happens:** `down` is a numeric column with `0` as a legitimate value (PAT), not a null --
an unfiltered `.filter(pl.col("down").is_not_null())` does not exclude it.
**How to avoid:** Filter `play_type.is_in(["run", "pass"]) & (down > 0)` before computing any
explosiveness/efficiency rate, mirroring `estimate_fourth_down_rates`'s `down == 4` predicate
pattern in `features/mutations.py`.
**Warning signs:** A rate that changes meaningfully when PAT rows are excluded/included.

## Code Examples

### Reproducing the workbook's `ExpPlays`/`Explosive %` formula on canonical plays
```python
# Source: this research, reverse-engineered from
# Player Analysis All Camps!R2:S2 formula cells (openpyxl data_only=False read)
# =COUNTIFS(Data!$P$2:$P$19562,$A2, Data!$J$2:$J$19562, ">12")
def hc_baseline_explosive(plays: pl.DataFrame) -> pl.DataFrame:
    pass_attempts = plays.filter(
        (pl.col("play_type") == "pass") & pl.col("yards_gained").is_not_null()
    )
    return (
        pass_attempts.group_by("qb")
        .agg(
            n=pl.len(),
            exp_plays=(pl.col("yards_gained") > 12).sum(),
        )
        .with_columns(explosive_pct=pl.col("exp_plays") / pl.col("n"))
    )
```

### Success rate (nflverse `EPA > 0` convention), reusing the project's rate_table
```python
# Source: nflverse convention + reports/aggregate.py's existing rate_table (this project)
from flag_football_ep.reports.aggregate import rate_table

def success_rate_by_qb(plays: pl.DataFrame) -> pl.DataFrame:
    scrimmage = plays.filter(
        pl.col("play_type").is_in(["run", "pass"])
        & (pl.col("down") > 0)
        & pl.col("epa").is_not_null()
    )
    return rate_table(scrimmage, ["qb"], pl.col("epa") > 0)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Flat 20-yard "explosive play" cutoff (Billick, 2011) | Play-type-split cutoffs (pass 20+/rush 10+) or EPA-magnitude-based (EPA>1.0, ~80th percentile) | Gradual shift through the 2010s-2020s as EPA-based analytics matured (nflfastR public since ~2018) | A single flat cutoff is now considered a simplification; most current NFL-analytics writers use either a play-type split or an EPA-based definition |
| Explosiveness as the primary offensive quality signal (older college-football commentary) | Efficiency/Success Rate as primary, explosiveness as a volatile secondary signal (Connelly, post-multi-season observation) | Connelly explicitly reversed his own earlier framing after observing IsoPPP's season-to-season instability | Directly relevant here: the HC's current rule effectively over-weights the volatile signal by conflating it with the stable one |

**Deprecated/outdated:** A single hard yard threshold as the sole explosive-play criterion is
widely critiqued in current literature (PFF, Sam Hoppen) as an oversimplification, though no
source claims it is "wrong" outright -- it remains in use for its interpretability, with the
tradeoff explicitly acknowledged by its own proponents.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `Data!O` ("Efficiency") in the HC workbook is manually charted rather than formula-derived, based on <80% agreement with three tested down/distance/yards rules | HC's Own Formulas, Pitfall 2 | If a fourth, untested formula actually explains it, our "manually charted" framing understates reproducibility -- low risk since EXP-D04 only requires literal reproduction, not full semantic explanation, and the open question is already flagged for the HC |
| A2 | 80th percentile of `epa` among successful plays (≈+2.3 in our corpus) is a reasonable "explosive" cut for Candidate B | Pattern 3, Standard Stack | This specific quantile level is a design choice, not a verified external standard (Sam Hoppen's own EPA>1.0 cutoff was also self-described as landing "around the 80th percentile" for the NFL, not derived from first principles) -- CONTEXT explicitly delegates "exact quantile levels" to Claude's discretion, so this is flagged, not silently locked |
| A3 | The HC's verbal "12 yards and/or positive EPA" rule and the workbook's literal "yards>12 only" formula are genuinely different (not a research/reading error) | Summary, HC's Own Formulas | Verified directly from the formula cell text (`data_only=False`); risk is low, but this is exactly the kind of discrepancy that should be confirmed with the HC before EXP-D04's "baseline" is finalized in the plan |

**HC's Own Formulas** (referenced above): documented in full under Architecture Patterns / Pattern
1 and Code Examples; source cells are `Player Analysis All Camps!R2:U2` and `Data!O` in
`data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx` (gitignored, PII).

## Open Questions

1. **Does the HC's `Explosive %` formula omit EPA intentionally, or is it a spreadsheet gap?**
   - What we know: the formula cell (`R2`/`S2`) checks only `Data!J > 12` (yards), no EPA term.
   - What's unclear: whether this is deliberate (yards-only was always the real rule; "and/or EPA"
     was aspirational) or an implementation gap the HC would want fixed.
   - Recommendation: reproduce the literal formula as the EXP-D01 baseline per CONTEXT, but surface
     both numbers (literal-formula 16.7% vs. verbal-rule 52.8%) to the HC explicitly at the
     October sync rather than picking one silently.

2. **What does `Data!Efficiency` (column O) actually encode?**
   - What we know: it's a per-play 0/1 (occasional outlier `9`) value, summed and divided by
     `Attempts+Drops` per QB. Three tested down/distance/yards formulas each explain <80% of it.
   - What's unclear: the exact charting rule (does it include broken-tackle/protection/decision
     quality judgment? is a `9` a data-entry error?).
   - Recommendation: reproduce the literal formula only; ask the HC directly (already logged as an
     open question in `docs/explosiveness-recherche.md`) rather than guessing further.

3. **Should "Explosive %"/"Efficiency" include rushing plays, or stay pass-only like the workbook?**
   - What we know: the workbook's per-QB `Attempts` denominator is pass-only.
   - What's unclear: whether a combined offense-wide metric (CONTEXT's "one simple headline
     number") should blend run+pass or report them separately, mirroring Connelly's own
     play-type-aware approach to success rate.
   - Recommendation: report both, since our own corpus shows meaningfully different rates by
     play type (12.6% vs 15.4% at yards>12) -- defer the final headline-number decision to the plan
     stage / discuss-phase.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| polars | Metric computation, quantiles | Yes | 1.5.0 (project `.venv`) | -- |
| scipy | Clopper-Pearson CI, optional logistic scoring | Yes | 1.14.1 (project `.venv`) | -- |
| openpyxl | One-time HC-workbook formula-cell verification (already done in this research; not needed again at plan/implementation time unless formulas are re-checked) | Yes | 3.1.5 (project `.venv`) | -- |
| `data/processed/plays_scored.parquet` | Corpus for calibration and testing | Yes (21,437 rows total; 15,006 run/pass scrimmage plays with `down` 1-4) | -- | -- |
| `data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx` | HC baseline formula source (gitignored, PII) | Yes, present locally | -- | If absent on another machine: the literal formula is already pinned as a documented constant in this research and in Pattern 1's code example, so re-reading the workbook is not required to implement the baseline |

**Missing dependencies with no fallback:** none.

**Missing dependencies with fallback:** none -- all required inputs are present.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (config: `pyproject.toml` `[tool.pytest.ini_options]`) |
| Config file | `pyproject.toml` (`testpaths = ["tests"]`, `addopts = "-q"`) |
| Quick run command | `./.venv/bin/pytest tests/test_features_explosiveness.py -q` (new file, plan creates it) |
| Full suite command | `./.venv/bin/pytest -q` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| HC-04 | HC baseline (`yards_gained > 12`, pass-only, per-QB) reproduces the workbook's literal `ExpPlays`/`Explosive %` formula on a fixture frame | unit | `pytest tests/test_features_explosiveness.py::test_hc_baseline_explosive -x` | ❌ Wave 0 |
| HC-04 | Success rate (`epa > 0`) matches `rate_table`'s existing Clopper-Pearson output shape/semantics | unit | `pytest tests/test_features_explosiveness.py::test_success_rate -x` | ❌ Wave 0 |
| HC-04 | Explosive-rate-EPA-magnitude (Candidate B) correctly computes the q80-of-successes threshold and flags plays at/above it | unit | `pytest tests/test_features_explosiveness.py::test_explosive_rate_epa_magnitude -x` | ❌ Wave 0 |
| HC-04 | Small-sample rows (n < MUTED_MIN_N) are muted, never hidden, consistent with `reports/aggregate.py` convention | unit | `pytest tests/test_features_explosiveness.py::test_muted_min_n_consistency -x` | ❌ Wave 0 |
| HC-04 | `down == 0` (PAT) rows are excluded from every rate (Pitfall 5 regression guard) | unit | `pytest tests/test_features_explosiveness.py::test_excludes_pat_rows -x` | ❌ Wave 0 |
| HC-04 | HC `Efficiency` formula (`SUMIF(Data!O)/(Attempts+Drops)`) reproduced literally from a fixture mirroring the workbook's charted 0/1 column, without asserting semantic correctness of column O itself | unit | `pytest tests/test_features_explosiveness.py::test_hc_baseline_efficiency -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `./.venv/bin/pytest tests/test_features_explosiveness.py -q`
- **Per wave merge:** `./.venv/bin/pytest -q` (full suite -- this module's `epa`/`yards_gained`
  dependency touches shared canonical-frame fixtures used across the test suite)
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_features_explosiveness.py` -- new file, covers HC-04 per the map above;
      use `flag_football_ep.testing.canonical_plays`/`canonical_plays_with_scores` factories
      (`src/flag_football_ep/testing.py`) for fixture frames, mirroring
      `tests/test_features_mutations.py`'s existing conventions -- do not invent a new fixture
      style.
- [ ] No new `conftest.py` fixtures needed -- `tests/conftest.py` is explicitly owned by phase
      01.2 plan 01 and later plans "must not edit this conftest"; use module-local fixtures or the
      `testing.py` factories instead.
- [ ] Framework install: none -- pytest and the `testing.py` factories already exist and are used
      by 100+ existing test files in this repo.

## Security Domain

`security_enforcement` is absent from `.planning/config.json` -- treated as enabled per the
protocol default. This phase is a local batch-analytics module with no network/auth/session
surface; most ASVS categories genuinely do not apply.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | No auth surface in this phase (local CLI/library code) |
| V3 Session Management | No | N/A |
| V4 Access Control | No | N/A |
| V5 Input Validation | Yes | Raise a named exception (mirroring `MissingFeatureColumns`/`DegenerateWeightRange` in `features/mutations.py`) when a required column (`epa`, `yards_gained`, `down`) is absent, rather than letting a silent `null` propagate into a rate calculation -- same discipline the existing module already enforces |
| V6 Cryptography | No | N/A |

### Known Threat Patterns for this stack
Not applicable -- this is an offline analytics module over locally-sourced parquet files with no
external input surface (STRIDE categories like tampering/spoofing/injection require a trust
boundary this module does not have). The one relevant discipline already covered under V5 above is
"fail loudly on malformed input columns" rather than a STRIDE-style threat.

## Sources

### Primary (HIGH confidence)
- `data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx` -- formula cells read
  directly via `openpyxl.load_workbook(..., data_only=False)` (`Player Analysis All Camps!R2:U2`,
  `Data!O` header/values) -- this research's own tool-verified extraction, not a secondary account
- `data/processed/plays_scored.parquet` -- distribution/EPA statistics computed directly via
  polars in this research session (n=15,006 scrimmage plays, downs 1-4)
- `src/flag_football_ep/features/mutations.py` -- existing `add_ep_variables`/`estimate_pat_baselines`/
  `estimate_fourth_down_rates` patterns (read directly)
- `src/flag_football_ep/reports/aggregate.py` -- existing `rate_table`/`MUTED_MIN_N`/Clopper-Pearson
  convention (read directly)

### Secondary (MEDIUM confidence -- WebSearch/WebFetch, cross-checked against multiple sources)
- [Football Study Hall: Five Factors -- efficiency, explosiveness, IsoPPP](https://www.footballstudyhall.com/2014/1/27/5349762/five-factors-college-football-efficiency-explosiveness-isoppp) -- Connelly's original efficiency/explosiveness split and S&P+ weighting (86/14)
- [Football Study Hall: Big plays are the 3-pointers of football](https://www.footballstudyhall.com/2017/8/22/16075050/college-football-big-plays-efficiency-five-factors) -- Connelly's later "efficiency is everything" reversal
- [PFF: Explosive plays and re-thinking offensive success](https://www.pff.com/news/nfl-explosive-plays-and-re-thinking-offensive-success) -- 20-yard threshold framed as illustrative, EPA-per-drive impact of explosive plays
- [Sam Hoppen: How should we define an explosive play?](https://samhoppen.substack.com/p/how-should-we-define-an-explosive) -- explicit critique of fixed-yardage thresholds, EPA>1.0/80th-percentile alternative, play-type-split derivation
- [Sharp Football Analysis: 2021 NFL Team Ranks -- Explosive Plays](https://www.sharpfootballanalysis.com/nfl-stats/offense/explosive-plays/team-rankings-explosive-plays-2021/) -- 20+/10+ play-type-split convention, Billick 2011 origin
- [nflanalytic.com: EPA vs. DVOA vs. Success Rate](https://nflanalytic.com/explainer-epa-vs-dvoa.html) -- nflverse `EPA > 0` success-rate convention
- [kiwidamien.github.io: Shrinkage and Empirical Bayes to improve inference](https://kiwidamien.github.io/shrinkage-and-empirical-bayes-to-improve-inference.html) -- small-sample shrinkage rationale for EXP-D05

### Tertiary (LOW confidence -- single source, not independently cross-checked)
- [nflanalytic.com: Explosive Plays -- Why Big Gains Matter](https://nflanalytic.com/explainer-explosive-plays.html) -- general framing, used only for corroborating the play-type-split convention already confirmed elsewhere
- Flag-football-specific literature: none found (WebSearch returned no flag-football-specific
  explosive-play/success-rate analytics; consistent with the prior CV-tracking research's finding
  that flag football is "a white spot" in sports analytics generally, `docs/research-notes.md`) --
  this is why EXP-D02 (calibrate on our own distribution) is the only viable path, not a gap in
  this research's search effort.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all verified present in the project's own `.venv`
- Architecture: HIGH -- follows existing, tested project patterns (`features/mutations.py`,
  `reports/aggregate.py`) rather than introducing new structure
- Literature/critique framing (efficiency vs. explosiveness dichotomy, threshold cliff-effect
  critique): HIGH -- multiple independent sources (Connelly, PFF, Sam Hoppen) converge on the same
  distinction and the same critique
- Our-corpus calibration numbers: HIGH -- computed directly from `plays_scored.parquet` in this
  session, reproducible
- HC workbook `Explosive %`/`ExpPlays` formula: HIGH -- read directly from the formula cell
- HC workbook `Efficiency` (Data!O) semantics: LOW -- literal formula reproducible, but the
  underlying per-play charting rule could not be reverse-engineered from available data (Open
  Question 2); flagged, not resolved
- Pitfalls: HIGH -- each is either a direct tool-verified finding (formula mismatch, `Efficiency`
  non-reproducibility, `down==0` pollution) or a direct corpus computation (field-zone tendency)

**Research date:** 2026-09-03
**Valid until:** 30 days for the literature/framing content (stable domain, unlikely to shift);
the corpus-calibration numbers should be re-verified whenever the M3-2 EPA retraining and/or the
HC-01/HC-02 workbook ingest lands (both are concurrent/upstream phases per the roadmap execution
order note "M3-3 research may start in parallel to M3-1") -- if `plays_scored.parquet` grows via
those phases before this phase's plan executes, re-run the distribution queries in this document
rather than treating them as frozen.
