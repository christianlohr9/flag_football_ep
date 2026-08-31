"""Fixed hyperparameters, feature lists and the hyperopt search space for EP/WP training.

Every value here is ported verbatim from the notebooks (source cell noted per constant).
`train.py` must not inline any of these -- this module is the single place hyperparameters
live, so the two training paths (and the default vs. `--tune` paths within each) cannot
drift from one another or from the frozen notebook baseline.
"""

from __future__ import annotations

from hyperopt import hp

# --- Feature-candidate adoption (REQ-S1-09 / plan 01.3-09 "Apply the combined
# feature-candidate adoption decision") ---------------------------------------------------
# Column names exactly match `features.mutations.add_competition_tier_features`'s naming
# rule ("tier_" prefix, "-" -> "_") applied to `reference.COMPETITION_TIERS`
# ("womens-international", "womens-national", "mixed-other"). Not imported from
# `reference` to avoid a hyperparams -> reference -> ... import for three literal strings;
# `tests/test_migration_equivalence.py` and `tests/test_features_mutations.py` both assert
# this naming rule stays in sync.
TIER_FEATURE_COLUMNS: tuple[str, ...] = (
    "tier_womens_international",
    "tier_womens_national",
    "tier_mixed_other",
)

# --- EP (models/ep_model.ipynb cell 2: "Spaltenauswahl") --------------------------------

EP_FEATURES: list[str] = [
    "yardline_50",
    "yards_to_go",
    "down0",
    "down1",
    "down2",
    "down3",
    "down4",
    # REQ-S1-09 adoption (01.3-07-SUMMARY.md, combined-confirmation run in 01.3-09-SUMMARY.md):
    # `half` measured +0.001790 pooled LOGO log-loss improvement for EP (adopted; rejected for
    # WP at -0.002360, so `half` is not in `WP_FEATURES` below).
    "half",
    # `competition_tier` one-hot measured +0.000331 for EP (adopted). Built by
    # `model/train.py::_build_competition_tier` (a `_train` `build_fn` hook) before
    # `prepare_ep_data`/`make_ep_model_mutations` run, mirroring
    # `model/experiments.py::_competition_tier_build`'s ordering.
    *TIER_FEATURE_COLUMNS,
]

EP_SELECTED_COLUMNS: list[str] = ["label", *EP_FEATURES, "Total_W_Scaled"]

# ep_model.ipynb cell 7 -- the fixed, previously-tuned hyperparameters used for the default
# (non-`--tune`) EP fit.
EP_PARAMS: dict = {
    "booster": "gbtree",
    "objective": "multi:softprob",
    "eval_metric": "mlogloss",
    "num_class": 5,
    "eta": 0.025,
    "gamma": 1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "max_depth": 5,
    "min_child_weight": 1,
}

# ep_model.ipynb cell 16 -- the EP model's predict() output column order.
EP_PROB_LABELS: list[str] = [
    "Touchdown_Prob",
    "Opp_Touchdown_Prob",
    "Safety_Prob",
    "Opp_Safety_Prob",
    "No_Score_Prob",
]

# --- WP (models/wp_model.ipynb cell 2: "Spaltenauswahl") --------------------------------

WP_FEATURES: list[str] = [
    "receive_2h_ko",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "Diff_Time_Ratio",
    "score_differential",
    "down",
    "yards_to_go",
    "yardline_50",
    # REQ-S1-09 adoption (01.3-07-SUMMARY.md, combined-confirmation run in 01.3-09-SUMMARY.md):
    # `competition_tier` one-hot measured +0.000023 for WP (adopted). `half` was rejected for
    # WP (-0.002360) and is intentionally absent here. Built by
    # `model/train.py::_build_competition_tier` before `prepare_wp_data`/
    # `make_wp_model_mutations`, same as EP.
    *TIER_FEATURE_COLUMNS,
]

WP_SELECTED_COLUMNS: list[str] = ["label", *WP_FEATURES]

# wp_model.ipynb cell 6 -- fixed hyperparameters for the default (non-`--tune`) WP fit.
WP_PARAMS: dict = {
    "booster": "gbtree",
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "eta": 0.2,
    "gamma": 0,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "max_depth": 4,
    "min_child_weight": 1,
}

# --- Shared hyperopt search space -------------------------------------------------------
# ep_model.ipynb cell 9 and wp_model.ipynb cell 8 define structurally identical search
# spaces; ported once here so both training paths share one tuning contract.
HYPEROPT_SPACE: dict = {
    "max_depth": hp.quniform("max_depth", 3, 18, 1),
    "gamma": hp.uniform("gamma", 0, 9),
    "reg_alpha": hp.uniform("reg_alpha", 0, 0.05),
    "reg_lambda": hp.uniform("reg_lambda", 0, 1),
    "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1),
    "min_child_weight": hp.quniform("min_child_weight", 0, 10, 1),
    "n_estimators": hp.quniform("n_estimators", 1, 1000, 50),
    "eta": hp.uniform("eta", 0.01, 0.1),
    "seed": 0,
}

# ep_model.ipynb cell 10 / wp_model.ipynb cell 9 ("model_objective"): the tuning objective
# fits an XGBClassifier per trial with early stopping against a held-out eval set. Both
# notebooks stop after 10 rounds without improvement.
TUNE_EARLY_STOPPING_ROUNDS: int = 10

# Keys in HYPEROPT_SPACE that hyperopt's `hp.quniform` returns as floats representing
# discrete steps -- these are int-cast before being handed to XGBoost, exactly as the
# notebooks' refit cells do (ep_model.ipynb cell 14, wp_model.ipynb cell 12).
# `reg_alpha`/`colsample_bytree` are `hp.uniform`, not `hp.quniform`, and must stay float:
# the notebooks' *objective* cells (ep cell 10, wp cell 9) int-cast them too, which zeroes
# out `colsample_bytree` for any sampled value below 1.0 -- an objective-cell bug the
# refit cells never actually apply, so it is not ported here (CONTEXT "obvious fixes only").
HYPEROPT_QUNIFORM_KEYS: tuple[str, ...] = ("max_depth", "min_child_weight", "n_estimators")

# --- Grouped-CV protocol (REQ-S1-07 / D-07 / CONTEXT "Grouped-CV protocol") -------------
# The prior play-level random split leaked same-game plays into both train and test. This
# phase replaces it with leave-one-game-out (LOGO) measurement and a grouped inner CV for
# tuning.

# The grouping key for the leave-one-group-out measurement loop (model/evaluate.py).
LOGO_GROUP_COLUMN: str = "game_id"

# Metadata columns that must survive `make_*_model_mutations`'s `.select(...)` so the
# evaluation loop can group by game, join out-of-fold predictions back on
# (game_id, play_id), and break metrics down per source. All three are CORE_COLUMNS and
# listed in NON_NULL_COLUMNS (canonical.py), so carrying them through `_train`'s
# `drop_nulls()` cannot drop rows.
GROUP_COLUMNS: tuple[str, ...] = ("game_id", "play_id", "source")

# `EP_SELECTED_COLUMNS`/`WP_SELECTED_COLUMNS` stay frozen -- tests/test_migration_equivalence.py
# compares them column-for-column against the frozen notebook baseline frames
# (tests/fixtures/baseline_{ep,wp}_model_data.parquet). These `*_TRAINING_COLUMNS` supersets
# are what `train_ep`/`train_wp` pass as `selected_columns` instead.
EP_TRAINING_COLUMNS: list[str] = [*EP_SELECTED_COLUMNS, *GROUP_COLUMNS]
WP_TRAINING_COLUMNS: list[str] = [*WP_SELECTED_COLUMNS, *GROUP_COLUMNS]

# Fold count for the grouped inner CV that scores hyperopt trials (model/train.py `_tune`).
# CONTEXT leaves the exact number to Claude's discretion (K=5 suggested, not locked). The
# inner loop deliberately is not LOGO, to avoid the folds x trials fit-count explosion
# (RESEARCH "Anti-Patterns to Avoid": nested LOGO-inside-hyperopt-trials).
INNER_CV_FOLDS: int = 5

# --- Feature-candidate experiments (REQ-S1-09 / CONTEXT "Feature re-tests & training-data
# mix") -----------------------------------------------------------------------------------
# Candidate runs (model/experiments.py) are measurement-only re-tests of individual feature
# additions against the current production feature set -- they never ship. Kept in a
# suffixed experiment so a candidate run can never be mistaken for a production run or
# picked up by `ffep promote`'s "most recent FINISHED run" default.
CANDIDATE_EXPERIMENT_SUFFIX: str = "_candidates"

# The pooled LOGO log-loss improvement (control - candidate) a candidate must exceed to be
# adopted. CONTEXT locks the adoption criterion as "improves" (not "improves by some
# margin"), so this is zero -- kept as a named constant so a future decision to require a
# margin has one place to change.
CANDIDATE_ADOPTION_MIN_DELTA: float = 0.0

# --- Recency weighting (REQ-S1-09 / CONTEXT "Recency weighting") -------------------------
# Exponential decay by `game_date`, half-life tuned by the grouped-CV comparison in
# `model/experiments.py::run_recency_candidate`. Brackets "only the last campaign matters"
# (60 days, roughly a season quarter) through "history barely decays" (730 days, two years)
# over the corpus's date span -- the grid itself is Claude's discretion per CONTEXT; the
# 60..730 range spans more than one order of magnitude (730 / 60 ~= 12.2).
RECENCY_HALF_LIFE_DAYS_GRID: tuple[float, ...] = (60.0, 180.0, 365.0, 730.0)

# The weight given to a row whose `game_date` is null. Policy: a dateless row is
# *undiscounted* (weight 1.0) rather than dropped or zero-weighted, so a source with poor
# date coverage keeps its full influence instead of silently vanishing from the fit
# (RESEARCH Pitfall 4 warns the alternative is an invisible NaN). `01.3-DATA-PROFILE.md`
# section 2 measured `game_date` as 100% null for every row, in every source, in the real
# corpus (ifaf, legacy, legacy-sportapp) -- there is currently no dated subset at all, so
# this constant is not a minor edge case: with today's data every row falls back to it and
# the recency weight is a no-op (Recency_W == 1.0 everywhere), making the candidate
# experiment's real-corpus verdict effectively "no signal available yet" rather than a
# genuine recency measurement. The mechanism is still implemented and tested against
# synthetic dates so it activates automatically once `game_date` is populated for any
# source.
RECENCY_NULL_DATE_WEIGHT: float = 1.0

# plan 01.3-09 final adoption decision (all four REQ-S1-09 candidates, see
# 01.3-09-SUMMARY.md): `recency` -- rejected for EP (every half-life arm identical to the
# unweighted control, per the no-op above); `real_clock` -- never adopted into production
# regardless of its measured delta, by explicit CONTEXT rule ("production stays synthetic
# until Hudl delivers real time" / REQ-S1-02); neither constant above is wired into
# `EP_FEATURES`/`WP_FEATURES` or `model/train.py`'s production path.
