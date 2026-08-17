"""Fixed hyperparameters, feature lists and the hyperopt search space for EP/WP training.

Every value here is ported verbatim from the notebooks (source cell noted per constant).
`train.py` must not inline any of these -- this module is the single place hyperparameters
live, so the two training paths (and the default vs. `--tune` paths within each) cannot
drift from one another or from the frozen notebook baseline.
"""

from __future__ import annotations

from hyperopt import hp

# --- EP (models/ep_model.ipynb cell 2: "Spaltenauswahl") --------------------------------

EP_FEATURES: list[str] = [
    "yardline_50",
    "yards_to_go",
    "down0",
    "down1",
    "down2",
    "down3",
    "down4",
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
