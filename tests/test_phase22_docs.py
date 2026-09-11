"""Structural gate over the Phase 2.2 plan 01 documentation reconciliation.

Guards T-2.2-03: keeps the gate-verdict reconciliation (D-01), the capture-protocol
Wunschzettel update (D-03), and the federation-approval documentation (D-06) from
silently drifting apart across `docs/pilot-gate-decision.md`, `docs/capture-protocol.md`,
`docs/capture-legal.md`, `docs/hackathon-challenge-reid.md`,
`docs/hackathon-challenge-prep.md`, `.planning/ROADMAP.md` and `.planning/REQUIREMENTS.md`.
A later edit to any of these six documents cannot silently restore the "only after
passed pilot" claim or drop one of the three Nachtrag sections without this test file
failing loudly.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

GATE_DOC = REPO_ROOT / "docs" / "pilot-gate-decision.md"
CAPTURE_PROTOCOL = REPO_ROOT / "docs" / "capture-protocol.md"
CAPTURE_LEGAL = REPO_ROOT / "docs" / "capture-legal.md"
HACKATHON_REID = REPO_ROOT / "docs" / "hackathon-challenge-reid.md"
HACKATHON_PREP = REPO_ROOT / "docs" / "hackathon-challenge-prep.md"
HACKATHON_BUNDLES = REPO_ROOT / "docs" / "hackathon-bundles.md"
ROADMAP_MD = REPO_ROOT / ".planning" / "ROADMAP.md"
REQUIREMENTS_MD = REPO_ROOT / ".planning" / "REQUIREMENTS.md"
DATASET_PLAN = REPO_ROOT / "docs" / "dataset-plan.md"
DATASET_BUILDOUT = REPO_ROOT / "docs" / "dataset-buildout.md"
DATASET_PUBLICATION = REPO_ROOT / "docs" / "dataset-publication.md"
DATASET_CARD = REPO_ROOT / "docs" / "dataset-card.md"

NACHTRAG_HEADING = "## Nachtrag 2026-08-31"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    """Extract the body of a `##`/`###` `Heading` section, stopping at the next
    top-level `## ` heading (line-anchored) -- mirrors the helper in
    tests/test_cv_gate_artifacts.py so both doc-gate tests share one extraction
    convention."""
    start = text.index(heading) + len(heading)
    next_heading = re.search(r"^## ", text[start:], re.MULTILINE)
    end = start + next_heading.start() if next_heading else len(text)
    return text[start:end]


def test_gate_doc_has_nachtrag_2026_08_31() -> None:
    text = _read(GATE_DOC)
    assert NACHTRAG_HEADING in text, (
        "docs/pilot-gate-decision.md is missing the Nachtrag 2026-08-31 section (D-01)"
    )


def test_capture_protocol_has_nachtrag_2026_08_31() -> None:
    text = _read(CAPTURE_PROTOCOL)
    assert NACHTRAG_HEADING in text, (
        "docs/capture-protocol.md is missing the Nachtrag 2026-08-31 section (D-03)"
    )


def test_capture_legal_has_nachtrag_2026_08_31() -> None:
    text = _read(CAPTURE_LEGAL)
    assert NACHTRAG_HEADING in text, (
        "docs/capture-legal.md is missing the Nachtrag 2026-08-31 section (D-06)"
    )


def test_capture_protocol_nachtrag_names_all_three_drone_wishes() -> None:
    text = _read(CAPTURE_PROTOCOL)
    section = _section(text, NACHTRAG_HEADING)
    for term in ("Hover-Winkel", "Endzone", "Kameraschnitt"):
        assert term in section, (
            f"docs/capture-protocol.md Nachtrag section is missing {term!r}"
        )


def test_capture_protocol_nachtrag_adds_no_new_side_camera_request() -> None:
    text = _read(CAPTURE_PROTOCOL)
    section = _section(text, NACHTRAG_HEADING)
    assert "seitenkamera" not in section.lower(), (
        "docs/capture-protocol.md Nachtrag mentions a side camera -- the drone-only wish "
        "list must not add a side-camera request (user decision, CONTEXT.md Deferred Ideas)"
    )


def test_roadmap_no_longer_claims_passed_pilot_precondition() -> None:
    text = _read(ROADMAP_MD)
    assert "only after passed pilot" not in text, (
        ".planning/ROADMAP.md still claims Phase 2.2 needs a passed pilot"
    )
    assert "gate PASSED" not in text, (
        ".planning/ROADMAP.md still claims Phase 2.1's gate PASSED (it is TEILWEISE)"
    )


def test_requirements_no_longer_claims_passed_pilot_precondition() -> None:
    text = _read(REQUIREMENTS_MD)
    assert "Only after passed pilot" not in text, (
        ".planning/REQUIREMENTS.md REQ-S2-03 still claims it needs a passed pilot"
    )


def test_hackathon_reid_datenschutz_records_dated_approval() -> None:
    text = _read(HACKATHON_REID)
    section = _section(text, "### Datenschutz")
    assert "2026-08-31" in section, (
        "docs/hackathon-challenge-reid.md Datenschutz section does not record the "
        "dated federation approval"
    )


def test_hackathon_reid_status_header_no_longer_pending() -> None:
    text = _read(HACKATHON_REID)
    assert "Voraussetzung vor Einreichung: Freigabe" not in text, (
        "docs/hackathon-challenge-reid.md status header still states the approval as "
        "an open precondition"
    )


def test_capture_legal_records_federation_approval_quote() -> None:
    text = _read(CAPTURE_LEGAL)
    assert "2026-08-31" in text, "docs/capture-legal.md is missing the approval date"
    assert "alle Befugnisse" in text, (
        "docs/capture-legal.md is missing the quoted federation approval statement"
    )


def test_hackathon_prep_verbandsfreigabe_checked() -> None:
    text = _read(HACKATHON_PREP)
    lines = [line for line in text.splitlines() if "Verbands-Freigabe" in line]
    assert lines, "docs/hackathon-challenge-prep.md has no Verbands-Freigabe line"
    assert lines[0].startswith("- [x]"), (
        f"Verbands-Freigabe checklist item is not checked off: {lines[0]!r}"
    )


def test_dataset_plan_states_stopping_rule_threshold() -> None:
    """RESEARCH Open Question 3 / D-12: the numeric mAP-improvement threshold that
    ends labelling must be pre-committed in writing before the first AL iteration --
    guards against the threshold being silently loosened after seeing results.
    """
    text = _read(DATASET_PLAN)
    assert "+0.010" in text, (
        "docs/dataset-plan.md is missing the pre-committed +0.010 stopping-rule threshold"
    )
    assert "mAP_50_95" in text, (
        "docs/dataset-plan.md's stopping rule does not name mAP_50_95 as the metric"
    )


def test_dataset_buildout_doc_exists_and_names_iteration_1() -> None:
    """Plan 02.2-11's hand-off artifact: the German running record of the AL
    iteration-1 selection/prelabel/CVAT-push must exist and name the iteration it
    documents, so plan 02.2-13's checkpoint can point the user at a document.
    """
    assert DATASET_BUILDOUT.exists(), (
        "docs/dataset-buildout.md is missing -- plan 02.2-11 Task 3 must create it"
    )
    text = _read(DATASET_BUILDOUT)
    assert "## Iteration 1" in text, (
        "docs/dataset-buildout.md does not name Iteration 1 as its own section"
    )
    assert "Labelling-Anleitung Iteration 1" in text, (
        "docs/dataset-buildout.md is missing the Labelling-Anleitung Iteration 1 subsection"
    )


def test_hackathon_bundles_names_puerto_rico_test_session() -> None:
    """Plan 02.2-21: the test set is now the real second drone game, not a clip
    subset of the pilot game -- the doc must name the Puerto Rico session id."""
    text = _read(HACKATHON_BUNDLES)
    assert "2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE" in text, (
        "docs/hackathon-bundles.md does not name the Puerto Rico test session id"
    )


def test_hackathon_bundles_states_game_disjoint_property() -> None:
    """DATA-04: dev and test must be documented as split by GAME, not by clip."""
    text = _read(HACKATHON_BUNDLES)
    assert "DATA-04" in text, "docs/hackathon-bundles.md does not reference DATA-04"
    assert "SPIEL getrennt" in text, (
        "docs/hackathon-bundles.md does not state the dev/test game-disjoint property"
    )


def test_hackathon_bundles_no_longer_describes_18_withheld_clips() -> None:
    """The D-07 fallback (18 withheld pilot clips as the private test set) is
    superseded by plan 02.2-21 and must not be described as the current test set."""
    text = _read(HACKATHON_BUNDLES)
    assert "43 von 61" not in text, (
        "docs/hackathon-bundles.md still states the superseded 43-of-61 dev pool split"
    )
    assert "Die 18 Clips mit `private_test = true`" not in text, (
        "docs/hackathon-bundles.md still describes the test set as 18 withheld pilot clips"
    )


# --- Plan 02.2-19: publication assessment + dataset card ---------------------------------
#
# Guards REQ-S2-03's third success criterion ("publication option assessed") and the dataset
# card's role as the hand-over artifact any future publication/hand-off needs. A later edit
# cannot silently drop a required section or the pending-licence marker without this file
# failing loudly.

DATASET_PUBLICATION_SECTIONS = (
    "## 1. Was veröffentlicht würde",
    "## 2. Rechtsgrundlage",
    "## 3. Plattformvergleich",
    "## 4. Lizenzempfehlung",
    "## 5. Zeitpunkt",
    "## 6. Checkliste für den Veröffentlichungszeitpunkt",
)

DATASET_CARD_SECTIONS = (
    "## Zweck und beabsichtigte Nutzung",
    "## Quellen und Aufnahmebedingungen je Domäne",
    "## Zusammensetzung",
    "## Split-Methodik",
    "## Labelling-Konvention",
    "## Provenienz",
    "## Bekannte Limitierungen",
    "## Datenschutz",
    "## Lizenz",
)


def test_dataset_publication_doc_exists_and_min_length() -> None:
    assert DATASET_PUBLICATION.exists(), (
        "docs/dataset-publication.md is missing -- plan 02.2-19 Task 1 must create it"
    )
    text = _read(DATASET_PUBLICATION)
    assert len(text.splitlines()) >= 70, (
        "docs/dataset-publication.md has fewer than 70 lines"
    )


def test_dataset_publication_has_all_six_sections() -> None:
    text = _read(DATASET_PUBLICATION)
    for heading in DATASET_PUBLICATION_SECTIONS:
        assert heading in text, (
            f"docs/dataset-publication.md is missing required section {heading!r}"
        )


def test_dataset_publication_names_three_platforms() -> None:
    text = _read(DATASET_PUBLICATION)
    section = _section(text, "## 3. Plattformvergleich")
    for platform in ("HuggingFace Datasets", "Zenodo", "Roboflow Universe"):
        assert platform in section, (
            f"docs/dataset-publication.md platform comparison is missing {platform!r}"
        )


def test_dataset_publication_names_three_licence_options_pending_confirmation() -> None:
    text = _read(DATASET_PUBLICATION)
    section = _section(text, "## 4. Lizenzempfehlung")
    for option in ("CC BY-NC 4.0", "CC BY 4.0", "Research-Use-Lizenz"):
        assert option in section, (
            f"docs/dataset-publication.md licence section is missing {option!r}"
        )
    assert "vorläufig" in section or "vorbehalten" in section, (
        "docs/dataset-publication.md licence section does not mark the recommendation as "
        "pending user confirmation"
    )


def test_dataset_publication_cites_capture_legal_and_approval_date() -> None:
    text = _read(DATASET_PUBLICATION)
    assert "docs/capture-legal.md" in text, (
        "docs/dataset-publication.md does not cite docs/capture-legal.md as the legal basis"
    )
    assert "2026-08-31" in text, (
        "docs/dataset-publication.md does not cite the 2026-08-31 federation approval date"
    )


def test_dataset_publication_checklist_has_at_least_five_steps() -> None:
    text = _read(DATASET_PUBLICATION)
    section = _section(text, "## 6. Checkliste für den Veröffentlichungszeitpunkt")
    numbered_steps = re.findall(r"^\d+\.\s", section, re.MULTILINE)
    assert len(numbered_steps) >= 5, (
        "docs/dataset-publication.md publication checklist has fewer than 5 numbered steps"
    )


def test_dataset_card_doc_exists_and_min_length() -> None:
    assert DATASET_CARD.exists(), (
        "docs/dataset-card.md is missing -- plan 02.2-19 Task 2 must create it"
    )
    text = _read(DATASET_CARD)
    assert len(text.splitlines()) >= 70, (
        "docs/dataset-card.md has fewer than 70 lines"
    )


def test_dataset_card_has_all_required_sections() -> None:
    text = _read(DATASET_CARD)
    for heading in DATASET_CARD_SECTIONS:
        assert heading in text, (
            f"docs/dataset-card.md is missing required section {heading!r}"
        )


def test_dataset_card_names_all_four_dataset_versions() -> None:
    text = _read(DATASET_CARD)
    for version in ("v1", "v1.1", "v1.2", "v2"):
        assert version in text, (
            f"docs/dataset-card.md does not name dataset version {version!r}"
        )


def test_dataset_card_states_clip_level_split_rule_and_reason() -> None:
    text = _read(DATASET_CARD)
    section = _section(text, "## Split-Methodik")
    assert "Clip-level" in section, (
        "docs/dataset-card.md split section does not state the clip-level rule"
    )
    assert "Near-Duplikate" in section, (
        "docs/dataset-card.md split section does not state the near-duplicate-frames reason"
    )


def test_dataset_card_limitations_name_at_least_four() -> None:
    text = _read(DATASET_CARD)
    section = _section(text, "## Bekannte Limitierungen")
    numbered_items = re.findall(r"^\d+\.\s", section, re.MULTILINE)
    assert len(numbered_items) >= 4, (
        "docs/dataset-card.md limitations section names fewer than 4 distinct limitations"
    )


def test_dataset_card_states_eval_gt_provenance_and_bias_caveat() -> None:
    text = _read(DATASET_CARD)
    assert "Vorlabel-Bias" in text, (
        "docs/dataset-card.md does not mention the prelabel-bias caveat for the eval GT"
    )
    assert "frozen_eval_clips.csv" in text, (
        "docs/dataset-card.md does not cite the frozen eval split provenance file"
    )


def test_dataset_card_licence_points_at_publication_assessment() -> None:
    text = _read(DATASET_CARD)
    section = _section(text, "## Lizenz")
    assert "dataset-publication.md" in section, (
        "docs/dataset-card.md licence section does not point at docs/dataset-publication.md"
    )
