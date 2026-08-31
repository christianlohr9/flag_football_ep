# Context

Domain and background notes with source attribution. DOC-type content plus contextual material from the plans.

---

## Team and stakeholders
- source: docs/plan-1-analytics-refresh.md, docs/plan-2-cv-tracking.md
- German women's flag football national team context (5v5, "Spielerinnen" throughout; competition level/gender considered as model covariate).
- Key stakeholders: the HC (head coach — primary consumer of reports and decision charts) and the Videoanalyst (video analyst — owns Hudl charting, data contract partner in Phase 1.1, capture-protocol ally in Phase 2.0, described as the most important mitigation lever for protocol adherence).

## Competitive calendar
- source: docs/research-notes.md (finding 10)
- IFAF Flag Football World Championship 2026 in Düsseldorf — last worlds before LA28. Scouting there runs everywhere via film exchange + manual charting.
- Flag football is an LA28 Olympic sport; AI officiating systems (flag-pull detection) are discussed for LA28 but nothing is public.
- IFAF.TV archives (worlds/euros) are a free scouting source and potential training-data source.

## Existing codebase and data assets
- source: docs/plan-1-analytics-refresh.md
- Pipeline follows the nflfastR approach: polars-based feature pipeline + XGBoost EP/WP models. Helpers in `Python/` (e.g. `helper_add_ep_wp.py` with hard-coded PAT baselines 50%/46%), started package at `src/flag_football_ep`, models currently saved as `ep_model.pkl` (overwritten).
- `data_raw.csv`: Hudl export, 47 games / ~3,700 plays, far richer than the worlds PBP schema — includes `OFF FORM`, `Off Str`, `OFF PLAY`, `TARGET ROUTE`, `RECEIVED BY`, `Thrown By`, `YAC`, `GN/LS`. Formation, play call, and route per play are scouting gold.
- `pbp_wc24_static.csv`: World Championship PBP dataset (lacks the rich charting fields).
- sportapp.fi / IFAF tournament data as second PBP source; `games_plays.csv` in repo root awaiting consolidation into `data/`.
- README self-flags current WP time handling as "flawful bc of missing times".

## Industry state of the art (research findings)
- source: docs/research-notes.md
- **White space:** No public dataset, no open-source project, no purchasable product for flag-football player tracking. NGS data exists only inside NFL infrastructure (Zebra RFID, since 2025 plus Sony Hawk-Eye SkeleTRACK optics); Hudl IQ / PFF / Sportlogiq cover tackle football only. Producing own flag tracking data is a real edge, even over top nations (finding 1).
- **Hybrid is the industry standard:** Nobody works fully automatically. Hudl IQ is explicitly CV + human data collectors; PFF charts every play by hand with 600+ analysts; colleges buy broadcast CV tracking from Sportlogiq/Telemetry. Manual per-play charting is much cheaper at 5v5 (10 players, no helmets, no piles) — the PFF approach scales for a single person (finding 2).
- **Drones:** standard at NFL/college trainings (~15–30 m, endzone/overhead angles), banned on game days (finding 3).
- **DIY pipeline fully open source in 2026** and single-person feasible: RF-DETR fine-tune (Apache 2.0, DINOv2 backbone = good domain transfer) → BoT-SORT/OC-SORT → SigLIP+UMAP+KMeans team split → homography → XY CSV. SoccerTrack/TeamTrack (university; soccer/handball/basketball) demonstrated exactly this with drone + fisheye (finding 4).
- **Labeling no longer the bottleneck:** CVAT + SAM2 video tracker (click once, propagate through clip) plus autodistill/Grounding DINO pre-labeling reduces effort ~10x; 1,500–3,000 verified frames across domains realistic in 2 weeks (finding 5).
- **Homography:** near-free for static/drone hover (one-time manual 4–8-point calibration); expensive for moving broadcast cameras (field-keypoint model needed; sparse flag markings = hardest part). Jersey OCR ~87% on SoccerNet with legibility filter → torso crop → PARSeq → tracklet voting; fundamentally unavailable top-down (finding 7).
- **Niche-sport pattern confirms the gap:** Lacrosse has zero datasets; Ultimate Frisbee has student projects plus a mature *manual* stats community (UltiAnalytics). "Mature PBP community, no positional tracking" is exactly the flag-football situation — and the manual PBP track has delivered coaching value there for years (finding 9).

## Key resources
- source: docs/research-notes.md (Wichtigste Ressourcen)
- Pipeline building blocks: roboflow/sports (detection + pitch keypoints + radar view, MIT), BoxMOT, SoccerNet/sn-gamestate + TrackLab (broadcast → minimap complete), SportsLabKit / SoccerTrack (drone + fisheye, "video → CSV"), jersey-number-pipeline.
- Datasets: SoccerTrack/TeamTrack (drone), SportsMOT, VisDrone (small aerial objects), SkillCorner opendata (10 games broadcast tracking, for metric development).
- Labeling: CVAT + SAM2 tracker (self-hosted, free) or Roboflow Annotate; autodistill for scriptable pre-labeling.
- Detector: RF-DETR (Apache 2.0), fallback D-FINE + DEIM recipe.
- Commercial reference for standardized capture: Veo Cam (explicitly markets flag football/7v7), Pixellot, Hudl Focus — all deliver auto-follow video but no raw XY data.
