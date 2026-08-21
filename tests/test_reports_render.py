"""Coverage for `flag_football_ep.reports.render`: the shared Jinja2 environment, template
escaping discipline, the base template shell, and the figure-embed / dated-run-folder
writer every Phase 1.4 report product sits on (REQ-S1-12, REQ-S1-16)."""

from __future__ import annotations

import base64
from datetime import date
from pathlib import Path

import matplotlib
import pytest
from jinja2 import Environment

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from flag_football_ep.reports.render import (
    build_environment,
    fig_to_data_uri,
    render_page,
    write_report_run,
)

_PNG_MAGIC = b"\x89PNG"


class TestBuildEnvironment:
    def test_returns_a_jinja2_environment(self):
        env = build_environment()

        assert isinstance(env, Environment)

    def test_autoescape_is_truthy_for_html_templates(self):
        env = build_environment()

        assert env.autoescape("page.html")

    def test_autoescape_is_truthy_for_real_dotj2_template_names(self):
        # Real template sources in this package are named `*.html.j2` -- the final suffix
        # `select_autoescape` matches on is `.j2`, not `.html`.
        env = build_environment()

        assert env.autoescape("base.html.j2")

    def test_can_load_base_template_via_package_loader(self):
        env = build_environment()

        template = env.get_template("base.html.j2")

        assert template is not None


class TestRenderPage:
    def test_title_with_script_tag_is_escaped(self):
        html = render_page("base.html.j2", title="<script>alert(1)</script>")

        assert "&lt;script&gt;" in html
        assert "<script>" not in html


class TestBaseTemplate:
    def test_starts_with_doctype_and_declares_german_lang(self):
        html = render_page("base.html.j2", title="X")

        assert html.startswith("<!DOCTYPE html")
        assert 'lang="de"' in html

    def test_contains_print_and_tablet_css_hooks(self):
        html = render_page("base.html.j2", title="X")

        assert "break-inside" in html
        assert "print-color-adjust" in html
        assert 'class="chart-img"' not in html  # class is defined in CSS, not emitted bare
        assert ".chart-img" in html

    def test_no_script_tag_and_no_external_resource_references(self):
        html = render_page("base.html.j2", title="X")

        assert "<script" not in html
        assert "http://" not in html
        assert "https://" not in html


class TestFigToDataUri:
    def _make_figure(self):
        fig, ax = plt.subplots()
        ax.plot([0, 1], [0, 1])
        return fig

    def test_returns_a_png_data_uri(self):
        fig = self._make_figure()

        uri = fig_to_data_uri(fig)

        assert uri.startswith("data:image/png;base64,")
        payload = uri.removeprefix("data:image/png;base64,")
        assert base64.b64decode(payload)[:4] == _PNG_MAGIC

    def test_closes_the_figure_after_success(self):
        fig = self._make_figure()
        fignum = fig.number

        fig_to_data_uri(fig)

        assert fignum not in plt.get_fignums()

    def test_closes_the_figure_even_when_savefig_raises(self, monkeypatch: pytest.MonkeyPatch):
        fig = self._make_figure()
        fignum = fig.number

        def _raise(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(fig, "savefig", _raise)

        with pytest.raises(RuntimeError):
            fig_to_data_uri(fig)

        assert fignum not in plt.get_fignums()


class TestWriteReportRun:
    def test_writes_dated_folder_and_identical_latest_copy(self, tmp_path: Path):
        rendered = {"a.html": "<p>a</p>", "b.html": "<p>b</p>"}

        run_dir, latest_dir = write_report_run(rendered, tmp_path)

        assert run_dir == tmp_path / date.today().isoformat()
        assert latest_dir == tmp_path / "latest"
        for filename, html in rendered.items():
            assert (run_dir / filename).read_text(encoding="utf-8") == html
            assert (latest_dir / filename).read_text(encoding="utf-8") == html

    def test_second_run_replaces_latest_rather_than_merging(self, tmp_path: Path):
        write_report_run({"old.html": "<p>old</p>"}, tmp_path)

        _, latest_dir = write_report_run({"new.html": "<p>new</p>"}, tmp_path)

        assert (latest_dir / "new.html").exists()
        assert not (latest_dir / "old.html").exists()

    def test_raises_valueerror_for_path_separator_in_key(self, tmp_path: Path):
        with pytest.raises(ValueError):
            write_report_run({"sub/escape.html": "x"}, tmp_path)

    def test_raises_valueerror_for_dotdot_in_key(self, tmp_path: Path):
        with pytest.raises(ValueError):
            write_report_run({"../escape.html": "x"}, tmp_path)

