"""Coverage for `flag_football_ep.reports.render`: the shared Jinja2 environment, template
escaping discipline, the base template shell, and the figure-embed / dated-run-folder
writer every Phase 1.4 report product sits on (REQ-S1-12, REQ-S1-16)."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment

from flag_football_ep.reports.render import build_environment, render_page


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

