#!/usr/bin/env python3
"""Convert user-guide.md to a standalone HTML page with tabbed code sections.

Usage:
    python3 doc/user-manual/build_html.py

Output:
    doc/user-manual/user-guide.html

The script is self-contained (no third-party dependencies). It handles the
markdown subset used in the user guide: headings, fenced code blocks, bold
text, bullet lists, tables, horizontal rules, and inline code.

Sections labelled "**CLI:**" and "**curl:**" followed by a fenced code block
are automatically wrapped in a tabbed interface so the reader can switch
between CLI and curl examples with a single click.
"""

from __future__ import annotations

import html
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SOURCE = SCRIPT_DIR / "user-guide.md"
OUTPUT = SCRIPT_DIR / "user-guide.html"

CSS = """
:root {
  /* ── Hugging Face inspired palette ──────────────────────────────── */
  --bg: #ffffff;
  --bg-subtle: #fafafa;
  --fg: #111827;
  --fg-default: #374151;
  --fg-muted: #6b7280;
  --sidebar-bg: #fafafa;
  --sidebar-border: #e5e7eb;
  --code-bg: #f3f4f6;
  --code-border: #e5e7eb;
  --border: #e5e7eb;
  --border-strong: #d1d5db;
  --accent: #ffd21e;
  --accent-hover: #f59e0b;
  --accent-soft: #fff4cc;
  --accent-on: #111827;
  --link: #155dfc;
  --link-hover: #0b4ed6;
  --tab-active-bg: #ffffff;
  --tab-inactive-bg: #f3f4f6;
  --tab-active-fg: #111827;
  --tab-inactive-fg: #6b7280;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117;
    --bg-subtle: #161b22;
    --fg: #e6edf3;
    --fg-default: #c9d1d9;
    --fg-muted: #8b949e;
    --sidebar-bg: #161b22;
    --sidebar-border: #30363d;
    --code-bg: #161b22;
    --code-border: #30363d;
    --border: #30363d;
    --border-strong: #484f58;
    --accent: #ffd21e;
    --accent-hover: #f59e0b;
    --accent-soft: #2a2410;
    --accent-on: #0d1117;
    --link: #58a6ff;
    --link-hover: #79b8ff;
    --tab-active-bg: #21262d;
    --tab-inactive-bg: #161b22;
    --tab-active-fg: #e6edf3;
    --tab-inactive-fg: #8b949e;
  }
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: "Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans", Roboto, sans-serif;
  background: var(--bg-subtle);
  color: var(--fg-default);
  line-height: 1.65;
  font-size: 15px;
}

/* ── Layout ─────────────────────────────────────────────────────── */
.layout {
  display: flex;
  min-height: 100vh;
}
.sidebar {
  width: 280px;
  flex-shrink: 0;
  background: var(--sidebar-bg);
  border-right: 1px solid var(--sidebar-border);
  position: fixed;
  top: 0;
  left: 0;
  bottom: 0;
  overflow-y: auto;
  padding: 1.5rem 0;
}
.content {
  margin-left: 280px;
  flex: 1;
  padding: 2rem 3rem;
  max-width: calc(100vw - 280px);
  background: var(--bg);
}

/* ── Sidebar ────────────────────────────────────────────────────── */
.sidebar-brand {
  padding: 0 1.5rem 1rem;
  font-size: 1.05rem;
  font-weight: 700;
  color: var(--fg);
  border-bottom: 1px solid var(--sidebar-border);
  margin-bottom: 0.75rem;
  letter-spacing: -0.01em;
}
.sidebar-brand .brand-mark {
  display: inline-block;
  width: 20px;
  height: 20px;
  background: var(--accent);
  border-radius: 4px;
  vertical-align: middle;
  margin-right: 0.5rem;
}
.sidebar nav { padding: 0 0.5rem; }
.sidebar nav a {
  display: block;
  padding: 0.35rem 1rem;
  color: var(--fg-muted);
  text-decoration: none;
  font-size: 0.875rem;
  border-radius: 6px;
  transition: all 0.12s;
  line-height: 1.4;
}
.sidebar nav a:hover {
  color: var(--link);
  background: var(--code-bg);
}
.sidebar nav a.active {
  color: var(--link);
  font-weight: 600;
  background: var(--code-bg);
}
.sidebar nav a.sub { padding-left: 2rem; font-size: 0.82rem; }

/* ── Content ────────────────────────────────────────────────────── */
.content h1 {
  font-size: 1.9rem;
  font-weight: 700;
  color: var(--fg);
  margin-bottom: 0.5rem;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid var(--border);
  letter-spacing: -0.02em;
}
.content h2 {
  font-size: 1.4rem;
  font-weight: 600;
  color: var(--fg);
  margin-top: 2.5rem;
  margin-bottom: 0.75rem;
  padding-bottom: 0.3rem;
  border-bottom: 1px solid var(--border);
  scroll-margin-top: 1rem;
  letter-spacing: -0.01em;
}
.content h3 {
  font-size: 1.15rem;
  font-weight: 600;
  color: var(--fg);
  margin-top: 1.75rem;
  margin-bottom: 0.5rem;
  scroll-margin-top: 1rem;
}
.content p { margin: 0.6rem 0; }
.content a { color: var(--link); text-decoration: none; }
.content a:hover { color: var(--link-hover); text-decoration: underline; }
.content ul { padding-left: 1.5rem; margin: 0.6rem 0; }
.content li { margin: 0.3rem 0; }
.content hr { border: none; border-top: 1px solid var(--border); margin: 2rem 0; }

/* ── Inline code ────────────────────────────────────────────────── */
code {
  font-family: "IBM Plex Mono", "SF Mono", "Fira Code", "Consolas", monospace;
  font-size: 0.85em;
  background: var(--code-bg);
  padding: 0.15em 0.4em;
  border-radius: 4px;
  border: 1px solid var(--code-border);
}

/* ── Code blocks ────────────────────────────────────────────────── */
pre {
  background: var(--code-bg);
  border: 1px solid var(--code-border);
  border-radius: 6px;
  padding: 0.85rem 1rem;
  overflow-x: auto;
  font-size: 0.84rem;
  line-height: 1.5;
}
pre code { background: none; border: none; padding: 0; font-size: inherit; }

/* ── Tables ─────────────────────────────────────────────────────── */
table {
  border-collapse: collapse;
  width: 100%;
  margin: 1rem 0;
  font-size: 0.88rem;
}
th, td {
  border: 1px solid var(--border);
  padding: 0.5rem 0.75rem;
  text-align: left;
}
th {
  background: var(--code-bg);
  font-weight: 600;
  color: var(--fg);
}
tr:hover td { background: var(--code-bg); }

/* ── Tabbed code ────────────────────────────────────────────────── */
.tab-group {
  margin: 1rem 0;
  border-radius: 6px;
  overflow: hidden;
  border: 1px solid var(--border);
}
.tab-bar {
  display: flex;
  background: var(--tab-inactive-bg);
  border-bottom: 1px solid var(--border);
}
.tab-btn {
  padding: 0.45rem 1.1rem;
  border: none;
  background: transparent;
  cursor: pointer;
  font-size: 0.8rem;
  font-weight: 500;
  color: var(--tab-inactive-fg);
  border-bottom: 2px solid transparent;
  transition: all 0.12s;
  font-family: inherit;
}
.tab-btn:hover { color: var(--fg); background: var(--tab-active-bg); }
.tab-btn.active {
  color: var(--tab-active-fg);
  background: var(--tab-active-bg);
  border-bottom-color: var(--accent);
  font-weight: 600;
}
.tab-panel { display: none; }
.tab-panel.active { display: block; }
.tab-panel-content { padding: 1rem; }
.tab-panel-content table { margin: 0.75rem 0; }
.tab-panel-content h4 { margin-top: 1.25rem; }
.tab-panel-content h4:first-child { margin-top: 0; }
.tab-panel pre {
  border-radius: 0 0 6px 6px;
  margin: 0;
  border: none;
  border-top: none;
}

/* ── Mobile ─────────────────────────────────────────────────────── */
@media (max-width: 768px) {
  .sidebar {
    position: static;
    width: 100%;
    border-right: none;
    border-bottom: 1px solid var(--sidebar-border);
    max-height: 300px;
  }
  .content {
    margin-left: 0;
    max-width: 100%;
    padding: 1.5rem;
  }
  .layout { flex-direction: column; }
}
"""

JS = """
// Tab switching
document.querySelectorAll('.tab-group').forEach(function(group) {
  var btns = group.querySelectorAll('.tab-btn');
  btns.forEach(function(btn) {
    btn.addEventListener('click', function() {
      var idx = btn.getAttribute('data-tab');
      btns.forEach(function(b) { b.classList.remove('active'); });
      group.querySelectorAll('.tab-panel').forEach(function(p) {
        p.classList.toggle('active', p.getAttribute('data-panel') === idx);
      });
      btn.classList.add('active');
    });
  });
});

// Sidebar active section tracking
var navLinks = document.querySelectorAll('.sidebar nav a');
var sections = [];
navLinks.forEach(function(link) {
  var id = link.getAttribute('href');
  if (id && id.startsWith('#')) {
    var el = document.getElementById(id.slice(1));
    if (el) sections.push({ id: id, el: el, link: link });
  }
});

function updateActive() {
  var scrollY = window.scrollY + 100;
  var current = sections[0];
  for (var i = 0; i < sections.length; i++) {
    if (sections[i].el.offsetTop <= scrollY) current = sections[i];
  }
  navLinks.forEach(function(l) { l.classList.remove('active'); });
  if (current) current.link.classList.add('active');
}
window.addEventListener('scroll', updateActive, { passive: true });
updateActive();
"""


def escape(text: str) -> str:
    return html.escape(text, quote=False)


def inline_format(text: str) -> str:
    text = escape(text)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    return text


def convert_table(lines: list[str]) -> str:
    rows = [l.strip().strip("|").split("|") for l in lines if l.strip().startswith("|")]
    if len(rows) < 2:
        return "".join(f"<p>{inline_format(l)}</p>" for l in lines)
    header = [c.strip() for c in rows[0]]
    body = [[c.strip() for c in r] for r in rows[2:]] if len(rows) > 2 else []
    out = "<table>\n<thead>\n<tr>"
    for h in header:
        out += f"<th>{inline_format(h)}</th>"
    out += "</tr>\n</thead>\n<tbody>\n"
    for row in body:
        out += "<tr>"
        for cell in row:
            out += f"<td>{inline_format(cell)}</td>"
        out += "</tr>\n"
    out += "</tbody>\n</table>\n"
    return out


def slugify(text: str) -> str:
    s = re.sub(r"[^\w\s-]", "", text.lower())
    return re.sub(r"[\s_-]+", "-", s).strip("-")


def convert_markdown(md: str) -> tuple[str, list[dict]]:
    """Convert markdown to HTML body, return (html, toc_entries)."""
    lines = md.split("\n")
    i = 0
    out: list[str] = []
    toc: list[dict] = []
    in_list = False

    def close_list():
        nonlocal in_list
        if in_list:
            out.append("</ul>\n")
            in_list = False

    while i < len(lines):
        line = lines[i]

        # Skip HTML comments
        if line.strip().startswith("<!--"):
            while i < len(lines) and "-->" not in lines[i]:
                i += 1
            i += 1
            continue

        # Fenced code block (standalone, not part of a tab group)
        if line.strip().startswith("```"):
            lang = line.strip().strip("`").strip()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            i += 1
            close_list()
            code = escape("\n".join(code_lines))
            out.append(f'<pre><code class="{lang}">{code}</code></pre>\n')
            continue

        # Tab pattern: **CLI:** / **curl:** / **Web UI:** followed by code block or other content
        if re.match(r"^\*\*(CLI|curl|Web UI):\*\*\s*$", line.strip()):
            tabs: list[tuple[str, str, bool]] = []  # (label, content, is_code)
            while i < len(lines):
                while i < len(lines) and lines[i].strip() == "":
                    i += 1
                if i >= len(lines):
                    break
                m = re.match(r"^\*\*(CLI|curl|Web UI):\*\*\s*$", lines[i].strip())
                if not m:
                    break
                label = m.group(1)
                if label == "CLI":
                    label = "cli"
                i += 1
                while i < len(lines) and lines[i].strip() == "":
                    i += 1
                if i < len(lines) and lines[i].strip().startswith("```"):
                    # Code-block tab (existing behaviour)
                    i += 1
                    cl: list[str] = []
                    while i < len(lines) and not lines[i].strip().startswith("```"):
                        cl.append(lines[i])
                        i += 1
                    i += 1
                    tabs.append((label, escape("\n".join(cl)), True))
                else:
                    # Raw-markdown tab — capture lines until next tab marker,
                    # a horizontal rule, a new ## heading, or EOF.
                    raw_lines: list[str] = []
                    while i < len(lines):
                        if re.match(r"^\*\*(CLI|curl|Web UI):\*\*\s*$", lines[i].strip()):
                            break
                        if lines[i].strip() == "---":
                            break
                        if re.match(r"^##\s+", lines[i]):
                            break
                        raw_lines.append(lines[i])
                        i += 1
                    tabs.append((label, "\n".join(raw_lines), False))

            if tabs:
                close_list()
                out.append('<div class="tab-group">\n<div class="tab-bar">\n')
                for idx, (label, _, _) in enumerate(tabs):
                    active = " active" if idx == 0 else ""
                    out.append(f'<button class="tab-btn{active}" data-tab="{idx}">{escape(label)}</button>\n')
                out.append("</div>\n")
                for idx, (_, content, is_code) in enumerate(tabs):
                    active = " active" if idx == 0 else ""
                    if is_code:
                        out.append(f'<div class="tab-panel{active}" data-panel="{idx}"><pre><code>{content}</code></pre></div>\n')
                    else:
                        raw_html, _ = convert_markdown(content)
                        out.append(f'<div class="tab-panel tab-panel-content{active}" data-panel="{idx}">{raw_html}</div>\n')
                out.append("</div>\n")
            continue

        # Headings
        m = re.match(r"^(#{1,4})\s+(.*)", line)
        if m:
            close_list()
            level = len(m.group(1))
            text_raw = m.group(2)
            text = inline_format(text_raw)
            slug = slugify(text_raw)
            if level <= 2:
                toc.append({"level": level, "text": text_raw, "slug": slug})
            out.append(f'<h{level} id="{slug}">{text}</h{level}>\n')
            i += 1
            continue

        # Horizontal rule
        if line.strip() == "---":
            close_list()
            out.append("<hr>\n")
            i += 1
            continue

        # Table
        if line.strip().startswith("|"):
            close_list()
            table_lines: list[str] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i])
                i += 1
            out.append(convert_table(table_lines))
            continue

        # Bullet list
        if re.match(r"^\s*[-*]\s+", line):
            if not in_list:
                out.append("<ul>\n")
                in_list = True
            text = inline_format(re.sub(r"^\s*[-*]\s+", "", line))
            out.append(f"<li>{text}</li>\n")
            i += 1
            continue

        # Numbered list
        if re.match(r"^\s*\d+\.\s+", line):
            close_list()
            text = inline_format(re.sub(r"^\s*\d+\.\s+", "", line))
            out.append(f"<li>{text}</li>\n")
            i += 1
            continue

        # Blank line
        if line.strip() == "":
            close_list()
            i += 1
            continue

        # Regular paragraph
        close_list()
        text = inline_format(line)
        out.append(f"<p>{text}</p>\n")
        i += 1

    close_list()
    return "".join(out), toc


def build_sidebar(toc: list[dict]) -> str:
    lines = ['<div class="sidebar-brand"><span class="brand-mark"></span>CrowdbKV</div>', "<nav>"]
    for entry in toc:
        cls = "sub" if entry["level"] == 2 else ""
        lines.append(
            f'<a href="#{entry["slug"]}" class="{cls}">{escape(entry["text"])}</a>'
        )
    lines.append("</nav>")
    return "\n".join(lines)


def build() -> None:
    if not SOURCE.exists():
        print(f"error: {SOURCE} not found", file=sys.stderr)
        sys.exit(1)

    md = SOURCE.read_text(encoding="utf-8")
    body, toc = convert_markdown(md)
    sidebar = build_sidebar(toc)

    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CrowdbKV User Guide</title>
<style>
{CSS}
</style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
{sidebar}
  </aside>
  <main class="content">
{body}
  </main>
</div>
<script>
{JS}
</script>
</body>
</html>
"""

    OUTPUT.write_text(doc, encoding="utf-8")
    print(f"wrote {OUTPUT} ({len(doc)} bytes)")


if __name__ == "__main__":
    build()
