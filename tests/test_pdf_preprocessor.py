"""Tests for pdf_preprocessor.py - Phase 2A PDF extraction pipeline.

Covers features #37-#46:
  #37: extract_all_pages
  #38: SECTION_KEYWORDS
  #39: find_section_pages
  #40: extract_section_context
  #41: write_output (JSON)
  #42: main pipeline
  #43: Traditional Chinese keywords
  #44: PyMuPDF fallback / garbled text detection
  #45: Table-aware extraction
  #46: Section priority scoring
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from scripts.pdf_preprocessor import (
    SECTION_KEYWORDS,
    SECTION_EXTRACT_CONFIG,
    DEFAULT_BUFFER_PAGES,
    DEFAULT_MAX_CHARS,
    extract_all_pages,
    fallback_extract_pymupdf,
    find_section_pages,
    extract_section_context,
    write_output,
    run_pipeline,
    is_garbled,
    _score_match,
    _tables_to_markdown,
    _truncate_at_boundary,
    parse_args,
)


# ============================================================
# Feature #38: SECTION_KEYWORDS
# ============================================================

class TestSectionKeywords:
    """Feature #38: Keyword library for 5 target sections."""

    def test_all_seven_sections_present(self):
        assert set(SECTION_KEYWORDS.keys()) == {"P2", "P3", "P4", "P6", "P13", "MDA", "SUB"}

    @pytest.mark.parametrize("section_id", ["P2", "P3", "P4", "P6", "P13", "MDA", "SUB"])
    def test_each_section_has_at_least_3_keywords(self, section_id):
        assert len(SECTION_KEYWORDS[section_id]) >= 3, (
            f"{section_id} has only {len(SECTION_KEYWORDS[section_id])} keywords"
        )

    def test_keywords_are_nonempty_strings(self):
        for section_id, keywords in SECTION_KEYWORDS.items():
            for kw in keywords:
                assert isinstance(kw, str) and len(kw) > 0

    # Feature #43: Traditional Chinese support
    @pytest.mark.parametrize("section_id", ["P2", "P3", "P4", "P6", "P13", "MDA", "SUB"])
    def test_has_traditional_chinese_keywords(self, section_id):
        """Each section should have at least one traditional Chinese variant."""
        trad_chars = set("權資產賬齡關聯負債損經項訟營諾決價際層討論與會報導")
        has_trad = any(
            any(ch in trad_chars for ch in kw)
            for kw in SECTION_KEYWORDS[section_id]
        )
        assert has_trad, f"{section_id} has no traditional Chinese keywords"


# ============================================================
# Feature #37: PDF text extraction
# ============================================================

class TestExtractAllPages:
    """Feature #37: pdfplumber page-by-page extraction."""

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError, match="PDF not found"):
            extract_all_pages("/nonexistent/file.pdf")

    @patch("scripts.pdf_preprocessor.pdfplumber")
    def test_extracts_pages_with_text(self, mock_pdfplumber):
        """Should return list of (page_number, text) tuples."""
        mock_page1 = MagicMock()
        mock_page1.extract_text.return_value = "Page one text"
        mock_page1.extract_tables.return_value = []

        mock_page2 = MagicMock()
        mock_page2.extract_text.return_value = "Page two text"
        mock_page2.extract_tables.return_value = []

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page1, mock_page2]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            f.write(b"dummy")
            f.flush()
            result = extract_all_pages(f.name)

        assert len(result) == 2
        assert result[0] == (1, "Page one text")
        assert result[1] == (2, "Page two text")

    @patch("scripts.pdf_preprocessor.pdfplumber")
    def test_handles_none_text_pages(self, mock_pdfplumber):
        """Pages with no extractable text should return empty string."""
        mock_page = MagicMock()
        mock_page.extract_text.return_value = None
        mock_page.extract_tables.return_value = []

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            f.write(b"dummy")
            f.flush()
            result = extract_all_pages(f.name)

        assert result[0] == (1, "")

    @patch("scripts.pdf_preprocessor.pdfplumber")
    def test_encrypted_pdf_raises_runtime_error(self, mock_pdfplumber):
        mock_pdfplumber.open.side_effect = Exception("encrypted PDF")

        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            f.write(b"dummy")
            f.flush()
            with pytest.raises(RuntimeError, match="encrypted"):
                extract_all_pages(f.name)


# ============================================================
# Feature #44: Garbled text detection and PyMuPDF fallback
# ============================================================

class TestGarbledDetection:
    """Feature #44: Detect garbled text and fallback to PyMuPDF."""

    def test_normal_chinese_text_not_garbled(self):
        text = "内蒙古伊利实业集团股份有限公司2024年年度报告"
        assert is_garbled(text) is False

    def test_normal_english_text_not_garbled(self):
        text = "Annual Report 2024 - Financial Summary"
        assert is_garbled(text) is False

    def test_mixed_text_not_garbled(self):
        text = "营业收入 115,393,310,976.69 元 (Revenue)"
        assert is_garbled(text) is False

    def test_garbled_text_detected(self):
        # Simulate garbled text with lots of unusual characters
        text = "".join(chr(0xFFFD) for _ in range(100))
        assert is_garbled(text) is True

    def test_empty_text_is_garbled(self):
        assert is_garbled("") is True

    def test_threshold_adjustable(self):
        # 20% normal, 80% garbled
        text = "AB" + "".join(chr(0xFFFD) for _ in range(8))
        # threshold=0.30 means we need 70% normal chars. We have 20% => garbled
        assert is_garbled(text, threshold=0.30) is True
        # threshold=0.90 means we need 10% normal chars. We have 20% => not garbled
        assert is_garbled(text, threshold=0.90) is False


class TestPyMuPDFFallback:
    """Feature #44: PyMuPDF fallback."""

    def test_returns_none_if_fitz_not_installed(self):
        with patch.dict("sys.modules", {"fitz": None}):
            result = fallback_extract_pymupdf("/dummy.pdf")
            assert result is None


# ============================================================
# Feature #45: Table-aware extraction
# ============================================================

class TestTableExtraction:
    """Feature #45: Convert tables to markdown."""

    def test_tables_to_markdown_basic(self):
        tables = [
            [
                ["项目", "期末余额", "期初余额"],
                ["货币资金", "10,000", "8,000"],
                ["应收账款", "5,000", "4,500"],
            ]
        ]
        md = _tables_to_markdown(tables)
        assert "| 项目 | 期末余额 | 期初余额 |" in md
        assert "| --- | --- | --- |" in md
        assert "| 货币资金 | 10,000 | 8,000 |" in md

    def test_tables_to_markdown_handles_none_cells(self):
        tables = [
            [
                ["A", "B"],
                [None, "value"],
            ]
        ]
        md = _tables_to_markdown(tables)
        assert "| A | B |" in md
        assert "|  | value |" in md

    def test_tables_to_markdown_empty_table(self):
        # Table with only header (< 2 rows) is skipped
        tables = [[["Header"]]]
        md = _tables_to_markdown(tables)
        assert md == ""

    def test_tables_to_markdown_empty_input(self):
        assert _tables_to_markdown([]) == ""

    @patch("scripts.pdf_preprocessor.pdfplumber")
    def test_page_with_table_appends_markdown(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Some text"
        mock_page.extract_tables.return_value = [
            [["Col1", "Col2"], ["a", "b"]]
        ]

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            f.write(b"dummy")
            f.flush()
            result = extract_all_pages(f.name)

        assert "[TABLE]" in result[0][1]
        assert "| Col1 | Col2 |" in result[0][1]


# ============================================================
# Feature #39: Keyword matching
# ============================================================

class TestFindSectionPages:
    """Feature #39: Locate sections by keyword scanning."""

    def _make_pages(self, page_texts: dict) -> list:
        """Helper: {page_num: text} -> [(page_num, text)]."""
        return [(pn, text) for pn, text in sorted(page_texts.items())]

    def test_finds_sections_by_keyword(self):
        pages = self._make_pages({
            1: "目录 第一节 释义",
            50: "所有权或使用权受限资产明细",
            100: "应收账款账龄分析",
            150: "关联方及关联交易",
            200: "或有事项说明",
            250: "非经常性损益明细表",
        })
        result = find_section_pages(pages)
        assert 50 in result["P2"]
        assert 100 in result["P3"]
        assert 150 in result["P4"]
        assert 200 in result["P6"]
        assert 250 in result["P13"]

    def test_empty_pages_returns_empty_lists(self):
        result = find_section_pages([])
        for section_id in ["P2", "P3", "P4", "P6", "P13", "MDA", "SUB"]:
            assert result[section_id] == []

    def test_no_matches_returns_empty_lists(self):
        pages = self._make_pages({1: "无关内容", 2: "无关内容"})
        result = find_section_pages(pages)
        for section_id in ["P2", "P3", "P4", "P6", "P13", "MDA", "SUB"]:
            assert result[section_id] == []

    def test_multiple_matches_returns_all_pages(self):
        pages = self._make_pages({
            7: "非经常性损益 目录",  # TOC mention
            213: "非经常性损益明细",  # actual content
            247: "扣除非经常性损益",  # another mention
        })
        result = find_section_pages(pages)
        assert len(result["P13"]) == 3

    def test_custom_keywords(self):
        custom = {"TEST": ["自定义关键词"]}
        pages = self._make_pages({1: "包含自定义关键词的文本"})
        result = find_section_pages(pages, section_keywords=custom)
        assert 1 in result["TEST"]


# ============================================================
# Feature #46: Section priority scoring
# ============================================================

class TestSectionScoring:
    """Feature #46: Prefer financial notes over TOC."""

    def test_later_pages_score_higher(self):
        text = "受限资产"
        score_early = _score_match(10, 270, text, "受限资产")
        score_late = _score_match(200, 270, text, "受限资产")
        assert score_late > score_early

    def test_toc_page_penalized(self):
        text_normal = "受限资产说明"
        text_toc = "目录 受限资产.........50"
        score_normal = _score_match(100, 270, text_normal, "受限资产")
        score_toc = _score_match(100, 270, text_toc, "受限资产")
        assert score_normal > score_toc

    def test_heading_context_bonus(self):
        text_heading = "31、所有权或使用权受限资产\n明细如下"
        text_plain = "其中受限资产部分已处理"
        score_heading = _score_match(200, 270, text_heading, "所有权或使用权受限资产")
        score_plain = _score_match(200, 270, text_plain, "所有权或使用权受限资产")
        assert score_heading > score_plain

    def test_cross_reference_penalized(self):
        text_xref = '货币资金受限情况详见注释七"31、所有权或使用权受限资产"'
        text_actual = "31、所有权或使用权受限资产\n受限明细如下"
        score_xref = _score_match(151, 270, text_xref, "受限资产")
        score_actual = _score_match(188, 270, text_actual, "受限资产")
        assert score_actual > score_xref

    def test_best_match_is_first_in_results(self):
        """find_section_pages should return pages sorted by score (best first)."""
        pages = [
            (5, "目录 非经常性损益.........8"),  # TOC page, early
            (250, "十七、非经常性损益明细如下"),  # heading in notes area
        ]
        result = find_section_pages(pages)
        assert result["P13"][0] == 250  # best match first


# ============================================================
# Feature #40: Context extraction
# ============================================================

class TestExtractSectionContext:
    """Feature #40: Target page +/- buffer, truncate to 2000 chars."""

    def _make_pages(self, n: int, char_per_page: int = 100) -> list:
        """Create n pages of dummy text."""
        return [(i + 1, f"Page {i + 1} content. " * (char_per_page // 20))
                for i in range(n)]

    def test_combines_three_pages(self):
        pages = self._make_pages(10)
        section_pages = {"P2": [5]}
        result = extract_section_context(pages, section_pages, buffer_pages=1)
        # Should contain text from pages 4, 5, 6
        assert "p.4" in result["P2"]
        assert "p.5" in result["P2"]
        assert "p.6" in result["P2"]

    def test_buffer_at_start(self):
        """Page 1 match: only page 1 and 2 (no page 0)."""
        pages = self._make_pages(5)
        section_pages = {"P2": [1]}
        result = extract_section_context(pages, section_pages, buffer_pages=1)
        assert "p.1" in result["P2"]
        assert "p.2" in result["P2"]

    def test_buffer_at_end(self):
        """Last page match: only last-1 and last."""
        pages = self._make_pages(5)
        section_pages = {"P2": [5]}
        result = extract_section_context(pages, section_pages, buffer_pages=1)
        assert "p.4" in result["P2"]
        assert "p.5" in result["P2"]

    def test_not_found_returns_none(self):
        pages = self._make_pages(5)
        section_pages = {"P2": []}
        result = extract_section_context(pages, section_pages)
        assert result["P2"] is None

    def test_truncation_at_max_chars(self):
        # Create long pages
        pages = [(i + 1, "长文本内容。" * 500) for i in range(5)]
        section_pages = {"P2": [3]}
        result = extract_section_context(pages, section_pages, max_chars=2000)
        assert len(result["P2"]) <= 2100  # some tolerance for boundary


class TestTruncateAtBoundary:
    """Helper function: truncation at sentence boundaries."""

    def test_no_truncation_if_short(self):
        text = "Short text."
        assert _truncate_at_boundary(text, 100) == text

    def test_truncates_at_chinese_period(self):
        # Build text where the period is well past 50% of max_chars
        text = "这是一段很长的话" * 8 + "。" + "后续内容" * 20
        result = _truncate_at_boundary(text, 100)
        assert result.endswith("。")
        assert len(result) <= 101

    def test_truncates_at_newline_if_no_period(self):
        text = "Line one\nLine two\nLine three" + "x" * 100
        result = _truncate_at_boundary(text, 30)
        assert result.endswith("\n")


# ============================================================
# Feature #41: JSON output
# ============================================================

class TestWriteOutput:
    """Feature #41: Write pdf_sections.json."""

    def test_writes_valid_json(self):
        contexts = {
            "P2": "受限资产内容",
            "P3": "应收账款内容",
            "P4": None,
            "P6": "或有负债内容",
            "P13": None,
            "MDA": "管理层讨论与分析内容",
            "SUB": "主要控股参股公司分析内容",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test_output.json")
            result = write_output(contexts, "/path/to/report.pdf", 270, output_path)

            # Check file was created
            assert os.path.exists(output_path)

            # Read and validate
            with open(output_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            assert data["metadata"]["pdf_file"] == "report.pdf"
            assert data["metadata"]["total_pages"] == 270
            assert data["metadata"]["sections_found"] == 5
            assert data["metadata"]["sections_total"] == 7
            assert data["P2"] == "受限资产内容"
            assert data["P4"] is None
            assert data["P13"] is None
            assert data["MDA"] == "管理层讨论与分析内容"
            assert data["SUB"] == "主要控股参股公司分析内容"

    def test_null_for_all_unfound_sections(self):
        contexts = {k: None for k in ["P2", "P3", "P4", "P6", "P13", "MDA", "SUB"]}
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "out.json")
            result = write_output(contexts, "test.pdf", 100, output_path)
            assert result["metadata"]["sections_found"] == 0

    def test_creates_output_directory(self):
        contexts = {"P2": "text"}
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = os.path.join(tmpdir, "sub", "dir", "out.json")
            write_output(contexts, "test.pdf", 10, nested)
            assert os.path.exists(nested)

    def test_metadata_has_extract_time(self):
        contexts = {"P2": "text"}
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "out.json")
            result = write_output(contexts, "test.pdf", 10, output_path)
            assert "extract_time" in result["metadata"]
            # Should be a valid datetime string
            datetime.strptime(result["metadata"]["extract_time"], "%Y-%m-%d %H:%M:%S")


# ============================================================
# Feature #42: Main pipeline (end-to-end with mocks)
# ============================================================

class TestRunPipeline:
    """Feature #42: Chain extraction -> matching -> context -> output."""

    @patch("scripts.pdf_preprocessor.extract_all_pages")
    @patch("scripts.config.validate_pdf")
    def test_pipeline_produces_json(self, mock_validate, mock_extract):
        mock_validate.return_value = (True, "Valid PDF")
        mock_extract.return_value = [
            (1, "目录内容"),
            (2, "公司概况"),
            (3, "31、所有权或使用权受限资产 受限明细如下"),
            (4, "应收账款账龄分析 1年以内80%"),
            (5, "十四、关联方及关联交易 采购原材料"),
            (6, "或有事项 对外担保"),
            (7, "非经常性损益明细 政府补贴"),
            (8, "董事会报告 管理层讨论与分析"),
            (9, "主要控股参股公司分析 子公司名称"),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test.json")
            result = run_pipeline("/dummy.pdf", output_path, verbose=True)

            assert result["metadata"]["total_pages"] == 9
            assert result["metadata"]["sections_found"] == 7

            for section in ["P2", "P3", "P4", "P6", "P13", "MDA", "SUB"]:
                assert result[section] is not None

    @patch("scripts.config.validate_pdf")
    def test_pipeline_invalid_pdf_raises(self, mock_validate):
        mock_validate.return_value = (False, "File too small")
        with pytest.raises(RuntimeError, match="Invalid PDF"):
            run_pipeline("/dummy.pdf", "/out.json")

    @patch("scripts.pdf_preprocessor.extract_all_pages")
    @patch("scripts.config.validate_pdf")
    def test_pipeline_no_pages_raises(self, mock_validate, mock_extract):
        mock_validate.return_value = (True, "Valid")
        mock_extract.return_value = []
        with pytest.raises(RuntimeError, match="no extractable pages"):
            run_pipeline("/dummy.pdf", "/out.json")


# ============================================================
# CLI argument parsing
# ============================================================

class TestParseArgs:
    """CLI argument parsing tests."""

    def test_required_pdf_arg(self):
        args = parse_args(["--pdf", "test.pdf"])
        assert args.pdf == "test.pdf"
        assert args.output == "output/pdf_sections.json"
        assert args.verbose is False
        assert args.dry_run is False

    def test_all_args(self):
        args = parse_args([
            "--pdf", "report.pdf",
            "--output", "custom.json",
            "--verbose",
            "--dry-run",
        ])
        assert args.pdf == "report.pdf"
        assert args.output == "custom.json"
        assert args.verbose is True
        assert args.dry_run is True


# ============================================================
# MDA extraction (per-section config)
# ============================================================

class TestMDAExtraction:
    """MDA section extraction with per-section config."""

    def test_mda_keywords_in_section_keywords(self):
        """MDA key exists with expected keywords."""
        assert "MDA" in SECTION_KEYWORDS
        keywords = SECTION_KEYWORDS["MDA"]
        assert "管理层讨论与分析" in keywords
        assert "董事会报告" in keywords

    def test_mda_extract_config(self):
        """MDA has buffer_pages=3, max_chars=8000."""
        assert "MDA" in SECTION_EXTRACT_CONFIG
        assert SECTION_EXTRACT_CONFIG["MDA"]["buffer_pages"] == 3
        assert SECTION_EXTRACT_CONFIG["MDA"]["max_chars"] == 8000

    def test_default_config_values(self):
        """Default buffer/max_chars are 1/4000."""
        assert DEFAULT_BUFFER_PAGES == 1
        assert DEFAULT_MAX_CHARS == 4000

    def test_mda_in_output_sections(self):
        """write_output includes MDA key."""
        contexts = {"MDA": "管理层讨论内容", "P2": None, "P3": None, "P4": None, "P6": None, "P13": None, "SUB": None}
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "out.json")
            result = write_output(contexts, "test.pdf", 100, output_path)
            assert "MDA" in result
            assert result["MDA"] == "管理层讨论内容"

    def test_extract_context_uses_per_section_config(self):
        """MDA gets larger buffer (3 pages) than default (1 page)."""
        # Create 20 pages of text
        pages = [(i + 1, f"Page {i + 1} content. " * 5) for i in range(20)]
        # Put MDA keyword on page 10, P2 keyword on page 15
        pages[9] = (10, "管理层讨论与分析 经营情况回顾")
        pages[14] = (15, "受限资产 明细如下")

        section_pages = find_section_pages(pages)
        contexts = extract_section_context(pages, section_pages)

        # MDA should include pages 7-13 (buffer=3), P2 should include pages 14-16 (buffer=1)
        mda_text = contexts["MDA"]
        p2_text = contexts["P2"]
        assert mda_text is not None
        assert p2_text is not None
        # MDA should contain more page references due to larger buffer
        assert "p.7" in mda_text  # 10 - 3
        assert "p.13" in mda_text  # 10 + 3
        # P2 should only have ±1 buffer
        assert "p.14" in p2_text
        assert "p.16" in p2_text
        assert "p.13" not in p2_text  # outside P2's buffer

    def test_mda_not_found_returns_null(self):
        """Null when no MDA keywords match."""
        pages = [(1, "无关内容"), (2, "其他文本")]
        section_pages = find_section_pages(pages)
        contexts = extract_section_context(pages, section_pages)
        assert contexts["MDA"] is None


# ============================================================
# SUB extraction (subsidiary holdings)
# ============================================================

class TestSUBExtraction:
    """SUB section extraction for subsidiary holdings data."""

    def test_sub_keywords_in_section_keywords(self):
        """SUB key exists with expected keywords."""
        assert "SUB" in SECTION_KEYWORDS
        keywords = SECTION_KEYWORDS["SUB"]
        assert "主要控股参股公司分析" in keywords
        assert "长期股权投资" in keywords

    def test_sub_extract_config(self):
        """SUB has buffer_pages=2, max_chars=6000."""
        assert "SUB" in SECTION_EXTRACT_CONFIG
        assert SECTION_EXTRACT_CONFIG["SUB"]["buffer_pages"] == 2
        assert SECTION_EXTRACT_CONFIG["SUB"]["max_chars"] == 6000

    def test_sub_in_output_sections(self):
        """write_output includes SUB key in JSON."""
        contexts = {"SUB": "控股参股公司内容", "P2": None, "P3": None, "P4": None, "P6": None, "P13": None, "MDA": None}
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "out.json")
            result = write_output(contexts, "test.pdf", 100, output_path)
            assert "SUB" in result
            assert result["SUB"] == "控股参股公司内容"

    def test_sub_keyword_matching(self):
        """find_section_pages finds SUB keywords on correct pages."""
        pages = [
            (1, "目录内容"),
            (100, "主要控股参股公司分析 子公司列表如下"),
            (200, "长期股权投资 明细"),
        ]
        result = find_section_pages(pages)
        assert 100 in result["SUB"]
        assert 200 in result["SUB"]

    def test_extract_context_uses_sub_config(self):
        """SUB gets buffer=2 (5 pages total: p.8-p.12 for match on p.10)."""
        pages = [(i + 1, f"Page {i + 1} content. " * 5) for i in range(15)]
        pages[9] = (10, "主要控股参股公司分析 子公司列表")

        section_pages = find_section_pages(pages)
        contexts = extract_section_context(pages, section_pages)

        sub_text = contexts["SUB"]
        assert sub_text is not None
        # buffer=2: pages 8-12
        assert "p.8" in sub_text   # 10 - 2
        assert "p.12" in sub_text  # 10 + 2

    def test_sub_not_found_returns_null(self):
        """None when no SUB keywords match."""
        pages = [(1, "无关内容"), (2, "其他文本")]
        section_pages = find_section_pages(pages)
        contexts = extract_section_context(pages, section_pages)
        assert contexts["SUB"] is None

    def test_sub_traditional_chinese_keywords(self):
        """At least one traditional Chinese keyword exists."""
        trad_chars = set("權資產賬齡關聯負債損經項訟營諾決價際層討論與會報導體")
        has_trad = any(
            any(ch in trad_chars for ch in kw)
            for kw in SECTION_KEYWORDS["SUB"]
        )
        assert has_trad, "SUB has no traditional Chinese keywords"


# ============================================================
# Import for datetime in test
# ============================================================
from datetime import datetime
