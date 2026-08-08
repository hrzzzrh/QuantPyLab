"""单元测试: utils/report_exporter.py 研报章节排序与元数据提取 (不触发浏览器)"""

import os

import pytest

from utils.report_exporter import ReportExporter


@pytest.fixture
def company_dir(tmp_path):
    """构造模拟研报目录结构"""
    base = tmp_path / "investigation" / "equities" / "贵州茅台"
    reports_dir = base / "reports"
    reports_dir.mkdir(parents=True)
    return base, reports_dir


def _write_file(path, content=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestGetSortedFiles:
    def test_chapter_order_and_manual(self, company_dir):
        base, reports_dir = company_dir
        _write_file(reports_dir / "研报章节一：公司概况.md", "# 章节一")
        _write_file(reports_dir / "研报章节二：行业分析.md", "# 章节二")
        _write_file(reports_dir / "研报章节三：业务拆解.md", "# 章节三")
        _write_file(reports_dir / "研报章节五：估值定价分析.md", "# 章节五")
        _write_file(reports_dir / "研报章节七：投资摘要与风险因素.md", "# 章节七")
        _write_file(
            reports_dir / "贵州茅台_完整版深度研究报告_2026年1月1日.md", "# 完整版"
        )
        _write_file(base / "跟踪手册.md", "# 跟踪手册")

        exporter = ReportExporter("贵州茅台", base_dir=str(base.parent))
        files = exporter._get_sorted_files()

        basenames = [os.path.basename(f) for f in files]
        assert basenames == [
            "研报章节一：公司概况.md",
            "研报章节二：行业分析.md",
            "研报章节三：业务拆解.md",
            "研报章节五：估值定价分析.md",
            "研报章节七：投资摘要与风险因素.md",
            "跟踪手册.md",
        ]

    def test_full_report_excluded(self, company_dir):
        """完整版合订本不应被合并进章节列表"""
        base, reports_dir = company_dir
        _write_file(reports_dir / "研报章节一：公司概况.md")
        _write_file(reports_dir / "贵州茅台_完整版深度研究报告_2026年1月1日.md")
        _write_file(base / "跟踪手册.md")

        exporter = ReportExporter("贵州茅台", base_dir=str(base.parent))
        files = exporter._get_sorted_files()
        assert all("完整版" not in os.path.basename(f) for f in files)

    def test_no_files_returns_empty(self, company_dir):
        base, _ = company_dir
        exporter = ReportExporter("贵州茅台", base_dir=str(base.parent))
        assert exporter._get_sorted_files() == []

    def test_manual_missing_still_returns_chapters(self, company_dir):
        base, reports_dir = company_dir
        _write_file(reports_dir / "研报章节一：公司概况.md")
        exporter = ReportExporter("贵州茅台", base_dir=str(base.parent))
        files = exporter._get_sorted_files()
        assert len(files) == 1

    def test_sorted_inside_same_chapter(self, company_dir):
        """同章节多版本文件时取字典序第一个"""
        base, reports_dir = company_dir
        _write_file(reports_dir / "研报章节一：公司概况 (v2).md", "v2")
        _write_file(reports_dir / "研报章节一：公司概况.md", "v1")
        exporter = ReportExporter("贵州茅台", base_dir=str(base.parent))
        files = exporter._get_sorted_files()
        assert os.path.basename(files[0]) == "研报章节一：公司概况 (v2).md"


class TestExtractMetadata:
    def test_extracts_rating_and_target(self, company_dir):
        _, reports_dir = company_dir
        _write_file(
            reports_dir / "研报章节七：投资摘要与风险因素.md",
            "评级：买入\n目标价：1800 元\n",
        )
        _write_file(
            reports_dir / "研报章节五：估值定价分析.md",
            "评级：增持\n目标价：1500 元\n",
        )

        exporter = ReportExporter("贵州茅台", base_dir=str(company_dir[0].parent))
        meta = exporter._extract_metadata(str(reports_dir))
        assert meta["rating"] == "买入"
        assert meta["target"] == "1800 元"

    def test_chapter_seven_takes_priority(self, company_dir):
        """章节七存在时应优先于章节五"""
        _, reports_dir = company_dir
        _write_file(
            reports_dir / "研报章节七：投资摘要与风险因素.md",
            "评级：买入\n目标价：1800 元\n",
        )
        _write_file(
            reports_dir / "研报章节五：估值定价分析.md",
            "评级：卖出\n目标价：100 元\n",
        )
        exporter = ReportExporter("贵州茅台", base_dir=str(company_dir[0].parent))
        meta = exporter._extract_metadata(str(reports_dir))
        assert meta["rating"] == "买入"
        assert meta["target"] == "1800 元"

    def test_missing_fields_default_na(self, company_dir):
        _, reports_dir = company_dir
        _write_file(reports_dir / "研报章节五：估值定价分析.md", "无评级信息")
        exporter = ReportExporter("贵州茅台", base_dir=str(company_dir[0].parent))
        meta = exporter._extract_metadata(str(reports_dir))
        assert meta == {"rating": "N/A", "target": "N/A"}

    def test_nonexistent_reports_dir(self, company_dir):
        base, _ = company_dir
        exporter = ReportExporter("贵州茅台", base_dir=str(base.parent))
        meta = exporter._extract_metadata(str(base / "no_such_dir"))
        assert meta == {"rating": "N/A", "target": "N/A"}
