import os
import re
from datetime import datetime

from markdown_it import MarkdownIt
from playwright.sync_api import sync_playwright


class ReportExporter:
    def __init__(self, company_name, base_dir="investigation/equities"):
        self.company_name = company_name
        self.company_dir = os.path.join(base_dir, company_name)
        self.reports_dir = os.path.join(self.company_dir, "reports")
        # 启用 GFM 模式以支持表格和任务列表
        self.md = MarkdownIt("gfm-like")

    def _get_sorted_files(self):
        """定义章节合并顺序：章节一至八 + 跟踪手册（不包含完整版报告）

        使用章节号前缀匹配，而不是完整文件名，以支持不同副标题命名风格。
        """
        if not os.path.exists(self.reports_dir):
            return []

        all_files = os.listdir(self.reports_dir)

        # 章节号前缀匹配顺序（支持不同的副标题命名）
        chapter_prefixes = [
            "研报章节一",
            "研报章节二",
            "研报章节三",
            "研报章节四",
            "研报章节五",
            "研报章节六",
            "研报章节七",
            "研报章节八",
        ]

        sorted_files = []
        for prefix in chapter_prefixes:
            # 查找匹配该章节前缀的文件
            matching_files = [
                f for f in all_files if f.startswith(prefix) and f.endswith(".md")
            ]
            if matching_files:
                # 按文件名排序，确保稳定性
                matching_files.sort()
                sorted_files.append(os.path.join(self.reports_dir, matching_files[0]))

        # 强制加上跟踪手册（位于公司根目录）
        manual = os.path.join(self.company_dir, "跟踪手册.md")
        if os.path.exists(manual):
            sorted_files.append(manual)

        return sorted_files

    def _extract_metadata(self, reports_dir):
        """从章节报告中尝试提取评级和目标价（通常在章节五或七）"""
        meta = {"rating": "N/A", "target": "N/A"}
        # 优先从章节五（估值）或章节七（摘要）中搜索
        search_files = [
            "研报章节七：投资摘要与风险因素.md",
            "研报章节五：估值定价分析.md",
        ]
        for fname in search_files:
            fpath = os.path.join(reports_dir, fname)
            if os.path.exists(fpath):
                with open(fpath, encoding="utf-8") as f:
                    content = f.read()
                    rating_match = re.search(r"评级[：:]\s*(.+)", content)
                    target_match = re.search(r"目标价[：:]\s*(.+)", content)
                    if rating_match and meta["rating"] == "N/A":
                        meta["rating"] = rating_match.group(1).split("\n")[0].strip()
                    if target_match and meta["target"] == "N/A":
                        meta["target"] = target_match.group(1).split("\n")[0].strip()
        return meta

    def _generate_cover_html(self, metadata):
        """生成封面 HTML"""
        return f"""
        <div class="cover">
            <h1 class="title">{self.company_name}</h1>
            <h2 class="subtitle">深度投资价值分析报告</h2>
            <div class="meta-box">
                <div class="meta-item">投资评级：<strong>{metadata["rating"]}</strong></div>
                <div class="meta-item">目标价：<strong>{metadata["target"]}</strong></div>
                <div class="meta-item">研究日期：{datetime.now().strftime("%Y年%m月%d日")}</div>
            </div>
            <div class="footer">QuantPyLab Institutional Standard (Simulation Mode)</div>
        </div>
        """

    def export(self, output_path=None):
        files = self._get_sorted_files()
        if not files:
            raise Exception(f"未找到 {self.company_name} 的研报文件")

        combined_md = ""
        # 提取封面数据
        metadata = self._extract_metadata(self.reports_dir)

        for fpath in files:
            with open(fpath, encoding="utf-8") as f:
                content = f.read()
                # 注入分页符和内容
                combined_md += (
                    content + "\n\n<div style='page-break-after: always;'></div>\n\n"
                )

        # 渲染 HTML
        html_body = self.md.render(combined_md)

        full_html = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Segoe UI", Roboto, sans-serif; line-height: 1.7; color: #333; padding: 30px; }}
                h1, h2, h3 {{ color: #1a3a5a; border-bottom: 1px solid #eee; padding-bottom: 0.3em; margin-top: 1.5em; page-break-after: avoid; }}
                
                /* 表格排版修正 */
                table {{ border-collapse: collapse; width: 100%; margin: 1.5em 0; border: 1px solid #ccc; }}
                th, td {{ border: 1px solid #ccc; padding: 12px; text-align: left; font-size: 10pt; }}
                th {{ background-color: #f2f2f2; font-weight: bold; color: #1a3a5a; }}
                tr:nth-child(even) {{ background-color: #fafafa; }}
                
                .cover {{ text-align: center; padding-top: 150px; height: 900px; display: flex; flex-direction: column; }}
                .title {{ font-size: 54pt; margin-bottom: 20px; border: none; font-weight: 800; }}
                .subtitle {{ font-size: 28pt; color: #555; border: none; margin-bottom: 100px; }}
                .meta-box {{ margin-top: 100px; font-size: 16pt; flex-grow: 1; }}
                .meta-item {{ margin: 15px 0; }}
                .footer {{ margin-top: auto; font-size: 11pt; color: #aaa; padding-bottom: 50px; }}
                
                blockquote {{ border-left: 5px solid #1a3a5a; padding: 10px 20px; color: #555; background: #f8f9fa; margin: 1.5em 0; }}
                img {{ max-width: 100%; height: auto; }}
                hr {{ border: 0; border-top: 1px solid #eee; margin: 2em 0; }}
            </style>
        </head>
        <body>
            {self._generate_cover_html(metadata)}
            <div style="page-break-after: always;"></div>
            {html_body}
        </body>
        </html>
        """

        if not output_path:
            filename = f"{self.company_name}_深度研究报告_合订本_{datetime.now().strftime('%Y%m%d')}.pdf"
            output_path = os.path.join(self.company_dir, filename)

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.set_content(full_html)
            # 等待渲染完成
            page.wait_for_timeout(1000)
            page.pdf(
                path=output_path,
                format="A4",
                margin={
                    "top": "1.5cm",
                    "bottom": "1.5cm",
                    "left": "1.5cm",
                    "right": "1.5cm",
                },
                print_background=True,
            )
            browser.close()

        return output_path
