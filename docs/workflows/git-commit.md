# Git Commit Workflow

本 workflow 用于提交前审查、生成 commit message、精确暂存、提交和推送。

## 入口判断

当用户要求提交、推送、生成 commit message、查看应该 add 哪些文件，或执行 commit and push 时，使用本 workflow。

## 提交前审查

1. 查看当前变更。
2. 区分本次任务相关变更与用户已有无关变更。
3. 不回滚、不覆盖、不整理用户已有无关改动。
4. 向用户说明建议暂存的精确文件清单。
5. 给出中文 commit message。

## 确认要求

必须等用户明确确认后，才能执行：

```bash
git add <精确文件>
git commit -m "<中文提交信息>"
git push
```

禁止使用：

```bash
git add .
```

## 提交后说明

完成后说明：

1. 实际暂存了哪些文件。
2. commit hash 或提交信息。
3. 是否已推送成功。
