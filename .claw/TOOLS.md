# TOOLS.md

## Local Evidence Sources

- `knowledge/material/material_label_map_w_chemicalname_分類_性質數據.xlsx`
- `knowledge/material/原材料分类.xlsx`
- `knowledge/fact-cache/`

## Bundled Formulation Surfaces

- `rust/crates/plugins/bundled/material-fact-lookup/`：材料事实查询
- `rust/crates/plugins/bundled/materials-search/`：材料搜索与归类
- `rust/crates/plugins/bundled/baseline-recipe-selection/`：基线配方筛选与 B-side 比例守卫

## Implementation Surfaces

- `rust/`：CLI、runtime、plugin 系统的主要实现层
- `src/`、`tests/`：参考实现与审计面

## Rule Of Thumb

- 配方类问题先查本地证据，再追问。
- 施工类问题默认覆盖混料顺序、脱泡、可操作时间、固化窗口、后固化与现场风险。
- 如果必须改代码，优先改与配方工作流直接相关的提示词、知识读取、工具接入和验证链路。
