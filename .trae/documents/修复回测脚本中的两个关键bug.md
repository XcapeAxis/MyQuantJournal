## 1. 数据覆盖审计

### 目标

生成数据库中每只股票的覆盖情况报告，确认数据完整性。

### 实现步骤

1. **创建数据覆盖审计函数**

   * 编写`generate_db_coverage_report`函数，查询每个股票的bars\_count、first\_date、last\_date

   * 输出`data/projects/2026Q1_mom/meta/db_coverage_report.csv`

   * 包含字段：code, bars\_count, first\_date, last\_date

2. **生成覆盖摘要**

   * 编写`generate_db_coverage_summary`函数，计算统计指标

   * 输出`data/projects/2026Q1_mom/meta/db_coverage_summary.json`

   * 包含字段：n\_codes\_in\_universe, n\_codes\_in\_db, median\_bars, p10\_bars, p90\_bars, min\_first\_date, max\_last\_date

3. **验证和报告**

   * 验证universe\_codes=3063

   * 分析DB覆盖分布

   * 明确回答回测实际可用的历史区间长度

## 2. Rank生成改进

### 目标

记录每个rebalance date参与打分的候选股票数量，提高rank的可信度。

### 实现步骤

1. **修改rank生成逻辑**

   * 更新`build_rank_topk_from_db`函数，记录每个date的candidate\_count

   * 计算当日可计算20日收益率且满足min\_bars的股票数量

   * 保存到`data/projects/2026Q1_mom/meta/rank_candidate_count.csv`

2. **更新manifest**

   * 修改`generate_run_manifest`函数，添加candidate\_count\_stats

   * 包含min/median/p10 candidate\_count

## 3. Bars全量回填模式

### 目标

添加两种运行模式，支持全量数据回填，确保足够的历史数据。

### 实现步骤

1. **添加CLI参数**

   * 在`main`函数中添加`--mode`参数，支持incremental和backfill模式

   * 添加`--start-date`和`--end-date`参数，用于backfill模式

2. **修改bars更新逻辑**

   * 更新数据加载逻辑，在backfill模式下忽略registry

   * 强制从指定start\_date补齐到end\_date

   * 确保3063支主板股票的数据完整性

3. **验证数据完整性**

   * 对任意抽样20支股票，验证bars\_count > 160

   * 确保DB覆盖区间与rank/date区间一致

## 4. Manifest进一步升级

### 目标

增强manifest信息，提高回测结果的可信度和可追溯性。

### 实现步骤

1. **更新manifest生成函数**

   * 修改`generate_run_manifest`函数，添加以下字段：

     * universe\_size=3063

     * rank\_unique\_codes=980

     * rank\_dates=480

     * candidate\_count\_stats

     * db\_coverage\_stats

     * effective\_backtest\_window

2. **计算effective\_backtest\_window**

   * 从实际calendar feed的区间获取回测实际发生交易的起止日期

## 预期结果

1. **数据覆盖审计**：生成完整的数据库覆盖报告，明确回测可用的历史区间
2. **Rank生成改进**：记录候选股票数量，提高rank的可信度
3. **Bars全量回填**：支持两种运行模式，确保足够的历史数据
4. **Manifest升级**：包含更丰富的回测信息，提高结果的可追溯性

## 执行顺序

1. 首先实现数据覆盖审计，了解当前数据状况
2. 然后修改rank生成逻辑，添加候选数量记录
3. 接着添加bars全量回填模式
4. 最后升级manifest，整合所有信息

## 验收标准

1. **数据覆盖审计**

   * 输出universe\_codes=3063

   * 输出DB覆盖分布（median/p10/p90 bars）

   * 明确回测实际可用的历史区间

2. **Rank生成改进**

   * candidate\_count的中位数明显大于5

   * 生成rank\_candidate\_count.csv文件

3. **Bars全量回填模式**

   * 支持incremental和backfill两种模式

   * 抽样20支股票的bars\_count > 160

   * DB覆盖区间与rank/date区间一致

4. **Manifest升级**

   * 包含所有要求的新增字段

   * 准确反映回测的关键信息

