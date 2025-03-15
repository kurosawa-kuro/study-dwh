以下のチュートリアルは、Kaggleの有名データセット **[House Prices - Advanced Regression Techniques](https://www.kaggle.com/competitions/house-prices-advanced-regression-techniques)** をモチーフに、**DuckDB** で **スター・スキーマ** を体験するサンプルコードです。実際のデータセット全体を使うと多くのカラムがあって複雑になりますので、ここではデータ構造を**簡略化**した **MVP（Minimum Viable Product）** 例を示します。

---

# DuckDB × House Prices データでスター・スキーマを試す

## 概要

- Kaggle の “House Prices” データは、**住宅属性**や**立地**、**築年数**など多数の特徴量を持ち、最終的な **SalePrice**（売却額）を予測することが目的。
- 今回は「DWHのスター・スキーマ」として使うため、下記のように**ディメンションテーブル**と**ファクトテーブル**を切り分ける例を作ります。

### スキーマ例

1. **ディメンションテーブル**  
   - `dim_house` : 住宅自体の特性（`house_key`, `Neighborhood`, `HouseStyle` など）  
   - `dim_date` : 住宅が売れた日付情報（`date_key`, `year`, `month`）  
     - (必要に応じて築年数、リフォーム年なども別テーブル化してもOK)  
2. **ファクトテーブル**  
   - `fact_sales` : 売却(セール)の事実テーブル  
     - `sale_id` (PK), `house_key` (FK), `date_key` (FK), `sale_price`, etc.

---

## 1. DuckDB のインストール

```python
# %% [code]
!pip install duckdb --quiet
```

---

## 2. サンプルDataFrameを作成 (pandas)

ここでは、Kaggleのデータセットから抜粋したと想定する**住宅属性**や**売却価格**などを、  
**簡略化**して手動で DataFrame にしています。実際には Kaggle の CSV を読み込んで前処理後に同様のテーブル設計を作るイメージです。

```python
# %% [code]
import pandas as pd

# ----------------------
# ディメンション1: dim_house
# ----------------------
df_house = pd.DataFrame({
    'house_key': [1, 2, 3, 4],
    'MSSubClass': [60, 20, 70, 50],            # 物件種別(例: 1階建, 2階建, etc.)
    'Neighborhood': ['NAmes', 'CollgCr', 'StoneBr', 'NAmes'],
    'HouseStyle': ['2Story', '1Story', '2Story', '1Story'],
    'OverallQual': [7, 6, 8, 5],               # 全体的な品質 (1~10)
})

df_house
```
<details>
<summary>表示イメージ</summary>

```
   house_key  MSSubClass Neighborhood HouseStyle  OverallQual
0          1          60        NAmes     2Story            7
1          2          20     CollgCr     1Story            6
2          3          70     StoneBr     2Story            8
3          4          50        NAmes     1Story            5
```
</details>

---

```python
# %% [code]
# ----------------------
# ディメンション2: dim_date
# (月・年だけで日付キーを作る簡易例)
# ----------------------
df_date = pd.DataFrame({
    'date_key': [202006, 202007],
    'year': [2020, 2020],
    'month': [6, 7]
    # 必要なら day や weekday なども追加
})

df_date
```
<details>
<summary>表示イメージ</summary>

```
   date_key  year  month
0    202006  2020      6
1    202007  2020      7
```
</details>

---

```python
# %% [code]
# ----------------------
# ファクトテーブル: fact_sales
# (実際はSalePrice, YrSold, MoSoldなどを整形)
# ----------------------
df_sales = pd.DataFrame({
    'sale_id': [1001, 1002, 1003],
    'house_key': [1, 3, 2],     # FK: dim_house
    'date_key': [202006, 202007, 202006],  # FK: dim_date
    'sale_price': [200000, 340000, 150000],
    'lot_area': [8000, 9500, 7200]  # 追加のメトリック例 (敷地面積など)
})

df_sales
```
<details>
<summary>表示イメージ</summary>

```
   sale_id  house_key  date_key  sale_price  lot_area
0     1001          1    202006      200000      8000
1     1002          3    202007      340000      9500
2     1003          2    202006      150000      7200
```
</details>

---

## 3. DuckDBにテーブルを作成してINSERT

```python
# %% [code]
import duckdb

# メモリ上にDB作成 (ファイル保存したければ 'house_prices.duckdb' なども可)
con = duckdb.connect(database=':memory:')

# --- ディメンション: dim_house
con.execute("""
CREATE TABLE dim_house (
  house_key INT,
  MSSubClass INT,
  Neighborhood VARCHAR,
  HouseStyle VARCHAR,
  OverallQual INT
);
""")

# --- ディメンション: dim_date
con.execute("""
CREATE TABLE dim_date (
  date_key INT,
  year INT,
  month INT
);
""")

# --- ファクト: fact_sales
con.execute("""
CREATE TABLE fact_sales (
  sale_id INT,
  house_key INT,
  date_key INT,
  sale_price INT,
  lot_area INT
);
""")

# ---- pandasのデータフレームをDuckDBに登録＆INSERT
con.register("temp_house", df_house)
con.register("temp_date", df_date)
con.register("temp_sales", df_sales)

con.execute("INSERT INTO dim_house SELECT * FROM temp_house")
con.execute("INSERT INTO dim_date  SELECT * FROM temp_date")
con.execute("INSERT INTO fact_sales SELECT * FROM temp_sales")
```

---

## 4. 確認してみる

```python
# %% [code]
res_house = con.execute("SELECT * FROM dim_house").df()
res_date  = con.execute("SELECT * FROM dim_date").df()
res_sales = con.execute("SELECT * FROM fact_sales").df()

display(res_house)
display(res_date)
display(res_sales)
```

---

## 5. スター・スキーマJOINで集計クエリ

### 例1: Neighborhood(近隣)別の平均売却価格

```python
# %% [code]
query1 = """
SELECT
    h.Neighborhood,
    AVG(s.sale_price) AS avg_price
FROM fact_sales AS s
JOIN dim_house AS h
  ON s.house_key = h.house_key
GROUP BY h.Neighborhood
ORDER BY avg_price DESC
"""

df_q1 = con.execute(query1).df()
df_q1
```
<details>
<summary>出力例</summary>

```
  Neighborhood       avg_price
0     StoneBr        340000.0
1       NAmes        200000.0
2     CollgCr        150000.0
```
</details>

---

### 例2: HouseStyle × month 別に売却数と平均敷地面積を集計

```python
# %% [code]
query2 = """
SELECT
    h.HouseStyle,
    d.month,
    COUNT(*) AS sales_count,
    AVG(s.lot_area) AS avg_lot_area
FROM fact_sales s
JOIN dim_house h ON s.house_key = h.house_key
JOIN dim_date d  ON s.date_key  = d.date_key
GROUP BY h.HouseStyle, d.month
ORDER BY h.HouseStyle, d.month
"""

df_q2 = con.execute(query2).df()
df_q2
```
<details>
<summary>出力例</summary>

```
  HouseStyle  month  sales_count  avg_lot_area
0     1Story      6            1        7200.0
1     2Story      6            1        8000.0
2     2Story      7            1        9500.0
```
</details>

---

## 6. まとめ

1. **スター・スキーマでの設計**  
   - **`dim_house`** (住宅属性) と **`dim_date`** (売却年月) は分析軸（ディメンション）。  
   - **`fact_sales`** は売却事実を格納し、価格や面積など指標値を持つ。  
2. **JOINして GROUP BY** するだけで「近隣×品質」や「家のタイプ×売却月」など多角的な集計が容易になる。  
3. **DuckDB** は軽量かつ高速で扱いやすいカラムナ型DB。スター・スキーマの概念を試すのに最適。

---

### 応用

- **Kaggleの実データ(CSV)を読み込み**、列の中から分析対象をピックアップしてディメンション・ファクトに切り分ける。  
- `MoSold` や `YrSold` を `dim_date` にして詳しい日付ディメンションを付与する（例: `date_actual`, `quarter` など）。  
- `Neighborhood`, `HouseStyle`, `OverallQual` 以外にも、GarageやBsmtなど多くの属性をディメンションテーブルに拡張してみる。  
- 売却価格だけでなく、リフォーム費用や固定資産税など別ファクトを作り、“複数ファクト”を分析できる形にも発展可能。

---

## おわりに

本サンプルは、**House Prices** のデータを使って **スター・スキーマ** を**DuckDB**で試してみる最小例です。  
実際には多種多様なカラムがあるので、**どうディメンション化するか**を検討しながらぜひ拡張してみてください。  

**スター・スキーマ** の理解が深まれば、大規模なDWH(例: Snowflake, Redshiftなど)への応用も容易になります。  
まずはローカル環境でスモールスタートし、分析SQLや可視化など、データに合わせて試行錯誤してみましょう。