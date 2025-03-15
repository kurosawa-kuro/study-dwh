以下に、**DuckDB** と **スター・スキーマ（ファクト/ディメンション分割）** を組み合わせた **MVP（Minimum Viable Product）チュートリアル**のコード一式を示します。  
実行環境としては、**Jupyterノートブック** や Google Colab のような環境を想定し、`# %% [code]`セル形式で記載しています。

---

## チュートリアル概要

- **目的**  
  - **ディメンションテーブル**として `dim_customer`, `dim_product`, `dim_date` の3つを用意  
  - **ファクトテーブル**として `fact_sales` (売上データ) を用意  
  - これら4テーブルをDuckDB上に作成してデータを投入し、**スター・スキーマ風にJOINして集計** する流れを体験します。

- **データ内容 (想定)**  
  - `dim_customer`: 顧客ID, 名前, 地域, 登録日  
  - `dim_product`: 商品ID, 商品名, カテゴリ, 単価  
  - `dim_date`: 日付ID, 年・月・日, 祝日フラグ  
  - `fact_sales`: 売上ID, 顧客ID, 商品ID, 日付ID, 売上金額, 購入数量  

- **手順**  
  1. DuckDBインストール (pip で導入)  
  2. pandasでサンプルDataFrameを作成  
  3. DuckDBメモリDBにテーブル作成 (CREATE TABLE)  
  4. pandasのDataFrameをDuckDBに INSERT  
  5. 簡単なJOIN + GROUP BY クエリで集計を確認  

---

### コードセル一覧

#### 1. DuckDBインストール (必要な場合)
```python
# %% [code]
!pip install duckdb --quiet
```

#### 2. サンプルDataFrame（dim_customer）
```python
# %% [code]
import pandas as pd

df_customer = pd.DataFrame({
    'customer_key': [101, 102, 103],
    'customer_name': ['Alice', 'Bob', 'Charlie'],
    'region': ['East', 'West', 'East'],
    'register_date': ['2023-01-10', '2023-02-05', '2023-01-20']
})

df_customer
```

<details>
<summary>想定出力</summary>

```
   customer_key customer_name region register_date
0           101         Alice   East    2023-01-10
1           102           Bob   West    2023-02-05
2           103       Charlie   East    2023-01-20
```
</details>

---

#### 3. サンプルDataFrame（dim_product）
```python
# %% [code]
df_product = pd.DataFrame({
    'product_key': [1001, 1002, 1003],
    'product_name': ['Laptop', 'Tablet', 'Headphones'],
    'category': ['Electronics', 'Electronics', 'Accessories'],
    'unit_price': [1200.0, 300.0, 50.0]
})

df_product
```

<details>
<summary>想定出力</summary>

```
   product_key product_name     category  unit_price
0         1001       Laptop  Electronics      1200.0
1         1002       Tablet  Electronics       300.0
2         1003   Headphones  Accessories        50.0
```
</details>

---

#### 4. サンプルDataFrame（dim_date）
```python
# %% [code]
df_date = pd.DataFrame({
    'date_key': [20230110, 20230120, 20230205],
    'year': [2023, 2023, 2023],
    'month': [1, 1, 2],
    'day': [10, 20, 5],
    'is_holiday': [False, False, True]
})

df_date
```

<details>
<summary>想定出力</summary>

```
    date_key  year  month  day  is_holiday
0  20230110  2023      1   10       False
1  20230120  2023      1   20       False
2  20230205  2023      2    5        True
```
</details>

---

#### 5. サンプルDataFrame（fact_sales）
```python
# %% [code]
df_sales = pd.DataFrame({
    'sales_id': [1, 2, 3],
    'customer_key': [101, 103, 102],     # 顧客ID
    'product_key': [1001, 1003, 1002],   # 商品ID
    'date_key': [20230110, 20230120, 20230205],  # 日付ID
    'sales_amount': [1200.0, 50.0, 600.0],
    'quantity': [1, 1, 2]
})

df_sales
```

<details>
<summary>想定出力</summary>

```
   sales_id  customer_key  product_key   date_key  sales_amount  quantity
0         1           101         1001  20230110        1200.0         1
1         2           103         1003  20230120          50.0         1
2         3           102         1002  20230205         600.0         2
```
</details>

---

#### 6. DuckDBへ接続 & テーブル作成
```python
# %% [code]
import duckdb

# メモリ上にDB作成 (ファイル保存したければ 'mydb.duckdb' などを指定)
con = duckdb.connect(database=':memory:')

# ディメンションテーブル
con.execute("""
CREATE TABLE dim_customer (
  customer_key   INT,
  customer_name  VARCHAR,
  region         VARCHAR,
  register_date  DATE
)
""")

con.execute("""
CREATE TABLE dim_product (
  product_key   INT,
  product_name  VARCHAR,
  category      VARCHAR,
  unit_price    DOUBLE
)
""")

con.execute("""
CREATE TABLE dim_date (
  date_key     INT,
  year         INT,
  month        INT,
  day          INT,
  is_holiday   BOOLEAN
)
""")

# ファクトテーブル
con.execute("""
CREATE TABLE fact_sales (
  sales_id      INT,
  customer_key  INT,
  product_key   INT,
  date_key      INT,
  sales_amount  DOUBLE,
  quantity      INT
)
""")
```

---

#### 7. pandasのDataFrameをDuckDBにINSERTする
```python
# %% [code]
# 1. 一時テーブルとしてDataFrame登録
con.register("temp_customer", df_customer)
con.register("temp_product", df_product)
con.register("temp_date", df_date)
con.register("temp_sales", df_sales)

# 2. 本テーブルへのINSERT
con.execute("INSERT INTO dim_customer SELECT * FROM temp_customer")
con.execute("INSERT INTO dim_product  SELECT * FROM temp_product")
con.execute("INSERT INTO dim_date     SELECT * FROM temp_date")
con.execute("INSERT INTO fact_sales   SELECT * FROM temp_sales")
```

---

#### 8. データ確認
```python
# %% [code]
res_cust  = con.execute("SELECT * FROM dim_customer").df()
res_prod  = con.execute("SELECT * FROM dim_product").df()
res_date  = con.execute("SELECT * FROM dim_date").df()
res_sales = con.execute("SELECT * FROM fact_sales").df()

display(res_cust)
display(res_prod)
display(res_date)
display(res_sales)
```

---

#### 9. スター・スキーマのJOIN集計例1

**カテゴリ別 × (年, 月) 別** の売上合計と数量合計を集計するSQLです。  
`fact_sales` → `dim_product` / `dim_date` のキーでJOINし、グループ化しています。

```python
# %% [code]
query = """
SELECT
  p.category,
  d.year,
  d.month,
  SUM(s.sales_amount) AS total_sales,
  SUM(s.quantity)     AS total_qty
FROM fact_sales s
JOIN dim_product p ON s.product_key = p.product_key
JOIN dim_date    d ON s.date_key    = d.date_key
GROUP BY
  p.category, d.year, d.month
ORDER BY
  p.category, d.year, d.month
"""

result = con.execute(query).df()
result
```

<details>
<summary>想定出力</summary>

```
       category  year  month  total_sales  total_qty
0  Accessories  2023      1         50.0          1
1  Electronics  2023      1       1200.0          1
2  Electronics  2023      2        600.0          2
```
</details>

---

#### 10. スター・スキーマのJOIN集計例2

**顧客の地域 (region) × 祝日フラグ (is_holiday) 別** に売上を集計した例です。

```python
# %% [code]
query2 = """
SELECT
  c.region,
  d.is_holiday,
  SUM(s.sales_amount) AS revenue
FROM fact_sales s
JOIN dim_customer c ON s.customer_key = c.customer_key
JOIN dim_date d     ON s.date_key    = d.date_key
GROUP BY
  c.region, d.is_holiday
ORDER BY
  c.region, d.is_holiday
"""

res2 = con.execute(query2).df()
res2
```

<details>
<summary>想定出力</summary>

```
  region  is_holiday  revenue
0   East       False   1250.0
1   West        True    600.0
```
</details>

---

## まとめ

- **スター・スキーマ**  
  - ファクトテーブル (`fact_sales`) と 複数のディメンションテーブル (`dim_customer`, `dim_product`, `dim_date`) に分割することで、集計クエリのJOINが分かりやすくなる。  
- **DuckDB**  
  - ローカル環境で簡単に試せる軽量カラムナDB。スター・スキーマ構成でも手軽に集計が可能。  
- **発展**  
  - 追加の属性（顧客のセグメント、商品のブランドなど）をディメンションに入れてさらに分析軸を増やす。  
  - 日付テーブルで週/四半期/曜日などのカラムを持たせれば、多彩な時間粒度で集計できる。  
  - CSV/Parquet/その他ファイルからのロードや Python ランタイムの連携による可視化なども容易。

このように **DuckDB + スター・スキーマ** を使った **シンプルなDWH風分析** を体験すると、DWHの設計思想やSQLによる集計の流れが一通り掴めるはずです。ぜひ活用してみてください。