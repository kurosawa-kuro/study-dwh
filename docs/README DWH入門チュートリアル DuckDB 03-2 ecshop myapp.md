以下では、**ECショップアプリ** のPrismaスキーマ(運用DB)をもとにした **スター・スキーマ構築**＋**DuckDBによるDWH分析** チュートリアルを示します。実際の運用DBでは多数のテーブルを正規化して管理していると思いますが、**データ分析**を効率化するためには、**ファクトテーブル(事実テーブル) と ディメンションテーブル** に分割する**スター・スキーマ**が有効です。

---

# DuckDB × ECショップアプリのスター・スキーマ DWHチュートリアル

## 1. Prismaスキーマの概要

以下のようなテーブルが確認できます。（一部抜粋）

- **User**  
  - Cognitoとの連携 (userId = String)  
  - email / status / createdAt / updatedAt  
- **Product / Category / ProductCategory**  
  - 商品とカテゴリの多対多構造  
- **ViewHistory / CartItem**  
  - ユーザが商品を閲覧した履歴、カートに追加したアイテム  
- **Order / OrderItem / Return / ReturnItem**  
  - 購入と返品  
- **UserActionLog**  
  - カート追加や注文などの操作ログ  

運用DBではアプリの整合性や更新操作が容易になるよう、正規化が行われています。しかし、**分析** (例えば「売上」「顧客セグメント」「商品別売上推移」「返品率」など) を高速かつ分かりやすく行うには、**スター・スキーマ(DWH設計)** が有効です。

---

## 2. スター・スキーマの例

### 2.1 ディメンションテーブル

1. **`dim_user`**  
   - ユーザの主要属性を保持 (ユーザID, email, 作成日, ステータス など)  
   - 運用DBの `User` テーブルがベース  
   - 付加情報: たとえばロール/プロフィールをまとめたい場合はここに統合する場合も

2. **`dim_product`**  
   - 商品の主要属性 (商品名, 定価, カテゴリID など)  
   - 運用DBの `Product` と `Category` の多対多をどう扱うかは要検討：  
     - 1商品に1カテゴリ（メインカテゴリ）のみ、であれば `category_key` を置くだけで単純  
     - 多カテゴリに対応するにはスノーフレーク的に別途 `dim_category` を用意 など方法あり

3. **`dim_date`**  
   - 日付ごとの情報 (日付キー, 年, 月, 日, 曜日, …)  
   - 購入日や閲覧日など各種イベントの発生日を紐付ける

(必要に応じて `dim_role`, `dim_display_type` 等を増やすことも検討可能です。)

### 2.2 ファクトテーブル

1. **`fact_sales`**  
   - 購入(注文)に関する事実テーブル
   - 粒度: **1行 = 1注文アイテム** (行レベルでユーザ × 商品 × 日付 のセット)
   - カラム例:  
     - `sale_id` (サロゲートキー)  
     - `order_id` (運用DBの Order.id)  
     - `order_item_id` (運用DBの OrderItem.id)  
     - `user_key` (FK→dim_user)  
     - `product_key` (FK→dim_product)  
     - `date_key` (FK→dim_date)  (Order.orderedAtのYYYYMMDD相当)  
     - `quantity`  
     - `price`  
     - `total_amount` (quantity × price)  

2. **(オプション) `fact_returns`**  
   - 返品情報に特化したファクトを作る  
   - 粒度: **1行 = 1返品アイテム**  
   - カラム例:  
     - `return_id`, `return_item_id`, `user_key`, `product_key`, `date_key` (返品日時)  
     - `status` (REQUESTED, COMPLETED), `quantity` など

3. **(オプション) `fact_user_actions`**  
   - `UserActionLog`, `ViewHistory`, `CartItem` などから抽出したユーザ操作ログをまとめたい場合は、1行=1操作(“view”, “add_to_cart”, “checkout”, etc.) のファクトテーブルにする

スター・スキーマにまとめる際のポイントは、**アプリDB上の複数テーブルをJOIN or UNION** して **「分析しやすい1枚のファクトテーブル」** として定義することです。

---

## 3. DuckDBでの実装手順(概要)

1. **運用DB(PostgreSQL) からデータを取得**  
   - PrismaやSQLで SELECT し、CSVやParquetに書き出す or PythonのDataFrameに取り込む  
2. **ディメンションテーブルを作成** (dim_user, dim_product, dim_date, …)  
   - ユーザIDや商品IDなどをサロゲートキーにしておく  
   - メインとなる属性を各ディメンションに格納  
3. **ファクトテーブル作成**  
   - (例) `fact_sales`: Order / OrderItem をJOINして **1行=1アイテム注文**  
   - userやproductに紐づくキーをFKとして保持  
   - 合計金額や個数などのメトリック列を配置  
4. **DuckDB** 上にこれらを CREATE TABLE → INSERT する  
5. **JOIN + GROUP BY** で集計分析を行う

以下は最小限の「デモ用ダミーデータ」でのサンプルです。  
実際には**Prismaスキーマのテーブル群**からデータを抽出し、スター・スキーマ用に変換してください。

---

## 4. サンプルコード (最小ダミーデータでスター・スキーマ)

### 4.1 ダミーDataFrame作成 (pandas)

```python
# %% [code]
!pip install duckdb --quiet

import pandas as pd

# --- ユーザ (dim_user) ---
df_user = pd.DataFrame({
    'user_key': ['u001', 'u002', 'u003'],  # 運用DBのUser.id (Cognito ID) をサロゲートキー的に
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
    'status': ['ACTIVE', 'ACTIVE', 'DISABLED'],
    'created_date': ['2023-01-10', '2023-02-05', '2023-03-01']
})

# --- 商品 (dim_product) ---
df_product = pd.DataFrame({
    'product_key': [101, 102, 103],
    'name': ['Laptop', 'T-Shirt', 'Coffee Beans'],
    'price': [1200.0, 20.0, 8.5],
    'category': ['Electronics', 'Apparel', 'Food']
})

# --- 日付 (dim_date) ---
df_date = pd.DataFrame({
    'date_key': [20230110, 20230205, 20230301, 20230305],
    'year': [2023, 2023, 2023, 2023],
    'month': [1, 2, 3, 3],
    'day': [10, 5, 1, 5],
    'weekday': [2, 6, 3, 7],  # 例: 1=Mon...7=Sun
})

# --- ファクトテーブル: fact_sales (Order+OrderItem相当)
# 1行=1商品購入
df_fact_sales = pd.DataFrame({
    'sale_id': [1001, 1002, 1003],
    'user_key': ['u001', 'u002', 'u002'],
    'product_key': [101, 102, 103],
    'date_key': [20230110, 20230205, 20230301],  # orderedAt日付 (YYYYMMDD)
    'quantity': [1, 2, 1],
    'price': [1200.0, 20.0, 8.5],
})

# total_amount は計算してもよいですが、ここでは省略
df_user, df_product, df_date, df_fact_sales
```

---

### 4.2 DuckDBでテーブル作成 & INSERT

```python
# %% [code]
import duckdb

con = duckdb.connect(database=':memory:')

# ディメンション
con.execute("""
CREATE TABLE dim_user (
  user_key VARCHAR,
  email VARCHAR,
  status VARCHAR,
  created_date DATE
);
""")

con.execute("""
CREATE TABLE dim_product (
  product_key INT,
  name VARCHAR,
  price DOUBLE,
  category VARCHAR
);
""")

con.execute("""
CREATE TABLE dim_date (
  date_key INT,
  year INT,
  month INT,
  day INT,
  weekday INT
);
""")

# ファクト
con.execute("""
CREATE TABLE fact_sales (
  sale_id INT,
  user_key VARCHAR,
  product_key INT,
  date_key INT,
  quantity INT,
  price DOUBLE
);
""")

# pandas DataFrameを一時テーブルとして登録
con.register("temp_user", df_user)
con.register("temp_product", df_product)
con.register("temp_date", df_date)
con.register("temp_sales", df_fact_sales)

# INSERT
con.execute("INSERT INTO dim_user SELECT * FROM temp_user")
con.execute("INSERT INTO dim_product SELECT * FROM temp_product")
con.execute("INSERT INTO dim_date SELECT * FROM temp_date")
con.execute("INSERT INTO fact_sales SELECT * FROM temp_sales")

# 確認
res_user = con.execute("SELECT * FROM dim_user").df()
res_prod = con.execute("SELECT * FROM dim_product").df()
res_date = con.execute("SELECT * FROM dim_date").df()
res_sales = con.execute("SELECT * FROM fact_sales").df()

display(res_user)
display(res_prod)
display(res_date)
display(res_sales)
```

---

### 4.3 簡単なJOIN + 集計クエリ例

1. **商品カテゴリ別の売上数量 / 合計金額** (price×quantity)

```python
# %% [code]
query1 = """
SELECT
  p.category,
  SUM(s.quantity) AS total_qty,
  SUM(s.quantity * s.price) AS total_sales_amount
FROM fact_sales s
JOIN dim_product p ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales_amount DESC
"""

df_q1 = con.execute(query1).df()
df_q1
```
<details>
<summary>想定出力例</summary>

```
     category  total_qty  total_sales_amount
0  Electronics          1             1200.0
1     Apparel          2               40.0
2        Food          1                8.5
```
</details>

2. **ユーザステータス別の購入回数**

```python
# %% [code]
query2 = """
SELECT
  u.status,
  COUNT(*) AS purchase_count
FROM fact_sales s
JOIN dim_user u ON s.user_key = u.user_key
GROUP BY u.status
ORDER BY purchase_count DESC
"""

df_q2 = con.execute(query2).df()
df_q2
```
<details>
<summary>想定出力例</summary>

```
    status  purchase_count
0   ACTIVE               2
1 DISABLED               1
```
</details>

3. **日付ディメンションを活用して月別の売上を出す**

```python
# %% [code]
query3 = """
SELECT
  d.year,
  d.month,
  SUM(s.quantity * s.price) AS total_amount
FROM fact_sales s
JOIN dim_date d ON s.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month
"""

df_q3 = con.execute(query3).df()
df_q3
```
<details>
<summary>想定出力例</summary>

```
   year  month  total_amount
0  2023      1        1200.0
1  2023      2          40.0
2  2023      3           8.5
```
</details>

---

## 5. まとめ & 発展

1. **ECショップでのスター・スキーマ**  
   - **`fact_sales`** を中心に **`dim_user`, `dim_product`, `dim_date`** が放射状に接続  
   - 購入アイテムごとの行をファクトにし、集計したい指標 (price, quantity, total_amount 等) を持たせる  
2. **追加要素**  
   - 返品(`fact_returns`) / 閲覧履歴(`fact_view`) / カート追加(`fact_cart`) / ユーザ操作ログ(`fact_user_actions`) など、分析したいイベント単位でファクトテーブルを増やす  
   - 商品多カテゴリの場合、**スノーフレークスキーマ**(dim_productとdim_categoryを分ける)を検討  
   - `dim_user` にユーザステータスや最終ログイン日、ロールなど付加情報を入れるとセグメント分析がしやすい
3. **DuckDB活用**  
   - ローカル環境で CSV/Parquet/データフレームを簡単にロードし、カラムナーDBの高速な集計を体験できる  
   - 本格的にスケールする場合は Redshift / BigQuery / Snowflake などへの移行を検討

---

# これでECショップDWHのスターター完成！

- Prismaスキーマ = 運用DB  
- そこから **ファクト/ディメンション** を定義 → **DuckDB** で**分析クエリ**を回すと、  
  分析用に最適化されたテーブル設計(スター・スキーマ)のメリットを即実感できます。  
- ぜひ、実際の注文・返品・商品・ユーザなどの**データを抽出**してスター・スキーマを構築し、  
  その上で BIツールやSQLを使ったレポーティングを行ってみてください。  

  ===============================================================================================================================================

  以下では、**商品とカテゴリが多対多**である点を反映し、  
**スノーフレークスキーマ** を取り入れた **DuckDB×ECショップDWH** のサンプルチュートリアルを改訂します。  

もともとスター・スキーマ的に `dim_product` へ `category` カラムを1つだけ持たせていましたが、  
「**1商品 : Nカテゴリ**」のケースでは、  
**`dim_product`** と **`dim_category`** を分割し、さらに **ブリッジテーブル** を挟むことで多対多を扱います。

---

# DuckDB × ECショップ: 多対多カテゴリ対応 (スノーフレーク化)

## 1. Prismaスキーマでの多対多

あなたのECショップでは、

```prisma
model Product {
  ...
  productCategories ProductCategory[]
}

model Category {
  ...
  productCategories ProductCategory[]
}

model ProductCategory {
  productId  Int
  categoryId Int
  ...
}
```

という形で商品(Product)とカテゴリ(Category)が多対多になっています。  
この**多対多**を分析DWHにも落とし込む場合、単純に「`dim_product` に1つのカテゴリIDを持たせる」設計では足りません。

---

## 2. スノーフレークスキーマの構成イメージ

下記のように、**商品ディメンション (dim_product)** と **カテゴリディメンション (dim_category)** を分け、  
**`product_category_bridge`** テーブルを設けることで「1商品が複数カテゴリに属する」関係を表現します。  
ファクトテーブル (`fact_sales` など) は引き続き `product_key` を持ち、そこから**2段階JOIN** でカテゴリへ辿るイメージです。

```
fact_sales
   └── (product_key) ─> dim_product
                         └── product_category_bridge ─> dim_category
```

- **dim_product** … 商品ID・商品名・価格など、単一の商品の属性  
- **dim_category** … カテゴリID・カテゴリ名・階層など  
- **product_category_bridge** … product_key, category_key の組み合わせを持つ多対多ブリッジ  

この構造がいわゆる**スノーフレーク**(snowflake)で、スター・スキーマをさらに分割し、ディメンション同士が1対多で連なる形です。

---

## 3. ダミーデータ例 (pandas)

### 3.1 ディメンション: `dim_product` (商品)

```python
df_product = pd.DataFrame({
    'product_key': [101, 102, 103],
    'name': ['Laptop', 'T-Shirt', 'Coffee Beans'],
    'price': [1200.0, 20.0, 8.5],
    # ここではカテゴリは入れず、多対多は bridge で扱う
})
```

### 3.2 ディメンション: `dim_category` (カテゴリ)

```python
df_category = pd.DataFrame({
    'category_key': [201, 202, 203, 204],
    'category_name': ['Electronics', 'Apparel', 'Beverage', 'Food']
})
```

### 3.3 ブリッジ: `product_category_bridge`

- 下記例では
  - Laptop (101) → [201=Electronics]  
  - T-Shirt (102) → [202=Apparel]  
  - Coffee Beans (103) → [203=Beverage, 204=Food] (複数カテゴリ)

```python
df_product_category_bridge = pd.DataFrame({
    'product_key': [101, 102, 103, 103],
    'category_key': [201, 202, 203, 204]
})
```

### 3.4 ファクト: `fact_sales`

- ここでは1つの注文明細相当を1行とし、
  - `user_key`: 誰が
  - `product_key`: どの商品を
  - `date_key`: いつ
  - `quantity`, `price` など

```python
df_fact_sales = pd.DataFrame({
    'sale_id': [1001, 1002, 1003],
    'user_key': ['u001', 'u002', 'u002'],
    'product_key': [101, 103, 103],   # 101=Laptop, 103=CoffeeBeans
    'date_key': [20230110, 20230205, 20230205],
    'quantity': [1, 1, 2],
    'price': [1200.0, 8.5, 8.5],
})
```

※ ユーザや日付は前回同様 `dim_user`, `dim_date` として準備してください。

---

## 4. DuckDBでスノーフレークスキーマを作る

### 4.1 テーブル作成

```python
import duckdb

con = duckdb.connect(database=':memory:')

# ディメンション: product
con.execute("""
CREATE TABLE dim_product (
  product_key INT,
  name VARCHAR,
  price DOUBLE
);
""")

# ディメンション: category
con.execute("""
CREATE TABLE dim_category (
  category_key INT,
  category_name VARCHAR
);
""")

# ブリッジ: product_category_bridge (多対多)
con.execute("""
CREATE TABLE product_category_bridge (
  product_key INT,
  category_key INT
);
""")

# ファクト: fact_sales
con.execute("""
CREATE TABLE fact_sales (
  sale_id INT,
  user_key VARCHAR,
  product_key INT,
  date_key INT,
  quantity INT,
  price DOUBLE
);
""")

# (dim_user, dim_dateなど他のテーブルも同様に作成してください)
```

### 4.2 ダミーデータINSERT

```python
con.register("temp_product", df_product)
con.register("temp_category", df_category)
con.register("temp_prodcatbridge", df_product_category_bridge)
con.register("temp_fact_sales", df_fact_sales)

con.execute("INSERT INTO dim_product SELECT * FROM temp_product")
con.execute("INSERT INTO dim_category SELECT * FROM temp_category")
con.execute("INSERT INTO product_category_bridge SELECT * FROM temp_prodcatbridge")
con.execute("INSERT INTO fact_sales SELECT * FROM temp_fact_sales")
```

---

以下は、**多対多のカテゴリ**を扱うために**スノーフレークスキーマ**を取り入れたバージョンの **JOIN + 集計クエリ例** です。  
(前提として、下記のように **`dim_product`**, **`dim_category`**, **`product_category_bridge`** を用意し、商品とカテゴリを多対多でつないでいることを想定しています。)

---

## 多対多カテゴリ対応のスノーフレーク構造

\`\`\`mermaid
flowchart LR
    fact_sales -- product_key --> dim_product
    dim_product -- product_key --> product_category_bridge
    product_category_bridge -- category_key --> dim_category
\`\`\`

1. **`fact_sales`** : ファクトテーブル (売上明細)  
2. **`dim_product`** : 商品ディメンション  
3. **`dim_category`** : カテゴリディメンション  
4. **`product_category_bridge`** : 多対多を表すブリッジテーブル (1商品 : Nカテゴリ)

---

## 1. カテゴリ別の売上数量 / 合計金額を求める

```python
# %% [code]
query1 = """
SELECT
  c.category_name,
  SUM(s.quantity) AS total_qty,
  SUM(s.quantity * s.price) AS total_sales_amount
FROM fact_sales s
JOIN dim_product p
  ON s.product_key = p.product_key
JOIN product_category_bridge pcb
  ON p.product_key = pcb.product_key
JOIN dim_category c
  ON pcb.category_key = c.category_key
GROUP BY c.category_name
ORDER BY total_sales_amount DESC
"""

df_q1 = con.execute(query1).df()
df_q1
```

<details>
<summary>想定出力例</summary>

```
  category_name  total_qty  total_sales_amount
0  Electronics          1             1200.0
1       Apparel         2               40.0
2     Beverage          3               25.5
3          Food         3               25.5
```
</details>

ここでは、**1つの商品が複数カテゴリに属している**場合、その売上が両カテゴリに加算されます。  
(例えば「Coffee Beans (ProductID=103)」が “Beverage” と “Food” の2つに属している、といったケース)

---

## 2. ユーザステータス別の購入回数

```python
# %% [code]
query2 = """
SELECT
  u.status,
  COUNT(*) AS purchase_count
FROM fact_sales s
JOIN dim_user u
  ON s.user_key = u.user_key
GROUP BY u.status
ORDER BY purchase_count DESC
"""

df_q2 = con.execute(query2).df()
df_q2
```

<details>
<summary>想定出力例</summary>

```
   status   purchase_count
0   ACTIVE               2
1 DISABLED               1
```
</details>

商品とカテゴリの多対多関係には関係なく、**ユーザの状態**（ACTIVE/INACTIVE など）ごとに購入した明細数を数えています。

---

## 3. 日付ディメンションを活用して月別の売上を出す

```python
# %% [code]
query3 = """
SELECT
  d.year,
  d.month,
  SUM(s.quantity * s.price) AS total_amount
FROM fact_sales s
JOIN dim_date d
  ON s.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month
"""

df_q3 = con.execute(query3).df()
df_q3
```

<details>
<summary>想定出力例</summary>

```
   year  month  total_amount
0  2023      1        1200.0
1  2023      2          40.0
2  2023      3           8.5
```
</details>

日付ディメンションの `year`, `month` 列を使って、月別の売上合計を集計しています。  
（他にも `weekday` や `quarter`, `fiscal_year` などを持たせれば、様々な粒度で集計が可能になります。）

---

## まとめ

- **多対多のカテゴリ**を正しく集計するには、スター・スキーマをそのまま使うのではなく、  
  **スノーフレークスキーマ** (商品ディメンションとカテゴリディメンションを分割し、ブリッジテーブルで多対多を表現) が必要になるケースが多いです。  
- その際は、ファクトテーブルとカテゴリを紐付けるために **2段階JOIN** (product → product_category_bridge → category) を行うことを忘れないようにしてください。  
- それ以外の分析（ユーザセグメント別や日付別の集計）は、従来のスター・スキーマと同様に簡単に行えます。  

これらのクエリ例をベースに、**実際のECデータ**で多対多カテゴリを考慮したスノーフレーク構造を設計し、各種レポートやダッシュボードで分析を行ってみてください。