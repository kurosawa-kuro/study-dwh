以下では、**Snowflake** を使って **スター・スキーマ** (ファクト/ディメンション) を体験するための **MVP（Minimum Viable Product）チュートリアル** をご用意しました。  
**あえて “ローカルCSV→PUT/COPY” は使わず、INSERT文で少量のサンプルデータを直接入力**する簡易版を想定しています。Snowflakeの導入ステップを少し詳しく説明しながら進めるので、まずは軽く触ってみたい方に最適です。

---

# Snowflakeでスター・スキーマDWHのMVP（INSERT版）

## 概要

1. **ECサイトの売上データ** を想定した **ファクトテーブル (fact_sales)**  
2. それを説明する 3つのディメンションテーブル (**dim_customer, dim_product, dim_date**)  

を **スター・スキーマ** で作成し、**Snowflake** 上で少量のINSERTを行い、簡単な集計クエリを実行してみます。  
Snowflake はマルチクラウド対応のDWHサービスで、サイズ変更や自動スケールが容易かつ高速なクエリが実行できるのが特徴です。

---

## チュートリアルの流れ

1. **Snowflakeアカウント作成 & 初期設定**  
2. **スター・スキーマのテーブル作成 (dim_…, fact_…)**  
3. **INSERT文でサンプルデータを投入**  
4. **JOIN＋GROUP BYで簡単な分析クエリ** を試す

---

## 1. 事前準備：Snowflakeアカウント作成 & 初期設定

### A. アカウント作成（無料トライアル）

1. **公式サイト** (例: <https://www.snowflake.com/jp/>) → 「**Try Snowflake**」などのボタンをクリック  
2. **サインアップフォーム** で必要情報（名前/メール/会社名等）を入力  
3. **クラウドプロバイダ** (AWS / Azure / GCP) と **リージョン** を選択  
4. **完了するとログインURL** が発行され、ブラウザからSnowflakeの管理コンソールにアクセスできる  
5. **最初のログイン時** に「ウェアハウス（仮想倉庫）」を1つだけ作っておく（XS～SmallでOK）

### B. ウェアハウスとデータベース作成

1. **左メニュー “Admin”** → “Warehouses” で**ウェアハウス**を作成  
   - 例: 名称 `MVP_WH`、サイズ `X-Small` or `Small` など  
2. **左メニュー “Databases”** → “Create” で**データベース**を作成  
   - 例: `MY_DWH`  
   - データベースに `PUBLIC` スキーマをそのまま使ってOK

これで最低限の環境（ウェアハウス＋DB）が用意できました。

---

## 2. Snowflakeでスター・スキーマのテーブルを作る

1. **Web UIの Worksheets** を開く  
2. 上部で **“Warehouse”** と **“Database”** / **“Schema”** を選択  
   - 例: `MVP_WH`, `MY_DWH`, `PUBLIC`  
3. SQLをここで実行します。

### A. スター・スキーマ用のテーブル定義

```sql
USE DATABASE MY_DWH;
USE SCHEMA PUBLIC;
USE WAREHOUSE MVP_WH;

-- (1) ディメンション: dim_customer
CREATE OR REPLACE TABLE dim_customer (
  customer_key INT,
  customer_name VARCHAR,
  region VARCHAR,
  register_date VARCHAR
);

-- (2) ディメンション: dim_product
CREATE OR REPLACE TABLE dim_product (
  product_key INT,
  product_name VARCHAR,
  category VARCHAR,
  unit_price DECIMAL(10,2)
);

-- (3) ディメンション: dim_date
CREATE OR REPLACE TABLE dim_date (
  date_key INT,
  year INT,
  month INT,
  day INT,
  is_holiday BOOLEAN
);

-- (4) ファクト: fact_sales
CREATE OR REPLACE TABLE fact_sales (
  sales_id INT,
  customer_key INT,
  product_key INT,
  date_key INT,
  sales_amount DECIMAL(10,2),
  quantity INT
);
```

*(ここでは非常にシンプルな定義にしています。)*

---

## 3. INSERTで少量データを投入

### A. ディメンションテーブルへINSERT

```sql
-- dim_customer
INSERT INTO dim_customer (customer_key, customer_name, region, register_date)
VALUES
(101, 'Alice', 'East', '2023-01-10'),
(102, 'Bob',   'West', '2023-02-05'),
(103, 'Charlie','East','2023-01-20');

-- dim_product
INSERT INTO dim_product (product_key, product_name, category, unit_price)
VALUES
(1001, 'Laptop', 'Electronics', 1200.00),
(1002, 'Tablet', 'Electronics', 300.00),
(1003, 'Headphones', 'Accessories', 50.00);

-- dim_date
INSERT INTO dim_date (date_key, year, month, day, is_holiday)
VALUES
(20230110, 2023, 1, 10, false),
(20230120, 2023, 1, 20, false),
(20230205, 2023, 2, 5,  true);
```

### B. ファクトテーブルへINSERT

```sql
-- fact_sales
INSERT INTO fact_sales (sales_id, customer_key, product_key, date_key, sales_amount, quantity)
VALUES
(1, 101, 1001, 20230110, 1200.00, 1),  -- Alice bought a Laptop
(2, 103, 1003, 20230120,  50.00,   1), -- Charlie bought Headphones
(3, 102, 1002, 20230205,  600.00,  2); -- Bob bought 2 Tablets
```

*(上記は完全に例であり、自由に追加して構いません。)*

---

## 4. 簡単な集計クエリで検証

### A. JOIN してスター・スキーマ分析

```sql
SELECT
  p.category,
  d.year,
  d.month,
  SUM(s.sales_amount) AS total_revenue,
  SUM(s.quantity)     AS total_quantity
FROM fact_sales AS s
JOIN dim_product p ON s.product_key = p.product_key
JOIN dim_date d    ON s.date_key    = d.date_key
GROUP BY
  p.category, d.year, d.month
ORDER BY
  p.category, d.year, d.month;
```

- ここで結果が返ってくれば成功です。**“誰が何をいつ買ったか”**の文脈ができあがり、様々な切り口で集計ができます。

### B. 他の分析パターン
- 「地域別 × 休日フラグ別で売上合計」  
- 「商品ごとの売上ランキング」  
- **JOINするディメンション**を切り替え、 `GROUP BY` すれば自由に多角的な分析が可能です。

---

## 5. まとめ & 発展

1. **スター・スキーマ設計**  
   - ファクト（売上）とディメンション（顧客/商品/日付）を分離し、キーでJOIN → OLAP分析がシンプル。  
2. **Snowflakeの操作感**  
   - SaaS型DWHなので、サーバー管理不要。  
   - ウェアハウスのサイズ変更 / 自動スケールに対応し、大量データも高速に集計可能。  
   - **Time Travel** や **Zero-Copy Clone** など独自機能も試してみると面白い。  
3. **MVP段階では INSERTするだけ**で十分  
   - 大規模データロードやS3ステージ等は後から学ぶ。  
   - まずは「Snowflakeにテーブルを作り、JOIN + GROUP BY できる」ところまで最短で体験。  

---

# これでSnowflakeのMVPが完成！

- **Snowflakeアカウント開設 → テーブル作成 → INSERT → JOIN集計** の流れを踏めば、スター・スキーマに基づくOLAP分析をサクッと試せます。  
- さらに大規模データを扱う際は、**PUT/COPY** や **外部ステージ**、**Transform**などの機能を使う拡張フェーズに進んでください。  
- **最初のハードル**は「何をどうやってロードするか」ですが、MVP段階ならINSERTだけでOK。**Snowflakeの操作・SQL実行感覚を手軽に掴んで**みましょう。