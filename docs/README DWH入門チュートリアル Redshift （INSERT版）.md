以下では、**Redshift** を使って **スター・スキーマ** (ファクト／ディメンション) を体験するための **MVP（Minimum Viable Product）チュートリアル** をご用意しました。  
**あえて S3 へのアップロードや COPY コマンドは使わず**、最小限の「テーブル作成 & INSERT文でサンプルデータを投入」してみる流れです。短時間で「スター・スキーマ＋Redshiftでの集計」を確認することが目的です。

---

# Redshiftでスター・スキーマDWHのMVP（INSERT版）

## 概要

- **ECサイトの売上データ** という題材を想定し、以下の4テーブルを作成:  
  1. **dim_customer (ディメンション)**  
  2. **dim_product (ディメンション)**  
  3. **dim_date (ディメンション)**  
  4. **fact_sales (ファクトテーブル)**  

- **INSERT文で少量のサンプルデータ** を直接書き込み、その後JOIN＋GROUP BYで集計クエリを試します。

> ※ 本番運用や大量データでは **S3 → COPY** を使うのが一般的ですが、ここでは学習用としてシンプルに進めます。

---

## 1. Redshiftの準備

### A. Redshift環境 (Serverlessを推奨)

1. **AWSアカウント作成 & ログイン**  
2. **Redshift Serverless** を起動  
   - AWSコンソール → Redshift → [Serverless] → ワークグループ作成  
   - デフォルト設定でOK（必要に応じてスーパーユーザーパスワードなど設定）。  
3. **ワークグループ エンドポイント**: 後ほど Query Editor V2 で接続するための情報。

### B. Query Editor V2 で接続

- AWSコンソール → Redshift → [Query Editor V2]  
- 対象のワークグループを選び、SQLを入力する画面へ移動

ここで CREATE/INSERT/SELECT などの操作を行います。

---

## 2. スター・スキーマ用テーブルを作成

以下の例では「ECサイトの売上(ファクト)」と、「顧客・商品・日付(ディメンション)」を作成します。

```sql
-- ========== ディメンション: dim_customer ========== 
CREATE TABLE public.dim_customer (
  customer_key INT,
  customer_name VARCHAR(100),
  region VARCHAR(50),
  register_date VARCHAR(50)
)
diststyle auto
sortkey auto;

-- ========== ディメンション: dim_product ==========
CREATE TABLE public.dim_product (
  product_key INT,
  product_name VARCHAR(100),
  category VARCHAR(100),
  unit_price DECIMAL(10,2)
)
diststyle auto
sortkey auto;

-- ========== ディメンション: dim_date ==========
CREATE TABLE public.dim_date (
  date_key INT,
  year INT,
  month INT,
  day INT,
  is_holiday BOOLEAN
)
diststyle auto
sortkey auto;

-- ========== ファクト: fact_sales ==========
CREATE TABLE public.fact_sales (
  sales_id INT,
  customer_key INT,
  product_key INT,
  date_key INT,
  sales_amount DECIMAL(10,2),
  quantity INT
)
diststyle auto
sortkey auto;
```

> diststyle/ sortkey を auto にすると、Redshiftが最適な分散・ソート戦略を自動設定してくれます。

---

## 3. INSERTでサンプルデータを直接投入

### A. ディメンションテーブルへINSERT

```sql
-- dim_customer
INSERT INTO public.dim_customer (customer_key, customer_name, region, register_date)
VALUES
(101, 'Alice', 'East', '2023-01-10'),
(102, 'Bob',   'West', '2023-02-05'),
(103, 'Charlie','East','2023-01-20');

-- dim_product
INSERT INTO public.dim_product (product_key, product_name, category, unit_price)
VALUES
(1001, 'Laptop', 'Electronics', 1200.00),
(1002, 'Tablet', 'Electronics', 300.00),
(1003, 'Headphones', 'Accessories', 50.00);

-- dim_date
INSERT INTO public.dim_date (date_key, year, month, day, is_holiday)
VALUES
(20230110, 2023, 1, 10, false),
(20230120, 2023, 1, 20, false),
(20230205, 2023, 2, 5,  true);
```

### B. ファクトテーブルへINSERT

```sql
INSERT INTO public.fact_sales (sales_id, customer_key, product_key, date_key, sales_amount, quantity)
VALUES
(1, 101, 1001, 20230110, 1200.00, 1),  -- Alice bought a Laptop on 2023-01-10
(2, 103, 1003, 20230120,  50.00,   1), -- Charlie bought Headphones
(3, 102, 1002, 20230205,  600.00,  2); -- Bob bought 2 Tablets
```

上記はあくまで**サンプルの少量データ**です。実際には INSERT文を連続して書くか、CSV→COPYなどで大量データを取り込むのが一般的。

---

## 4. 簡単な集計クエリを試す

### A. JOIN＋GROUP BY でスター・スキーマ分析

例えば、「商品カテゴリ別の売上合計と数量合計を、年月単位で見る」場合:

```sql
SELECT
  p.category,
  d.year,
  d.month,
  SUM(s.sales_amount) AS total_revenue,
  SUM(s.quantity)     AS total_quantity
FROM public.fact_sales s
JOIN public.dim_product p ON s.product_key = p.product_key
JOIN public.dim_date    d ON s.date_key    = d.date_key
GROUP BY
  p.category, d.year, d.month
ORDER BY
  p.category, d.year, d.month;
```

出力例（仮）:
```
category      | year | month | total_revenue | total_quantity
--------------+------+------+---------------+---------------
Accessories   | 2023 | 1     |    50.00      |   1
Electronics   | 2023 | 1     |  1200.00      |   1
Electronics   | 2023 | 2     |   600.00      |   2
```
**スター・スキーマ** によってファクト(売上)にディメンション(商品/日付)をJOINし、手軽に多軸分析ができます。

### B. さらに分析軸を増やす

同様に、`dim_customer` の `region` をJOINすれば地域別分析なども簡単です。

```sql
SELECT
  c.region,
  d.is_holiday,
  SUM(s.sales_amount) AS total_revenue
FROM public.fact_sales s
JOIN public.dim_customer c ON s.customer_key = c.customer_key
JOIN public.dim_date d     ON s.date_key    = d.date_key
GROUP BY c.region, d.is_holiday
ORDER BY c.region, d.is_holiday;
```

---

## 5. まとめ

1. **スター・スキーマ設計**  
   - ファクトテーブル (売上明細) と、ディメンションテーブル (顧客/商品/日付) を分割し、キーでJOIN → 多角的な分析をシンプルに実行。
2. **RedshiftでINSERTするだけの簡易MVP**  
   - 実行環境: Query Editor V2  
   - テーブル作成 → INSERT だけで、スター・スキーマの雰囲気を把握できる。  
   - 本格的には S3 + COPY で大量データをロードする運用になるが、MVP段階ではINSERTで十分。
3. **拡張**  
   - データ量を増やす → パフォーマンスを体感。  
   - BIツール連携 (QuickSight等) でダッシュボード化。  
   - DISTKEY/SORTKEY のチューニング、大規模ETLフローを試す など。

---

# Redshiftでスター・スキーマDWHを超お手軽に体験！

- **INSERTオンリー** の学習用アプローチでも、**ファクト/ディメンション** の概念や JOIN 集計がどう動くかは十分に体感できます。  
- より大規模な実験をする際は、CSV→S3→COPY で本格的にデータをロードし、Redshiftのスケーラブル性能を試しましょう。  
- まずは少量データからスター・スキーマを組んで分析し、**DWH/OLAPの基本を身につけて**みてください。