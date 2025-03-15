以下では、**Snowflake** を使って **スター・スキーマ** (ファクト/ディメンションの分割) を体験するための **MVP（Minimum Viable Product）チュートリアル** をご用意しました。  
短時間で「DWHらしいテーブル設計」と「クラウドDWHによる集計」を確認できる内容になっています。

---

# Snowflakeでスター・スキーマDWHのMVPを作ってみよう

## 概要

このチュートリアルでは以下を実践します。

1. **ECサイトの売上データ** を想定した **ファクトテーブル (fact_sales)**  
2. それを説明する3つのディメンションテーブル (**dim_customer, dim_product, dim_date**)  

を **スター・スキーマ** で作成し、Snowflake にロードして簡単な集計クエリを実行してみます。  
Snowflake はマルチクラウド対応のSaaS型DWHで、大量データの取り込みや高速クエリに優れています。

---

## チュートリアルの流れ

1. **Snowflakeアカウント作成 & ウェアハウス設定**  
2. **サンプルCSVファイルをS3またはローカルに用意**  
3. **Snowflake上でスター・スキーマのテーブル作成** (dim_…, fact_…)  
4. **ステージング (PUT/COPY) を使ってデータをロード**  
5. **基本的な集計クエリを実行**してスター・スキーマのメリットを確認

---

## 1. 事前準備

### A. Snowflakeアカウント & ウェアハウスの作成

1. **Snowflake公式サイト** で無料トライアルアカウントを作成  
   - \[Try Snowflake\] などのボタンからサインアップし、クラウドプロバイダ(AWS/GCP/Azure)とリージョンを選択。  
   - アカウントが作成されると、Web UIにログイン可能になる。
2. **ウェアハウス**(仮想ウェアハウス)の設定  
   - Web UIで \[Admin\] → \[Warehouses\] → 新規作成  
   - サイズは X-Small や Small など最小限でOK（無料トライアル範囲内で運用）。
3. **Database & Schema** の作成  
   - Web UIの \[Databases\] → \[Create\] で `MY_DWH` のようなデータベース名を作る。  
   - `MY_DWH` 内に `PUBLIC` or `MY_SCHEMA` を使っておけばOK。

### B. サンプルCSVファイル

ECサイトデータを単純化し、以下4つのCSVを用意します（好きなデータでもOK）。

1. `customer.csv`  
   - 顧客ID, 氏名, 地域, 登録日 など  
2. `product.csv`  
   - 商品ID, 商品名, カテゴリ, 単価 など  
3. `date.csv`  
   - 日付ID, 年, 月, 日, 週, 祝日フラグ など  
4. `sales.csv`  
   - 1行＝1明細: 受注ID, 商品ID, 顧客ID, 日付ID, 売上金額, 購入数量 など  

例:  
- [customer.csv](https://raw.githubusercontent.com/example/dwh/customer.csv)  
- [product.csv](https://raw.githubusercontent.com/example/dwh/product.csv)  
- [date.csv](https://raw.githubusercontent.com/example/dwh/date.csv)  
- [sales.csv](https://raw.githubusercontent.com/example/dwh/sales.csv)

#### 1) ファイルの保管場所

- **S3等の外部ステージ**：Snowflakeで外部ステージを設定し、`COPY INTO` で参照するパターン。  
- **ローカルPCから直接PUT**：Snowflakeの内部ステージにアップロードし、その後 `COPY INTO` する。  

本チュートリアルでは簡単に「**ローカル → Snowflake internal stage**」へのPUTを想定します。

---

## 2. Snowflakeでスター・スキーマのテーブルを作る

1. **Snowflake Web UI** へログイン  
2. **Worksheet** から SQL を実行できる画面を開く  
3. SQLを実行する際に \[Warehouse\] と \[Database\]、 \[Schema\] を指定

### A. テーブル作成 (DWHらしいキー設計)

```sql
USE DATABASE MY_DWH;
USE SCHEMA PUBLIC;

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

ここでは簡易的にVARCHAR や DECIMAL を使用し、キーや制約は最低限でOKです。

---

## 3. CSVをSnowflakeにロードする

### A. 内部ステージを使う方法 (PUT + COPY)

1. **Snowflakeにステージ(一時領域)を作る**  
   ```sql
   CREATE STAGE my_stage;
   ```
2. **ローカルCSVを PUT でアップロード**  
   - Web UIのSnowsql or WorksheetsからはPUTコマンドが使えません。  
   - **Snowsql CLI** か **Python connector** 等を使い、例:  
     ```bash
     snowsql -a <YOUR_ACCOUNT> -u <USER>
     ```
     ```sql
     USE DATABASE MY_DWH;
     USE SCHEMA PUBLIC;

     PUT file://path/to/customer.csv @my_stage auto_compress=true;
     PUT file://path/to/product.csv  @my_stage auto_compress=true;
     PUT file://path/to/date.csv     @my_stage auto_compress=true;
     PUT file://path/to/sales.csv    @my_stage auto_compress=true;
     ```
3. **COPY INTO テーブル**  
   ```sql
   COPY INTO dim_customer
   FROM @my_stage/customer.csv.gz
   FILE_FORMAT = (TYPE = CSV field_optionally_enclosed_by='"' skip_header=1)
   ON_ERROR = CONTINUE;

   COPY INTO dim_product
   FROM @my_stage/product.csv.gz
   FILE_FORMAT = (TYPE = CSV field_optionally_enclosed_by='"' skip_header=1)
   ON_ERROR = CONTINUE;

   COPY INTO dim_date
   FROM @my_stage/date.csv.gz
   FILE_FORMAT = (TYPE = CSV field_optionally_enclosed_by='"' skip_header=1)
   ON_ERROR = CONTINUE;

   COPY INTO fact_sales
   FROM @my_stage/sales.csv.gz
   FILE_FORMAT = (TYPE = CSV field_optionally_enclosed_by='"' skip_header=1)
   ON_ERROR = CONTINUE;
   ```
   - `auto_compress=true` によってPUT時にファイルが `.gz` (圧縮) になり、COPYでは `.csv.gz`を指定。  
   - `ON_ERROR=CONTINUE` は簡易サンプル用。実運用では `ON_ERROR=ABORT_STATEMENT` などエラー時に停止するのが一般的です。

#### ★ S3等の外部ステージを使う場合
- 事前に `CREATE STAGE my_s3_stage URL='s3://my-bucket/dwh' CREDENTIALS=(...)`  
- その後 `COPY INTO dim_customer FROM @my_s3_stage/customer.csv ...` のように実行  
- IAMロールやAWSキーの設定が必要です。

---

## 4. 簡単な集計クエリを試す

### A. スター・スキーマJOINで集計

例: 「商品カテゴリ別の売上合計と数量合計を、年月単位で見る」  

```sql
USE DATABASE MY_DWH;
USE SCHEMA PUBLIC;

SELECT
  p.category,
  d.year,
  d.month,
  SUM(s.sales_amount) AS total_revenue,
  SUM(s.quantity)     AS total_quantity
FROM fact_sales s
JOIN dim_product p ON s.product_key = p.product_key
JOIN dim_date d    ON s.date_key    = d.date_key
GROUP BY
  p.category, d.year, d.month
ORDER BY
  p.category, d.year, d.month;
```

Snowflakeは自動スケーリングする仮想ウェアハウス上で実行されるため、大規模データでも高速に処理されるのが特徴です。

### B. 他の分析パターン

- 「顧客の `region` × 日付の `is_holiday` で売上を比較」  
- 「特定の月に売れた商品カテゴリ Top 5 を調べる」  
- など、JOIN先を切り替えて `GROUP BY` するだけで、スター・スキーマなら柔軟に分析できます。

---

## 5. まとめ＆発展

1. **スター・スキーマ設計**  
   - ファクトテーブル(売上明細)にディメンションキー(顧客, 商品, 日付)を持たせる → JOINでOLAP集計がスムーズ  
2. **Snowflakeで体験するメリット**  
   - **SaaS型DWH** なのでインフラ管理が不要  
   - 仮想ウェアハウスのサイズ変更や自動スケールで、大量データも高速にクエリ可能  
   - COPY INTO の設定が簡単、Time TravelやZero-Copy Clone等の独自機能も充実  
3. **追加ステップ**  
   - BIツール連携: Tableau/Looker/PowerBIなど、Snowflakeを直接データソースとしてダッシュボード化  
   - SCD2, スノーフレークスキーマ, セキュリティモデルなど、さらに高度なDWH運用

---

# これで Snowflake での DWH入門もバッチリ！

- **Snowflake** はマルチクラウド対応の完全マネージドDWHで、大量データの分析を柔軟かつ高速に行えます。  
- **ファクト/ディメンション** を切り分けるスター・スキーマ設計を体験することで、OLAP分析に必要な思考やSQLパターンを理解できるでしょう。  
- ぜひ本チュートリアルを踏まえて、より大規模なデータや複雑なETLを試してみてください。Snowflakeの真価をさらに実感できるはずです。