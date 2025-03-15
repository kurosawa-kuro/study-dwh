以下では、**Redshift** を使って **スター・スキーマ** (ファクト／ディメンションの分割) を体験するための **MVP（Minimum Viable Product）チュートリアル** をご用意しました。  
短時間で「DWHらしいテーブル設計」と「クラウドDWHによる集計」を確認できる内容になっています。

---

# Redshiftでスター・スキーマDWHのMVPを作ってみよう

## 概要

このチュートリアルでは、

1. **ECサイトの売上データ** を想定した **ファクトテーブル (fact_sales)**  
2. それを説明する 3 つのディメンションテーブル (**dim_customer, dim_product, dim_date**)  

を **スター・スキーマ** で作成し、Redshift にロードして簡単な集計クエリを実行してみます。  
AWS上で動くクラウドDWHである **Redshift** を使うと、本番さながらのスケーラブルなDWH体験ができます。

---

## チュートリアルの流れ

1. **Redshift環境を用意 (Serverless推奨)**  
2. **サンプルCSVファイルをS3に配置**  
3. **Redshift上でスター・スキーマのテーブル作成** (dim_…, fact_…)  
4. **S3からCOPYコマンドでデータをロード**  
5. **基本的な集計クエリを実行**してスター・スキーマのメリットを確認

---

## 1. 事前準備

### A. Redshift環境 (Serverless)

1. **AWSアカウント作成**  
   - クレジットカード登録後、無料利用枠やプロモクレジットがある場合もあるので確認。
2. **Redshift Serverless を立ち上げ**  
   - AWSコンソール → Amazon Redshift → [Serverless]タブ → ワークグループ作成  
   - デフォルト設定で構築すればOK。必要に応じてスーパーユーザーのパスワードを設定。
3. **ワークグループのエンドポイントやVPC設定を確認**  
   - 後ほどSQLクライアント(コンソールのQuery Editor V2やData API)で接続するために必要。

### B. サンプルCSVファイル

ここではECサイトをざっくり想定した、以下4つのCSVを用意します（各自好きなデータでもOK）。

1. `customer.csv`  
   - 顧客ID, 氏名, 地域, 登録日 など  
2. `product.csv`  
   - 商品ID, 商品名, カテゴリ, 単価 など  
3. `date.csv`  
   - 日付ID, 年, 月, 日, 週, 祝日フラグ など  
   - （手動で作る、あるいは [date dimension generator](https://gist.github.com) 等を利用）  
4. `sales.csv`  
   - 1行＝1明細: 受注ID, 商品ID, 顧客ID, 日付ID, 売上金額, 購入数量 など  

例として以下のGithubダミーデータ（実在しない例示です）:
- [customer.csv](https://raw.githubusercontent.com/example/dwh/customer.csv)  
- [product.csv](https://raw.githubusercontent.com/example/dwh/product.csv)  
- [date.csv](https://raw.githubusercontent.com/example/dwh/date.csv)  
- [sales.csv](https://raw.githubusercontent.com/example/dwh/sales.csv)

#### 1) ファイルをS3にアップロード
- AWSコンソール → S3 → バケット作成（例: `my-dwh-bucket`）  
- `customer.csv` などをアップロードし、S3パス: `s3://my-dwh-bucket/dwh/customer.csv` のように配置しておく。

---

## 2. Redshiftでスター・スキーマのテーブルを作る

### A. Redshiftへの接続
1. **コンソールのQuery Editor V2**を使用（簡単）  
   - Redshift → Query Editor V2 → ワークグループ選択  
   - SQLを直接入力し実行できる  
2. **PSQLなどローカルCLI** → Data API or JDBC/ODBCでも可能  
   - 好みの方法で接続し、SQLを発行する  

本チュートリアルでは、**Query Editor V2** を想定します。

### B. テーブル作成 (DWHっぽいキー設計)

```sql
-- (1) ディメンション: dim_customer
CREATE TABLE public.dim_customer (
  customer_key INT,
  customer_name VARCHAR(100),
  region VARCHAR(50),
  register_date VARCHAR(50)
)
diststyle auto
sortkey auto;

-- (2) ディメンション: dim_product
CREATE TABLE public.dim_product (
  product_key INT,
  product_name VARCHAR(100),
  category VARCHAR(100),
  unit_price DECIMAL(10,2)
)
diststyle auto
sortkey auto;

-- (3) ディメンション: dim_date
CREATE TABLE public.dim_date (
  date_key INT,
  year INT,
  month INT,
  day INT,
  is_holiday BOOLEAN
)
diststyle auto
sortkey auto;

-- (4) ファクト: fact_sales
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

- **diststyle auto** / **sortkey auto** によってRedshiftが最適化を自動的にやってくれます。  
- ここではカラム名と型を簡易にしていますが、本番ではデータ量やクエリパターンに応じてDISTKEY / SORTKEYを検討すると良いです。

---

## 3. S3からCOPYコマンドでデータをロード

CSVが配置されている **S3** からRedshiftへロードするには**COPY**コマンドを使います。  
下記のように**IAMロール**をRedshiftに付与しておくか、**CREDENTIALS** でキーを指定します。

```sql
-- (A) ディメンションテーブルのロード (customer, product, date)
COPY dim_customer
FROM 's3://my-dwh-bucket/dwh/customer.csv'
IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
CSV
IGNOREHEADER 1
REGION 'ap-northeast-1';

COPY dim_product
FROM 's3://my-dwh-bucket/dwh/product.csv'
IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
CSV
IGNOREHEADER 1
REGION 'ap-northeast-1';

COPY dim_date
FROM 's3://my-dwh-bucket/dwh/date.csv'
IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
CSV
IGNOREHEADER 1
REGION 'ap-northeast-1';

-- (B) ファクトテーブルのロード (sales)
COPY fact_sales
FROM 's3://my-dwh-bucket/dwh/sales.csv'
IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
CSV
IGNOREHEADER 1
REGION 'ap-northeast-1';
```

- **IAM_ROLE** は事前に「RedshiftにS3読み取り権限を与えるIAMロール」を作成して、クラスタにアタッチする必要があります。  
- `IGNOREHEADER 1` はCSVのヘッダー行を無視するオプション。  
- 区切り文字や圧縮形式が異なる場合はパラメータを追加します。

COPYコマンド実行後、成功メッセージが出ればロード完了です。

---

## 4. 簡単な集計クエリを試す

### A. スター・スキーマJOINで集計

例: 「商品カテゴリ別の売上合計と数量合計を、年月単位で見る」などの分析を行います。  
以下のように **fact_sales** を中心に **dim_product**, **dim_date** をJOINし、カテゴリや年月ごとの集計をするクエリを書いてみましょう。

```sql
SELECT
  p.category,
  d.year,
  d.month,
  SUM(s.sales_amount) AS total_revenue,
  SUM(s.quantity)     AS total_quantity
FROM fact_sales AS s
JOIN dim_product AS p ON s.product_key = p.product_key
JOIN dim_date    AS d ON s.date_key    = d.date_key
GROUP BY
  p.category, d.year, d.month
ORDER BY
  p.category, d.year, d.month;
```

結果として、カテゴリ別×年月別の売上と数量がまとまって表示されます。  
**スター・スキーマ** でファクト・ディメンションを分けているので、JOINしやすく、`GROUP BY` による集計軸も明確です。

### B. 分析軸を変えてみる

- 「地域(顧客のregion)別に、どのカテゴリが多く売れたか？」  
- 「日付テーブルの `is_holiday` フラグで祝日の売上の差を測る？」  

など、JOINするディメンションを変えれば、自由に別の切り口で分析できます。

#### 例: 祝日 vs 平日での売上比較

```sql
SELECT
  d.is_holiday,
  SUM(s.sales_amount) AS total_revenue,
  COUNT(DISTINCT s.sales_id) AS order_count
FROM fact_sales s
JOIN dim_date d ON s.date_key = d.date_key
GROUP BY 1
ORDER BY 1;
```

---

## 5. まとめ＆発展

1. **スター・スキーマ設計**  
   - **ファクトテーブル** (売上明細) + **ディメンションテーブル** (顧客, 商品, 日付) にキーを持たせることで、多軸分析がシンプルに。
2. **Redshiftで体験するメリット**  
   - **クラウドDWH** なので、スケーリングや連携が容易。本番さながらの環境で検証できる。  
   - COPYコマンドやGlueを使ったETLも含めて、実務レベルのデータパイプラインをミニチュア体験しやすい。
3. **追加的な検討**  
   - DISTKEY / SORTKEY の最適化: 大規模データでのパフォーマンス向上。  
   - BIツール連携 (QuickSight など) でダッシュボード化し、可視化や運用にも踏み込む。  
   - スノーフレークスキーマやSCD2など更に高度なDWH手法を試す。

---

# これで Redshift での DWH入門はバッチリ！

- **Redshift** はカラムナ型ストレージ＋スケーラブル設計で、大量データのOLAPに最適。  
- **ファクト/ディメンション** を切り分ける考え方と JOIN＋集計の流れを一度体験しておくと、Snowflake や BigQuery 等 他のDWHにも応用しやすくなります。  
- ぜひこのチュートリアルを踏まえて、自身の業務データや Kaggleデータなどでスター・スキーマDWHを構築・分析してみてください。DWHの基礎概念とクラウドDWHの利点が一気に実感できるはずです。