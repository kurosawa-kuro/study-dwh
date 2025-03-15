以下に、**DuckDB** を使って **スター・スキーマ** (ファクト/ディメンションの分割) を体験するための **MVP（Minimum Viable Product）チュートリアル** をご用意しました。短時間で「DWHらしいテーブル設計」と「カラムナDBによる集計」を確認できる内容になっています。

---

# DuckDBでスター・スキーマDWHのMVPを作ってみよう

## 概要

本チュートリアルでは、
1. **ECサイトの売上データ** を想定した**ファクトテーブル (fact_sales)**  
2. それを説明する3つのディメンションテーブル (**dim_customer, dim_product, dim_date**)  
を **スター・スキーマ** で作成し、DuckDB で簡単な集計クエリを実行する流れを体験します。

DuckDB はローカルでサクッと動かせる「SQLite for Analytics」とも呼ばれる軽量カラムナDBエンジンです。スター・スキーマを試す入門用に最適です。

---

## チュートリアルの流れ

1. **DuckDBのインストール / 起動**  
2. **サンプルCSVファイルをダウンロード**（エンジニアがよく触れるECデータのミニ版）  
3. **スター・スキーマに沿ったテーブル作成** (dim_…, fact_…)  
4. **CSVをDuckDBにロード**  
5. **基本的な集計クエリを実行**してスター・スキーマのメリットを確認

---

## 1. 事前準備

### A. DuckDBのインストール

- **CLI版 (バイナリ)**  
  公式リリースから各OS向け実行ファイルを入手:  
  <https://duckdb.org/docs/installation/index>
- **Python経由で使う場合**  
  ```bash
  pip install duckdb
  ```
  その後 `python -c "import duckdb; print(duckdb.__version__)"` で動作確認。

本チュートリアルでは **CLI版** で進めますが、Pythonからでも同様のSQLを実行可能です。

### B. サンプルCSVファイル

ここではECサイトをざっくり想定した、以下4つのCSVを用意します（各自好きなデータでもOKです）。

1. `customer.csv`
   - 顧客ID, 氏名, 地域, 登録日 など
2. `product.csv`
   - 商品ID, 商品名, カテゴリ, 単価 など
3. `date.csv`
   - 日付ID, 年, 月, 日, 週, 祝日フラグ など  
   - （手動で作る、または [date dimension generator](https://gist.github.com) 等を利用）
4. `sales.csv`
   - 1行＝1明細: 受注ID, 商品ID, 顧客ID, 日付ID, 売上金額, 購入数量 など

参考用に簡易データを下記リンクで提供（例）:  
- [customer.csv](https://raw.githubusercontent.com/kurosawa-kuro/dummy-data/master/dwh/customer.csv)  
- [product.csv](https://raw.githubusercontent.com/kurosawa-kuro/dummy-data/master/dwh/product.csv)  
- [date.csv](https://raw.githubusercontent.com/kurosawa-kuro/dummy-data/master/dwh/date.csv)  
- [sales.csv](https://raw.githubusercontent.com/kurosawa-kuro/dummy-data/master/dwh/sales.csv)  

(※実在リンクではなく例示)

事前にこれらを **同ディレクトリ** に保存しておきます。

---

## 2. DuckDBでスター・スキーマのテーブルを作る

DuckDBを起動し、以下のようなSQLを順番に実行してテーブルを作成します。今回は **“ロード時にテーブル作成＋データ挿入”** スタイルを採用します。

### A. テーブルスキーマ (DWHっぽいキー設計)
- **ディメンションテーブル**  
  - `dim_customer`: `customer_key`（サロゲートキー）をPKに、その他属性を保持  
  - `dim_product`: `product_key`をPKに、カテゴリなどの属性  
  - `dim_date`: `date_key`をPKに、年/月/日など
- **ファクトテーブル**  
  - `fact_sales`: `sales_id` をPKに、`customer_key`, `product_key`, `date_key` を外部キーとして売上金額などを持たせる

なお、チュートリアルでは **CSVに入っているIDをそのままサロゲートキーとみなす** 簡易設計にします。

### B. SQL例

```sql
-- (1) DuckDB起動 (CLIモード)
-- 例: ./duckdb mydatabase.duckdb

-- (2) ディメンション: dim_customer
CREATE TABLE dim_customer (
  customer_key INTEGER,
  customer_name VARCHAR,
  region VARCHAR,
  register_date VARCHAR
);

-- (3) ディメンション: dim_product
CREATE TABLE dim_product (
  product_key INTEGER,
  product_name VARCHAR,
  category VARCHAR,
  unit_price DECIMAL(10,2)
);

-- (4) ディメンション: dim_date
CREATE TABLE dim_date (
  date_key INTEGER,
  year INTEGER,
  month INTEGER,
  day INTEGER,
  is_holiday BOOLEAN
);

-- (5) ファクト: fact_sales
CREATE TABLE fact_sales (
  sales_id INTEGER,
  customer_key INTEGER,
  product_key INTEGER,
  date_key INTEGER,
  sales_amount DECIMAL(10,2),
  quantity INTEGER
);
```

*(DuckDBの場合、列型にDECIMALを使うかREALを使うかはご自由に。)*

---

## 3. CSVからデータをロードする

DuckDBはシンプルに `COPY FROM` 文や `read_csv_auto()` 関数を利用できます。サンプルとして `COPY FROM` を示します。

```sql
-- (A) ディメンションテーブルにデータ取り込み
COPY dim_customer FROM 'customer.csv' (AUTO_DETECT=TRUE);
COPY dim_product  FROM 'product.csv'  (AUTO_DETECT=TRUE);
COPY dim_date     FROM 'date.csv'     (AUTO_DETECT=TRUE);

-- (B) ファクトテーブルにデータ取り込み
COPY fact_sales   FROM 'sales.csv'    (AUTO_DETECT=TRUE);
```

これで4つのテーブルにデータがロードされます。  
もし列名や区切り文字をカスタムしたい場合は `(DELIMITER ',', HEADER=TRUE, ...)` などのオプションを付けてください。

---

## 4. 簡単な集計クエリを試す

### A. スター・スキーマJOINで集計

例: 「カテゴリー別の売上合計と数量合計を、年月単位で見る」などの分析を行います。  
下記のように **fact_sales** を中心に **dim_product**, **dim_date** をJOINし、カテゴリや年月ごとの集計をするクエリを書いてみましょう。

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

結果として、カテゴリ別×年月別の売上と数量がまとまって表示されます。スター・スキーマのシンプルなJOINがDWHらしさを感じさせるポイントです。

### B. 切り口を変えてみる

- 「地域(顧客のregion)別に、どのカテゴリが多く買われたか」  
- 「日付テーブルの `is_holiday` で祝日の売上と通常日を比較する」  

など、切り替えたい軸（ディメンション）をJOINすれば、容易に別の集計を取ることができます。

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

## 5. まとめと応用

1. **スター・スキーマ設計**  
   - ファクトテーブル(売上実績)に、ディメンションテーブル(顧客/商品/日付)を参照するキーを持たせることで、集計軸が明確に分離され、さまざまな分析クエリが書きやすくなる。  
2. **DuckDB での体験**  
   - カラムナ型DBなので GROUP BY や SUM などの集計クエリが高速。  
   - ローカル環境でも簡単にDWH風の体験ができる。  
3. **発展**  
   - ディメンションに追加属性を入れたり、ファクトテーブルに別の測定値(割引金額、利益など)を加える。  
   - CSV以外のParquetファイルを `read_parquet_auto()` で読み込む。  
   - Python経由で pandas DataFrame と連携し、BIツール的に可視化してみる。  

このように、**スター・スキーマ（ファクト/ディメンション設計）**を最小限に押さえた上でDuckDBを使ってSQLを投げるだけでも、**DWHの核心となる集計フロー**を身近に体験できます。

---

# これでDWH入門完了！

- **DuckDB** は軽量かつ高性能、構築コストもほぼゼロなので「スター・スキーマをまずサクッと学ぶ」には絶好のツール。  
- **ファクト/ディメンション** を切り分ける考え方と JOIN 集計を一度体験すれば、Redshift や Snowflake など大規模DWHへの応用も楽になります。  

ぜひこのチュートリアルを踏まえて、好みのデータで「自作スター・スキーマDWH」を構築・分析してみてください。DWHの基礎概念とカラムナDBの利点が一気に実感できるはずです。