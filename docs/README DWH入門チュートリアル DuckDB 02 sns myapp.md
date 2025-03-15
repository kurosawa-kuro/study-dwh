以下のチュートリアルは、前回の **SNSアプリDWH** 入門を**スノーフレークスキーマ対応**にパワーアップさせたバージョンです。  
なぜなら、SNSアプリの **投稿 (Micropost) とカテゴリ (Category) が多対多** という要件があるため、  
単純に「`dim_micropost` に `category` カラムを1つだけ持つ」ようなスター・スキーマでは不十分です。  

ここでは、**Micropost : Category = 多対多** を正しくモデリングするため、  
**`dim_micropost`** と **`dim_category`** を分け、両者を結ぶ **ブリッジテーブル** を使った**スノーフレークスキーマ**を構築します。  

---

# DuckDB × SNSアプリ (スノーフレークスキーマ対応)

## 1. なぜスノーフレークが必要か？

**多対多 (N:M)** の関係がある場合、スター・スキーマの「1つのディメンションにカラムを生やす」だけでは表現が難しいです。  
SNSアプリでは、**1つの投稿(Micropost)** が **複数のカテゴリ** を持つ可能性があるため、  
スター・スキーマの `dim_micropost` に `category_id` を1つだけ持つ設計では足りません。

そこで、スノーフレーク化（ディメンションをさらに分割し、**多対多のブリッジテーブル**を挟む）する方法をとります。

---

## 2. スノーフレーク構造のイメージ

```
fact_micropost_actions
   ├── (micropost_key) ──> dim_micropost
   │                       └─> micropost_category_bridge ─> dim_category
   ├── (user_key)     ──> dim_user
   └── (date_key)     ──> dim_date
```

- **`dim_micropost`** : 投稿に関する属性（タイトル、作者、投稿日時など）  
- **`dim_category`** : カテゴリに関する属性（カテゴリ名、階層など）  
- **`micropost_category_bridge`** : 「どの投稿がどのカテゴリに属しているか」を管理する中間テーブル (多対多)  
- **`fact_micropost_actions`** : 投稿に対する“view/like/comment”などのアクションログ

---

## 3. ダミーデータ例 (pandas)

以下では、**カテゴリが多対多** になるように想定したデモデータを作成します。

### 3.1 ディメンション1: `dim_user`

```python
df_user = pd.DataFrame({
    'user_key': [1, 2, 3],
    'user_name': ['Alice', 'Bob', 'Charlie'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
    'registration_date': ['2023-01-10', '2023-02-05', '2023-03-01']
})
```

### 3.2 ディメンション2: `dim_micropost`

```python
df_micropost = pd.DataFrame({
    'micropost_key': [101, 102, 103],
    'title': ['First Post', 'Second Post', 'Hello World'],
    'author_user_key': [1, 2, 3],    # 投稿のオーナー
    'created_date': ['2023-01-11', '2023-02-06', '2023-03-02']
})
```

> ここでは **カテゴリ情報は持たせず**、多対多のブリッジで扱う。

### 3.3 ディメンション3: `dim_category`

```python
df_category = pd.DataFrame({
    'category_key': [201, 202, 203],
    'category_name': ['General', 'Tech', 'Diary']
})
```

### 3.4 ブリッジ: `micropost_category_bridge`

- 多対多対応の中間テーブル。
- 例: Post101 → “General”, Post102 → “General” + “Tech”, Post103 → “Diary”

```python
df_micropost_category_bridge = pd.DataFrame({
    'micropost_key': [101, 102, 102, 103],
    'category_key': [201, 201, 202, 203]
})
```

### 3.5 ディメンション4: `dim_date`

```python
df_date = pd.DataFrame({
    'date_key': [20230110, 20230111, 20230206, 20230302],
    'year': [2023, 2023, 2023, 2023],
    'month': [1, 1, 2, 3],
    'day': [10, 11, 6, 2],
    'is_weekend': [False, False, False, False]
})
```

### 3.6 ファクト: `fact_micropost_actions`

- 1行=1アクション  
- `action_type`: “view”, “like”, “comment” etc.

```python
df_fact_actions = pd.DataFrame({
    'action_id': [1001, 1002, 1003],
    'user_key': [2, 1, 3],      # 誰が
    'micropost_key': [101, 101, 102],   # どの投稿
    'date_key': [20230111, 20230111, 20230206],
    'action_type': ['view', 'like', 'comment'],
    'view_ip': ['192.168.0.2', None, None],
    'comment_text': [None, None, 'Nice post!']
})
```

---

## 4. DuckDBでテーブル作成

```python
import duckdb

con = duckdb.connect(database=':memory:')

# ディメンション: user
con.execute("""
CREATE TABLE dim_user (
  user_key INT,
  user_name VARCHAR,
  email VARCHAR,
  registration_date DATE
);
""")

# ディメンション: micropost
con.execute("""
CREATE TABLE dim_micropost (
  micropost_key INT,
  title VARCHAR,
  author_user_key INT,
  created_date DATE
);
""")

# ディメンション: category
con.execute("""
CREATE TABLE dim_category (
  category_key INT,
  category_name VARCHAR
);
""")

# ブリッジ: micropost_category_bridge
con.execute("""
CREATE TABLE micropost_category_bridge (
  micropost_key INT,
  category_key INT
);
""")

# ディメンション: date
con.execute("""
CREATE TABLE dim_date (
  date_key INT,
  year INT,
  month INT,
  day INT,
  is_weekend BOOLEAN
);
""")

# ファクト: fact_micropost_actions
con.execute("""
CREATE TABLE fact_micropost_actions (
  action_id INT,
  user_key INT,
  micropost_key INT,
  date_key INT,
  action_type VARCHAR,
  view_ip VARCHAR,
  comment_text VARCHAR
);
""")
```

---

## 5. データ登録

```python
# pandasのDataFrameを一時テーブルとして登録
con.register("temp_user", df_user)
con.register("temp_micropost", df_micropost)
con.register("temp_category", df_category)
con.register("temp_mcbridge", df_micropost_category_bridge)
con.register("temp_date", df_date)
con.register("temp_fact_actions", df_fact_actions)

con.execute("INSERT INTO dim_user SELECT * FROM temp_user")
con.execute("INSERT INTO dim_micropost SELECT * FROM temp_micropost")
con.execute("INSERT INTO dim_category SELECT * FROM temp_category")
con.execute("INSERT INTO micropost_category_bridge SELECT * FROM temp_mcbridge")
con.execute("INSERT INTO dim_date SELECT * FROM temp_date")
con.execute("INSERT INTO fact_micropost_actions SELECT * FROM temp_fact_actions")
```

---

ご要望にあるような「**多対多**を含むスノーフレークスキーマを使った**スター・スキーマ風のDWH**」を想定した分析クエリ例を改めてまとめると、以下のようになります。  

## サンプルクエリ例：スノーフレークスキーマ対応

### 1. **カテゴリ別の売上数量 / 合計金額** (多対多カテゴリ)
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

> - **多対多対応ポイント**: `product_category_bridge` を挟んで `dim_category` をJOINする。  
> - 商品が複数カテゴリに属している場合は、その商品の売上が**すべてのカテゴリに加算**されることに注意。

---

### 2. **ユーザステータス別の購入回数**

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

> - ここではカテゴリとのJOINは不要。  
> - **スター・スキーマ**の基本的な「ファクトテーブル×ユーザディメンションの集計」パターン。

---

### 3. **日付ディメンションを活用して月別売上を集計**

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

> - スター・スキーマの王道例: **fact_sales** → **dim_date** へのJOIN + 月別グループ化。  
> - `dim_date` に `quarter`, `weekday` などの列を持たせれば、さらに多彩な時系列分析が可能。

---

## まとめ: 多対多カテゴリのスノーフレークでもスター・スキーマ的分析が可能

1. **事実テーブル (`fact_sales`)** に売上や数量を持ち、  
2. **ディメンションテーブル (`dim_user`, `dim_date`, `dim_product`)** を参照し、  
3. 多カテゴリを扱うために **`product_category_bridge` → `dim_category`** のようにスノーフレーク化しておく  
   - JOINは増えるが、複数カテゴリへまたがる商品を正しく集計できる。

上記のようなクエリセットが、典型的な **DWH + スノーフレークスキーマ** の分析パターンです。  
「**カテゴリ別の売上**」「**ユーザステータス別の集計**」「**日付軸での売上推移**」といったよくある分析をシンプルに表現できます。

このサンプルを応用すれば、SNSアプリやECショップなど、複雑な多対多関係を持つドメインでもスター・スキーマ（＋スノーフレーク）を活かしたデータ分析ができるようになるはずです。