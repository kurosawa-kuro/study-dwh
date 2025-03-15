以下のチュートリアルでは、**SNSアプリ** のデータベース設計（Prismaスキーマ）をもとに、**DuckDB** を使って **DWH（データウェアハウス）風のスター・スキーマ** を体験できるサンプルを作成します。実運用DB設計とは少し考え方が異なりますが、あくまで「分析用途に特化したテーブル設計」を学ぶための例としてご覧ください。

---
# DuckDB × SNSアプリ データをスター・スキーマで分析する

## 1. Prisma スキーマの概要

あなたが作成したSNSアプリには以下のようなテーブル(モデル)があります（抜粋）:

- **User / UserProfile / UserRole / Role / Follow**  
  - ユーザ、役割(Roles)、フォロー関係など
- **Micropost / Category / CategoryMicropost**  
  - 投稿 (Micropost) とカテゴリ (Category) の多対多関係  
- **Comment / Like / MicropostView**  
  - 投稿に対するコメント、いいね、閲覧(MicropostView)ログ

これらは「運用DBでの正規化スキーマ」を意識した構成です。  
一方で **DWH のスター・スキーマ** では「分析のしやすさ」を優先し、**ファクトテーブル** と **ディメンションテーブル** に分割して設計します。

---

## 2. DWHでのテーブル設計(サンプル)

本チュートリアルでは、SNSにおける **「投稿(Micropost)への様々なアクション(閲覧/コメント/いいね)」** を分析したい、という想定で**スター・スキーマ**を構築します。

### 2.1 ディメンションテーブル

1. **`dim_user`**  
   - ユーザの属性 (名前, メール, 登録日など)  
   - 運用DBの `User` や `UserProfile` をまとめて格納するイメージ  
   - **キー**: `user_key` (サロゲートキー)

2. **`dim_micropost`**  
   - 投稿そのものの属性 (タイトル, 所属ユーザ, カテゴリ, 作成日 など)  
   - 運用DBの `Micropost` + `CategoryMicropost` + `Category` 辺りを束ねる  
   - **キー**: `micropost_key` (サロゲートキー)

3. **`dim_date`**  
   - 日付情報 (year, month, day, weekday, etc.)  
   - いいねやコメントした日、投稿を閲覧した日などを紐づけやすくする  
   - **キー**: `date_key` (YYYYMMDD等)

> 必要に応じて、「dim_category」や「dim_role」など独立ディメンションを増やすことも考えられます。

### 2.2 ファクトテーブル

1. **`fact_micropost_actions`**  
   - 投稿に対するアクション(“view”, “like”, “comment”)を記録するテーブル  
   - 粒度: 1行 = 1アクション (あるユーザが、ある投稿に対して、ある日時に、どの種別のアクションを行ったか)  
   - 列例:  
     - `action_id` (サロゲートキー)  
     - `user_key` (FK→dim_user)  
     - `micropost_key` (FK→dim_micropost)  
     - `date_key` (FK→dim_date)  
     - `action_type` (“view” / “like” / “comment” / …)  
     - `comment_length`, `view_ipaddress` など、アクション種別に応じたメトリクスを追加

> **フォロー関係(Follow)** も分析対象なら、別のファクトテーブル `fact_follow` を作るなど拡張もできます。  
> 今回は「投稿周りの指標を分析する」ことに絞って、アクションファクトを1つ置いています。

---

## 3. DuckDB での実装フロー

1. **Python + DuckDB 環境の用意**  
2. **(デモ) 運用DBからダンプ or PrismaからエクスポートしたCSV等を準備**  
3. **ディメンションテーブルを作成 & データ投入**  
4. **ファクトテーブルを作成 & データ投入**  
5. **スター・スキーマJOINで分析クエリ** を試す

以下は **最小限のデモ用サンプル**です。  
実際には **Prismaで管理しているPostgreSQL** から必要なデータを `SELECT` して取得し、  
スター・スキーマに合わせて整形 → DuckDBへロード という流れを構築するとよいでしょう。

---

### 3.1 ダミーデータ (pandas DataFrame)

```python
# %% [code]
!pip install duckdb --quiet

import pandas as pd

# -------------------------
# 1) ユーザ(dim_user)
# -------------------------
df_user = pd.DataFrame({
    'user_key': [1, 2, 3],
    'user_name': ['Alice', 'Bob', 'Charlie'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
    'registration_date': ['2023-01-10', '2023-02-05', '2023-03-01']
})

# -------------------------
# 2) 投稿(dim_micropost)
#    (本来はMicropost+CategoryをJOINしてタイトルやカテゴリを格納)
# -------------------------
df_micropost = pd.DataFrame({
    'micropost_key': [101, 102, 103],
    'title': ['First Post', 'Second Post', 'Hello World'],
    'author_user_key': [1, 2, 3],  # 投稿のオーナーが誰か
    'category': ['General', 'Tech', 'Diary'],  # 例として1カテゴリだけ保持
    'created_date': ['2023-01-11', '2023-02-06', '2023-03-02']
})

# -------------------------
# 3) 日付(dim_date)
# -------------------------
df_date = pd.DataFrame({
    'date_key': [20230110, 20230111, 20230205, 20230206, 20230301, 20230302],
    'year': [2023, 2023, 2023, 2023, 2023, 2023],
    'month': [1, 1, 2, 2, 3, 3],
    'day': [10, 11, 5, 6, 1, 2],
    'is_weekend': [False, False, False, False, False, False]
})

# -------------------------
# 4) アクション(fact_micropost_actions)
#    例: user2がPost101を閲覧(view)した, user1がPost101にlikeした, user3がコメントした, etc.
# -------------------------
df_fact_actions = pd.DataFrame({
    'action_id': [1001, 1002, 1003, 1004, 1005],
    'user_key': [2, 1, 3, 2, 3],        # 誰が
    'micropost_key': [101, 101, 102, 102, 103],  # どの投稿に
    'date_key': [20230111, 20230111, 20230206, 20230206, 20230302],  # いつ
    'action_type': ['view', 'like', 'comment', 'view', 'view'],
    'view_ip': ['192.168.0.2', None, None, '192.168.0.2', '10.0.0.9'],
    'comment_text': [None, None, 'Nice post!', None, None]
})

df_user, df_micropost, df_date, df_fact_actions
```

---

### 3.2 DuckDBでテーブル作成＆INSERT

```python
# %% [code]
import duckdb

con = duckdb.connect(database=':memory:')

# --- ディメンション
con.execute("""
CREATE TABLE dim_user (
  user_key INT,
  user_name VARCHAR,
  email VARCHAR,
  registration_date DATE
)
""")

con.execute("""
CREATE TABLE dim_micropost (
  micropost_key INT,
  title VARCHAR,
  author_user_key INT,
  category VARCHAR,
  created_date DATE
)
""")

con.execute("""
CREATE TABLE dim_date (
  date_key INT,
  year INT,
  month INT,
  day INT,
  is_weekend BOOLEAN
)
""")

# --- ファクト
con.execute("""
CREATE TABLE fact_micropost_actions (
  action_id INT,
  user_key INT,
  micropost_key INT,
  date_key INT,
  action_type VARCHAR,
  view_ip VARCHAR,
  comment_text VARCHAR
)
""")

# pandasのDataFrameを一時テーブルとして登録
con.register("temp_user", df_user)
con.register("temp_micropost", df_micropost)
con.register("temp_date", df_date)
con.register("temp_actions", df_fact_actions)

# INSERT
con.execute("INSERT INTO dim_user SELECT * FROM temp_user")
con.execute("INSERT INTO dim_micropost SELECT * FROM temp_micropost")
con.execute("INSERT INTO dim_date SELECT * FROM temp_date")
con.execute("INSERT INTO fact_micropost_actions SELECT * FROM temp_actions")

# 確認
res_user = con.execute("SELECT * FROM dim_user").df()
res_micropost = con.execute("SELECT * FROM dim_micropost").df()
res_date = con.execute("SELECT * FROM dim_date").df()
res_actions = con.execute("SELECT * FROM fact_micropost_actions").df()

display(res_user)
display(res_micropost)
display(res_date)
display(res_actions)
```

---

## 4. 分析クエリ例

### 4.1 どの投稿が最も「アクション」されているか？

- `fact_micropost_actions` を中心に、`dim_micropost` をJOINして **投稿タイトル** を参照  
- GROUP BY でアクション数を集計

```python
# %% [code]
q1 = """
SELECT
  m.title,
  COUNT(*) AS action_count
FROM fact_micropost_actions f
JOIN dim_micropost m ON f.micropost_key = m.micropost_key
GROUP BY m.title
ORDER BY action_count DESC
"""

df_q1 = con.execute(q1).df()
df_q1
```
<details>
<summary>想定出力</summary>

```
          title  action_count
0   Second Post             2
1    First Post             2
2   Hello World             1
```
</details>

---

### 4.2 アクションタイプ別に集計

```python
# %% [code]
q2 = """
SELECT
  f.action_type,
  COUNT(*) AS cnt
FROM fact_micropost_actions f
GROUP BY f.action_type
ORDER BY cnt DESC
"""

df_q2 = con.execute(q2).df()
df_q2
```
<details>
<summary>想定出力</summary>

```
 action_type   cnt
0       view     3
1       like     1
2    comment     1
```
</details>

---

### 4.3 ユーザごとの「他人の投稿に対する」アクション数

- 投稿作者 = `dim_micropost.author_user_key`  
- アクションユーザ = `fact_micropost_actions.user_key`  
- “他人の投稿”へのアクション数だけを数える → `author_user_key != user_key`

```python
# %% [code]
q3 = """
SELECT
  u.user_name AS actor_name,
  COUNT(*) AS actions_on_others
FROM fact_micropost_actions f
JOIN dim_micropost m ON f.micropost_key = m.micropost_key
JOIN dim_user u ON f.user_key = u.user_key
WHERE m.author_user_key != f.user_key
GROUP BY u.user_name
ORDER BY actions_on_others DESC
"""

df_q3 = con.execute(q3).df()
df_q3
```
<details>
<summary>想定出力</summary>

```
 actor_name  actions_on_others
0   Alice                   1
1   Bob                     1
2   Charlie                 1
```
</details>

---

### 4.4 “view” アクションのユニークIP数を投稿別に集計

- `action_type = 'view'` に絞る  
- IPアドレス(`view_ip`) の重複を排除してカウント

```python
# %% [code]
q4 = """
SELECT
  m.title,
  COUNT(DISTINCT f.view_ip) AS unique_viewers_ip
FROM fact_micropost_actions f
JOIN dim_micropost m ON f.micropost_key = m.micropost_key
WHERE f.action_type = 'view'
GROUP BY m.title
ORDER BY unique_viewers_ip DESC
"""

df_q4 = con.execute(q4).df()
df_q4
```
<details>
<summary>想定出力</summary>

```
          title  unique_viewers_ip
0    First Post                  1
1   Second Post                  1
2   Hello World                  1
```
</details>

(データが少なく全部1だけど、実際には様々なIPがある想定です)

---

## 5. まとめ & 発展

1. **スター・スキーマのポイント**  
   - **`fact_micropost_actions`**: 投稿に対する閲覧/いいね/コメント等を「1アクション1行」で記録  
   - **`dim_user`, `dim_micropost`, `dim_date`**: アクションを説明する文脈(属性)を切り出す  
   - JOIN + GROUP BY するだけで「誰が何をいつどう操作したか？」を分析しやすい形になる

2. **実運用データとの連携**  
   - Prismaスキーマの各テーブル (User, Micropost, Comment, Like, MicropostView…) から適宜データを抽出し、  
     スター・スキーマに変換 → DuckDB などDWH系DBにロードする  
   - 定期的に「更新分のみ」ロードして、BIツールやSQLで分析

3. **さらに拡張**  
   - 「フォロー関係(Follow)」をファクト化して、日付/ユーザを軸にフォロー数の推移を見たり  
   - 役割(Role)やUserProfileの追加属性を `dim_user` に含める(例: location、bio、birthDateなど)  
   - `dim_micropost` にカテゴリを多対多で持たせる場合はスノーフレークスキーマ気味になるが、  
     カテゴリが1つの場合や主カテゴリのみを保持すると単純化できる

4. **DuckDBのメリット**  
   - 軽量かつローカルで高速な列指向DB  
   - 大規模になっても CSV/Parquet ファイルやSQLで簡単に扱える  
   - 他の本格DWH (Snowflake, Redshift, BigQueryなど) へ移行する前のプロトタイプとして最適

---

# おわりに

以上は、**SNSアプリ** の運用DBを基に **スター・スキーマ** を構築し、**DuckDB** で分析クエリを実行するチュートリアルです。  
運用DBの正規化構造(Prismaスキーマ)とは別に、**分析やレポーティングに最適化**されたデータモデルを作ることで、JOIN + GROUP BY を多用してもわかりやすく、高速な分析が実現できます。  

ぜひ本チュートリアルを参考に、実際のアプリDBから **ファクト/ディメンション** を抽出・変換してみてください。スター・スキーマの考え方が身につくと、SNSアプリのみならず様々なサービスでのデータ利活用がはかどるはずです。