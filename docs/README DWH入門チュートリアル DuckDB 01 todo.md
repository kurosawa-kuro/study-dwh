以下のコード例は、前回までのECサイト売上データのスター・スキーマを、**Todoアプリ**に置き換えたバージョンです。  
ディメンションテーブルを「ユーザー」「日付」「タスク」、ファクトテーブルを「タスクに対するアクション履歴」として設計しています。  
DuckDB を使った **スター・スキーマ**の学習用チュートリアルとしてご利用ください。

---

# DuckDB × Todoアプリのスター・スキーマ MVPチュートリアル

## 全体構成

- **Dimension Tables (3つ)**
  1. `dim_user` … ユーザー情報 (user_key, user_name, email, …)
  2. `dim_date` … 日付情報 (date_key, year, month, day, …)
  3. `dim_task` … タスク情報 (task_key, task_name, category, due_date_key, …)

- **Fact Table (1つ)**
  - `fact_task_actions` … タスクに対するアクション履歴  
    (例: “作成(create)”, “完了(complete)”, “更新(update)”など)

---

## 1. 前準備: DuckDB のインストール
```python
# %% [code]
!pip install duckdb --quiet
```

---

## 2. サンプルDataFrameを用意 (pandas)

```python
# %% [code]
import pandas as pd

# ---- ユーザー (dim_user) ----
df_user = pd.DataFrame({
    'user_key': [1, 2, 3],
    'user_name': ['Alice', 'Bob', 'Charlie'],
    'email': ['alice@example.com', 'bob@example.com', None],
    'registration_date': ['2023-01-10', '2023-02-15', '2023-03-20']
})

df_user
```
<details>
<summary>表示イメージ</summary>

```
   user_key user_name              email registration_date
0         1     Alice alice@example.com        2023-01-10
1         2       Bob   bob@example.com        2023-02-15
2         3  Charlie               None        2023-03-20
```
</details>

---

```python
# %% [code]
# ---- 日付 (dim_date) ----
df_date = pd.DataFrame({
    'date_key': [20230110, 20230111, 20230215, 20230320, 20230321],
    'year': [2023, 2023, 2023, 2023, 2023],
    'month': [1, 1, 2, 3, 3],
    'day': [10, 11, 15, 20, 21],
    'is_weekend': [False, False, False, False, False]
})

df_date
```
<details>
<summary>表示イメージ</summary>

```
    date_key  year  month  day  is_weekend
0  20230110  2023      1   10       False
1  20230111  2023      1   11       False
2  20230215  2023      2   15       False
3  20230320  2023      3   20       False
4  20230321  2023      3   21       False
```
</details>

---

```python
# %% [code]
# ---- タスク (dim_task) ----
df_task = pd.DataFrame({
    'task_key': [101, 102, 103],
    'task_name': ['Buy groceries', 'Prepare report', 'Exercise'],
    'category': ['Private', 'Work', 'Private'],
    'due_date_key': [20230111, 20230215, 20230321]  # タスクの締切日を日付キーで結びつける例
})

df_task
```
<details>
<summary>表示イメージ</summary>

```
   task_key       task_name category  due_date_key
0       101  Buy groceries  Private     20230111
1       102  Prepare report     Work     20230215
2       103       Exercise  Private     20230321
```
</details>

---

```python
# %% [code]
# ---- タスクアクション (fact_task_actions) ----
# 例: アクションID、誰がいつ、どのタスクに何をしたか
df_task_actions = pd.DataFrame({
    'task_action_id': [1001, 1002, 1003, 1004, 1005],
    'task_key': [101, 101, 102, 103, 103],  # FK: dim_task
    'user_key': [1, 1, 2, 3, 3],            # FK: dim_user
    'date_key': [20230110, 20230111, 20230215, 20230320, 20230321],  # FK: dim_date
    'action_type': ['create', 'complete', 'create', 'create', 'complete'],
    'elapsed_minutes': [None, 60, None, None, 30],
    'is_completed': [False, True, False, False, True]
})

df_task_actions
```
<details>
<summary>表示イメージ</summary>

```
   task_action_id  task_key  user_key   date_key action_type  elapsed_minutes  is_completed
0            1001       101         1  20230110      create             NaN         False
1            1002       101         1  20230111    complete            60.0          True
2            1003       102         2  20230215      create             NaN         False
3            1004       103         3  20230320      create             NaN         False
4            1005       103         3  20230321    complete            30.0          True
```
</details>

---

## 3. DuckDB への接続 & テーブル作成

```python
# %% [code]
import duckdb

# メモリ上にDB作成 (ファイル保存したければ 'my_todo.duckdb' など)
con = duckdb.connect(database=':memory:')

# -- ディメンションテーブル --
con.execute("""
CREATE TABLE dim_user (
  user_key INT,
  user_name VARCHAR,
  email VARCHAR,
  registration_date DATE
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

con.execute("""
CREATE TABLE dim_task (
  task_key INT,
  task_name VARCHAR,
  category VARCHAR,
  due_date_key INT
)
""")

# -- ファクトテーブル --
con.execute("""
CREATE TABLE fact_task_actions (
  task_action_id INT,
  task_key INT,
  user_key INT,
  date_key INT,
  action_type VARCHAR,
  elapsed_minutes INT,
  is_completed BOOLEAN
)
""")
```

---

## 4. pandas DataFrame を DuckDB にINSERT

```python
# %% [code]
# 1. 一時テーブルとしてDataFrameをDuckDBに登録
con.register("temp_user", df_user)
con.register("temp_date", df_date)
con.register("temp_task", df_task)
con.register("temp_actions", df_task_actions)

# 2. 本テーブルへINSERT
con.execute("INSERT INTO dim_user SELECT * FROM temp_user")
con.execute("INSERT INTO dim_date SELECT * FROM temp_date")
con.execute("INSERT INTO dim_task SELECT * FROM temp_task")
con.execute("INSERT INTO fact_task_actions SELECT * FROM temp_actions")
```

---

## 5. データをSELECTしてみる

```python
# %% [code]
res_user   = con.execute("SELECT * FROM dim_user").df()
res_date   = con.execute("SELECT * FROM dim_date").df()
res_task   = con.execute("SELECT * FROM dim_task").df()
res_action = con.execute("SELECT * FROM fact_task_actions").df()

display(res_user)
display(res_date)
display(res_task)
display(res_action)
```

---

## 6. スター・スキーマ JOIN の集計例

### 例1: ユーザー別の「完了したタスク数」を集計

```python
# %% [code]
query1 = """
SELECT
  u.user_name,
  COUNT(*) AS completed_count
FROM fact_task_actions AS f
JOIN dim_user AS u
  ON f.user_key = u.user_key
WHERE f.is_completed = TRUE
GROUP BY u.user_name
ORDER BY completed_count DESC
"""

df_result1 = con.execute(query1).df()
df_result1
```
<details>
<summary>想定出力例</summary>

```
  user_name  completed_count
0     Alice                1
1   Charlie               1
```
</details>

---

### 例2: タスクカテゴリ × アクションタイプ ごとの件数を集計

```python
# %% [code]
query2 = """
SELECT
  t.category,
  f.action_type,
  COUNT(*) AS action_count
FROM fact_task_actions f
JOIN dim_task t
  ON f.task_key = t.task_key
GROUP BY t.category, f.action_type
ORDER BY t.category, f.action_type
"""

df_result2 = con.execute(query2).df()
df_result2
```
<details>
<summary>想定出力例</summary>

```
   category  action_type  action_count
0  Private      complete             2
1  Private       create             2
2     Work       create             1
```
</details>

---

### 例3: タスクの「締切日 (due_date_key)」と「完了日 (fact側のdate_key)」の差を取る

タスクに結びつく「締切日(due_date_key)」と、完了アクションの「date_key」をJOINして、その差(日数)を計算するイメージです。  

DuckDBでは `dim_date` テーブルに `date_key` (YYYYMMDD形式) を置いているので、厳密な日数差を求めるには `DATE`型列を持たせるか `DATEDIFF` を使うなど工夫が必要です。ここではサンプルとして差分を雑に計算します。

```python
# %% [code]
query3 = """
WITH completed AS (
  SELECT
    f.task_action_id,
    f.task_key,
    f.date_key AS complete_date_key,
    t.due_date_key
  FROM fact_task_actions f
  JOIN dim_task t
    ON f.task_key = t.task_key
  WHERE f.is_completed = TRUE
)
SELECT
  c.task_action_id,
  c.task_key,
  c.due_date_key,
  c.complete_date_key,
  (c.complete_date_key - c.due_date_key) AS date_diff_raw
FROM completed c
"""
df_result3 = con.execute(query3).df()
df_result3
```
<details>
<summary>想定出力例</summary>

```
   task_action_id  task_key  due_date_key  complete_date_key  date_diff_raw
0            1002       101       20230111           20230111             0
1            1005       103       20230321           20230321             0
```
</details>

> **備考**: `date_diff_raw = 0` なので「締切当日に完了した」と推測できます。ちゃんと日数計算するなら `dim_date` 側に `DATE`型カラムを持たせて `DATEDIFF('day', due_date, complete_date)` などを使うと良いでしょう。

---

## 7. まとめ

- **dim_user, dim_date, dim_task** → スター・スキーマのディメンション  
- **fact_task_actions** → タスクごとのアクション履歴を蓄積  
- **JOINして GROUP BY** することで、ユーザー別/タスク別/日付別など多軸の分析が可能に  

このように **DuckDB** を使うと、ローカル環境でも非常に簡単に「DWHっぽいスター・スキーマの体験」ができます。  
タスク完了率や所要時間、期日との比較などを気軽にSQLで集計してみてください。