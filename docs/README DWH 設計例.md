スター・スキーマの基本は**「ファクト(数値を集計するテーブル)」を中心に、複数のディメンションテーブル(集計の切り口)を放射状に繋げる」**構造です。ここでは、3つの題材（ToDoアプリ、SNS(X/Twitter的)、ECショップ）について、簡易的なスター・スキーマ例を示します。

---

# 1. Todoアプリのスター・スキーマ例

### 要件イメージ
- ユーザーがタスク（Todo）を作成、完了などの操作を行う。  
- 分析したい例: 「ユーザー別のタスク完了率」「タスクの種類や期日ごとの傾向」など。

### テーブル例

1. **ファクトテーブル: `fact_task_actions`**  
   - **粒度**: タスクに対するアクション(Todo作成、更新、完了、削除など)を1行として記録  
   - **主なカラム**  
     - `task_action_id` : (PK, サロゲートキー)  
     - `task_id` : タスク自体を表すID (もしくはディメンションキーにするか検討)  
     - `user_key` : ユーザー(ディメンション)の外部キー  
     - `date_key` : 日付ディメンションの外部キー (作成日や完了日をどう扱うか要検討)  
     - `action_type` : “create”, “complete”, “update” など  
     - `elapsed_minutes` : (完了などにかかった時間を測るなら)  
     - `priority_flag` : タスクの優先度（集計対象の指標扱いにするなら）  
     - **メトリック(集計指標)**: アクション回数(=1)、もしくは `is_completed` (0/1) など

   - **備考**  
     - 「ファクトテーブルに何を数値（メトリック）として持たせるか？」が難しいケース。完了率や所要時間などを計算したいなら、その対象をファクトに含める。

2. **ディメンションテーブル: `dim_user`**  
   - **主なカラム**  
     - `user_key` : (PK, サロゲートキー)  
     - `user_id` : (実アプリのユーザーID)  
     - `user_name` : ユーザー名  
     - `email` : メールアドレス（分析用途なら必要に応じて）  
     - `registration_date` : 登録日(分析に使うなら)

3. **ディメンションテーブル: `dim_date`**  
   - **主なカラム**  
     - `date_key` : (PK, YYYYMMDDなど)  
     - `date_actual` : (DATE型)  
     - `day_of_week`, `month`, `year`, `is_weekend` など、分析に便利な列

4. (オプション) **ディメンションテーブル: `dim_task`**  
   - **主なカラム**  
     - `task_key` : (PK, サロゲートキー)  
     - `task_id` : (実アプリのタスクID)  
     - `title` : タスク名  
     - `category` : タスクカテゴリ（“仕事”, “プライベート”など）  
     - `due_date_key` : (締切を日付ディメンションで関連付けたい場合)  
   - **備考**  
     - 小規模のToDoアプリだとタスク自体をファクト側に含めても良いが、汎用的に考えるならタスクをディメンション化して「どのようなタスクが多いか」を分析できる。

### まとめ (Todoアプリ)
- **fact_task_actions** が中心  
- `dim_user`, `dim_date`, `dim_task` が放射状につく  
- 分析例:  
  - 「ユーザー別の1週間のタスク完了数」  
  - 「カテゴリ別にタスク作成数の推移」  
  - 「due_dateとaction_dateの差で遅延率を計測」など

---

# 2. SNS (X / Twitter的) アプリのスター・スキーマ例

### 要件イメージ
- ユーザーが投稿(Tweet)を作成、他ユーザーが「いいね」「リツイート」などの反応をする。  
- 分析例: 「どのユーザーがどれくらい投稿/いいねをしたか」「日時やデバイス別の投稿数の傾向」など。

### テーブル例

1. **ファクトテーブル: `fact_post_interactions`**  
   - **粒度**: 1行＝1回のインタラクション（投稿/いいね/リツイート/返信など）  
   - **主なカラム**  
     - `interaction_id` : (PK, サロゲートキー)  
     - `date_key` : 日付ディメンションの外部キー (いつ行われたか)  
     - `user_key` : インタラクションをしたユーザー(ディメンション)の外部キー  
     - `post_key` : 対象の投稿(ディメンション)の外部キー  
     - `device_key` : (モバイル, PCなどデバイス種別を持つディメンションがあるなら)  
     - `interaction_type` : (投稿, いいね, リツイート, リプライなど)  
     - **メトリック**: `count_interaction`(=1)，いいね数やリツイート数を集計するなら “0/1” など各種フラグ  
       - 例: `is_like`(0/1), `is_retweet`(0/1) など

2. **ディメンションテーブル: `dim_user`**  
   - `user_key` : (PK)  
   - `username`  
   - `join_date_key` : ユーザーのアカウント作成日（もし分析に使うならdate_key参照にしてもOK）  
   - `location` : ユーザーの所在地など？

3. **ディメンションテーブル: `dim_post`**  
   - `post_key` : (PK)  
   - `post_id` : (実アプリの投稿ID)  
   - `author_user_key` : 投稿をしたユーザー。ユーザーdimへのFKにするか、ファクトに含めるかは設計次第。  
   - `content` : 本文（分析用途によってはテキストマイニング向けに保持するか検討）  
   - `post_date_key` : 投稿時の日付キー

4. **ディメンションテーブル: `dim_date`**  
   - `date_key` : (PK, YYYYMMDDなど)  
   - `date_actual` : (DATE型)  
   - `year`, `month`, `day_of_week`, `is_weekend` など

5. (オプション) **ディメンションテーブル: `dim_device`**  
   - `device_key` : (PK)  
   - `device_type` : (iOS, Android, Web, etc.)  
   - これを入れておくと「デバイス別の利用状況」を分析可能。

### まとめ (SNS)
- **fact_post_interactions** がメインのファクト  
- ディメンションは `dim_user`, `dim_date`, `dim_post`（必要に応じて`dim_device`など）  
- 分析例:  
  - 「日付×デバイス×interaction_type 別に集計して、ピーク時間帯を知る」  
  - 「ユーザーごとに月間投稿数・いいね数・リツイート数の推移を可視化」  

---

# 3. EC ショップのスター・スキーマ例

### 要件イメージ
- 典型的なDWHの題材。注文履歴(売上)が大量にたまっていく。  
- 分析例: 「商品別売上」「顧客属性別の購入傾向」「日次売上推移」など。

### テーブル例

1. **ファクトテーブル: `fact_sales`**  
   - **粒度**: 1行＝1回の注文明細（注文ID + 商品ID）  
   - **主なカラム**  
     - `sales_id` : (PK, サロゲートキー)  
     - `order_id` : オーダー単位ID（ディメンション化する場合もあり）  
     - `product_key` : 商品ディメンションへのFK  
     - `customer_key` : 顧客ディメンションへのFK  
     - `date_key` : 注文日ディメンションへのFK  
     - `store_key` : オンライン or 実店舗などがあるなら店舗ディメンションへのFK  
     - **メトリック**: `sales_amount` (売上金額), `quantity` (購入数量), `discount_amount` など

2. **ディメンションテーブル: `dim_product`**  
   - `product_key` : (PK, サロゲートキー)  
   - `product_id` : 実DB上のID  
   - `product_name`, `category`, `brand`, `price` など商品属性

3. **ディメンションテーブル: `dim_customer`**  
   - `customer_key` : (PK, サロゲートキー)  
   - `customer_id` : 実DB上のユーザーID  
   - `name`, `age`, `gender`, `region` など  
   - 会員登録日を `registration_date_key` で持たせてもよい

4. **ディメンションテーブル: `dim_date`**  
   - `date_key` : (PK, YYYYMMDDなど)  
   - `date_actual`, `year`, `quarter`, `month`, `day_of_week`, `is_holiday`, … 

5. (オプション) **ディメンションテーブル: `dim_store`**  
   - `store_key` : (PK)  
   - `store_id` : 実システムの店舗ID  
   - `store_name`, `location`, `store_type` (オンライン/実店舗) など

### まとめ (EC)
- **fact_sales** が売上を記録するコア  
- ディメンション: `dim_product`, `dim_customer`, `dim_date`, `dim_store` など  
- 分析例:  
  - 「月別×商品カテゴリ別の売上金額推移」  
  - 「顧客属性(地域/年齢層)別の平均購入金額」  
  - 「店舗ごとの在庫回転率やディスカウント効果を計測」など

---

# 共通のポイント

1. **ファクトテーブルの粒度**  
   - 「1タスク×1アクション」「1投稿×1インタラクション」「1注文×1商品行」など、どの単位でレコードを作るかが重要。  
2. **ディメンションキー (サロゲートキー)**  
   - 多くの場合、業務システムのIDとは別に連番キーを使う。  
   - ただし小規模アプリなら既存のIDをそのまま使っても良い。  
3. **日時ディメンション (`dim_date`)**  
   - DWHで頻出。年/月/週/祝日フラグなどを持たせると分析に便利。  
   - 実アプリDBの “timestamp” をそのまま使うのではなく、日付軸に正規化するのがスター・スキーマらしい作法。  
4. **ETL/データ整形**  
   - スター・スキーマ構築には「（元データ）→ディメンション/ファクトに分割」するETL処理が必要。  
   - 初期は手動でINSERT/COPYしても良いが、実際の運用ではETLジョブを自動化しがち。

---

## まとめ
- **Todoアプリ** や **SNS(X/Twitter風)**、**ECショップ** といった題材でも、**“何をファクトとするか”、“どんなディメンションがあるか”** を考えることでスター・スキーマを組み立てられます。  
- **ファクトテーブル** = 数値指標やイベントの実体(売上, アクション, インタラクション…)  
- **ディメンションテーブル** = 集計軸となるマスタ情報(ユーザー, 日付, 商品, 店舗, カテゴリ…)  
- スター・スキーマをまずは小規模データでローカルに作り、実際にSQLで集計してみるとDWHの仕組みがよく分かります。

これらのサンプル設計を出発点に、独自の項目やテーブルを足し引きしながら、ご自身のアプリや興味のあるドメインに合わせてカスタマイズしてみてください。

===================================================

以下では、**DuckDB** を使ったスター・スキーマ（Todoアプリ例）のチュートリアルを示します。  
**ポイント**: CSVファイルから読み込むのではなく、直接 SQL の INSERT 文で数件のデータを投入してみます。  

---

## 1. DuckDB の準備

DuckDB はシングルバイナリで動作し、以下のように簡単に試せます。

1. **インストール**  
   - Python環境下で `pip install duckdb` する  
   - または [DuckDB公式サイト](https://duckdb.org/)からバイナリを入手  
2. **DuckDBシェルを起動**  
   - ターミナル/コマンドラインで `duckdb` と入力してシェルを起動  
   - または Python上で `import duckdb` 後 `duckdb.connect('mydb.duckdb')` する  

以下のサンプルSQLは、DuckDBシェル上で動かす想定です（`mydb.duckdb` というファイルがあればそこに保存されます）。

---

## 2. スター・スキーマのテーブル作成

### 2.1 ディメンションテーブル

#### `dim_user` (ユーザー)

```sql
CREATE TABLE dim_user (
    user_key           INTEGER NOT NULL,  -- サロゲートキー
    user_id            VARCHAR,           -- 実アプリのユーザーID
    user_name          VARCHAR,
    email              VARCHAR,
    registration_date  DATE,
    PRIMARY KEY (user_key)
);
```

#### `dim_date` (日付)

```sql
CREATE TABLE dim_date (
    date_key       INTEGER NOT NULL,  -- YYYYMMDD の形を想定
    date_actual    DATE NOT NULL,
    year           INTEGER,
    month          INTEGER,
    day_of_week    INTEGER,           -- 0=日曜,1=月曜...など
    is_weekend     BOOLEAN,
    PRIMARY KEY (date_key)
);
```

#### `dim_task` (タスク)  
※ 必要に応じて作成します。小規模なら省略してもOK。

```sql
CREATE TABLE dim_task (
    task_key         INTEGER NOT NULL, -- サロゲートキー
    task_id          VARCHAR,          -- 実アプリのタスクID
    title            VARCHAR,
    category         VARCHAR,          -- "仕事", "プライベート" など
    due_date_key     INTEGER,          -- 締切日を日付ディメンションに紐づけたい場合
    PRIMARY KEY (task_key)
);
```

### 2.2 ファクトテーブル

#### `fact_task_actions` (タスクに対するアクション)

```sql
CREATE TABLE fact_task_actions (
    task_action_id  INTEGER NOT NULL,  -- サロゲートキー
    task_id         VARCHAR,           -- (または task_key をFKにする)
    user_key        INTEGER,           -- FK: dim_user
    date_key        INTEGER,           -- FK: dim_date (いつのアクションか)
    action_type     VARCHAR,           -- "create", "complete", "update", etc.
    elapsed_minutes INTEGER,           -- 完了までにかかった時間など
    priority_flag   BOOLEAN,           -- 優先タスクかどうか
    is_completed    BOOLEAN,           -- 0/1 または FALSE/TRUE
    -- メトリック例: アクション回数=1 とか、実際に集計したい値を列に追加しても良い
    PRIMARY KEY (task_action_id)
    -- FOREIGN KEY (user_key) REFERENCES dim_user(user_key),  (DuckDBでは外部キーはまだ実験的)
    -- FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);
```

---

## 3. サンプルデータの投入 (直接INSERT)

CSVからの読み込みではなく、単純化のため少量のデータを**INSERT文**で投入してみます。

### 3.1 ディメンションテーブルへのINSERT

#### `dim_user`
```sql
INSERT INTO dim_user 
    (user_key, user_id, user_name, email, registration_date)
VALUES
    (1, 'usr001', 'Alice', 'alice@example.com', '2023-01-10'),
    (2, 'usr002', 'Bob',   'bob@example.com',   '2023-02-15'),
    (3, 'usr003', 'Charlie', NULL,              '2023-03-20');
```

#### `dim_date`  
(※ `date_key` に YYYYMMDD 形式を想定)

```sql
INSERT INTO dim_date
    (date_key, date_actual, year, month, day_of_week, is_weekend)
VALUES
    (20230110, DATE '2023-01-10', 2023, 1, 2, FALSE),
    (20230111, DATE '2023-01-11', 2023, 1, 3, FALSE),
    (20230215, DATE '2023-02-15', 2023, 2, 3, FALSE),
    (20230320, DATE '2023-03-20', 2023, 3, 1, FALSE),
    (20230321, DATE '2023-03-21', 2023, 3, 2, FALSE); -- 例として
```

#### `dim_task`  
(※ タスクディメンションも一例として)

```sql
INSERT INTO dim_task
    (task_key, task_id, title, category, due_date_key)
VALUES
    (101, 'tsk001', 'Buy groceries',   'プライベート', 20230111),
    (102, 'tsk002', 'Prepare report',  '仕事',         20230215),
    (103, 'tsk003', 'Exercise',        'プライベート', 20230321);
```

### 3.2 ファクトテーブルへのINSERT

#### `fact_task_actions`  
(※ ファクトなので、タスクに対するアクションを1行ずつ記録)

```sql
-- 例: Alice が 2023/01/10 に tsk001を "作成" した
INSERT INTO fact_task_actions
    (task_action_id, task_id, user_key, date_key, action_type, elapsed_minutes, priority_flag, is_completed)
VALUES
    (1001, 'tsk001', 1, 20230110, 'create',  NULL, FALSE, FALSE),
    (1002, 'tsk001', 1, 20230111, 'complete', 60,   FALSE, TRUE),

    -- Bob が 2023/02/15 に tsk002 を作成
    (1003, 'tsk002', 2, 20230215, 'create',  NULL, TRUE,  FALSE),

    -- Charlie が 2023/03/20 に tsk003 を作成
    (1004, 'tsk003', 3, 20230320, 'create',  NULL, FALSE, FALSE),
    -- 2023/03/21 に完了
    (1005, 'tsk003', 3, 20230321, 'complete', 30,  FALSE, TRUE);
```

---

## 4. 分析クエリ例

### 4.1 ユーザー別のタスク完了数を集計

以下の例では、`fact_task_actions` から完了アクション (`action_type = 'complete'`) を抽出し、  
ユーザー名とその完了数を一覧にします。

```sql
SELECT u.user_name,
       COUNT(*) AS completed_task_count
FROM fact_task_actions f
JOIN dim_user u ON f.user_key = u.user_key
WHERE f.action_type = 'complete'
GROUP BY u.user_name;
```

#### 実行結果（例）
```
 user_name | completed_task_count 
-----------+----------------------
 Alice     | 1
 Charlie   | 1
(など)
```

### 4.2 期日(due_date_key)と完了日(date_key)を比較して遅延率を出す (例)

タスクディメンション(`dim_task`)で `due_date_key` を持ち、  
ファクト側で `complete` アクションの日付を取得して、差を比較します。

```sql
-- タスク完了アクションに限定し、期日(due_date)と完了日(complete_date)の差を出す
WITH completed_tasks AS (
  SELECT
    f.task_id,
    f.date_key AS complete_date_key,
    t.due_date_key
  FROM fact_task_actions f
  JOIN dim_task t 
    ON f.task_id = t.task_id
  WHERE f.action_type = 'complete'
)
SELECT
  c.task_id,
  c.due_date_key,
  c.complete_date_key,
  (c.complete_date_key - c.due_date_key) AS days_late
    -- (※ date_keyをYYYYMMDDとして引き算すると正しい“日数”にはならない場合もあるので注意。 
    --  正確に差を取りたければ dim_date やDATE型の列同士を引き算)
FROM completed_tasks c;
```

**Note**: `YYYYMMDD` 形式をそのまま差し引くと真の日数差にはならないので、  
`dim_date` の `date_actual` をJOINして `DATEDIFF` を使うなどが一般的です。DuckDBであれば:

```sql
SELECT
  DATEDIFF('day', d_due.date_actual, d_comp.date_actual) AS days_late
FROM completed_tasks c
JOIN dim_date d_due ON c.due_date_key = d_due.date_key
JOIN dim_date d_comp ON c.complete_date_key = d_comp.date_key;
```

---

## 5. まとめ & 追加ポイント

1. **ファクトテーブル `fact_task_actions`**  
   - タスクのアクションごとに行が増える  
   - 分析したいメトリック（完了時間, 優先フラグなど）を持たせておく  
2. **ディメンション `dim_user`, `dim_date`, `dim_task`**  
   - ユーザー、日付、タスクなどをキーでJOINし、集計用の属性データを補足  
3. **INSERTだけでなくCSVやParquetから読み込みも可能**  
   - ただし今回はINSERT例に限定  
4. **分析クエリ**  
   - JOINしてグループ化する、日付差を計算する、などがスター・スキーマの基本パターン

DuckDB は軽量かつ分析に強いDBエンジンなので、上記のようなスキーマ設計においても、  
サクッと試せるのがメリットです。必要な行数を増やしたり、パーティショニング的に外部ファイルを参照することも比較的容易にできるので、活用してみてください。