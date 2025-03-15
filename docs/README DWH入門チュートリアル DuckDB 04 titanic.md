以下のチュートリアルでは、**Kaggleの Titanic: Machine Learning from Disaster** データセット（有名な「タイタニック号の乗客データ」）を題材に、**DuckDB** で **スター・スキーマ** を体験するサンプルを作成します。  

実際の Titanic データセットは単一の表に様々な列（PassengerId、Name、Age、Sex、Fare、Survived など）が格納されていますが、ここではあえて**DWH的なスター・スキーマ**に“分割”してみることで、基本的な構造設計や JOIN 集計の流れを学びます。

---

# DuckDB × Titanic データでスター・スキーマを試す

## 1. 想定するスター・スキーマ

Titanicデータは本来、「乗客ごとの情報」が1行にまとまっています。  
今回はそれを**以下のように分割**して、DWHらしい **1つのファクトテーブル** と **複数のディメンションテーブル** を作ってみます。

### 1.1 ディメンションテーブル

1. **`dim_passenger`**  
   - 乗客の基本情報（名前、性別、年齢層など）をまとめる  
   - `passenger_key` （サロゲートキー）を主キーとし、外部キーでファクトから参照される  
2. **`dim_ticket_class`**  
   - チケットクラスや運賃など、チケット情報をまとめる  
   - Pclass（1〜3）を中心に、Fare の料金帯など追加で持ってもいいでしょう  
3. **`dim_port`**  
   - Titanicの乗船港や下船港などをテーブル化（Embarkedの “C”, “Q”, “S”）  
   - ここでは簡易的に、PortKey=1,2,3 として “C=Cherbourg”, “Q=Queenstown”, “S=Southampton” など。

### 1.2 ファクトテーブル

1. **`fact_sailing`**  
   - 「Titanic 乗船記録」として、  
     - 各乗客ごとに、いつ・どこから乗ったか、チケットクラスは何か、**最終的に生存したか(Survived)** などを保持  
   - 主キーとして `sailing_id` を採用 (1行1乗客とする)  
   - 外部キーとして `passenger_key` , `ticket_class_key` , `embarked_port_key` を持つ  

### 参考イメージ図

```
        dim_passenger                dim_ticket_class          dim_port
       (passenger_key)              (ticket_class_key)       (port_key)
        /          | \                   |           \           |
       /           |  \                  |            \          |
       ------------+---------------------+-------------+----------
                          fact_sailing
                       (sailing_id, passenger_key, ticket_class_key, embarked_port_key, survived, ...)
```

> **注意**: 元データでは多くの列が乗客ごとに1行でまとまっています。本来は機械学習用の“フラットテーブル”ですが、スター・スキーマを体験するために強引に分割している点にご留意ください。

---

## 2. DuckDB セットアップ

```python
# %% [code]
!pip install duckdb --quiet
```

---

## 3. Titanic のサンプルデータを用意（pandas）

本来であれば、Kaggle の [Titanic - Machine Learning from Disaster](https://www.kaggle.com/competitions/titanic) から CSV (`train.csv`, `test.csv`) をダウンロードし、  
前処理（欠損値処理・型変換等）してテーブル設計に合わせて抽出・変換する流れになります。  

ここではサンプル用に **超簡略版** の Titanic データ（10行だけ）を手動で定義します。

```python
# %% [code]
import pandas as pd

# -----------------------------------------------------------
# 元の Titanic データに相当するフラットテーブル (簡略版)
# -----------------------------------------------------------
df_titanic_raw = pd.DataFrame({
    'PassengerId': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'Survived': [0, 1, 1, 1, 0, 0, 0, 1, 1, 0],
    'Pclass': [3, 1, 3, 1, 3, 3, 1, 3, 3, 2],
    'Name': [
        'Mr. Owen Harris', 
        'Mrs. John Bradley (Florence Briggs Thayer)', 
        'Miss. Laina', 
        'Mrs. Jacques Heath (Lily May Peel)', 
        'Mr. William Henry', 
        'Mr. James', 
        'Major. Arthur Godfrey', 
        'Miss. Elisabeth Walton', 
        'Mr. Juhan', 
        'Mrs. Fatima'
    ],
    'Sex': ['male','female','female','female','male','male','male','female','male','female'],
    'Age': [22, 38, 26, 35, 35, 27, 45, 29, 25, 30],
    'Fare': [7.25, 71.2833, 7.9250, 53.1000, 8.0500, 8.4583, 27.7208, 10.5167, 7.8958, 16.7],
    'Embarked': ['S','C','S','S','S','Q','S','S','S','S']
})

df_titanic_raw
```
<details>
<summary>表示イメージ</summary>

```
   PassengerId  Survived  Pclass                                              Name     Sex  Age     Fare Embarked
0            1         0       3                                Mr. Owen Harris    male   22   7.2500        S
1            2         1       1  Mrs. John Bradley (Florence Briggs Thayer)    female   38  71.2833        C
2            3         1       3                                   Miss. Laina   female   26   7.9250        S
3            4         1       1       Mrs. Jacques Heath (Lily May Peel)    female   35  53.1000        S
4            5         0       3                             Mr. William Henry    male   35   8.0500        S
5            6         0       3                                     Mr. James    male   27   8.4583        Q
6            7         0       1                       Major. Arthur Godfrey    male   45  27.7208        S
7            8         1       3                       Miss. Elisabeth Walton  female   29  10.5167        S
8            9         1       3                                    Mr. Juhan    male   25   7.8958        S
9           10         0       2                                   Mrs. Fatima  female   30  16.7000        S
```
</details>

---

## 4. スター・スキーマへの切り分け (サンプル)

### 4.1 ディメンションテーブル用の DataFrame

#### (A) dim_passenger

- 乗客のキー (passenger_key)  
- 名称や性別、年齢層などを保持（ここでは厳密に年齢層を区分せず、Ageを直接入れてみます）  

```python
# %% [code]
df_passenger = pd.DataFrame({
    'passenger_key': df_titanic_raw['PassengerId'],
    'name': df_titanic_raw['Name'],
    'sex': df_titanic_raw['Sex'],
    'age': df_titanic_raw['Age']
})

df_passenger.head()
```

---

#### (B) dim_ticket_class

- Pclass（1,2,3）を主キーにするか、独自の `ticket_class_key` を採用するかはお好みです。  
- ここでは `ticket_class_key` をサロゲートキー、`pclass` を元データとして保持してみます。  
- `fare_range`（料金帯）などをマスタ化しても面白いですが、今回はシンプルにします。

```python
# %% [code]
# Pclass は1,2,3しかないので、サロゲートキーを付けなくてもOKですが、
# サンプルとして "dim_ticket_class" テーブルらしく作成。
unique_pclass = df_titanic_raw['Pclass'].unique()

df_ticket_class = pd.DataFrame({
    'ticket_class_key': unique_pclass,  # 1,2,3
    'pclass': unique_pclass
    # 必要に応じて FareRange など追加してもよい
})

df_ticket_class
```
<details>
<summary>表示イメージ</summary>

```
   ticket_class_key  pclass
0                 3       3
1                 1       1
2                 2       2
```
</details>

> この例だと `ticket_class_key == pclass` で同じ値です。もしさらにマスタ属性を追加したい場合、例えば:
> - `class_name` (e.g. "First Class", "Second Class", "Third Class")
> - `ticket_benefit_level` (1=上級, 2=中級, 3=一般)  
> …などを持たせることができます。

---

#### (C) dim_port

- 乗船港（Embarked）に対するマスタ。  
- “C= Cherbourg”, “Q= Queenstown”, “S= Southampton” と定義できます。  
- ここでは “Embarked” の3種類を `port_key=1,2,3` に割り当てる簡単な例にします。

```python
# %% [code]
unique_ports = df_titanic_raw['Embarked'].unique()

port_mapping = {'S': 1, 'C': 2, 'Q': 3}  # 適当な割り当て

df_port = pd.DataFrame({
    'port_key': [port_mapping[p] for p in unique_ports],
    'port_code': unique_ports,
    'port_name': ['Southampton','Cherbourg','Queenstown']  # 順番は注意
})

df_port
```
<details>
<summary>表示イメージ</summary>

```
   port_key port_code     port_name
0         1         S  Southampton
1         2         C    Cherbourg
2         3         Q  Queenstown
```
</details>

> 本来は “S=1, C=2, Q=3” のようなマッピングをする際、列順などに注意が必要です。  
> ここでは簡易的に `unique_ports` の順序に合わせる形にしています。

---

### 4.2 ファクトテーブル: fact_sailing

- 1行=1乗客の乗船事実とし、`sailing_id` を主キーにする  
- 外部キーとして
  - `passenger_key` (＝Titanic元の PassengerId)  
  - `ticket_class_key` (＝Pclassをマッピング)  
  - `embarked_port_key` (＝Embarkedをマッピング)
- “Survived” (生存フラグ) をファクト側に持たせる  
- “Fare” や “SibSp”, “Parch” など、分析したいメトリック/ディメンション的な列をどこに置くかは検討次第ですが、  
  ここでは “Fare” をファクトに残して、料金を集計できるようにします。

```python
# %% [code]
# passenger_key は元の df_titanic_raw['PassengerId'] と一致させる。
# ticket_class_key は pclass をそのまま当てる (1,2,3)
# embarked_port_key は port_mappingを使って数値化

df_fact_sailing = pd.DataFrame({
    'sailing_id': df_titanic_raw['PassengerId'],  # シンプルに同じIDで
    'passenger_key': df_titanic_raw['PassengerId'],
    'ticket_class_key': df_titanic_raw['Pclass'],
    'embarked_port_key': df_titanic_raw['Embarked'].map(port_mapping),
    'fare': df_titanic_raw['Fare'],
    'survived': df_titanic_raw['Survived']
})

df_fact_sailing.head()
```
<details>
<summary>表示イメージ</summary>

```
   sailing_id  passenger_key  ticket_class_key  embarked_port_key    fare  survived
0           1              1                 3                  1   7.2500         0
1           2              2                 1                  2  71.2833         1
2           3              3                 3                  1   7.9250         1
3           4              4                 1                  1  53.1000         1
4           5              5                 3                  1   8.0500         0
```
</details>

---

## 5. DuckDB にテーブルを作成 & データ登録

```python
# %% [code]
import duckdb

# メモリ上にDB作成
con = duckdb.connect(database=':memory:')

# -- dim_passenger
con.execute("""
CREATE TABLE dim_passenger (
  passenger_key INT,
  name VARCHAR,
  sex VARCHAR,
  age INT
)
""")

# -- dim_ticket_class
con.execute("""
CREATE TABLE dim_ticket_class (
  ticket_class_key INT,
  pclass INT
)
""")

# -- dim_port
con.execute("""
CREATE TABLE dim_port (
  port_key INT,
  port_code VARCHAR,
  port_name VARCHAR
)
""")

# -- fact_sailing
con.execute("""
CREATE TABLE fact_sailing (
  sailing_id INT,
  passenger_key INT,
  ticket_class_key INT,
  embarked_port_key INT,
  fare DOUBLE,
  survived INT
)
""")

# pandas DataFrame を一時登録
con.register("temp_passenger", df_passenger)
con.register("temp_ticket_class", df_ticket_class)
con.register("temp_port", df_port)
con.register("temp_sailing", df_fact_sailing)

# INSERT
con.execute("INSERT INTO dim_passenger SELECT * FROM temp_passenger")
con.execute("INSERT INTO dim_ticket_class SELECT * FROM temp_ticket_class")
con.execute("INSERT INTO dim_port SELECT * FROM temp_port")
con.execute("INSERT INTO fact_sailing SELECT * FROM temp_sailing")

# 確認
res_passenger = con.execute("SELECT * FROM dim_passenger").df()
res_class = con.execute("SELECT * FROM dim_ticket_class").df()
res_port = con.execute("SELECT * FROM dim_port").df()
res_sailing = con.execute("SELECT * FROM fact_sailing").df()

print("dim_passenger:")
display(res_passenger)
print("dim_ticket_class:")
display(res_class)
print("dim_port:")
display(res_port)
print("fact_sailing:")
display(res_sailing)
```

---

## 6. スター・スキーマでの集計例

### 6.1 チケットクラス (ticket_class_key) 別の生存率

```python
# %% [code]
query_class_survival = """
SELECT
  c.pclass,
  COUNT(*) AS total_passengers,
  SUM(CASE WHEN f.survived=1 THEN 1 ELSE 0 END) AS survived_count,
  ROUND(AVG(CAST(f.survived AS DOUBLE))*100, 2) AS survival_rate_percent
FROM fact_sailing f
JOIN dim_ticket_class c
  ON f.ticket_class_key = c.ticket_class_key
GROUP BY c.pclass
ORDER BY c.pclass
"""

df_class_survival = con.execute(query_class_survival).df()
df_class_survival
```
<details>
<summary>想定出力例</summary>

```
   pclass  total_passengers  survived_count  survival_rate_percent
0       1                 2               2                  100.0
1       2                 1               0                    0.0
2       3                 7               3                   42.86
```
</details>

ここでは**ファクトテーブル (fact_sailing)** を中心に、**ディメンション (dim_ticket_class)** をJOINしてクラス別にグルーピング。  
「合計乗客数」と「生存者数」「生存率」を求めています。

---

### 6.2 乗船港 (port_name) 別の平均運賃 (Fare)

```python
# %% [code]
query_port_fare = """
SELECT
  p.port_name,
  ROUND(AVG(f.fare), 2) AS avg_fare
FROM fact_sailing f
JOIN dim_port p
  ON f.embarked_port_key = p.port_key
GROUP BY p.port_name
ORDER BY avg_fare DESC
"""

df_port_fare = con.execute(query_port_fare).df()
df_port_fare
```
<details>
<summary>想定出力例</summary>

```
     port_name  avg_fare
0    Cherbourg    71.28
1  Queenstown     8.46
2  Southampton   14.56
```
</details>

このように「Embarked列」をディメンション化 (`dim_port`) しておくことで、可読性の高い`port_name` で集計・表示ができます。

---

### 6.3 性別 × チケットクラス × 生存率をクロス集計

最後に、乗客ディメンション(`dim_passenger`) とチケットクラスディメンション(`dim_ticket_class`) をJOINして、**性別×Pclass別に生存率**を求める例です。

```python
# %% [code]
query_sex_class_survival = """
SELECT
  p.sex,
  c.pclass,
  COUNT(*) AS total_passengers,
  SUM(CASE WHEN f.survived=1 THEN 1 ELSE 0 END) AS survived_count,
  ROUND(
    AVG(CAST(f.survived AS DOUBLE))*100,
    2
  ) AS survival_rate_percent
FROM fact_sailing f
JOIN dim_passenger p
  ON f.passenger_key = p.passenger_key
JOIN dim_ticket_class c
  ON f.ticket_class_key = c.ticket_class_key
GROUP BY p.sex, c.pclass
ORDER BY p.sex, c.pclass
"""

df_sex_class_survival = con.execute(query_sex_class_survival).df()
df_sex_class_survival
```
<details>
<summary>想定出力例</summary>

```
      sex  pclass  total_passengers  survived_count  survival_rate_percent
0  female       1                 2               2                100.00
1  female       3                 3               2                 66.67
2    male       2                 1               0                  0.00
3    male       3                 4               1                 25.00
```
</details>

**スター・スキーマ** の利点として、ディメンションを JOIN して **多次元の切り口** で集計・分析することが容易になる点が挙げられます。

---

## 7. まとめ

1. **Titanicデータをスター・スキーマに分解**  
   - `dim_passenger`, `dim_ticket_class`, `dim_port` の3つのディメンション  
   - `fact_sailing` (乗船事実) のファクト  
2. **DuckDBでテーブル作成 → INSERT → JOIN集計**  
   - シンプルなSQLで可視化しやすい形にデータを整理できる  
3. **応用**  
   - `SibSp` や `Parch`（兄弟/両親同行数）などを別ディメンションにするか、ファクトに残すか、自由に拡張可能  
   - `Age` を年齢層としてディメンション化し、さらに面白い分析をしてみる  
   - kaggle の `train.csv`, `test.csv` を実際に加工し、本チュートリアルを大規模化  

> スター・スキーマ自体はBI/DWHの典型パターンで、機械学習の前処理としては必須ではありません。  
> ですが **「多次元分析 + JOIN 集計」** の利便性を体験するうえで、Titanicのようなよく知られたデータをスター・スキーマ化してみるのは面白いアプローチです。

---

### おわりに

- **DuckDB** はローカル環境で手軽に**カラムナ型DB**の恩恵を受けられ、スター・スキーマの学習にも最適です。  
- **Titanic** データは本来フラットテーブルで完結しますが、DWHの基本構造を体験するサンプルとして**あえて複数テーブルに分解**してみると、データ設計の考え方が掴みやすくなります。  

ぜひ実際の **Kaggle Titanic** データ（891行の `train.csv`）を読み込み、同様に分解してスター・スキーマ分析を体験してみてください。さらに列を増やして細かなディメンション分析を行うと、DWH的な視点でのデータ活用のイメージがつかめるはずです。