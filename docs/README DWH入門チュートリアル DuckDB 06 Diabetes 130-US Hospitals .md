以下では、**Kaggleの “Diabetes 130-US hospitals for years 1999-2008”** データセットを題材に、  
**DuckDB** を使って **スター・スキーマ (DWH設計)** を簡単に体験するためのチュートリアルを示します。  

> **注意**: 実データの全列をそのままDWHに落とし込むと非常に多くのカラムやIDが絡んで複雑になります。本チュートリアルでは、代表的な列を抜粋・再構成し、**スター・スキーマを組むポイント**を解説するための**MVP (最小実装)**を紹介します。

---

# 1. データセット概要 (Diabetes 130-US Hospitals)

Kaggle上の [**Diabetes 130-US hospitals**](https://www.kaggle.com/datasets/brandao/diabetes) には、1999-2008年にわたる130の米国病院での糖尿病患者入院情報が含まれます。  
各行は**1回の入院(Encounters)** を表していて、以下のような特徴量が多数あります。 (列名は例)

- `encounter_id` … 入院エピソードを一意に表すID  
- `patient_nbr` … 患者ID (複数回入院した場合は同じ患者番号)  
- `race`, `gender`, `age` … 患者の属性  
- `admission_type_id`, `discharge_disposition_id`, `admission_source_id` … 入院の種別、退院方法、入院元など  
- `time_in_hospital` … 入院日数  
- `num_lab_procedures` … ラボ検査数  
- `diag_1`, `diag_2`, `diag_3` … 診断コード (ICD-9?)  
- 薬の使用状況 (metformin, insulinなど多数)  
- `readmitted` … 30日以内の再入院かどうか  

多くの列は患者の状態や治療内容を示すカテゴリ変数です。  
DWHを作るには、これらを**どこまでディメンション化するか**が鍵となります。

---

# 2. スター・スキーマ構築の方針

本データでは **1行=1入院(Encounter)** という構造自体が **ファクト(事実)** に近いです。  
そのため、**ファクトテーブル**を「入院エピソード (fact_encounters)」とみなし、そこに次の情報を保持すると考えます:

- **事実(メジャー)**: `time_in_hospital`, `num_lab_procedures`, `num_medications`, etc. (数量など分析対象)  
- **ディメンションの外部キー**: 患者ID, 入院日/年(があれば), etc.

さらに、以下のように主な列をディメンション化していきます。

## 2.1 ディメンション例

1. **`dim_patient`**  
   - `patient_key` (サロゲートキー)  
   - `race`, `gender`, `age` など患者属性  
   - (必要なら、複数回入院しても同じpatient_keyでJOINできる)

2. **`dim_date`** (オプション)  
   - もし入院日や退院日が具体的にあれば、`date_key` を使って「いつ入院したか」を解析可能  
   - データセット上は日時情報がなく、`time_in_hospital` (日数) があるだけ。  
   - ここでは**省略**か、`dim_admission_type` / `dim_discharge_disposition` などを追加する方法も。

3. **(オプション) `dim_diagnosis`**  
   - ICDコード (diag_1, diag_2, diag_3) をディメンションにまとめる？  
   - 1エピソードに診断が複数あるため、多対多(ブリッジ)が生じることも…

4. **(オプション) `dim_medication`**  
   - 各薬(metformin, insulin etc.)に対するUp/Down/Steady/None などが多いため、  
     これをどうディメンション化するかは分析目的次第です。

今回は最小化のため、**`dim_patient`** のみ作り、**その他の列はファクト側に配置** する例を示します。

## 2.2 ファクトテーブル: `fact_encounters`

- 粒度: **1行=1回の入院**  
- 主キー: `encounter_key` (または元の `encounter_id`)  
- 外部キー: `patient_key`, (date_keyがあれば`date_key`)  
- メジャー(分析対象): `time_in_hospital`, `num_lab_procedures`, `num_medications`, etc.  
- カテゴリ: `admission_type_id`, `readmitted`, etc.  
  - これらをさらにディメンション化してもOK (スノーフレーク化)

---

# 3. サンプル: スター・スキーマをDuckDBで試す

以下はダミー化した少量のデータで、スター・スキーマを作る流れをデモします。  
実際には Kaggleから `diabetic_data.csv` をダウンロード後、必要列を抽出・変換してください。

## 3.1 ダミーデータ作成 (pandas)

```python
!pip install duckdb --quiet

import pandas as pd

# -----------------------------
# ディメンション: dim_patient
# patient_nbr ごとに race, gender, age をまとめる例
# -----------------------------
df_patient = pd.DataFrame({
    'patient_key': [100, 101, 102],
    'patient_nbr': [11111, 22222, 33333],  # datasetのpatient_nbr
    'race': ['Caucasian', 'AfricanAmerican', 'Hispanic'],
    'gender': ['Female', 'Male', 'Female'],
    'age': ['70-80', '50-60', '30-40']
})

# -----------------------------
# ファクト: fact_encounters
# 1行=1入院(Encounter)
# -----------------------------
df_encounters = pd.DataFrame({
    'encounter_key': [5001, 5002, 5003],
    'encounter_id': [12345, 12346, 12347],  # from dataset
    'patient_key': [100, 101, 101],         # user 101 has multiple admissions
    'admission_type_id': [1, 3, 1],
    'discharge_disposition_id': [1, 11, 1],
    'time_in_hospital': [3, 5, 1],
    'num_medications': [13, 8, 5],
    'readmitted': ['NO', '<30', 'NO']
})
```

> ※ 本来は `encounter_id` がユニークなのでそれをファクトのPKにしてもOKですが、  
>   DWHでは**サロゲートキー** (`encounter_key`) を独自に振る選択肢もあります。

---

## 3.2 DuckDBでテーブル作成 & INSERT

```python
import duckdb

con = duckdb.connect(database=':memory:')

# ディメンション: dim_patient
con.execute("""
CREATE TABLE dim_patient (
  patient_key INT,
  patient_nbr INT,
  race VARCHAR,
  gender VARCHAR,
  age VARCHAR
);
""")

# ファクト: fact_encounters
con.execute("""
CREATE TABLE fact_encounters (
  encounter_key INT,
  encounter_id INT,
  patient_key INT,
  admission_type_id INT,
  discharge_disposition_id INT,
  time_in_hospital INT,
  num_medications INT,
  readmitted VARCHAR
);
""")

con.register("temp_patient", df_patient)
con.register("temp_encounters", df_encounters)

con.execute("INSERT INTO dim_patient SELECT * FROM temp_patient")
con.execute("INSERT INTO fact_encounters SELECT * FROM temp_encounters")

# 確認
res_pat = con.execute("SELECT * FROM dim_patient").df()
res_enc = con.execute("SELECT * FROM fact_encounters").df()

display(res_pat)
display(res_enc)
```

---

## 3.3 分析クエリ例

### 3.3.1 人種 (race) 別の平均入院日数

```python
query1 = """
SELECT
  p.race,
  ROUND(AVG(e.time_in_hospital),2) AS avg_days
FROM fact_encounters e
JOIN dim_patient p
  ON e.patient_key = p.patient_key
GROUP BY p.race
ORDER BY avg_days DESC
"""

df_q1 = con.execute(query1).df()
df_q1
```

<details>
<summary>想定出力</summary>

```
            race  avg_days
0  AfricanAmerican      5.00
1       Caucasian       3.00
2        Hispanic       1.00
```
</details>


### 3.3.2 再入院 (readmitted) の割合を男女別に見る

```python
query2 = """
SELECT
  p.gender,
  e.readmitted,
  COUNT(*) AS encounter_count
FROM fact_encounters e
JOIN dim_patient p
  ON e.patient_key = p.patient_key
GROUP BY p.gender, e.readmitted
ORDER BY p.gender, e.readmitted
"""

df_q2 = con.execute(query2).df()
df_q2
```

<details>
<summary>想定出力</summary>

```
  gender  readmitted  encounter_count
0 Female NO                      2
1 Male   <30                     1
```
</details>

※ ここではダミーデータが少ないので男女の差を語るのは難しいですが、本物のデータなら興味深い分析が可能です。

---

## 4. 追加の発展

1. **ディメンションを増やす**  
   - `dim_admission_type` (入院タイプID→名称)  
   - `dim_discharge_disposition` (退院方法ID→名称)  
   - `dim_medication` (薬の種類やUP/Down/Steadyなど) → 多対多になる可能性も  
2. **SCD (緩やかに変化するディメンション)**  
   - もし患者の人種・年齢帯が途中で変わる？ 実際には変わりにくいですが、シミュレーションとしてType2を学べる  
3. **診断コード (diag_1, diag_2, diag_3) をどう扱うか**  
   - 3カラムともにICD-9コードが入るため、多対多的に扱う or diag_1のみ主要診断とみなす など、モデル化の選択肢  
4. **日付要素が無い/少ない問題**  
   - データ自体には具体的日付がないので、`dim_date` を活用しにくい  
   - もし別途「入院年度」「季節」などを使って集計したいなら、`admission_year` カラムをなんらかの推定で付与してDWHに取り込むことも

---

## 5. まとめ

- **Diabetes 130-US Hospitals** のように、「入院エピソード」を**ファクト**として捉え、**患者**などを**ディメンション**化する、というDWH的設計が可能。  
- **fact_encounters** は1行=1入院で、メジャー(入院日数・薬数など)やカテゴリ(入院タイプ, readmittedフラグ)を持たせる。  
- **dim_patient** や **dim_admission_type** などをJOINすれば、患者属性別の集計・入院傾向の分析がSQLで簡単にできる。  
- さらに詳細な薬情報や診断コードなどを**どうディメンション化するか**は分析要件次第。多対多ならスノーフレークスキーマも検討する。

実際の `diabetic_data.csv` は約10万行あり、多数の列も含まれますが、  
スター・スキーマ的に**設計を分けて取り込む**と、BIツールやSQLでの多角的な分析や可視化がしやすくなります。  
ぜひ本チュートリアルを足がかりに、**自前のDWH** を構築・拡張して糖尿病患者の入院傾向を深堀りしてみてください。  