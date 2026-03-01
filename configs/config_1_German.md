# Config 1 Deutsch — Namenskonventionen & Programmierrichtlinien

Gilt fuer: **allen Databricks-Code, SQL, PySpark und Python-Notebooks** in diesem Projekt.

---

## 1. Zeichenkodierung

Alle Bezeichner (Tabellennamen, Spaltennamen, Schemanamen, Variablennamen, Notebooknamen)
duerfen nur ASCII-Zeichen `A-Z`, `a-z`, `0-9` und `_` enthalten.

Da deutsche Worte haeufig Umlaute enthalten, **muss** die folgende Ersetzungstabelle
konsequent angewendet werden — auch wenn der urspruengliche Begriff ein deutsches Wort ist.

### Umlaut- und Sonderzeichen-Ersetzungstabelle

| Zeichen | Ersetzen durch |
|---------|---------------|
| `ae`       | `ae`        |
| `oe`       | `oe`        |
| `ue`       | `ue`        |
| `Ae`       | `Ae`        |
| `Oe`       | `Oe`        |
| `Ue`       | `Ue`        |
| `ss`       | `ss`        |
| `e`, `e`, `e` | `e`    |
| `a`, `a`  | `a`         |
| `i`, `i`  | `i`         |
| `o`       | `o`         |
| `u`       | `u`         |
| `c`       | `c`         |
| `n`       | `n`         |
| Leerzeichen ` ` | `_`   |
| `-`       | `_`         |

**Beispiele:**
- `Ueberpruefung` → `ueberpruefung` (als Bezeichner)
- `Strasse` → `strasse`
- `Geschaeftsjahr` → `geschaeftsjahr`
- `dq_Ergebnisse` → `dq_ergebnisse`

---

## 2. Objektbenennung — Deutsch

Alle neu erstellten Objekte (Tabellen, Spalten, Variablen, Schemata, Funktionen, Notebooks)
werden mit **deutschen Begriffen** benannt. Technische Fachbegriffe ohne sinnvolle
deutsche Entsprechung bleiben auf Englisch (siehe Ausnahmen unten).

### 2a. Woerterbuch haeufiger Begriffe

| Englisch | Deutsch (Bezeichner) |
|----------|---------------------|
| `results` | `ergebnisse` |
| `rules` | `regeln` |
| `rule` | `regel` |
| `mappings` | `zuordnungen` |
| `mapping` | `zuordnung` |
| `generic` | `generisch` |
| `enriched` | `angereichert` |
| `clean` / `cleaned` | `bereinigt` |
| `raw` | `roh` |
| `target` | `ziel` |
| `source` | `quelle` |
| `table` | `tabelle` |
| `column` | `spalte` |
| `records` | `datensaetze` |
| `category` | `kategorie` |
| `relevant` | `relevant` |
| `total` | `gesamt` |
| `count` | `anzahl` |
| `number` | `anzahl` |
| `rate` / `pct` | `quote` |
| `pass` | `bestanden` |
| `fail` | `fehlgeschlagen` |
| `load` / `ingest` | `laden` |
| `transform` | `transformieren` |
| `output` | `ausgabe` |
| `input` | `eingabe` |
| `apply` | `anwenden` |
| `filter` | `filter` |
| `job` | `auftrag` |
| `run` | `ausfuehren` |
| `status` | `status` |
| `timestamp` | `zeitstempel` |
| `date` | `datum` |
| `name` | `name` |
| `type` | `typ` |
| `value` | `wert` |
| `key` | `schluessel` |
| `id` | `id` *(bleibt englisch)* |
| `no` (number) | `nr` |

### 2b. Technische Begriffe — bleiben auf Englisch

Diese Begriffe haben keine sinnvolle deutsche Entsprechung im Datenbankkontext
und werden unveraendert verwendet:

`catalog`, `schema`, `delta`, `bronze`, `silver`, `gold`, `widget`,
`notebook`, `cluster`, `pipeline`, `bundle`, `spark`, `mlflow`,
`unity`, `df` *(DataFrame-Suffix)*, `dq` *(Data-Quality-Praefix)*

### 2c. Benennungsbeispiele

| Englisch | Deutsch |
|----------|---------|
| `dq_results` | `dq_ergebnisse` |
| `dq_rule_mappings` | `dq_regelzuordnungen` |
| `dq_rules_generic` | `dq_regeln_generisch` |
| `bronze_taxi_raw` | `bronze_taxi_roh` |
| `silver_taxi_clean` | `silver_taxi_bereinigt` |
| `enriched_df` | `angereichert_df` |
| `target_table` | `ziel_tabelle` |
| `source_table` | `quell_tabelle` |
| `rule_no` | `regel_nr` |
| `pass_pct` | `bestanden_quote` |
| `total_rules` | `regeln_gesamt` |
| `relevant_records` | `relevante_datensaetze` |
| `dq_category` | `dq_kategorie` |
| `apply_generic_rule()` | `generische_regel_anwenden()` |

---

## 3. SQL-Konventionen

### 3a. Gross-/Kleinschreibung

| Element | Schreibweise | Beispiel |
|---------|-------------|---------|
| Schluesselwoerter | `GROSS` | `SELECT`, `FROM`, `WHERE`, `JOIN`, `ON`, `GROUP BY`, `ORDER BY`, `HAVING`, `WITH`, `AS`, `CASE`, `WHEN`, `THEN`, `ELSE`, `END`, `AND`, `OR`, `NOT`, `IN`, `LIKE`, `IS`, `NULL`, `BETWEEN`, `EXISTS`, `UNION`, `ALL`, `DISTINCT`, `LIMIT`, `OFFSET`, `LEFT`, `RIGHT`, `INNER`, `OUTER`, `FULL`, `CROSS`, `INSERT`, `INTO`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `TABLE`, `VIEW`, `SET`, `VALUES` |
| Aggregat- & Skalarfunktionen | `GROSS` | `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`, `COALESCE()`, `NULLIF()`, `CAST()`, `ROUND()`, `TRIM()`, `UPPER()`, `LOWER()`, `LENGTH()`, `SUBSTR()`, `CONCAT()`, `DATE_ADD()`, `DATE_SUB()`, `CURRENT_DATE()`, `CURRENT_TIMESTAMP()`, `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST()`, `LAST()`, `NVL()`, `IFF()`, `IFNULL()` |
| Fensterfunktions-Schluesselwoerter | `GROSS` | `OVER`, `PARTITION BY`, `ORDER BY`, `ROWS`, `RANGE`, `UNBOUNDED`, `PRECEDING`, `FOLLOWING`, `CURRENT ROW` |
| Tabellennamen | `klein` | `dq_ergebnisse`, `dq_regelzuordnungen` |
| Spalten- / Feldnamen | `klein` | `regel_nr`, `quell_tabelle`, `dq_kategorie` |
| Schemanamen | `klein` | `dbdemos_dq_professional` |
| Katalognamen | `klein` | `dbdemos` |
| Aliase | `klein` | `e`, `z`, `r`, `anzahl_fahrten` |
| Zeichenkettenliterale | unveraendert | `'GENAUIGKEIT'`, `'BESTANDEN'` |

### 3b. Formatierungsregeln

- Jede Hauptklausel (`SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`) beginnt auf einer neuen Zeile.
- Einrueckung von Fortsetzungszeilen: 4 Leerzeichen.
- Kommas stehen am **Anfang** der Fortsetzungszeilen in SELECT-Listen (fuehrendes Komma).
- `ON`-Bedingungen werden am `JOIN`-Schluesselwort ausgerichtet.
- Eine Leerzeile zwischen separaten SQL-Anweisungen.

**Korrektes Beispiel:**
```sql
SELECT
    e.dq_regel_nr
    , COALESCE(z.dq_kategorie, r.dq_kategorie)       AS dq_kategorie
    , COUNT(DISTINCT e.dq_regel_nr)                   AS regeln_gesamt
    , SUM(e.dq_anzahl_relevante_datensaetze)          AS relevante_datensaetze
    , ROUND(
        SUM(e.dq_anzahl_pos_ergebnisse)
        / NULLIF(SUM(e.dq_anzahl_relevante_datensaetze), 0) * 100
      , 2)                                             AS bestanden_quote
FROM dbdemos.dbdemos_dq_professional.dq_ergebnisse e
JOIN dbdemos.dbdemos_dq_professional.dq_regelzuordnungen z
    ON e.dq_regel_nr = z.regel_nr
JOIN dbdemos.dbdemos_dq_professional.dq_regeln_generisch r
    ON z.generische_regel_id = r.generische_regel_id
WHERE e.dq_anzahl_relevante_datensaetze > 0
GROUP BY
    COALESCE(z.dq_kategorie, r.dq_kategorie)
ORDER BY dq_kategorie
```

---

## 4. PySpark-Konventionen

| Element | Konvention | Beispiel |
|---------|-----------|---------|
| Variablennamen | `snake_case`, deutsch | `angereichert_df`, `regel_nr`, `ziel_tabelle` |
| DataFrame-Variablen | `snake_case` + `_df`-Suffix | `dq_df`, `bereinigt_df`, `angereichert_df` |
| Konstanten / Konfigurationswerte | `snake_case` (kein ALL_CAPS) | `catalog`, `dq_schema` |
| Funktionsnamen | `snake_case`, deutsch | `generische_regel_anwenden()` |
| Spaltenname-Zeichenketten | `klein` | `F.col("regel_nr")`, `F.col("dq_kategorie")` |
| Spark-SQL-f-Strings | SQL-Konventionen von §3 befolgen | `spark.sql(f"SELECT ... FROM {catalog}.{dq_schema}.dq_ergebnisse e")` |

---

## 5. Python / Allgemeine Konventionen

| Element | Konvention |
|---------|-----------|
| Variablennamen | `snake_case`, deutsch |
| Datei- / Notebooknamen | `snake_case`, mit Sequenznummer-Praefix, z.B. `01_daten_laden.py` |
| Keine Sonderzeichen | Ersetzungstabelle aus §1 anwenden |
| Widget-Parameternamen | `snake_case` kleingeschrieben, z.B. `ziel_tabelle`, `dq_schema` |
| Ausgabebeschriftungen (print) | Satzschreibweise, **Deutsch** |

---

## 6. Code-Kommentare

Jeder logische Code-Block erhaelt eine einzelne `#`-Kommentarzeile direkt darueber,
die beschreibt, was dieser Block tut.

### Regeln

- **Block = eine Gruppe zusammengehoeriger Zeilen, die einen Schritt erledigen** (z.B. Daten laden, Datensaetze filtern, Tabelle schreiben, Schema definieren).
- Nicht jede einzelne Zeile kommentieren — den Zweck des Blocks beschreiben, nicht jede Operation.
- Kommentare muessen auf **Deutsch** sein, Satzschreibweise, praegnant (maximal eine Zeile pro Block).
- SQL-Zellen in Notebooks: `-- Kommentar`-Zeile oberhalb der ersten Anweisung jedes logischen Blocks.

### Beispiel

```python
# Widget-Parameter mit Fallback-Standardwerten laden
try:
    catalog = dbutils.widgets.get("catalog")
except:
    catalog = "dev_catalog"

# Rohdaten der Taxifahrten aus der Bronze-Schicht einlesen
bronze_df = spark.table(f"{catalog}.{schema}.bronze_taxi_roh")

# Datensaetze ohne Abholort oder Zielort herausfiltern
bereinigt_df = bronze_df.filter(
    F.col("abholort_id").isNotNull()
    & F.col("zielort_id").isNotNull()
)

# Bereinigte Daten in die Silber-Schicht schreiben
bereinigt_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_taxi_bereinigt")
```

```sql
-- Bestandene DQ-Quote je Kategorie berechnen
SELECT
    dq_kategorie
    , ROUND(
        SUM(dq_anzahl_pos_ergebnisse)
        / NULLIF(SUM(dq_anzahl_relevante_datensaetze), 0) * 100
      , 2) AS bestanden_quote
FROM dbdemos.dbdemos_dq_professional.dq_ergebnisse
WHERE dq_anzahl_relevante_datensaetze > 0
GROUP BY dq_kategorie
ORDER BY dq_kategorie
```
