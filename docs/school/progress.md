# Edenemisraport

> **Juhend:** See fail on projektitöö teise nädala väljund. Uuenda lühidalt iga esitamise eel. Kustuta see juhendrida.

## Mis on valmis

- [x] Docker Compose käivitab kõik teenused
- [x] Andmeid saadakse allikast kätte
- [x] Andmed laetakse `bronze` kihti
- [x] Vähemalt üks transformatsioon toimib
- [x] Vähemalt üks näidikulaud on nähtaval
- [x] Vähemalt üks andmekvaliteedi test läbib

Kõik teenused töötavad Docker Compose'i kaudu — Airflow, dbt, PostgreSQL, Superset, DataHub ja FastAPI. Igapäevane pipeline tõmbab automaatselt hüdroloogilised ja meteoroloogilised andmed Keskkonnaagentuuri avalikust API-st ning laadib need `bronze` kihti. dbt transformatsioonid teisendavad andmed läbi `silver` ja `gold` kihtide ning `api` kihi kaudu on andmed kättesaadavad avaliku REST API-na aadressil `api.deng.ee`. Superset näidikulaud kuvab pipeline'i monitooringut ja vaatlusandmeid. DataHub andmekataloog indekseerib kõik tabelid, dbt mudelid ja Superset näidikulauad koos täieliku andmeliiniga. dbt käivitab automaatselt 26 andmekvaliteedi testi iga `dbt build` jooksul — kõik testid läbivad.

---

## Järgmised sammud

- DataHub metaandmete rikastamine — kirjelduste ja ärikonteksti lisamine DCAT-AP standardi alusel
- dbt andmekvaliteedi testide laiendamine silver, gold ja api kihtidele
- Avaliku andmekataloogi kasutajaliidese arendamine DataHub GraphQL API põhjal

---

## Mis takistab

Praegu pole blokeerivaid probleeme — pipeline töötab stabiilselt ja kõik põhifunktsionaalsused on töökorras. Järgmised sammud on arenduslikud, mitte vigade parandamine.

---

## Kontrollpunkt

Käsud, millega saab kontrollida, et töövoog töötab allikast näidikulauani:

**1. Viimane edukas pipeline'i jooks**

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT task_id, target_date, status, rows_processed, rows_loaded
   FROM bronze.etl_log
   WHERE status = 'success'
   ORDER BY started_at DESC
   LIMIT 5;"
```

Tegelik tulemus:

```
   task_id    | target_date | status  | rows_processed | rows_loaded
--------------+-------------+---------+----------------+-------------
 run_dbt      |             | success |                |
 ingest_hydro | 2026-05-26  | success |          14520 |       14520
 ingest_meteo | 2026-05-26  | success |           5664 |        5664
 fetch_meteo  | 2026-05-26  | success |           5664 |
 fetch_hydro  | 2026-05-26  | success |          14520 |
(5 rows)
```

**2. Andmed on jõudnud lõppkihti**

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT count(*) AS kokku_ridu,
          min(observation_ts_local) AS vanim,
          max(observation_ts_local) AS värskeim
   FROM gold.hydro_meteo;"
```

Tegelik tulemus:

```
 kokku_ridu |        vanim        |      värskeim
------------+---------------------+---------------------
     928315 | 2024-12-29 00:00:00 | 2026-05-26 23:00:00
(1 row)
```

**3. Viimase sisestuse värskus**

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT max(observation_ts_local) AS viimane_vaatlus
   FROM gold.hydro_meteo;"
```

Tegelik tulemus:

```
   viimane_vaatlus
---------------------
 2026-05-26 23:00:00
(1 row)
```

Oodatav tulemus: kuupäev, mis on tänasest ~3 päeva tagasi (API avaldamise viivituse tõttu).

**4. Bronze kihis pole duplikaate**

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT jaam_kood, timeline_ts_utc, aegrida_nimi, count(*) AS arv
   FROM bronze.hydro
   GROUP BY jaam_kood, timeline_ts_utc, aegrida_nimi
   HAVING count(*) > 1
   LIMIT 5;"
```

Tegelik tulemus:

```
 jaam_kood | timeline_ts_utc | aegrida_nimi | arv
-----------+-----------------+--------------+-----
(0 rows)
```

Null rida — bronze kihis pole ühtegi duplikaati. See on tagatud nii andmebaasi unikaalsuspiiranguga kui ka dbt automaattestidega.

> *Märkus: päring töötab suurel tabelil (~14 500 rida päevas alates 2025-01-01) — tulemus võib mõne sekundi võtta.*

**Näidikulaud**

Superset monitooringuarmatuurlaud (pipeline'i seire): [superset.deng.ee](https://superset.deng.ee)

Tableau analüüsi näidikulaud (veetasemed ja seosed): [Vaata Tableau Public'us](https://public.tableau.com/app/profile/thea.milder5692/viz/deng_ee/Veetasemedjaseosed)

---

*deng-hydro-climate — Edenemisraport — uuendatud 2026-05-29*
