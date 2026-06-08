# TP 2 - Premier DAG Airflow

## Objectif

Dans ce TP, le but etait de creer un premier DAG Airflow simple avec 3 taches, de definir leur ordre d'execution, puis de lancer le DAG manuellement depuis l'interface web.

## DAG realise

J'ai cree un DAG nomme `meteo_pipeline` dans le fichier [dags/mon_premier_dag.py](/Users/hodafede/Desktop/Airflow/airflow_tp2/dags/mon_premier_dag.py:1).

Ce DAG contient 3 taches :

- `extract_data`
- `transform_data`
- `load_data`

L'ordre d'execution est le suivant :

```text
extract_data
    ↓
transform_data
    ↓
load_data
```

Les dependances ont ete definies explicitement dans le code avec :

```python
task_extract >> task_transform >> task_load
```

## Role des taches

- `extract_data` : simule l'extraction de donnees meteo.
- `transform_data` : simule la transformation des donnees.
- `load_data` : simule le chargement des donnees en base.

Les noms choisis sont simples et explicites. Le DAG reste lisible et chaque etape a un role precis.

## Lancement d'Airflow

J'ai lance Airflow en local puis j'ai ouvert l'interface web a l'adresse suivante :

```text
http://localhost:8080
```

J'ai ensuite accede a l'interface avec le compte `admin`.

## Execution du DAG

Dans l'interface Airflow, j'ai active le DAG `meteo_pipeline`, puis je l'ai lance manuellement avec le bouton `Trigger`.

Une execution manuelle a bien ete prise en compte avec le statut `Success`.

Informations observees dans l'interface :

- DAG ID : `meteo_pipeline`
- type d'execution : `Manual`
- utilisateur : `admin`
- statut : `Success`
- duree observee : `00:04:39`

## Verification dans l'interface

Dans l'interface Airflow, on voit bien que :

- le DAG `meteo_pipeline` apparait dans la liste ;
- les 3 taches sont presentes ;
- une execution manuelle a fonctionne ;
- les details des taches sont consultables ;
- les logs peuvent etre ouverts pour verifier l'execution.

J'ai aussi remarque un ancien run `Scheduled` en echec, mais cela ne remet pas en cause le TP puisque l'execution demandee etait une execution manuelle, et celle-ci a bien fonctionne.

## Conclusion

Ce TP m'a permis de creer un premier DAG Airflow simple et de comprendre le principe d'un workflow avec plusieurs taches dependantes.  
Le DAG `meteo_pipeline` est compose de 3 etapes claires : extraction, transformation et chargement.  
Il a ete visible dans l'interface web et une execution manuelle a ete realisee avec succes.
