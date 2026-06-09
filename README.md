TP 2A – Préparation d'une ingestion API météo avec Open-Meteo


Présentation du projet
Dans le cadre de ce TP, l'objectif est de mettre en place un premier pipeline d'ingestion de données météo à partir de l'API Open-Meteo.
L'idée n'est pas seulement de récupérer des données depuis une API, mais également de réfléchir à la façon dont ces données seront utilisées par la suite dans un pipeline de données. Pour cela, il est important de séparer clairement les différentes responsabilités du workflow : récupération, transformation et préparation des données.
Le pipeline a été conçu pour récupérer les informations météo de plusieurs villes françaises puis les transformer dans un format cohérent et facilement exploitable pour une future base de données ou un outil de reporting.

Villes étudiées
Les données sont récupérées pour les villes : Paris, Lyon, Marseille ,  elles permettent de tester le pipeline sur plusieurs sources de données tout en conservant un volume raisonnable pour un premier exercice.

Fonctionnement général du pipeline
Le DAG est composé de trois tâches principales :
extract_data  transform_data  load_data
Chaque tâche possède une responsabilité précise afin de conserver un workflow lisible et facilement maintenable.

Description des tâches
1. extract_data
Cette première étape est chargée de communiquer avec l'API Open-Meteo.
Pour chaque ville, une requête est envoyée afin de récupérer les informations météo disponibles. Les réponses sont conservées dans leur format d'origine (JSON) afin de préserver les données brutes.
Cette étape ne réalise aucune transformation. Son unique objectif est de récupérer les données.
Entrée - API Open-Meteo
Sortie - fichier JSON brut contenant les données des trois villes
2. transform_data
Cette étape prépare les données pour leur utilisation future.
Elle lit les réponses JSON récupérées précédemment puis extrait uniquement les informations utiles au besoin métier.
Les données sont ensuite restructurées afin d'obtenir un format homogène et facilement exploitable.
Les principaux traitements réalisés sont : extraction des champs utiles, ajout du nom de la ville, restructuration des données, préparation d'une table plate (une ligne par ville et par date).
Entrée - données JSON brutes
Sortie - données préparées au format JSON

3. load_data
Cette dernière étape simule le chargement des données.
Les données transformées sont converties dans un format tabulaire CSV, souvent utilisé avant une intégration dans une base de données ou un outil de visualisation.
Cette étape représente le point d'entrée vers une future couche de stockage.
Entrée - données préparées
Sortie - fichier CSV exploitable

Données retenues
L'API Open-Meteo retourne un grand nombre d'informations. Cependant, toutes ne sont pas nécessaires pour le besoin métier identifié.
Les champs retenus sont :
Champ	Description
city	Ville concernée
date	Date de la mesure ou de la prévision
max_temperature_c	Température maximale
min_temperature_c	Température minimale
precipitation_mm	Quantité de précipitations
timezone	Fuseau horaire

Pourquoi ces champs ?
Ces informations permettent de répondre à plusieurs besoins métier simples : suivre l'évolution des températures, comparer les conditions météorologiques entre plusieurs villes, identifier les périodes de fortes précipitations, préparer des analyses ou tableaux de bord météo. Les autres informations retournées par l'API n'ont pas été conservées car elles ne sont pas nécessaires pour  ce TP.

Fichiers générés
À l'issue de l'exécution du pipeline, trois fichiers sont produits :
raw_open_meteo.json : Contient la réponse brute de l'API  conserve une trace des données récupérées.
prepared_open_meteo.json : Contient les données après transformation  format intermédiaire facilitant les contrôles et les traitements futurs.
prepared_open_meteo.csv : Contient les données finales prêtes à être chargées dans une base de données ou utilisées dans un outil d'analyse.
Exemple de données préparées
city	date	max_temperature_c	min_temperature_c	precipitation_mm
Paris	2025-06-08	24.1	14.3	0.0
Lyon	2025-06-08	26.2	15.8	0.5
Marseille	2025-06-08	28.4	18.2	0.0
Chaque ligne représente les données météo d'une ville pour une date donnée.

Choix d'architecture
L'un des objectifs du TP était de distinguer les étapes métier des détails techniques.
Par ex : l'appel à l'API constitue une étape métier, la transformation des données constitue une étape métier, la génération du fichier final constitue une étape métier.
En revanche, certaines opérations comme : le parsing JSON, le renommage des colonnes, les conversions de formats, la manipulation des structures Python sont considérées comme des détails techniques et sont regroupées dans les tâches métier concernées. Ça permet d'obtenir un DAG plus lisible et plus facile à maintenir.
Exécution du projet
Après avoir placé le DAG dans le dossier dags, il suffit de démarrer Airflow :
airflow standalone
L'interface est ensuite accessible à l'adresse : http://localhost:8080
Depuis cette interface, on peut activer le DAG, lancer une exécution manuelle, consulter les logs et suivre l'état d'avancement des tâches.
Perspectives d'évolution
Ce pipeline constitue une première étape et pourrait facilement être enrichi avec :
•	une validation automatique des données ;
•	des alertes en cas d'échec ;
•	un chargement direct dans PostgreSQL ;
•	un stockage dans un Data Lake ;
•	une alimentation automatique d'un tableau de bord Power BI.

Conclusion : Ce TP a permis de concevoir un pipeline d'ingestion simple mais structuré autour des bonnes pratiques Airflow. La séparation entre récupération, transformation et préparation des données rend le workflow plus clair, plus maintenable et plus facilement extensible.
Le DAG obtenu constitue une base solide pour la mise en place de pipelines de données plus complexes dans un contexte de Data Engineering.
