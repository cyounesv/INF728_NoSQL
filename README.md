# Projet NO-SQL INF728

Pour les taches du projet, j'ai fais un kanboard (https://github.com/Martinez-TAD/INF728_NoSQL/projects/1) avec quelques tâches.
N'hesitez pas à en ajouter pour ne pas que l'on fasse 2x la meme chose!


## Chargement des données:
Editer les fichiers build.sbt et build_and_submit pour y mettre vos variables (comme pour le projet Spark)

Il faut lancer le script suivant:
./build_and_submit.sh LoadingCSVFiles

J'ai fais en sorte que le lancement se fasse en arriere plan. Si vous voulez l'avoir en premier plan, il faut retirer la commande nohup de la derniere ligne du script (on aura donc directement le spark_submit) et le '&' en fin de ligne.

## Creation du cluster de test Cassandra:
Il suffit de faire l'installation de ccm pour avoir un cluster. Cassandra n'est pas necessaire; il sera directement télécharger.
Pour installer ccm (https://academy.datastax.com/planet-cassandra/getting-started-with-ccm-cassandra-cluster-manager):
on clone le git du projet:

git clone https://github.com/pcmanus/ccm.git

Puis on l'installe:
cd ccm && ./setup.py install


Une fois installée, on peut lancer les commandes suivantes pour faire un git. Pour les personnes sur Mac (Nico, je parle de toi :p), regarde la doc, tu as 2 commandes à faire en plus.

* ccm create test -v 3.11.5
* ccm list
* ccm populate -n 3
* ccm list
* ccm start
* ccm status

Arriver ici, vous devriez avoir:

Cluster: 'test'

node1: UP
node2: UP
node3: UP

On va ensuite faire la table de la requete 1 dans un nouveau keyspace via cqlsh:

cqlsh
ccm node1 cqlsh

## A revoir: un seul keyspace pour les 4 ou 4 keyspace? Strategy? 

Un seul keyspace.

* CREATE KEYSPACE NoSQL WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
* use nosql ;

## Requete 1:
A voir si on ne met pas le pays comme partition pour avoir quelquechose de plus présentable
On ne peut pas trier les resultats sur le count si count n'est pas dans les champs du clustering. A voir, surtout pour la requete 2

CREATE TABLE requete1 (
         ... jour DATE, 
         ... pays text,
         ... langue text,
         ... count int,
         ... PRIMARY KEY ((jour), pays, langue))
         ... ;

Une fois tout cela en place, sortez de cqlsh et lancer:

./build_and_submit.sh EventMentionETL

Ca va charger les fichiers qui sont dans /tmp et les mettre dans Cassandra.

###Dispo dans le master

## Requete 2
Plusieurs points à revoir dans la requete 2:
 - comment faire la somme triée des données par mois et année?
   => Apres étude avec Thomas, on ne voit pas comment faire. 
   Il faudrait faire surement 3 requetes: une pour les jours, une pour les mois et une pour l'année
 - Faire la recherche des evenements lorsque l'on a une mention sur un event du(es)  jour(s) precedents:
   => L'idée est surement de faire une table intermediaire pour les evenements et taper dedans. Autre idée?

En attendant:

create table requete2(year int, monthyear int, day int, country text, count int, eventid text, PRIMARY KEY((country), year, monthyear, day, eventid)) WITH CLUSTERING ORDER BY (year desc, monthyear asc, day asc, eventid desc);

create table requete2mapping(eventid text, day int, country text, actor1countrycode text, actor2countrycode text, PRIMARY KEY(eventid));  

Permet de faire les requetes avec count par jour en ordre descendant, mois et année (select sum(count) from requete2 where country="FR" group by eventid, year, monthYear ) mais l'ordre de la somme n'est pas pris en compte!
==> Plusieurs requetes?

### Dispo dans la branche requete2



