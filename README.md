# Projet NO-SQL INF728


## Chargement des données:
Editer les fichiers build.sbt et build_and_submit pour y mettre vos variables (comme pour le projet Spark)

Il faut lancer le script suivant:
./build_and_submit.sh LoadingCVSFiles

J'ai fais en sorte que le lancement se fasse en arriere plan. Si vous voulez l'avoir en premier plan, il faut retirer la commande nohup de la derniere ligne du script (on aura donc directement le spark_submit)

## Creation du cluster de test Cassandra:
Il suffit de faire l'installation de ccm pour avoir un cluster. Cassandra n'est pas necessaire; il sera directement télécharger.
Pour installer ccm (https://academy.datastax.com/planet-cassandra/getting-started-with-ccm-cassandra-cluster-manager):
on clone le git du projet:

git clone https://github.com/pcmanus/ccm.git

Puis on l'installe:
cd ccm && ./setup.py install


Une fois installée, on peut lancer les commandes suivantes pour faire un git. Pour les personnes sur Mac (Nico, je parle de toi :p), regarde la doc, tu as 2 commandes à faire en plus.

ccm create test -v 3.11.5
ccm list
ccm populate -n 3
ccm list
ccm start
ccm status

Arriver ici, vous devriez avoir:

Cluster: 'test'
---------------
node1: UP
node2: UP
node3: UP

On va ensuite faire la table de la requete 1 dans un nouveau keyspace via cqlsh:

cqlsh

### A revoir: un seul keyspace pour les 4 ou 4 keyspace? Strategy? 

CREATE KEYSPACE NoSQL WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
use nosql ;

### Requete 1:
A voir si on ne met pas le pays comme partition pour avoir quelquechose de plus présentable

CREATE TABLE requete1 (
         ... jour DATE, 
         ... pays text,
         ... langue text,
         ... count int,
         ... PRIMARY KEY ((jour), pays, langue))
         ... ;


