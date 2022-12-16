# MLOPS
Richard LAY
Steven TIEN

# Changelog
Nous avons changé le programme en prenant en compte les remarques faites pendant la soutenance:
## 1 
L'utilisation de pyspark pour insérer dans la DB a été changé en KafkaConsumer car nous ne faisons pas de preprocessing avant d'insérer dans la DB
## 2
Les analytics ont été fait en amont avant de pouvoir les visualiser dans le frontend.

# Architecture
Nous simulons une pipeline complète:
Nous créeons un faux paiement que nous envoyons qui sera vérifié server side.
Le résultat du paiement sera enregistré dans une base de donnée.
Ces données sont ensuite traités à des fins d'analyse puis affiché sur un front end.

# Lancer le programme
Lancer KAFKA
Pour lancer les kafka streams:
```
sh src/launch_serv.sh
```
Pour lancer la génération de donnée:
```
python3 src/producer.py
```
Pour lancer les analytics:
```
sh src/launch_analytics.sh
```