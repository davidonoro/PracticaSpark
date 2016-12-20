# Practica Spark
Ejercicios obligatorios Spark

Autor: David Oñoro

## 1. Ejercicio 1
Comparación de las medallas de China y EEUU por año.

Partiendo del RDD de registros de medallas se filtran los records en los que el país es diferente de China o USA:

`olympicMedalRecordsRDD.filter(rec => rec.getCountry=="United States" || rec.getCountry=="China")`

Se mapea como clave una tupla formada por el país y el año, y como valor la suma de las medallas:

`.map(x=> ((x.getOlympicGame,x.getCountry),x.getGoldMedals+x.getSilverMedals+x.getBronzeMedals))`

Se suman las medallas por país mediante un reduceByKey:

`reduceByKey(_+_)`


Para reorganizar los datos de los paises en un mismo año se utiliza otro map reduce, que utilizará tuplas con la siguiente
estructura:

`(año,(medallas USA, medallas China, diferencia))`

El map rellenará las tuplas con los datos que tenga de forma directa, dejando a 0 los datos que desconozca. Por ejemplo la tupla
((2012,USA),234) genera la nueva tupla (2012,(234,0,0)), mientras que la tupla ((2004,China),47) genera la nueva tupla
(2004,(0,47,0))

`map(x=>{
       if(x._1._2=="United States"){
         (x._1._1,(x._2,0,0))
       }else{
         (x._1._1,(0,x._2,0))
       }
     })`


Por ultimo, se utiliza un reduceByKey para consolidar los datos con la tupla del otro país. Como solamente van a entrar 2 tuplas
por cada clave, una por país, es posible completar los datos de la tupla que no estén ya rellenos

`reduceByKey((acc,curr)=>{
       if(curr._1==0){
         (acc._1,curr._2,acc._1-curr._2)
       }else{
         (curr._1,acc._2,curr._1-acc._2)
       }
     })`

Se presentan los resultados del ejercicio ordenados por año

## 2. Ejercicio 2
Calcular la máxima puntuación que ha conseguido cada país y el año en que lo ha conseguido.

Se parte de RDD de regitros de medallas y se calcula el total de puntuación por pais y año mapeando como clave (pais,año) y como valor
la puntuación calculada a partir del número de medallas. Se suman las puntuaciones totales con un reduceByKey:

`olympicMedalRecordsRDD.map(rec=>((rec.getCountry,rec.getOlympicGame),rec.getGoldMedals*3+rec.getSilverMedals*2+rec.getBronzeMedals)).reduceByKey(_+_)`

Como segundo paso se hace un map en el que se deja como clave el pais y como valor la tupla formada por año y score:

`.map(x=>(x._1._1,(x._1._2,x._2)))`

Por ultimo, con un reduceByKey, se va acumulando la máxima puntuación:

`.reduceByKey((acc,curr)=>{
         if (curr._2 > acc._2){
           (curr._1,curr._2)
         }else{
           (acc._1,acc._2)
         }
   })`

El resultado se presenta ordenado por score


## 3. Ejercicio 3. Top K
Obtener los 3 mejores medallistas por juegos olimpicos según el número total de medallas

Se parte del RDD de registros de medallas y se calcula el numero total de medallas por deportista y año. En el map se devuelve como clave
el nombre del deportista y como valor una tupla con el año y el numero de medallas:

`olympicMedalRecordsRDD.map(rec=>((rec.getOlympicGame),(rec.getName,rec.getGoldMedals+rec.getSilverMedals+rec.getBronzeMedals)))`

Se utiliza una función aggragateByKey para calcular la lista con los 3 mejores por año. Los parametros de la función son los siguientes:

* Valor 0.

Se va a agregar en una lista inicialmente vacía de tuplas (String,Int), que simbolizan el año y el numero de medallas


* Función de agregacion

Se va creando una lista temporal agregando el elemento current a la lista acumulador. Esta lista se ordena por numero de medallas en
orden descendente y se devuelven las 3 primeras posiciones de la lista:

`(acc,curr)=>{
         val lista = curr :: acc;
         lista.sortBy(- _._2).take(3)
       }`


* Funcion de combinacion

Función que combina listas provenientes de diferentes workers. Mezcla las listas, las ordena y devuelve los 3 mejores tuplas ordenadas
por el número de medallas.

`(left,right)=>{
         val lista = left ::: right;
         lista.sortBy(- _._2).take(3)
       }`



Se presenta el resultado ordenado por el año