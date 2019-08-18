# Nasa


####Qual​ ​o​ ​objetivo​ ​do​ ​comando​ ​​cache​ ​​em​ ​Spark?

Melhora a performance pois o cache coloca os dados na memoria e pode aumentar o desempenho de algumas consultas.

####O​ ​mesmo​ ​código​ ​implementado​ ​em​ ​Spark​ ​é​ ​normalmente​ ​mais​ ​rápido​ ​que​ ​a​ ​implementação​ ​equivalente​ ​em MapReduce.​ ​Por​ ​quê?

Porque o spark trabalha com dados em memória já o mapReduce trabalha com dados em disco. Isso torna o spark superiormente mais rápido para 
realizar processamentos, consultas e transformações dos dados em memória.

####Qual​ ​é​ ​a​ ​função​ ​do​ ​​SparkContext​?

É responsavel por realizar as configuracoes do spark e manter a conexao com o ambiente de execucao do spark.
No spark context voce pode definir varias configurações como: ambiente de implantacao, nome do aplicativo, paralelismo,etc.
O spark context é utilizado para criar RDDs e é utilizado para as versoes anteriores do spark 2.0. Para as versoes posteriores 
é utilizado o spark session que contempla todas as funcionalidades do spark context e também a utilização de APIs e dataframes.

####Explique​ ​com​ ​suas​ ​palavras​ ​​ ​o​ ​que​ ​é​ ​​Resilient​ ​Distributed​ ​Datasets​​ ​(RDD).

é um conjunto de dados que é distribuído no cluster para processamento e tolerante a falhas, ou seja, se um nó ficar indisponivel ele consegue 
recuperar essa partição.

####GroupByKey​ ​​é​ ​menos​ ​eficiente​ ​que​ ​​reduceByKey​ ​​em​ ​grandes​ ​dataset.​ ​Por​ ​quê?

Porque o o reduceByKey realiza a combinação das mesmas chaves/valores antes de embaralhar os dados, permitindo que o volume de dados em memória 
fique menor em relação ao groupByKey que não realiza a combinação dos dados apenas embaralha os dados.


####Explique​ ​o​ ​que​ ​o​ ​código​ ​Scala​ ​abaixo​ ​faz.

val​​ ​​textFile​​ ​​=​​ ​​sc​.​textFile​(​"hdfs://..."​)
val​​ ​​counts​​ ​​=​​ ​​textFile​.​flatMap​(​line​​ ​​=>​​ ​​line​.​split​(​"​ ​"​)) ​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​
            .​map​(​word​​​​=>​​​​(​word​,​​​​1​))
​ ​​ ​​ ​​ ​​ ​​ ​​ ​​ ​​ ​​ ​​ ​​ ​​.​reduceByKey​(​_​​ ​​+​​ ​​_​) 
counts​.​saveAsTextFile​(​"hdfs://..."​)


Realiza a leitura do arquivo no HDFS.
Realiza a separacao das palavras de cada linha do arquivo
Mapeia as palavras do arquivo no formato Key/value onde key = palavra e value é um 1.
Realiza a combinação/redução das mesmas chaves/valores
Realiza a contagem das palavras
Grava em arquivo o resultado da contagem mo HDFS.


##Teste prático:

###Bibliotecas utilizadas:

from pyspark.shell import spark
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

Fonte​ ​oficial​ ​do​ ​dateset​:​ ​​http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html Dados​:

● Jul​ ​01​ ​to​ ​Jul​ ​31,​ ​ASCII​ ​format,​ ​20.7​ ​MB​ ​gzip​ ​compressed​,​ ​205.2​ ​MB.

● Aug​ ​04​ ​to​ ​Aug​ ​31,​ ​ASCII​ ​format,​ ​21.8​ ​MB​ ​gzip​ ​compressed​,​ ​167.8​ ​MB.

Sobre o dataset​: Esses dois conjuntos de dados possuem todas as requisições HTTP para o servidor da NASA Kennedy Space​ ​Center​ ​WWW​ ​na​ ​Flórida​ ​para​ ​um​ ​período​ ​específico.
Os​ ​logs​ ​estão​ ​em​ ​arquivos​ ​ASCII​ ​com​ ​uma​ ​linha​ ​por​ ​requisição​ ​com​ ​as​ ​seguintes​ ​colunas:

● Host fazendo a requisição​. Um hostname quando possível, caso contrário o endereço de internet se o nome
não​ ​puder​ ​ser​ ​identificado.

● Timestamp​ ​​no​ ​formato​ ​"DIA/MÊS/ANO:HH:MM:SS​ ​TIMEZONE"

● Requisição​ ​(entre​ ​aspas)

● Código​ ​do​ ​retorno​ ​HTTP

● Total​ ​de​ ​bytes​ ​retornados

####Questões

​ ​Responda​ ​as​ ​seguintes​ ​questões​ ​devem​ ​ser​ ​desenvolvidas​ ​em​ ​Spark​ ​utilizando​ ​a​ ​sua​ ​linguagem​ ​de​ ​preferência.

1. Número​ ​de​ ​hosts​ ​únicos.
2. O​ ​total​ ​de​ ​erros​ ​404.
3. Os​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​ ​404.
4. Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia.
5. O​ ​total​ ​de​ ​bytes​ ​retornados.

##Evidencias da Execução:

###Layout original:

199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085
burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0
199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179
burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0
burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/video/livevideo.gif HTTP/1.0" 200 0
205.212.115.106 - - [01/Jul/1995:00:00:12 -0400] "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985
d104.aa.net - - [01/Jul/1995:00:00:13 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985
129.94.144.152 - - [01/Jul/1995:00:00:13 -0400] "GET / HTTP/1.0" 200 7074
unicomp6.unicomp.net - - [01/Jul/1995:00:00:14 -0400] "GET /shuttle/countdown/count.gif HTTP/1.0" 200 40310
unicomp6.unicomp.net - - [01/Jul/1995:00:00:14 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 200 786
unicomp6.unicomp.net - - [01/Jul/1995:00:00:14 -0400] "GET /images/KSC-logosmall.gif HTTP/1.0" 200 1204


###Layout do arquivo após a separação em colunas:
+-------------------------+-------------------------+------------------------------------------------------------+-------------------+-----------+
|host_requisicao          |timestamp                |requisicao                                                  |codigo_retorno_http|total_bytes|
+-------------------------+-------------------------+------------------------------------------------------------+-------------------+-----------+
|199.72.81.55             |01/Jul/1995:00:00:01-0400|GET/history/apollo/HTTP/1.0                                 |200                |6245       |
|unicomp6.unicomp.net     |01/Jul/1995:00:00:06-0400|GET/shuttle/countdown/HTTP/1.0                              |200                |3985       |
|199.120.110.21           |01/Jul/1995:00:00:09-0400|GET/shuttle/missions/sts-73/mission-sts-73.htmlHTTP/1.0     |200                |4085       |
|burger.letters.com       |01/Jul/1995:00:00:11-0400|GET/shuttle/countdown/liftoff.htmlHTTP/1.0                  |304                |0          |
|199.120.110.21           |01/Jul/1995:00:00:11-0400|GET/shuttle/missions/sts-73/sts-73-patch-small.gifHTTP/1.0  |200                |4179       |
|burger.letters.com       |01/Jul/1995:00:00:12-0400|GET/images/NASA-logosmall.gifHTTP/1.0                       |304                |0          |
|burger.letters.com       |01/Jul/1995:00:00:12-0400|GET/shuttle/countdown/video/livevideo.gifHTTP/1.0           |200                |0          |
|205.212.115.106          |01/Jul/1995:00:00:12-0400|GET/shuttle/countdown/countdown.htmlHTTP/1.0                |200                |3985       |
|d104.aa.net              |01/Jul/1995:00:00:13-0400|GET/shuttle/countdown/HTTP/1.0                              |200                |3985       |
|129.94.144.152           |01/Jul/1995:00:00:13-0400|GET/HTTP/1.0                                                |200                |7074       |
|unicomp6.unicomp.net     |01/Jul/1995:00:00:14-0400|GET/shuttle/countdown/count.gifHTTP/1.0                     |200                |40310      |
|unicomp6.unicomp.net     |01/Jul/1995:00:00:14-0400|GET/images/NASA-logosmall.gifHTTP/1.0                       |200                |786        |
|unicomp6.unicomp.net     |01/Jul/1995:00:00:14-0400|GET/images/KSC-logosmall.gifHTTP/1.0                        |200                |1204       |
|d104.aa.net              |01/Jul/1995:00:00:15-0400|GET/shuttle/countdown/count.gifHTTP/1.0                     |200                |40310      |
|d104.aa.net              |01/Jul/1995:00:00:15-0400|GET/images/NASA-logosmall.gifHTTP/1.0                       |200                |786        |
|d104.aa.net              |01/Jul/1995:00:00:15-0400|GET/images/KSC-logosmall.gifHTTP/1.0                        |200                |1204       |
|129.94.144.152           |01/Jul/1995:00:00:17-0400|GET/images/ksclogo-medium.gifHTTP/1.0                       |304                |0          |
|199.120.110.21           |01/Jul/1995:00:00:17-0400|GET/images/launch-logo.gifHTTP/1.0                          |200                |1713       |
|ppptky391.asahi-net.or.jp|01/Jul/1995:00:00:18-0400|GET/facts/about_ksc.htmlHTTP/1.0                            |200                |3977       |
|net-1-141.eden.com       |01/Jul/1995:00:00:19-0400|GET/shuttle/missions/sts-71/images/KSC-95EC-0916.jpgHTTP/1.0|200                |34029      |
+-------------------------+-------------------------+------------------------------------------------------------+-------------------+-----------+
only showing top 20 rows



### 1. Número​ ​de​ ​hosts​ ​únicos.
+-------------------------------------+-----+
|host_requisicao                      |count|
+-------------------------------------+-----+
|193.166.184.116                      |1    |
|204.120.34.242                       |1    |
|dutton.cmdl.noaa.gov                 |1    |
|rickf.seanet.com                     |1    |
|vdfcomm.vdfnet.com                   |1    |
|ldvgpi33.ldv.e-technik.tu-muenchen.de|1    |
|tenebris.rutgers.edu                 |1    |
|144.191.11.42                        |1    |
|inf-pc43.fbm.htw-kempten.de          |1    |
|conan.ids.net                        |1    |
|obiwan.tdtech.com                    |1    |
|204.180.143.17                       |1    |
|n1-28-222.macip.drexel.edu           |1    |
|chi007.wwa.com                       |1    |
|137.148.36.27                        |1    |
|129.219.88.17                        |1    |
|nu.sim.es.com                        |1    |
|jobstgb1.bradley.edu                 |1    |
|ip-pdx1-51.teleport.com              |1    |
|194.20.140.83                        |1    |
+-------------------------------------+-----+
only showing top 20 rows

+------------------+
|total_hosts_unicos|
+------------------+
|9269              |
+------------------+


### 2. O​ ​total​ ​de​ ​erros​ ​404.
+-------------------+-----------+
|codigo_retorno_http|total_erros|
+-------------------+-----------+
|404                |20686      |
+-------------------+-----------+



### 3. Os​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​
[--------------------+---------------+
|host_requisicao     |total_erros_url|
+--------------------+---------------+
|piweba3y.prodigy.com|21988          |
|piweba4y.prodigy.com|16437          |
|piweba1y.prodigy.com|12825          |
|edams.ksc.nasa.gov  |11944          |
|163.206.89.4        |9697           |
+--------------------+---------------+


### 4. Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia.
+-----------+---------------+
|timestamp  |total_erros_dia|
+-----------+---------------+
|31/Aug/1995|525            |
|30/Aug/1995|567            |
|29/Aug/1995|420            |
|28/Aug/1995|408            |
|27/Aug/1995|370            |
|26/Aug/1995|365            |
|25/Aug/1995|411            |
|24/Aug/1995|420            |
|23/Aug/1995|340            |
|22/Aug/1995|287            |
|21/Aug/1995|305            |
|20/Aug/1995|312            |
|19/Aug/1995|206            |
|18/Aug/1995|248            |
|17/Aug/1995|269            |
|16/Aug/1995|258            |
|15/Aug/1995|323            |
|14/Aug/1995|286            |
|13/Aug/1995|215            |
|12/Aug/1995|195            |
+-----------+---------------+
only showing top 20 rows


### 5. O​ ​total​ ​de​ ​bytes​ ​retornados.
+-----------+
|total_bytes|
+-----------+
|65143741103|
+-----------+# Nasa
