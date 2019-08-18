######## -*- coding: utf-8 -*-########

from pyspark.shell import spark
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType


class ArquivosDadosIO:
    _spark = None
    _fs = None

    def __init__(self,job):
        if ArquivosDadosIO._spark is None:
            ArquivosDadosIO._spark = SparkSession.builder.appName(job).getOrCreate()
            ArquivosDadosIO._fs = ArquivosDadosIO._spark._jvm.org.apache.hadoop.fs.FileSystem.\
            get(ArquivosDadosIO._spark._jsc.hadoopConfiguration())
            java_import(ArquivosDadosIO._spark._jvm, 'org.apache.hadoop.fs.Path')


    def spark_session(self):
        return ArquivosDadosIO._spark

    def df_logs(self):
        print ('lendo arquivos - Local: /arquivos')
        return ArquivosDadosIO._spark.read.text("arquivos/")

class Gerenciador:

    _dados_io = None
    _spark_session = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._spark_session = _dados_io.spark_session()
        self._logs=_dados_io.df_logs()

    def gera_regras(self):
        print ('layout arquivo')
        layout = df_logs_layout(self._logs)
        layout.show(20,False)

        print('## 1. Número​ ​de​ ​hosts​ ​únicos.')
        count_hosts = df_logs_count_hosts(layout)
        count_hosts.show(20,False)
        sum_hosts=df_logs_sum_hosts(count_hosts)
        sum_hosts.show(20,False)

        print('## 2. O​ ​total​ ​de​ ​erros​ ​404.')
        total_erros=df_logs_total_erros(layout)
        total_erros.show(20,False)

        print('## 3. Os​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​')
        urls_erros=df_logs_urls_erro(layout)
        urls_erros.show(20,False)

        print('## 4. Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia.')
        result=df_erros_dia(layout)
        result.show(20, False)

        print('## 5. O​ ​total​ ​de​ ​bytes​ ​retornados.')
        bytes_total=df_logs_bytes_total(layout)
        bytes_total.show(20,False)

        return layout


def df_logs_layout(_logs):
    return (_logs
            .withColumn('host_requisicao',f.split(_logs['value'],' ').getItem(0))
            .withColumn('timestamp',f.concat(f.substring(f.split(_logs['value'], ' ').getItem(3),2,20),
                                              f.substring(f.split(_logs['value'], ' ').getItem(4),1,5)))
            .withColumn('requisicao',f.concat(f.substring(f.split(_logs['value'], ' ').getItem(5),2,3),
                                    f.split(_logs['value'], ' ').getItem(6),
                                    f.substring(f.split(_logs['value'], ' ').getItem(7),1,8)))
            .withColumn('codigo_retorno_http', f.split(_logs['value'], ' ').getItem(8).cast(IntegerType()))
            .withColumn('total_bytes', f.split(_logs['value'], ' ').getItem(9).cast(IntegerType()))
            .drop('value'))

## 1. Número​ ​de​ ​hosts​ ​únicos.

def df_logs_count_hosts(_logs):
    return(_logs
           .groupby('host_requisicao')
           .agg(f.count(f.col('host_requisicao')).alias('count'))
           .filter((f.col('count'))==1)
            )

def df_logs_sum_hosts(_logs):
    return(_logs.agg(f.sum(f.col('count')).alias('total_hosts_unicos')))

## 2. O​ ​total​ ​de​ ​erros​ ​404.

def df_logs_total_erros(_logs):
    return(_logs
           .filter(f.col('codigo_retorno_http')==404)
           .groupby('codigo_retorno_http')
           .agg(f.count(f.col('codigo_retorno_http')).alias('total_erros'))
           )

## 3. Os​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​ ​

def df_logs_urls_erro(_logs):
    return(_logs
           .groupby('host_requisicao')
           .agg(f.count(f.col('codigo_retorno_http')).alias('total_erros_url'))
           .orderBy('total_erros_url',ascending=False)
           .limit(5)
            )


## 4. Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia.

def df_erros_dia(_logs):
    return(_logs
           .filter(f.col('codigo_retorno_http') == 404)
           .groupby(f.substring('timestamp',1,11).alias('timestamp'))
           .agg(f.count(f.col('codigo_retorno_http')).alias('total_erros_dia'))
           .orderBy(f.unix_timestamp(f.substring('timestamp',1,11),'dd/MMM/yyyy'),ascending=False)
           )


## 5. O​ ​total​ ​de​ ​bytes​ ​retornados.
def df_logs_bytes_total(_logs):
    return(_logs.agg(f.sum(f.col('total_bytes')).alias('total_bytes')))


print('iniciando processo')
_dados_io = ArquivosDadosIO('LOGS_NASA')
_gerenciador = Gerenciador(_dados_io)
_final = _gerenciador.gera_regras()
print('fim do processo')
spark.stop()






