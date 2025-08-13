from pyspark.sql.types import StringType, StructField, StructType


class SourceCollumns:
    DATA_INICIO = "DATA_INICIO"
    DATA_FIM = "DATA_FIM"
    CATEGORIA = "CATEGORIA"
    LOCAL_INICIO = "LOCAL_INICIO"
    LOCAL_FIM = "LOCAL_FIM"
    DISTANCIA = "DISTANCIA"
    PROPOSITO = "PROPOSITO"


class SinkCollumns:
    DT_REFE = "Data de referência."
    QT_CORR = "Quantidade de corridas."
    QT_CORR_NEG = "Quantidade de corridas com a categoria Negócio."
    QT_CORR_PESS = "Quantidade de corridas com a categoria Pessoal."
    VL_MAX_DIST = "Maior distância percorrida por uma corrida."
    VL_MIN_DIST = "Menor distância percorrida por uma corrida."
    VL_AVG_DIST = "Média das distâncias percorridas."
    QT_CORR_REUNI = "Quantidade de corridas com o propósito de Reunião."
    QT_CORR_NAO_REUNI = (
        "Quantidade de corridas com o propósito declarado e diferente de Reunião."
    )


class ProposalEnums:
    NEGOCIO = "Negocio"
    PESSOAL = "Pessoal"
    REUNIAO = "Reuniao"


BRONZE_SCHEMA: StructType = StructType(
    [
        StructField(SourceCollumns.DATA_INICIO, StringType(), True),
        StructField(SourceCollumns.DATA_FIM, StringType(), True),
        StructField(SourceCollumns.CATEGORIA, StringType(), True),
        StructField(SourceCollumns.LOCAL_INICIO, StringType(), True),
        StructField(SourceCollumns.LOCAL_FIM, StringType(), True),
        StructField(SourceCollumns.DISTANCIA, StringType(), True),
        StructField(SourceCollumns.PROPOSITO, StringType(), True),
    ]
)
