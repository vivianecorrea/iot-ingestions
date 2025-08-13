CREATE OR REFRESH STREAMING TABLE sandbox_first_catalog.transports.rides_bronze
AS SELECT *
FROM STREAM read_files(
  '/Volumes/sandbox_first_catalog/transports/landing-zone/',
  format => "csv",
  header => "true",
  delimiter => ";",
  schema => """
    DATA_INICIO STRING,
    DATA_FIM STRING,
    CATEGORIA STRING,
    LOCAL_INICIO STRING,
    LOCAL_FIM STRING,
    DISTANCIA STRING,
    PROPOSITO STRING
  """,
  schemaEvolutionMode => "none");

CREATE OR REFRESH  STREAMING TABLE sandbox_first_catalog.transports.rides_silver(
CONSTRAINT valid_start_date EXPECT (DATA_INICIO IS NOT NULL),
CONSTRAINT valid_distance EXPECT (DISTANCIA IS NOT NULL)
)
COMMENT "Dados de corridas aplicativo"
AS SELECT
  date_format(to_timestamp(DATA_INICIO, 'MM-dd-yyyy H:mm'), 'yyyy-MM-dd') AS DATA_INICIO,
  date_format(to_timestamp(DATA_FIM, 'MM-dd-yyyy H:mm'), 'yyyy-MM-dd') AS DATA_FIM,
  DOUBLE(DISTANCIA) AS DISTANCIA,
  PROPOSITO,
  CATEGORIA
FROM STREAM sandbox_first_catalog.transports.rides_bronze;

CREATE OR REFRESH STREAMING TABLE sandbox_first_catalog.transports.rides_daily_info_gold
COMMENT "Dados agrupados pela data de início do transporte "
SELECT
  DATA_INICIO AS DT_REFE,
  COUNT(*)                                                         AS QT_CORR,
  SUM(CASE WHEN PROPOSITO = 'Negocio' THEN 1 ELSE 0 END)           AS QT_CORR_NEG,
  SUM(CASE WHEN PROPOSITO = 'Pessoal' THEN 1 ELSE 0 END)           AS QT_CORR_PESS,
  MAX(DISTANCIA)                                              AS VL_MAX_DIST,
  MIN(DISTANCIA)                                              AS VL_MIN_DIST,
  ROUND(AVG(DISTANCIA), 2)                                    AS VL_AVG_DIST,
  SUM(CASE WHEN PROPOSITO = 'Reuniao' THEN 1 ELSE 0 END)          AS QT_CORR_REUNI,
  SUM(CASE
        WHEN PROPOSITO IS NOT NULL AND PROPOSITO  <> 'Reuniao'
        THEN 1 ELSE 0
      END)                                                        AS QT_CORR_NAO_REUNI
FROM STREAM sandbox_first_catalog.transports.rides_silver
GROUP BY DT_REFE
