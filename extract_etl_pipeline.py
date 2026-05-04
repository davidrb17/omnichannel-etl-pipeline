#=================================================================================================================
# OMNICHANNEL FINANCIAL ETL PIPELINE – DATA ENGINEERING PROJECT
#=================================================================================================================
#
#=================================================================================================================
# IMPORTACIÓN DE DEPENDENCIAS
#=================================================================================================================
from dotenv import load_dotenv       # Carga de variables del entorno virtual.
import os                            # Interacción con el S.O. y manejo de archivos.
import logging                       # Gestiona el registro de eventos, errores y- 
#                                      mensajes de depuración durante la ejecución- 
#                                      del programa.
import pandas as pd                  # Lectura y manipulación de datos.
from urllib.parse import quote_plus  # Codificación de contraseñas.
from sqlalchemy import create_engine # Interacción con bases de datos relacionales-
#                                      mediante objetos Python en lugar de escribir-
#                                      consultas navitas en SQL. Facilita la gestión- 
#                                      de conexiones, mapeo de tablas a clases.
import chardet                       # Detectar automáticamente la codificación de-
                                     # caracteres de un archivo o flujo de bytes-
                                     # desconocido.
from tabulate import tabulate
from datetime import datetime
import time
import logging
from sqlalchemy import text  # ✅ IMPORTANTE
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text, inspect


#==================================================================================================================
# CARGA DE VARIABLES DEL ARCHIVO .env
#==================================================================================================================

load_dotenv()

#===================================================================================================================
# CONEXIONES A SISTEMAS GESTORES DE BASES DE DATOS (DBMS)
#===================================================================================================================

# Variables de conexión a SQL Server--------------------------------------------------------------------------------

DB_DRIVER=os.getenv("DB_DRIVER")
DB_SERVER=os.getenv("DB_SERVER")
DB_NAME=os.getenv("DB_NAME")
DB_UID=os.getenv("DB_UID")
DB_PWD=os.getenv("DB_PWD")
DB_ENCRYPT=os.getenv("DB_ENCRYPT")

# Variables de conexión a MySQL--------------------------------------------------------------------------------------

MYSQL_HOST=os.getenv("MYSQL_HOST")
MYSQL_PORT=os.getenv("MYSQL_PORT")
MYSQL_DB=os.getenv("MYSQL_DB")
MYSQL_USER=os.getenv("MYSQL_USER")
MYSQL_PASSWORD=os.getenv("MYSQL_PASSWORD")

# Variables de conexión a PostgreSQL----------------------------------------------------------------------------------

PG_HOST=os.getenv("PG_HOST")
PG_PORT=os.getenv("PG_PORT")
PG_DB=os.getenv("PG_DB")
PG_USER=os.getenv("PG_USER")
PG_PASSWORD=os.getenv("PG_PASSWORD")

#======================================================================================================================
# REGISTRO DE EVENTOS Y MENSAJES - Configura el sistema de registro de eventos (logging) en Python
#======================================================================================================================

logging.basicConfig(
    # Se registrarán mensajes desde el nivel INFO en adelante (INFO, WARNING, ERROR, etc.)
    level=logging.INFO,
    # Establece el formato del mensaje del log, mostrando: fecha/hora – nivel del mensaje – texto del mensaje   
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Silenciar logs ruidosos de librerías
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
logging.getLogger("mysql").setLevel(logging.WARNING)
logging.getLogger("psycopg2").setLevel(logging.WARNING)

#======================================================================================================================
# CREACIÓN DE ENGINES (MOTORES) PARA LECTURA DE TABLAS PARA CADA DMBS
#======================================================================================================================
#
# Engine de SQL Server------------------------------------------------------ -------------------------------------------

def engine_sqlserver(run_id, stage):
    PIPELINE_ID = "OMNICHANNEL_ETL"
    source="SQLSERVER"
    source = source.upper()
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]"
    start_time = time.perf_counter()

    try:
        logging.info(f"{log_prefix}[STARTED] Creando engine...")
        cadena_conexion = (
            f"mssql+pyodbc://{DB_UID}:{quote_plus(DB_PWD)}@{DB_SERVER}/{DB_NAME}"
            f"?driver={quote_plus(DB_DRIVER)}&Encrypt={DB_ENCRYPT}"
        )
        engine = create_engine(cadena_conexion)
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        logging.info(f"{log_prefix}[SUCCESS] Engine creado correctamente | duration_ms: {duration_ms:.2f}\n")                
        return engine
    except Exception as e:
        logging.error(f"{log_prefix}[ERROR] Error al crear engine: {str(e)}")
        raise
#
# Engine de MySQL--------------------------------------------------------------------------------------------------------
# 
def engine_mysql(run_id, stage):    
    PIPELINE_ID = "OMNICHANNEL_ETL"
    source="MYSQL"
    source = source.upper()
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]"
    start_time = time.perf_counter()

    try:
        logging.info(f"{log_prefix}[STARTED] Creando engine...")
        cadena_conexion = (
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        )
        engine = create_engine(cadena_conexion)
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        logging.info(f"{log_prefix}[SUCCESS] Engine creado correctamente | duration_ms: {duration_ms:.2f}\n")       
        return engine
    except Exception as e:
        logging.error(f"{log_prefix}[ERROR] Error al crear engine: {str(e)}")
        raise
#
# Engine de PostgreSQL----------------------------------------------------------------------------------------------------
#
def engine_postgresql(run_id, stage):    
    PIPELINE_ID = "OMNICHANNEL_ETL"
    source="POSTGRESQL"
    source = source.upper()
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]"
    start_time = time.perf_counter()  

    try:
        logging.info(f"{log_prefix}[STARTED] Creando engine...")
        cadena_conexion = (
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
            f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
        )
        engine = create_engine(cadena_conexion)
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        logging.info(f"{log_prefix}[SUCCESS] Engine creado correctamente | duration_ms: {duration_ms:.2f}\n")        
        return engine
    except Exception as e:
        logging.error(f"{log_prefix}[ERROR] Error al crear engine: {str(e)}")
        raise

#===========================================================================================================================
# LECTURA DE TABLAS (LECTURA DE DATASETS) POR CADA DBMS
#===========================================================================================================================

# Lectura dataset SQL Server------------------------------------------------------------------------------------------------

def sqlserver_tabla(engine_sqlsv, tabla):
    try:
        query = text(f"SELECT * FROM {tabla}")
        df = pd.read_sql(query, engine_sqlsv)
        #logging.info(f"Datos extraidos de SQL Sever: {df.shape[0]} filas")
        return df
    except Exception as e:
        logging.info(f"Error al leer la tabla de SQL", exc_info=True)
        return pd.DataFrame()

# Lectura dataset MySQL Server-----------------------------------------------------------------------------------------------

def mysql_tabla(engine_msql, tabla):
    try:
        query=f"SELECT * FROM {tabla}"
        df = pd.read_sql(query, engine_msql)
        #logging.info(f"Datos extraidos de MySQL: {df.shape[0]} filas")
        return df
    except Exception as e:
        logging.info(f"Error al leer la tabla de MySQL", exc_info=True)
        return pd.DataFrame()

# Lectura dataset PostgreSQL-------------------------------------------------------------------------------------------------

def postgresql_tabla(engine_pstsql, tabla):
    try:
        query=f"SELECT * FROM {tabla}"
        df = pd.read_sql(query, engine_pstsql)
        #logging.info(f"Datos extraidos de PostgreSQL: {df.shape[0]} filas")
        return df
    except Exception as e:
        logging.info(f"Error al leer la tabla de PostgreSQL", exc_info=True)
        return pd.DataFrame()

# Lectura dataset CSV----------------------------------------------------------------------------------------------------------

def extrae_csv(filename="ventanilla_sucursal_transacciones_2025.csv"):
    #logging.info("Extrayendo datos del archivo CSV...")
     
    try:
        # Ruta completa
        csv_path = os.path.join(os.getcwd(), filename)

        # Verificar existencia
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Archivo no encontrado: {csv_path}")

        # Detectar encoding
        with open(csv_path, "rb") as f:
            dato_origen = f.read(50000)  # Leer un fragmento del archivo
            encoding_detectado = chardet.detect(dato_origen)["encoding"]

        #logging.info(f"Encoding detectado: {encoding_detectado}")

        try:
            # Intento principal
            df = pd.read_csv(csv_path, encoding=encoding_detectado)
        except Exception:
            logging.info("Aviso: Falló el encoding detectado. Usando fallback UTF-8.")
            df = pd.read_csv(csv_path, encoding="utf-8", errors="replace")

        #logging.info(f"Datos extraídos del CSV: {df.shape[0]} filas\n")
        #print(df.head())

        return df

    except Exception as e:
        logging.info("Error en la fase de extracción CSV:", exc_info=True)
        return pd.DataFrame()

# Función adaptadora (wrapper)----------------------------------------------------------------------------------------------------
# Creando un wrapper (adaptador) para mantener una interfaz uniforme con las demás extracciones
def csv_tabla(engine_dummy, tabla):
    return extrae_csv("ventanilla_sucursal_transacciones_2025.csv")

#=================================================================================================================================
# EXTRACTORES DE DATOS
#=================================================================================================================================

EXTRACTORES = {
    "sqlserver": sqlserver_tabla,   # Viene de la función que contiene el query a sql
    "mysql": mysql_tabla,           # Viene de la función que contiene el query a mysql
    "postgresql": postgresql_tabla, # Viene de la función que contiene el query a postgresql
    "csv": csv_tabla
}

#=================================================================================================================================
# DICCIONARIO DE TABLAS
#=================================================================================================================================

TABLAS = {
    "sqlserver": [
        "app_clientes",
        "app_transacciones"

    ],
    "mysql": [
        "atm_clientes",
        "atm_transacciones"

    ],
    "postgresql": [
        "portal_clientes",
        "portal_transacciones"
       
    ],
    "csv": ["csv_ventanilla"]
}

#===================================================================================================================================
# TABLAS DE CLIENTES POR MOTOR
#===================================================================================================================================

TABLAS_CLIENTES = {
    "sqlserver": "app_clientes",
    "mysql": "atm_clientes",
    "postgresql": "portal_clientes"
}

COLUMNAS_OBJETIVO=[
    "id_transaccion",
    "id_cliente",
    "fecha_transaccion",
    "monto",
    "tipo_transaccion",
    "canal_origen"
]

#====================================================================================================================================
# COMENZAMOS LA FASE DE TRANSFORMACIÓN 
#====================================================================================================================================
#
#------------------------------------------------------------------------------------------------------------------------------------
# MAPEO DE COLUMNAS POR CADA DATASET / RENOMBRAR COLUMNAS PARA LA COINCIDENCIA CON "ESQUEMA OBJETIVO" → DWH
#------------------------------------------------------------------------------------------------------------------------------------

mapeo_sqlserver={
    "transaccion_id": "id_transaccion",
    "cliente_id":"id_cliente",
    "fecha_transaccion":"fecha_transaccion",
    "monto_transaccion": "monto",
    "tipo_transaccion":"tipo_transaccion",
    "canal_origen":"canal_origen"
}

mapeo_mysql={
    "transaccion_id": "id_transaccion",
    "cliente_id": "id_cliente",
    "fecha_transaccion":"fecha_transaccion",
    "monto_transaccion": "monto",
    "tipo_transaccion":"tipo_transaccion",
    "canal_origen":"canal_origen" 
}
mapeo_postgresql={
    "transaccion_id": "id_transaccion",
    "cliente_id": "id_cliente",
    "fecha_transaccion":"fecha_transaccion",
    "monto_transaccion": "monto",
    "tipo_transaccion":"tipo_transaccion",
    "canal_origen":"canal_origen" 
}
mapeo_csv = {
    "transaccion_id": "id_transaccion",
    "cliente_id": "id_cliente",
    "fecha_transaccion": "fecha_transaccion",
    "monto_transaccion": "monto",
    "tipo_transaccion": "tipo_transaccion",
    "canal_origen": "canal_origen"
}

# --------------------------------------------------------------------------------------------------------------------------------------
# "ESQUEMA OBJETIVO" → DWH
# --------------------------------------------------------------------------------------------------------------------------------------

def alinear_esquema_transacciones(df, run_id, mapeo_columnas, columnas_objetivo, source):

    PIPELINE_ID = "OMNICHANNEL_ETL"
    stage = "TRANSFORM"
    source = source.upper()
    pipeline_prefix = f"[{run_id}][{PIPELINE_ID}][PIPELINE][{stage}]"
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]"    
    
    source = source.upper()

    # ==============================
    # 1. VALIDACIÓN INICIAL
    # ==============================

    if df is None or df.empty:
        raise ValueError(f"[{source}] DataFrame vacío o None")
    
    #print(f"\n[{source}] ANTES:", df.columns.tolist())

    logging.info(
    "%s[TRANSACCIONES][SCHEMA][ENFORCEMENT_LAYER][%s] Analizando Esquema Original: %s",
    log_prefix,
    source,
    df.columns.tolist()
)
    
    # Guardamos estado inicial
    antes = set(df.columns)

    # ==============================
    # 2. RENOMBRADO
    # ==============================
    df = df.rename(columns=mapeo_columnas)

    # ==============================
    # 3. VALIDACIÓN DE ESQUEMA
    # ==============================
    cols_actual = set(df.columns)
    cols_objetivo = set(columnas_objetivo)

    faltantes_esquema = cols_objetivo - cols_actual
    extras_esquema = cols_actual - cols_objetivo

    # Error crítico
    if faltantes_esquema:
        raise ValueError(
            f"[{source}] Faltan columnas requeridas: {faltantes_esquema}"
        )
    
    # Extras (solo informativo)
    if extras_esquema:
        print(f"[{source}] COLUMNAS EXTRA DETECTADAS (SE ELIMINARÁN):", extras_esquema)
    
    # ==============================
    # 4. CORTE FINAL (NORMALIZACIÓN)
    # ==============================
    df = df[columnas_objetivo]

    # ==============================
    # 5. VALIDACIÓN DE ORDEN
    # ==============================
    if list(df.columns) != columnas_objetivo:
        raise ValueError(f"[{source}] El orden de columnas no es correcto")

    # ==============================
    # 6. DEBUG DE MAPEO
    # ==============================
    despues = set(df.columns)

    renombradas = {
        k: v for k, v in mapeo_columnas.items()
        if k in antes and k != v
    }

    faltantes_mapeo = [k for k in mapeo_columnas.keys() if k not in antes]

    # ==============================
    # 7. LOGS FINALES
    # ==============================
    print(f"[{source}] COLUMNAS RENOMBRADAS:", renombradas)
    print(f"[{source}] COLUMNAS NO ENCONTRADAS (antes):", faltantes_mapeo)
    print(f"[{source}] ESQUEMA FINAL ESTANDARIZADO:", df.columns.tolist())

    return df

# --------------------------------------------------------------------------------------------------------------------------------------
# "ESQUEMA OBJETIVO" → DWH
# --------------------------------------------------------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------------------------------------------------------
# FUNCIONES DE ESTANDARIZACIÓN: Estandariza Transacciones, 
# -----------------------------------------------------------------------------------------------------------------------------------------

# Función Estandarización de Transacciones:     
#-------------------------------------------------------------------------------------------------------------------------------------------

# df: "dataframe crudo"
# mapeo_columnas: diccionario de columnas que seran homologadas al tipo de dato correcto

def estandariza_transacciones(df, run_id, mapeo_columnas, source):
    
    PIPELINE_ID = "OMNICHANNEL_ETL"
    stage = "TRANSFORM"
    source = source.upper()   
    log_prefix = f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]"
         
    # ===============================================================
    # 1. VALIDACIÓN ESTRUCTURAL 
    #    Revisa que se haya cargado el DataFrame
    #    Revisa que el DataFrame contenga columnas a estandarizar
    #    Revisa que el DataFrame cargado contega datos 
    # ===============================================================
    
    # Revisa que el DataFrame exista  
    if df is None:
        raise ValueError(f"[{log_prefix}] | DataFrame no cargado - Falló extracción - Proceso detenido")
        return df
    
    # Revisa que el DataFrame contenga columnas
    if df.columns.size == 0:
        raise ValueError(f"{log_prefix} DataFrame sin columnas - Proceso detenido")
    
    # Revisa que el DataFrame contenga datos
    if df.empty:
        raise ValueError(f"{log_prefix} | DataFrame vacío - DataFrame sin registros - Proceso detenido")
    
    # Creamos copia del DataFrame: una práctica de seguridad para que la transformación sea confiable
    # Garantizar aislamiento de datos y evitar efectos colaterales en el pipeline
    df = df.copy()

    # Renombrar columnas (de acuerdo al mapeo y esquema objetivo)
    
    # mapeo_columnas: diccionario de columnas que seran homologadas al tipo de dato correcto
    df = df.rename(columns=mapeo_columnas)
    
    # Determina que Columnas Objetivo no se encuentran en el DataFrame renombrado
    faltantes = [col for col in COLUMNAS_OBJETIVO if col not in df.columns]

    if faltantes:
        raise ValueError(f"{log_prefix} Columnas faltantes después del mapeo: {faltantes} - Proceso detenido")

    # Filtra el DataFrame y se queda solo con las COLUMNAS OBJETIVOS
    # ✔ Elimina columnas innecesarias
    # ✔ Define el esquema final
    df = df[COLUMNAS_OBJETIVO].copy()
    # copy() -> Evita: referencias al DF original, efectos secundarios y SettingWithCopyWarning
    # “Quédate solo con las columnas que me interesan y crea un DataFrame limpio e independiente”
   
    # 🔹 1. df.dtypes.items() -> Devuelve pares: (columna, tipo) -> k v
    # 🔹 2. List comprehension -> muestra una lista en pares "id_cliente: Int64"
    columnas_str = "\n".join([f"    - {k}: {v}" for k, v in df.dtypes.items()])    
    logging.info(f"{log_prefix}[TRANSACCIONES][SCHEMA] Estableciendo Esquema Objetivo...\n\nColumnas:\n\n{columnas_str}\n")
    
    preview_str = df.head(3).to_string(index=False)
    
    logging.info(
        "[%s][%s][%s][%s][TRANSACCIONES][SCHEMA][PREVIEW] Vista previa del esquema:\n\n%s\n",
        run_id,
        PIPELINE_ID,
        source.upper(),
        stage,
        preview_str    
    )

    # Datos originales ANTES DE CAST

    id_cliente_original = df["id_cliente"].copy()
    fecha_transaccion_original = df["fecha_transaccion"].copy()
    monto_original = df["monto"].copy()
    tipo_transaccion_original = df["tipo_transaccion"].copy()
    canal_origen_original = df["canal_origen"].copy()
   
    dtypes_before = df.dtypes.astype(str).to_dict()

    # errors="coerce" indica que, si un valor no puede convertirse al tipo de-
    # dato solicitado, se reemplaza por NaN o NaT en lugar de generar un error
    
    # Una vez establecido es DataFrame con el esquema objetivo inician las transformaciones

    # Inicia estandarización de columnas de transacciones

    #=============================================================================================
    # 2. CAST 
    #    Casting: (convertimos una variable de un tipo a otro) convirtiendo "id_cliente a Int64"
    #    Tracking: seguimiento guardamos el tipo original y el nuevo tipo de dato
    #==============================================================================================

    # Convierte a numérico "id_cliente", Int64 mantiene integridad de llave.
    logging.info(f"{log_prefix}[STARTED] Convirtiendo a 'numeric' id_cliente...")
    df["id_cliente"] = pd.to_numeric(df["id_cliente"], errors="coerce").astype("Int64")
    logging.info(f"{log_prefix}[SUCCESS] Conversión completada.")

    # Convierte a date "fecha_transaccion"
    logging.info(f"{log_prefix}[STARTED] Convirtiendo a 'date' fecha_transaccion...")
    df["fecha_transaccion"] = pd.to_datetime(df["fecha_transaccion"], errors="coerce")        
    logging.info(f"{log_prefix}[SUCCESS] Conversión completada.")
    
    # Convierte a númerico "monto"
    logging.info(f"{log_prefix}[STARTED] Convirtiendo a 'numeric' monto...")
    df["monto"] = pd.to_numeric(df["monto"], errors="coerce")
    logging.info(f"{log_prefix}[SUCCESS] Conversión completada.")

    # Convierte a cadena "tipo_transaccion"
    logging.info(f"{log_prefix}[STARTED] Convirtiendo a 'string' tipo_transaccion...")
    df["tipo_transaccion"] = df["tipo_transaccion"].astype("string")
    logging.info(f"{log_prefix}[SUCCESS] Conversión completada.")
    
    # Convierte a cadena "canal_origen"
    logging.info(f"{log_prefix}[STARTED] Convirtiendo a 'string' canal_origen...")
    df["canal_origen"] = df["canal_origen"].astype("string")
    logging.info(f"{log_prefix}[SUCCESS] Conversión completada.\n")
    
    # =============================================================================
    # 3. VALIDACIONES CRÍTICAS DESPUÉS DEL CAST   
    # =============================================================================
      
    #  Validación de errores de conversión (no casteados)
    #  Detecta:
    #       tenía valor -> notna()
    #       no estaba vacío -> != ""
    #       terminó en NaN después del cast -> isna()
    #       💣 = no casteado

    invalid_id = (
        id_cliente_original.notna() &
        (id_cliente_original.astype(str).str.strip() != "") &
        df["id_cliente"].isna()
    )
    
    # Si encuentra un valor no casteado detiene el proceso
    if invalid_id.any():
        raise ValueError(f"{log_prefix} id_cliente no casteados: {int(invalid_id.sum())} | Proceso detenido.")
    
    invalid_fecha = (
        fecha_transaccion_original.notna() &
        (fecha_transaccion_original.astype(str).str.strip() !="") &
        df["fecha_transaccion"].isna()
    )

    if invalid_fecha.any():
        raise ValueError(f"{log_prefix} fecha_transaccion no casteados: {int(invalid_fecha.sum())} | Proceso detenido.")
    
    invalid_monto = (
        monto_original.notna() &
        (monto_original.astype(str).str.strip() !="") &
        df["monto"].isna()
    )
    
    if invalid_monto.any():
        raise ValueError(f"{log_prefix} monto no casteados: {int(invalid_monto.sum())} | Proceso detenido.")    

    # Validación de errores NULOS y DUPLICADOS

    # Valida si la columna "id_cliente" contiene nulos
    if df["id_cliente"].isna().any():
        raise ValueError("Existen id_cliente NULL | Proceso detenido")
    
    # Valida si la columna "fecha_transaccion" contiene nulos
    if df["fecha_transaccion"].isna().any():
        raise ValueError("Existen fecha_transaccion nulos | Proceso detenido.")

    # Valida si la columna "monto" contiene nulos
    if df["monto"].isna().any():
        raise ValueError(f"Existen monto nulos | Proceso detenido.")
    
    # Valida si la columna "tipo_transaccion" contiene nulos
    if df["tipo_transaccion"].isna().any():
        raise ValueError(f"Existen tipo_transaccion nulos | Proceso detenido.")
    
    # Valida si la columna "tipo_transaccion" tiene valores vacios
    if (df["tipo_transaccion"].str.strip() == "").any():
        raise ValueError("Existen tipo_transaccion vacíos | Proceso detenido")

    # Establecemos valores válidos para tipo_transaccion
    valores_validos_trans = ["RETIRO", "PAGO_SERVICIO", "COMPRA_ONLINE", "TRANSFERENCIA", "DEPOSITO", "RECARGA"]

    # Si el DataFrame contiene valores no validos se detiene el proceso
    if ~df["tipo_transaccion"].isin(valores_validos_trans).all():
        raise ValueError("Valores no válidos en tipo_transaccion | Proceso detenido")
    
    # Valida si la columna "canal_origen" contiene nulos
    if df["canal_origen"].isna().any():
        raise ValueError(f"Existen canal_origen nulos | Proceso detenido")
    
    # Valida si la columna "canal_origen" contiene valores vacios
    if (df["canal_origen"].str.strip() == "").any():
        raise ValueError(f"Existen canal_origen vacios | Proceso detenido")
    
    # Establecemos valores válidos para tipo_transaccion

    valores_validos_canal = ["ATM", "PORTAL_WEB", "APP_ANDROID", "VENTANILLA"]
    if ~df["canal_origen"].isin(valores_validos_canal).all():
        raise ValueError("Valores no válidos en canal_origen | Proceso detenido")

    dtypes_after = df.dtypes.astype(str).to_dict()
    
    resumen = pd.DataFrame({
        "COLUMNA": df.columns,
        "TIPO_ANTES": [dtypes_before.get(col, "N/A") for col in df.columns],
        "TIPO_DESPUES": [dtypes_after.get(col, "N/A") for col in df.columns]
    })

    resumen["CAMBIO_TIPO"] = resumen["TIPO_ANTES"] + " → " + resumen["TIPO_DESPUES"]

    tabla = tabulate(
        resumen.astype(str),
        headers="keys",
        tablefmt="psql",
        showindex=False
    )

    logging.info(
        f"{log_prefix}[TRANSACCIONES][SCHEMA][TRACKING] Trazabilidad de tipos:\n\n{tabla}\n"
    )

    #=======================================================================================
    # MÉTRICAS DE CALIDAD
    #=======================================================================================
 
    total_rows = len(df)

    # Calculo de nulos
    nulls_id = int(df["id_cliente"].isna().sum())
    nulls_fecha = int(df["fecha_transaccion"].isna().sum())
    nulls_monto = int(df["monto"].isna().sum())
    nulls_tipo = int(df["tipo_transaccion"].isna().sum())
    nulls_canal = int(df["canal_origen"].isna().sum())

    # Calculo de Porcentajes
    nulls_id_pct = (nulls_id / total_rows) * 100 if total_rows > 0 else 0
    nulls_fecha_pct = (nulls_fecha / total_rows) * 100 if total_rows > 0 else 0
    nulls_monto_pct = (nulls_monto / total_rows) * 100 if total_rows > 0 else 0
    nulls_tipo_pct = (nulls_tipo / total_rows) * 100 if total_rows > 0 else 0
    nulls_canal_pct = (nulls_canal / total_rows) * 100 if total_rows > 0 else 0

    # Valores no válidos  
    invalid_id_cal = int(invalid_id.sum())
    invalid_fecha_cal = int(invalid_fecha.sum())
    invalid_monto_cal = int(invalid_monto.sum())
    invalid_tipo_cal = int((~df["tipo_transaccion"].isin(valores_validos_trans)).sum())
    invalid_canal_cal = int((~df["canal_origen"].isin(valores_validos_canal)).sum())

    invalid_id_pct = (invalid_id_cal / total_rows) * 100 if total_rows > 0 else 0
    invalid_fecha_pct = (invalid_fecha_cal / total_rows) * 100 if total_rows > 0 else 0
    invalid_monto_pct = (invalid_monto_cal / total_rows) * 100 if total_rows > 0 else 0

    logging.info(
    "%s[QUALITY][METRICS] Filas: %s | "
    "id_cliente nulls: %s (%.2f%%) | "
    "fecha nulls: %s (%.2f%%) | "
    "monto nulls: %s (%.2f%%) | "
    "tipo nulls: %s (%.2f%%) | "
    "canal nulls: %s (%.2f%%) | "
    "id invalidos: %s (%.2f%%) | "
    "fecha invalidos: %s (%.2f%%) | "
    "monto invalidos: %s (%.2f%%) | "
    "tipo inválidos: %s | "
    "canal inválidos: %s",
    log_prefix,
    total_rows,
    nulls_id, nulls_id_pct,
    nulls_fecha, nulls_fecha_pct,
    nulls_monto, nulls_monto_pct,
    nulls_tipo, nulls_tipo_pct,
    nulls_canal, nulls_canal_pct,
    invalid_id_cal, invalid_id_pct,
    invalid_fecha_cal, invalid_fecha_pct,
    invalid_monto_cal, invalid_monto_pct,
    invalid_tipo_cal,
    invalid_canal_cal
)


    logging.info(f"{log_prefix}[TRANSACCIONES][SCHEMA][ROWS] Filas: {len(df)} \n")
    logging.info(f"{log_prefix}[TRANSACCIONES][SCHEMA][RESULT] Resultado de la estandarización de Transacciones:\n\n{tabla}\n")

    #logging.info(f"DEBUG FINAL COLUMNAS: {df.columns.tolist()}")     
    return df
    
#--------------------------------------------------------------------------------------------------------------------------------------------
# Función Estandarización de Clientes: Realiza la normalización del identificador único de clientes antes de integrarlo en otras fuentes
# 
#  Revisa que el DataFrame exista o no venga vacío
#  Si comprueba el DataFrame crea una copia
#  1. VALIDA COLUMNAS - Revisa que el DataFrame tenga columnas
#  2. RENAME - Renombra la columna "cliente_id" a "id_cliente"
#  3. VALIDACIÓN:
#       Valida que la columna "id_cliente" exista, 
#       Revisa si la columna "id_cliente" contiene Nulos,
#       Revisa si la columna "id_cliente" contiene duplicados 
#  4. CAST + TRAKING - Convierte el tipo de dato a Int64 y guarda el tipo original y el tipo de dato convertido
#  Imprime resultados de conversion
#  Calcúla métricas: total de estandarizados, nulos, no casteados (no convertidos) y duplicados 
#--------------------------------------------------------------------------------------------------------------------------------------------
def estandariza_clientes(df, source, run_id, stage):   
    
    PIPELINE_ID = "OMNICHANNEL_ETL"
    stage = "TRANSFORM"
    source = source.upper()
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]"
    
    # ===============================================================
    # 1. VALIDACIÓN ESTRUCTURAL 
    #    Revisa que se haya cargado el DataFrame
    #    Revisa que el DataFrame contenga columnas a estandarizar
    #    Revisa que el DataFrame cargado contega datos 
    # ===============================================================

    # Revisa que el DataFrame exista
    if df is None:
        raise ValueError(f"{log_prefix} | DataFrame no cargado - Falló extracción - Proceso detenido")
    
    # Revisa que el DataFrame contenga columnas
    if df.columns.size == 0:
        raise ValueError(f"{log_prefix} DataFrame sin columnas - Proceso detenido")
    
    # Revisa que el DataFrame contenga datos
    if df.empty:
        raise ValueError(f"{log_prefix} | DataFrame vacío - DataFrame sin registros - Proceso detenido")

    # Crea una copia del DataFrame    
    df = df.copy()

    # Imprime DataFrame con columnas originales que serán estandarizadas   
    logging.info("%s[DF_SCHEMA] Columnas originales: %s", log_prefix, df.columns.tolist())

    # ================================================================================
    # 2. RENAME
    #    Si la columna "cliente_id" existe en el DataFrame renombrala a "id_cliente" 
    # ================================================================================
    
    if "cliente_id" in df.columns:
        logging.info(
        "%s[STEP][RENAME] 'cliente_id' encontrada. Renombrando a 'id_cliente'",
        log_prefix
        )
        df = df.rename(columns={"cliente_id": "id_cliente"})
    else:
        logging.warning("%s[STEP][RENAME] 'cliente_id' no encontrada. Columnas disponibles: %s", log_prefix, df.columns.tolist())

    #=============================================================================================
    # 3. CAST 
    #    Casting: (convertimos una variable de un tipo a otro) convirtiendo "id_cliente a Int64"
    #    Tracking: seguimiento guardamos el tipo original y el nuevo tipo de dato
    #==============================================================================================

    if "id_cliente" not in df.columns:
        raise ValueError(f"{log_prefix} No existe columna id_cliente - Proceso Detenido")
    
    col = "id_cliente"
    
    # Guardar original
    col_original = df[col].copy()
    
    logging.info("%s[STEP][CAST][STARTED] Convirtiendo 'id_cliente' a Int64...", log_prefix)

    # CAST
    df[col] = pd.to_numeric(col_original, errors="coerce").astype("Int64")

    # Tipo de datos antes y después
    tipoDato_anterior = str(col_original.dtype)
    tipoDato_nuevo = str(df[col].dtype)

    # =============================================================================
    # 3. VALIDACIONES CRÍTICAS DESPUÉS DEL CAST
    #    Valida si la columna "id_cliente" contiene nulos
    # =============================================================================
    
    #  Validación de errores de conversión (no casteados)
    if (col_original.notna()&(col_original.astype(str).str.strip() != "")&df[col].isna()).any():
        raise ValueError("Existen valores no casteados  - Proceso Detenido")
    
    # Valida si la columna "id_cliente" contiene Nulos  
    if df["id_cliente"].isna().any():
        raise ValueError("Existen id_cliente NULL - Proceso detenido")

    # Valida si la columna "id_cliente" contiene duplicados
    if df["id_cliente"].duplicated().any():
        raise ValueError("Existen id_cliente duplicados - Proceso Detenido")     

    # Imprime el tipo de dato original y el nuevo tipo de dato     
    logging.info(
        "%s[STEP][CAST][RESULT] %s | tipoDato_anterior: %s | tipoDato_nuevo: %s",
        log_prefix,
        col,
        tipoDato_anterior,
        tipoDato_nuevo
    )

    # ==============================
    # 5. MÉTRICAS
    # ==============================
    
    # Total filas estandarizadas
    total_rows = len(df)

    # Cálcula total de nulos
    nulls = df[col].isna().sum()

    # Cálcula total de registros no convertidos
    invalid_cast = (
        col_original.notna() &
        (col_original.astype(str).str.strip() != "") &
        df[col].isna()
    ).sum()

    # Calcula duplicados
    duplicate = (
        df["id_cliente"].duplicated()
        ).sum()  

    duplicate= int(duplicate)

    # Porcentajes de nulos y no converitdos
    nulls_pct = (nulls / total_rows) * 100 if total_rows > 0 else 0
    invalid_cast_pct = (invalid_cast / total_rows) * 100 if total_rows > 0 else 0

    # Log
    logging.info(
        "%s[VALIDATION] %s | nulls: %s (%.2f%%) | Duplicados: %s | No convertidos: %s (%.2f%%)",
        log_prefix,
        col,
        nulls,
        nulls_pct,     
        duplicate,     
        invalid_cast,
        invalid_cast_pct
    )
    
    # Si todos son nulos se detiene el proceso
    if df["id_cliente"].isna().all():
        raise ValueError(f"{log_prefix} Todos los id_cliente son NULL después del cast - Se detiene el proceso")

    return df[["id_cliente"]]

# FUNCIÓN VALIDACIÓN DE INTEGRIDAD: revisa la calidad básica de los datos de un DataFrame antes de continuar el proceso ETL---------------
# realiza la validación mínima para garantizar que las métricas MVP no fallen
 
def validar_integridad_basica(df, source, run_id, stage):
    
    PIPELINE_ID = "OMNICHANNEL_ETL"
    stage = "TRANSFORM"
    source = source.upper()
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]"  
    
    #=======================================
    # VALIDACIÓN ESTRUCTURAL
    #=======================================    
    
    # Revisa que el DataFrame exista
    if df is None:
        raise ValueError(f"{log_prefix} | DataFrame no cargado - Falló extracción - Proceso detenido")
    
    # Revisa que el DataFrame contenga columnas
    if df.columns.size == 0:
        raise ValueError(f"{log_prefix} DataFrame sin columnas - Proceso detenido")
    
    # Revisa que el DataFrame contenga datos
    if df.empty:
        raise ValueError(f"{log_prefix} | DataFrame vacío - DataFrame sin registros - Proceso detenido")

    # Crea una copia del DataFrame    
    df = df.copy()
          
    # Define las columnas críticas / son los campos esenciales para el análisis o negocio
    columnas_criticas = [
        "id_cliente",
        "fecha_transaccion",
        "monto",
        "canal_origen",
        'tipo_transaccion'
    ]

    # ============================ DETECCIÓN DE COLUMNAS ============================
    columnas_presentes = [col for col in columnas_criticas if col in df.columns]
    columnas_faltantes = [col for col in columnas_criticas if col not in df.columns]

    if not columnas_presentes:
        raise ValueError(f"{log_prefix} No hay columnas críticas presentes. No se puede validar | Proceso detenido.")
    
    if columnas_faltantes:
         raise ValueError(f"{log_prefix} Columnas críticas faltantes:{columnas_faltantes} | Proceso detenido.")

    #======================================
    # VALIDACIÓN DE DATOS
    #======================================

    # Crea un diccionario para guardar los resultados / aquí se almacenarán las métricas de validación.
    resultados = {}

    # Calcula valores nulos en esas columnas / cuenta cuántos valores faltan en cada columna crítica.
    nulos = df[columnas_presentes].isnull().sum()

    # Porcentajes de nulos    
    porcentaje_nulos = (df[columnas_presentes].isnull().mean() * 100).round(2)
 
    # Detecta duplicados exactos / cuenta cuántas filas idénticas existen en el DataFrame.
    duplicados = df.duplicated().sum()

    # 1 Validación de tipos
    # Obtiene los tipos de datos / muestra el tipo de cada columna (int, float, string, datetime, etc.).
    tipos = df.dtypes  

    # Guarda los resultados
    resultados["nulos_criticos"] = nulos
    resultados["tipos_datos"] = tipos
    resultados["duplicados_exactos"] = duplicados
    resultados["porcentaje_nulos"] = porcentaje_nulos
    resultados["columnas_faltantes"] = columnas_faltantes

    # Convetir diccionarios a Datafreame
    print("\n Convertir diccionarios a DataFrame\n")
    df_nulos = pd.DataFrame(list(nulos.items()), columns=["COLUMNA", "NULOS"])
    df_tipos = pd.DataFrame([(col, tipos[col]) for col in columnas_presentes], columns=["COLUMNA", "TIPO_DATO"])
    df_porcentaje = pd.DataFrame(list(porcentaje_nulos.items()), columns=["COLUMNA", "%_NULOS"])

    # Unir tablas por columna
    reporte = df_nulos.merge(df_tipos, on="COLUMNA", how="outer")
    reporte = reporte.merge(df_porcentaje, on="COLUMNA", how="outer")
    reporte = reporte[["COLUMNA","NULOS","%_NULOS","TIPO_DATO"]]
   
    tabla = tabulate(
        reporte.astype(str),
        headers="keys",
        tablefmt="psql",
        showindex=False
    )

    nulos_totales = df[columnas_presentes].isnull().sum().sum()
    
    if nulos_totales == 0 and duplicados == 0 and not columnas_faltantes:
        estado = "OK"
    else:
        estado = "REVISAR"
    
    if estado == "REVISAR":
        logging.error("%s Fallo en calidad de datos", log_prefix)
        logging.warning("%s Columnas críticas faltantes: %s", log_prefix, columnas_faltantes)
        raise ValueError("Fallo en calidad de datos: revisar dataset")
  
    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Calculando Filas Totales en dataset...")
    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Filas totales del dataset: {len(df)}\n")   

    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Calculando Duplicados Exactos en dataset...")
    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Duplicados exactos en dataset: {duplicados}\n")

    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Calculando Nulos Totales en columnas críticas...")
    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Nulos totales en columnas críticas: {nulos_totales}\n")

    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Calculando Columnas faltantes...")
    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Columnas faltantes: {columnas_faltantes}\n")   

    logging.info(f"{log_prefix}[STEP][TRANSACCIONES] Estado de calidad de datos: {estado}\n\n")

    logging.info(f"{log_prefix}[STEP][TRANSACCIONES][SCHEMA][RESULT] Resultados de la validación básica:\n\n{tabla}\n")
   
    # Devuelve los resultados / La función retorna un diccionario con las métricas de calidad de datos-
    # para que puedan revisarse o usarse en validaciones posteriores.
    
    return resultados


# FUNCIÓN DE VALIDACIÓN DE INTEGRIDAD REFERENCIAL: Verifica que todos los id_cliente en transacciones-
# existan en la tabla clientes / Verificar que todas las transacciones tengan un cliente válido / integridad referencial
#
#def validar_integridad_referencial(df_trans, df_clientes, fuente_db, tabla_trans, run_id):
def validar_integridad_referencial(df_trans, df_clientes, run_id, fuente_db, tabla_trans):

    # Parámetros:
    #
    #   * df_trans → DataFrame de transacciones
    #   * df_clientes → DataFrame de clientes
    #   * fuente → nombre del sistema o fuente de datos (ej. api, csv, banco)
    #   * tabla_trans → nombre de la tabla de transacciones

    PIPELINE_ID = "OMNICHANNEL_ETL"
    stage = "TRANSFORM"
    substage = "REFERENTIAL_INTEGRITY"
    source = fuente_db.upper()    
    
    log_prefix = f"[{run_id}][{PIPELINE_ID}][{source}][{stage}][{substage}]"  

    # =========================
    # Copias defensivas
    # =========================
    df_trans = df_trans.copy()
    df_clientes = df_clientes.copy()

    #=========================================================
    # VALIDACIÓN ESTRUCTURAL
    #=========================================================

    #   Revisa si existen los DataFrame
    if df_trans is None or df_clientes is None:
        raise ValueError(
            "%s %s | DataFrame No cargafo - Falló de extracción - Proceso detenido ",
        log_prefix,
        tabla_trans)
        
    #   Revisa que los DataFrames tengan columnas
    if df_trans.columns.size == 0 or df_clientes.columns.size == 0:
        raise ValueError(
            "%s %s | DataFrame sin columnas - Proceso detenido ",
        log_prefix,
        tabla_trans)

    #   Veficicar si los dataframes estan vacios                      
    if df_trans.empty or df_clientes.empty:
         raise ValueError(
            "%s %s | DataFrames vacios - DataFrame sin registros - Proceso detenido ",
        log_prefix,
        tabla_trans)

    # =========================
    # Validación de columnas
    # =========================
    cols_requeridas_trans = {"id_cliente", "monto"}
    col_requeridas_trans = {"id_cliente"}

    trans_faltantes = cols_requeridas_trans - set(df_trans.columns)
    clientes_faltantes = col_requeridas_trans - set(df_trans.columns)

    if trans_faltantes:
        raise ValueError(f"{log_prefix} Columnas faltantes en df_trans: {trans_faltantes}")
    if clientes_faltantes:
        raise ValueError(f"{log_prefix} Columnas faltantes en df_clientes: {clientes_faltantes}")

    #===========================================================
    # NORMALIZACIÓN DE TIPOS (NUMÉRICO - PK)
    #===========================================================

    # Asegurar tipo homogéneo del ID / convierte la columna cliente_id a número   
    logging.info(f"{log_prefix}[STARTED] Convirtiendo a 'numeric' id_cliente...")
    df_clientes["id_cliente"] = pd.to_numeric(df_clientes["id_cliente"], errors="coerce").astype("Int64")    
    df_trans["id_cliente"] = pd.to_numeric(df_trans["id_cliente"], errors="coerce").astype("Int64")
    logging.info(f"{log_prefix}[SUCCESS] Conversión completada.")
    
    logging.info(f"{log_prefix}[STARTED] Convirtiendo a 'numeric' monto...")
    df_trans["monto"] = pd.to_numeric(df_trans["monto"], errors="coerce")
    logging.info(f"{log_prefix}[SUCCESS] Conversión completada.")

    #=============================================================
    # VALIDACIONES DE CALIDAD
    #=============================================================

    # IDs nulos en transacciones
    invalidos_id = df_trans[df_trans["id_cliente"].isna()]

    # Montos invalidos
    invalidos_monto = df_trans[(df_trans["monto"].isna()) | (df_trans["monto"] == 0)]

    # =========================================================
    # VALIDACIÓN REFERENCIAL (FK)
    # =========================================================

    # Obtener conjunto de clientes válidos
    # df_clientes["cliente_id"] - obtiene la columna de IDs
    # .dropna() - Elimina los valores nulos
    # set() - convierte a conjunto
    # Los sets permiten búsquedas muy rápidas.
    clientes_validos = set(df_clientes["id_cliente"].dropna().unique())

    # Obtener IDs de clientes en transacciones
    # Extrae la columna id_cliente de las transacciones.
    ids_trans = df_trans["id_cliente"]   

    # Detectar transacciones huérfanas
    # ids_trans.isin(clientes_validos) - verifica si cada ID está en el set de clientes - [True, True, False, True]
    # ~ - invierte el resultado -[False, False, True, False]
    huerfanos_mask = ~ids_trans.isin(clientes_validos)
    # Filtrado final - Obtiene solo los IDs que NO existen en clientes - [999] - Estos son registros huérfanos.
    huerfanos = df_trans[huerfanos_mask & ids_trans.notna()]
    
    #===========================================================
    # MÉTRICAS DE CALIDAD
    #===========================================================

    total = len(df_trans)
    
    n_invalidos_id = len(invalidos_id)
    n_invalidos_monto = len(invalidos_monto)
    n_huerfanos = len(huerfanos)

    pct_huerfanos = (n_huerfanos / total * 100) if total > 0 else 0.0

    logging.info(
        f"{log_prefix} Métricas de calidad -> Total: {total} | "
        f"ID nulos: {n_invalidos_id} | "
        f"Monto inválido: {n_invalidos_monto} | "
        f"Huérfanos: {n_huerfanos} | "
        f"% Huérfanos: {pct_huerfanos:.2f}%"
    )

     

    # Si hay huérfanos
    # Muestra: 
    #   advertencia
    #   IDs problemáticos
    # FUENTE | TABLA - IDs huérfanos detectados: X
    # API | transacciones - IDs huérfanos detectados: 4
    # IDs huérfanos detectados: [999, 888, 777]
    if n_huerfanos > 0:
        raise ValueError(
            "%s IDs huérfanos detectados: %s | Proceso detenido",
            log_prefix,
            huerfanos.unique()[:10]
        )

    # Cierre del bloque
    #logging.info("============================================")
    return None

# FUNCIÓN VALIDACIÓN DE REGLAS DE NEGOCIO: validaciones de negocio;
#   1. Montos negativos
#   2. Montos inválidos
#   3. Fechas futuras

def validar_reglas_negocio(df, fuente_db, run_id, tabla_trans):
   
    PIPELINE_ID = "OMNICHANNEL_ETL"
    stage = "TRANSFORM"
    source = fuente_db.upper()
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{source}][{stage}]" 

    #=========================================================
    # VALIDACIÓN ESTRUCTURAL
    #=========================================================
        
    # Revisa que el DataFrame exista
    if df is None:
        logging.warning(
            f"{log_prefix} | {fuente_db.upper()} | {tabla_trans} | DataFrame no cargado - Falló extracción."
        )
        return None
    
    # Revisa que el DataFrame contenga columnas
    if df.columns.size == 0:
        logging.warning(
            f"{fuente_db.upper()} | {tabla_trans} | DataFrame sin columnas."
        )
        return None
    
    # Validar si el Dataframe esta vacío    
    if df.empty:
        # Escribe una advertencia en el log indicando que no se puede validar.
        logging.warning(
            f"{log_prefix} | {fuente_db.upper()} | {tabla_trans} - DataFrame vacío. No se valida negocio."
        )
        return None
  
    #=================================================
    # Validación de Datos
    #=================================================

    # Obtiene el número total de filas del DataFrame.
    total_registros = df.shape[0]

    # Detectar montos negativos:
    # Filtra registros donde: monto < 0
    negativos = df[df["monto"] < 0]
    # Cuenta cuántos registros tienen monto negativo.
    cant_negativos = negativos.shape[0]

    # Detectar montos inválidos (0 o NaN)
    # Filtra registros donde el monto es: NaN ó 0
    invalidos = df[(df["monto"].isna()) | (df["monto"] == 0)]
    
    #Cuenta cuántos montos inválidos hay.
    cant_invalidos = invalidos.shape[0]

    # Detectar fechas futuras
    # Obtiene la fecha y hora actual.
    hoy = pd.Timestamp.now()
    futuras = df[df["fecha_transaccion"] > hoy]
    # Filtra registros donde:fecha_transaccion > hoy - Es decir, transacciones del futuro.
    # Cuenta cuántas fechas futuras hay.
    cant_futuras = futuras.shape[0]

    # Log total de registros
    # FUENTE | TABLA - Total registros: X    
    
    logging.info(f"{log_prefix} Total registros: {total_registros}")
    #logging.info(
    #    "%s | %s - Total registros: %s",
    #    fuente_db.upper(),
    #    tabla,
    #    total_registros
    #)

    # Log montos negativos
    # Registra cuántos montos negativos existen.
    logging.info(f"{log_prefix} Cantidad de negativos: {cant_negativos}")
    #logging.info(
    #    "%s | %s - Montos negativos: %s",
    #    fuente_db.upper(),
    #    tabla,
    #    cant_negativos
    #)
    # Log montos inválidos
    # Registra cuántos montos 0 o NaN hay.
    logging.info(f"{log_prefix} Cantidad de invalidos: {cant_invalidos}")
    #logging.info(
    #    "%s | %s - Montos inválidos (0 o NaN): %s",
    #    fuente_db.upper(),
    #    tabla,
    #    cant_invalidos
    #)
    # Log fechas futuras
    # Muestra cuántas transacciones tienen fecha en el futuro. 
    logging.info(f"{log_prefix} Cantidad de fechas futuras: {cant_futuras}\n")
    #logging.info(
    #    "%s | %s - Fechas futuras: %s",
    #    fuente_db.upper(),
    #    tabla,
    #    cant_futuras
    #)
    # Mostrar ejemplos de errores
    # Si existen montos negativos:
    if cant_negativos > 0:
        raise ValueError(
            "%s Montos negativos:\n%s",
            log_prefix,
            negativos.head(), "- Proceso Detenido"
        )
    # Si existen montos inválidos:
    if cant_invalidos > 0:
        raise ValueError(f"{log_prefix} Montos inválidos:\n%s", invalidos.head(), "- Proceso Detenido")
    # Si hay fechas futuras:
    if cant_futuras > 0:
        raise ValueError(f"{log_prefix} Fechas futuras:\n%s\n", futuras.head(), "- Proceso Detenido")
    
    # Imprime una línea final para cerrar la sección del log.
    # logging.info("===============================================")

    return None
#=======================================================================================
# Integration Layer / Consolidación Omnicanal
#=======================================================================================

def consolidar_transacciones(dataframes_std, run_id):

    PIPELINE_ID = "OMNICHANNEL_ETL"
    stage = "TRANSFORM"
    #source = fuente_db.upper()
    log_prefix=f"[{run_id}][{PIPELINE_ID}][{stage}]" 
    datasets = []

    # Crea un mapa de motores asigandales un "id_canal"
    mapping_canales = {
        "sqlserver": 1,   # Mobile App
        "postgresql": 2,  # Web Banking
        "mysql": 3,       # ATM
        "csv": 4          # Branch
    }
    
    for motor_db, tablas in dataframes_std.items():

        for tabla, df in tablas.items():

            # Filtra DataFrames vacíos:
            # Esto evita procesar datos sin contenido, No valida calidad, solo existencia
            if df.empty:
                continue

            df = df.copy()

            # Asignación de canal - Esto sí es una regla de negocio, porque:-Traduce el origen técnico → significado de negocio (canal)
            # mapping_canales es un diccionario que relaciona cada fuente (motor_db) con un número (por ejemplo: "sqlserver" → 1, "postgresql" → 2, etc.).
            # mapping_canales.get(motor_db) busca el valor correspondiente al motor actual.
            # df["id_canal"] = ... crea (o sobrescribe) la columna id_canal en el DataFrame y le asigna ese valor a todas las filas.
            df["id_canal"] = mapping_canales.get(motor_db)

            datasets.append(df)

    if not datasets:
        logging.warning("No hay datasets para consolidar", log_prefix)
        return pd.DataFrame()

    df_final = pd.concat(datasets, ignore_index=True, sort=False)
    logging.info(        
        "Dataset consolidado creado | Filas: %s",
       
        df_final.shape[0]
    )

    return df_final


#=================================================================================================================================
# FASE LOAD - DATA WAREHOUSE (REFactor)
#=================================================================================================================================

def load_dwh(dim_clientes, dim_canal, fact_transacciones, run_id):

    import logging
    import pandas as pd
    from sqlalchemy import text, inspect
    from sqlalchemy.types import Integer, String, Float, DateTime

    stage = "LOAD"
    PIPELINE_ID = "OMNICHANNEL_ETL"
    log_prefix = f"[{run_id}][{PIPELINE_ID}][PIPELINE][{stage}]"

    logging.info(f"{log_prefix}[STARTED] Iniciando carga a DWH...")

    # =========================================
    # 1. CONEXIÓN
    # =========================================
    try:
        engine = engine_sqlserver(run_id, stage)
        inspector = inspect(engine)
    except Exception as e:
        logging.error(f"{log_prefix} Error creando conexión: {e}")
        return

    # =========================================
    # 2. VALIDACIONES PREVIAS
    # =========================================
    for name, df in {
        "dim_clientes": dim_clientes,
        "dim_canal": dim_canal,
        "fact_transacciones": fact_transacciones
    }.items():
        if df is None or df.empty:
            raise ValueError(f"{log_prefix} {name} vacío o no definido")

    # =========================================
    # 3. NORMALIZACIÓN DE COLUMNAS
    # =========================================
    dim_clientes.columns = dim_clientes.columns.str.strip()
    dim_canal.columns = dim_canal.columns.str.strip()
    fact_transacciones.columns = fact_transacciones.columns.str.strip()

    # Estándar final
    dim_clientes = dim_clientes.rename(columns={"id_cliente": "cliente_id"})
    dim_canal = dim_canal.rename(columns={"id_canal": "canal_id"})
    fact_transacciones = fact_transacciones.rename(columns={
        "id_cliente": "cliente_id",
        "id_canal": "canal_id"
    })

    # =========================================
    # 4. TIPOS CONSISTENTES (CRÍTICO)
    # =========================================
    dim_clientes["cliente_id"] = pd.to_numeric(dim_clientes["cliente_id"], errors="coerce").astype("Int64")
    dim_canal["canal_id"] = pd.to_numeric(dim_canal["canal_id"], errors="coerce").astype("Int64")

    fact_transacciones["cliente_id"] = pd.to_numeric(fact_transacciones["cliente_id"], errors="coerce").astype("Int64")
    fact_transacciones["canal_id"] = pd.to_numeric(fact_transacciones["canal_id"], errors="coerce").astype("Int64")

    # =========================================
    # 5. DATA QUALITY
    # =========================================
    if dim_clientes["cliente_id"].isna().any():
        raise ValueError(f"{log_prefix} Nulls en cliente_id (dim_clientes)")

    if fact_transacciones["cliente_id"].isna().any():
        raise ValueError(f"{log_prefix} Nulls en cliente_id (fact)")

    # =========================================
    # 6. SURROGATE KEY FACT
    # =========================================
    fact_transacciones = fact_transacciones.reset_index(drop=True)
    fact_transacciones["id_fact"] = fact_transacciones.index + 1

    # =========================================
    # 7. DROP TABLAS (IDEMPOTENTE)
    # =========================================
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('dbo.fact_transacciones', 'U') IS NOT NULL DROP TABLE dbo.fact_transacciones"))
        conn.execute(text("IF OBJECT_ID('dbo.dim_clientes', 'U') IS NOT NULL DROP TABLE dbo.dim_clientes"))
        conn.execute(text("IF OBJECT_ID('dbo.dim_canal', 'U') IS NOT NULL DROP TABLE dbo.dim_canal"))

    # =========================================
    # 8. CARGA CON TIPOS EXPLÍCITOS
    # =========================================
    
    dim_clientes.to_sql(
        "dim_clientes",
        engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        dtype={"cliente_id": Integer()}
    )

    dim_canal.to_sql(
        "dim_canal",
        engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        dtype={
            "canal_id": Integer(),
            "canal_nombre": String(50)
        }
    )

    fact_transacciones.to_sql(
        "fact_transacciones",
        engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        dtype={
            "id_fact": Integer(),
            "cliente_id": Integer(),
            "canal_id": Integer(),
            "fecha_transaccion": DateTime(),
            "monto": Float(),
            "tipo_transaccion": String(50)
        }
    )

    # =========================================
    # 9. CONSTRAINTS (PK / FK)
    # =========================================
    with engine.begin() as conn:

        # 🔥 FIX CLAVE
        conn.execute(text("""
            ALTER TABLE dbo.dim_clientes 
            ALTER COLUMN cliente_id INT NOT NULL
        """))        
        
        # PKs
        conn.execute(text("ALTER TABLE dbo.dim_clientes ADD CONSTRAINT pk_dim_clientes PRIMARY KEY (cliente_id)"))
        conn.execute(text("ALTER TABLE dbo.dim_canal ADD CONSTRAINT pk_dim_canal PRIMARY KEY (canal_id)"))
        conn.execute(text("ALTER TABLE dbo.fact_transacciones ADD CONSTRAINT pk_fact PRIMARY KEY (id_fact)"))

        # FKs
        conn.execute(text("""
            ALTER TABLE dbo.fact_transacciones
            ADD CONSTRAINT fk_fact_clientes
            FOREIGN KEY (cliente_id)
            REFERENCES dbo.dim_clientes(cliente_id)
        """))

        conn.execute(text("""
            ALTER TABLE dbo.fact_transacciones
            ADD CONSTRAINT fk_fact_canal
            FOREIGN KEY (canal_id)
            REFERENCES dbo.dim_canal(canal_id)
        """))

    # =========================================
    # 10. ÍNDICES PARA BI
    # =========================================
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX idx_fact_cliente ON dbo.fact_transacciones(cliente_id)"))
        conn.execute(text("CREATE INDEX idx_fact_canal ON dbo.fact_transacciones(canal_id)"))
        conn.execute(text("CREATE INDEX idx_fact_fecha ON dbo.fact_transacciones(fecha_transaccion)"))

    # =========================================
    # 11. LOG FINAL
    # =========================================
    logging.info(f"{log_prefix}[SUCCESS] DWH cargado correctamente")
    logging.info(f"{log_prefix} Filas fact: {fact_transacciones.shape[0]}")
    logging.info(f"{log_prefix} Clientes únicos: {dim_clientes.shape[0]}")
    logging.info(f"{log_prefix} Canales: {dim_canal.shape[0]}")

#==================================================================================================================================
# MAIN ETL (ORQUESTADOR)
#=================================================================================================================================

def main():    
    
    # Indentidficador del PIPELINE ETL
    PIPELINE_ID = "OMNICHANNEL_ETL"
    
    # Fecha del PIPELINE ETL - Permite trazabilidad completa del pipeline mediante logs estructurados.
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    stage = "INFRASTRUCTURE"

    # Prefijo de log de PIPELINE para la fase de EXTRACCIÓN
    pipeline_prefix = f"[{run_id}][{PIPELINE_ID}][PIPELINE][{stage}]"

    #------------------------------------------------------------------------------------------------------------------------------------
    # FASE DE INFRAESTRUCTURA - CONEXIÓN A DBMS POR MEDIO DE ENGINES
    #------------------------------------------------------------------------------------------------------------------------------------

    logging.info("====================================================================================================================================")
    logging.info(f"{pipeline_prefix}[STARTED] Inicializando Engines...\n")

    start_time = time.perf_counter()

    engines = {
        "sqlserver": engine_sqlserver(run_id, stage),
        "mysql": engine_mysql(run_id, stage),
        "postgresql": engine_postgresql(run_id, stage),
        "csv": None
    } 

    end_time = time.perf_counter()
    duration_ms = (end_time - start_time) * 1000
    
    logging.info(f"{pipeline_prefix}[SUCCESS] Inicialización de Engines completada | duration_ms: {duration_ms:.2f}")

    logging.info("====================================================================================================================================\n")

    #----------------------------------------------------------------------------------------------------------------------------------
    # FASE DE EXTRACCIÓN
    #----------------------------------------------------------------------------------------------------------------------------------

    logging.info("====================================================================================================================================")
    
    # La Staging Layer (capa de preparación) o staging area es un área de almacenamiento intermedio utilizado en procesos ETL -
    # (Extracción, Transformación y Carga) para recopilar datos de diversas fuentes antes de cargarlos en un almacén de datos -
    # (Data Warehouse). Actúa como un búfer para limpiar, estandarizar y formatear datos rápidamente sin ralentizar los sistemas -
    # operativos origen

    # ///DFS CRUDOS - STAGNING LAYER///
    
    dataframes_raw = {} # DataFrames Crudos     

    # Indetidicador de fase "FASE DE EXTRACCIÓN"
    stage = "EXTRACT"
    
    # Prefijo de log de PIPELINE para la fase de EXTRACCIÓN
    pipeline_prefix = f"[{run_id}][{PIPELINE_ID}][PIPELINE][{stage}]"
    logging.info(f"{pipeline_prefix}[STARTED] Inicializando Fase de Extracción...\n")
    start_time = time.perf_counter()

    total_filas = 0

    # TABLAS es un diccionario de datos que guarda los de las tablas por cada motor de base de datos
    # TABLAS.items(): Se obtiene un iterable con pares (clave, valor):
    for motor_db, tablas in TABLAS.items():

        # Guarda la conexión a la base de datos por cada motor
        engine = engines[motor_db]
        # Guarda los datos de las tablas provenientes de las funciones que contienen los querys 
        extractor = EXTRACTORES[motor_db] 
        
        for tabla in tablas:
            try: 
                df = extractor(engine, tabla) # Conexión y DataFrames por cada DBMS
                # Si motor_db ya existe en dataframes_raw, devuelve su valor.
                # Si NO existe, lo crea con {} (un diccionario vacío) y lo devuelve.
                # Estás guardando el DataFrame df dentro de ese diccionario, usando tabla como clave.
                dataframes_raw.setdefault(motor_db, {})[tabla] = df # DATAFRAMES CRUDOS - STAGNING - PREPARACIÓN

                filas = df.shape[0] # → Número de filas
                total_filas += filas # Suma todas las filas de todos los DataFrames   

                # Imprime el nombre de las tablas y su total de filas
                logging.info(
                    "[%s][%s][%s][EXTRACT][SUCCESS] Tabla: %s | Filas: %s\n",
                    run_id,
                    PIPELINE_ID,
                    motor_db.upper(),
                    tabla,
                    filas
                )

            except Exception as e:
                logging.error(
                    "[%s][%s][%s][EXTRACT] Error en  Tabla: %s : %s\n",
                    run_id,
                    PIPELINE_ID,
                    motor_db.upper(),
                    tabla,
                    e
                )

    end_time = time.perf_counter()
    duration_ms = (end_time - start_time) * 1000
   
    logging.info(f"{pipeline_prefix}[SUCCESS] Inicialización Fase de Extracción completada | duration_ms: {duration_ms:.2f}")
    
    logging.info("====================================================================================================================================\n")
#----------------------------------------------------------------------------------------------------------------------------------
# FASE DE TRANSFORMACIÓN
#----------------------------------------------------------------------------------------------------------------------------------
    logging.info("====================================================================================================================================")    
    
    # Indetidicador de fase "FASE DE TRANSFORMACIÓN"
    stage = "TRANSFORM"

    # ///DFS ESTANDARIZADOS - Transform Phase/// 
    dataframes_std = {}    

    # Prefijo de log de PIPELINE para la fase de TRANSFORMACIÓN
    pipeline_prefix = f"[{run_id}][{PIPELINE_ID}][PIPELINE][{stage}]"
    logging.info(f"{pipeline_prefix}[STARTED] Inicializando Fase de Transformación...\n")   

    # Mapeo de tablas para la estandarización de tipos de datos
    
    mapeos = {
        "sqlserver": mapeo_sqlserver,
        "mysql": mapeo_mysql,
        "postgresql": mapeo_postgresql,
        "csv": mapeo_csv
    }

    # ====================================================================================================================================
    # PASO 1: ESTANDARIZACIÓN DE CLIENTES.
    # Realiza la normalización del identificador único de clientes antes de integrarlo en otras fuentes.
    # ====================================================================================================================================
    
    pipeline_prefix = f"[{run_id}][{PIPELINE_ID}][PIPELINE][{stage}]"
    
    logging.info("------------------------------------------------------------------------------------------------------------------------------------")
    
    clientes_std = {}
    
    logging.info(f"{pipeline_prefix}[STEP_ONE][CLIENTES][STARTED] Inicializando Estandarización de clientes...\n")   
    
    start_total_time = time.perf_counter()  
    
    for motor_db, tabla_cliente in TABLAS_CLIENTES.items():
          
        df_cliente_raw = dataframes_raw.get(motor_db, {}).get(tabla_cliente) # DATAFRAMES CRUDOS - STAGNING - PREPARACIÓN

        # Si el DataFrame esta vacío o no existe
        if df_cliente_raw is None:
            raise ValueError(f"{motor_db.upper()} | DataFrame no cargado - Falló extracción - Proceso detenido")
            
        if df_cliente_raw.empty:
            raise ValueError(f"{motor_db.upper()} | DataFrame vacío - DataFrame sin registros - Proceso detenido")
            
        start_time = time.perf_counter()       
        
        # Llamamos a la función "estandariza_clientes"
        df_cliente_std = estandariza_clientes(df_cliente_raw, motor_db, run_id, stage)
        
        end_time = time.perf_counter()
        
        duration_ms = (end_time - start_time) * 1000
        
        CONFIRMACION="SUCCESS"

        # “Guarda el DataFrame de clientes ya limpio/estandarizado bajo la llave del motor actual”
        clientes_std[motor_db] = df_cliente_std
        
        # Imprime por motor_db el total de filas de clientes estandarizadas
        logging.info(
            "[%s][%s][%s][%s][%s] Clientes estandarizados | Filas: %s | duration_ms: %.2f\n",
            run_id,
            PIPELINE_ID,
            motor_db.upper(),            
            stage,
            CONFIRMACION,
            df_cliente_std.shape[0],
            duration_ms
        )
    end_total_time = time.perf_counter()
    duration_ms = (end_total_time - start_total_time) * 1000
    logging.info(f"{pipeline_prefix}[STEP][CLIENTES][SUCCESS] Inicialización de Estandarización de clientes completada | duration_ms: {duration_ms:.2f}") 
    logging.info("------------------------------------------------------------------------------------------------------------------------------------\n")
    
    # ======================================
    # ESTANDARIZACIÓN DE TRANSACCIONES
    # ======================================
            
    for motor_db, tablas in dataframes_raw.items():
        
        mapeo = mapeos[motor_db]

        logging.info("------------------------------------------------------------------------------------------------------------------------------------")        

        logging.info(
                "%s [STEP][TRANSACCIONES][STAGING][TRANSACCIONES-RAW] Inicializando almacenamiento RAW...\n",
                pipeline_prefix                
            )
        
        for tabla, df in tablas.items():
            
            start_time_t = time.perf_counter()
            
            #----------------------------------
            # Tablas en fase de Staging - RAW
            #----------------------------------
            
            # SE QUEDA PARA PRODUCCIÓN
            #logging.info(
            #    "%s [STEP][TRANSACCIONES][STAGING][TRANSACCIONES-RAW] Fuente: %s | Tabla: %s | Schema: %s",
            #    pipeline_prefix,
            #    motor_db.upper(),
            #    tabla,
            #    {col: str(dtype) for col, dtype in df.dtypes.items()}
            #)

            schema_str = "\n".join(
                [f"{col:20} | {dtype}" for col, dtype in df.dtypes.items()]
            )

            # Este es para DEBUGG
            logging.info(
                "%s[STAGING][TRANSACCIONES-RAW] Almacenamiento RAW:\nFuente: %s | Tabla: %s\n%s\n",
                pipeline_prefix,
                motor_db.upper(),
                tabla,
                schema_str
            )
            end_time_t = time.perf_counter()
            duration_ms_t = (end_time_t - start_time_t) * 1000
           
            if "transacciones" not in tabla and "csv" not in tabla:
                continue

            logging.info(
                "%s [STEP][TRANSACCIONES][STAGING][TRANSACCIONES-RAW] Inicialización almacenamiento RAW completada | %.2f ms \n",
                pipeline_prefix,
                duration_ms_t
            )
            logging.info("------------------------------------------------------------------------------------------------------------------------------------")            
            
            logging.info(f"{pipeline_prefix}[STEP][TRANSACCIONES][STARTED] Inicializando Estandarización de Transacciones...\n") 
            
            start_time_t = time.perf_counter()
            
            # ------------------------------
            # 1. ESTANDARIZAR ESQUEMA
            # ------------------------------            
            
            df = alinear_esquema_transacciones(df, run_id, mapeo, COLUMNAS_OBJETIVO, motor_db.upper())

            # ------------------------------..
            # 2. ESTANDARIZAR TIPOS Y REGLAS
            # --------------------------------

            if df.empty:
                raise ValueError(f"{pipeline_prefix}[STEP][TRANSACCIONES][STARTED]{motor_db} | {tabla} | DataFrame vacío - Proceso detenido)      ")
                            
            df_std = estandariza_transacciones(df, run_id, mapeo, motor_db)

            df = df_std    

            end_time_t = time.perf_counter()
            duration_ms_t = (end_time_t - start_time_t) * 1000
            
            logging.info(f"{pipeline_prefix}[STEP][TRANSACCIONES][SUCCESS] Inicialización de Estandarización de Transacciones completada | duration_ms: {duration_ms_t:.2f}.") 
            logging.info("--------------------------------------------------------------------------------------------------------------------------------------\n")
            # print(f"\n===== TRANS STD {motor_db.upper()} | {tabla} =====")
            # print(df_std.columns.tolist())
            # logging.info(f"DEBUG COLUMNAS STD2: {df_std.columns.tolist()}")
            # ------------------------------
            # VALIDACIONES
            # ------------------------------
            logging.info("--------------------------------------------------------------------------------------------------------------------------------------")
            logging.info(f"{pipeline_prefix}[TRANSACCIONES][STEP][STARTED] Inicializando Validación Básica de Integridad...\n") 
            
            start_time_ib = time.perf_counter()
            
            validar_integridad_basica(df_std, motor_db, run_id, stage)
                        
            end_time_ib = time.perf_counter()
            #logging.info(f"DEBUG COLUMNAS STD2: {df_std.columns.tolist()}")
            duration_ms_ib = (end_time_ib - start_time_ib) * 1000
           
            logging.info(f"{pipeline_prefix}[STEP][TRANSACCIONES][SUCCESS] Inicialización de Validación Básica de Integridad completada | duration_ms: {duration_ms:.2f}.\n") 
            logging.info("--------------------------------------------------------------------------------------------------------------------------------------\n")
            logging.info("--------------------------------------------------------------------------------------------------------------------------------------")    
            logging.info(f"{pipeline_prefix}[STEP][TRANSACCIONES][STARTED] Inicializando Validación de Reglas de Negocio...\n")     
           
            df_clientes = clientes_std.get(motor_db) 
            validar_reglas_negocio(df_std, motor_db, run_id, tabla)
            
            logging.info(f"{pipeline_prefix}[STEP][TRANSACCIONES][SUCCESS] Inicialización de Validación de Reglas de Negocio completada.")   
            logging.info("--------------------------------------------------------------------------------------------------------------------------------------\n")
            logging.info("--------------------------------------------------------------------------------------------------------------------------------------")
            # ------------------------------
            # INTEGRIDAD REFERENCIAL
            # ------------------------------

            logging.info(f"{pipeline_prefix}[STEP][TRANSACCIONES][STARTED] Inicializando Validación de Integridad Referencial...\n")   
            df_clientes = clientes_std.get(motor_db)
            if df_clientes is not None:
                validar_integridad_referencial(
                    df_std,
                    df_clientes,
                    run_id,
                    motor_db,
                    tabla
                )           

            #print("\n")
            #print(df_std.head(), motor_db.upper())
            #print("\n")   
            # ==============================
            # GUARDAR RESULTADO
            # ==============================

            dataframes_std.setdefault(motor_db, {})[tabla] = df_std
            #    logging.info(
            #    "TRANSFORMADO | %s | %s | Filas: %s",
            #    motor_db.upper(),
            #    tabla,
            #    df_std.shape[0])
            
    logging.info(f"{pipeline_prefix}[STEP][TRANSACCIONES][STARTED] Inicialización de Validación de Integridad Referencial completada.")
    logging.info("--------------------------------------------------------------------------------------------------------------------------------------\n")
    
    logging.info(f"{pipeline_prefix}[SUCCESS] Inicialización de Fase de Transformación completada...") 
    logging.info("======================================================================================================================================\n")        
    

    # ==============================
    # EXPLORACIÓN
    # ==============================

    logging.info("======================================================================================================================================")        
    pipeline_prefix_exp = f"[{run_id}][{PIPELINE_ID}][PIPELINE]"
    logging.info(f"{pipeline_prefix_exp} [EXPLORE] Exploración de datos transformados...\n")     

    for motor, tablas in dataframes_std.items():

        for nombre_tabla, df in tablas.items():

            logging.info("=====================================================")
            logging.info("FUENTE: %s | TABLA: %s", motor.upper(), nombre_tabla)
            logging.info("=====================================================")
            logging.info("Filas: %s | Columnas: %s", df.shape[0], df.shape[1])
            
            logging.info("\n\n[PREVIEW]%s\n", df.head())
            
            logging.info("\n\n[TIPOS DE DATOS]\n%s", df.dtypes)
            logging.info("\n\n[VALORES NULOS]\n%s", df.isnull().sum())
            logging.info("\n\n[CONTEO POR TIPO_TRANSACCION]%s\n", df["tipo_transaccion"].value_counts(dropna=False)
            )
    logging.info("======================================================================================================================================\n")        

    # ==============================
    # CONSOLIDACIÓN
    # ==============================

    logging.info("======================================================================================================================================")

    df_consolidado = consolidar_transacciones(dataframes_std, run_id)

    total_fuentes = len(dataframes_raw)
    total_tablas = sum(len(tablas) for tablas in dataframes_raw.values())
    total_filas = sum(df.shape[0] for tablas in dataframes_std.values() for df in tablas.values())
    
    total_registros = len(df_consolidado)
    total_errores = 0

    cant_negativos = (df_consolidado["monto"] < 0).sum()
    invalidos = df_consolidado[(df_consolidado["monto"].isna()) | (df_consolidado["monto"] == 0)]   
    cant_invalidos = invalidos.shape[0]
    fechas_futuras = (df_consolidado["fecha_transaccion"] > pd.Timestamp.now()).sum()
    total_errores = cant_negativos + cant_invalidos + fechas_futuras
    
    if total_registros > 0:
        dq_score = ((total_registros - total_errores) / total_registros) * 100
    else:
        dq_score = 0
    
    logging.info("Data Quality Score: %.2f%%", dq_score)

    df_consolidado = consolidar_transacciones(dataframes_std, run_id)
    
    logging.info(
    "[SUMMARY] Fuentes: %s | Tablas: %s | Filas: %s",
    total_fuentes,
    total_tablas,
    total_filas
    )
    
    # logging.info("[SUMMARY] Total tablas: %s | Total filas: %s", total_tablas, total_filas)
    pipeline_prefix_cons = f"[{run_id}][{PIPELINE_ID}][PIPELINE]"
    logging.info(f"{pipeline_prefix_cons}[CONSOLIDATE] Dataset Omnicanal Consolidado...\n")
       
    
    df_final = df_consolidado
    duplicados = df_final.duplicated(subset=["id_transaccion", "canal_origen"]).sum()  

    if duplicados > 0:
        logging.warning("[DATA QUALITY] Duplicados detectados: %s", duplicados)
    else:
        logging.info("[DATA QUALITY] Sin duplicados detectados")

    df_final = df_final.drop_duplicates(subset=["id_transaccion", "canal_origen"])
    logging.info("\n\nDATASET OMNICANAL PREVIEW\n%s\n\n", df_final.head())  
    #print(df_final.head())

    logging.info("======================================================================================================================================\n")

    #===========================================
    # FASE LOAD
    #===========================================

    try:
        dim_clientes = df_final[["id_cliente"]].drop_duplicates().reset_index(drop=True)

        dim_canal = pd.DataFrame({
            "id_canal": [1, 2, 3, 4],
            "canal_nombre": ["APP_ANDROID", "WEB", "ATM", "SUCURSAL"]
        })

        fact_transacciones = df_final.copy()

    except Exception as e:
        logging.error(f"Error creando dimensiones/fact: {e}")
        return
    
    load_dwh(dim_clientes, dim_canal, fact_transacciones, run_id)

    # =========================================
    # 1️⃣ Conexión al DWH
    # =========================================
    try:
        engine_dwh = engine_sqlserver(run_id, stage)
    except Exception as e:
        logging.error("Error creando engine DWH")
        return

    # =========================================
    # 2️⃣ Función para renombrar columnas seguras
    # =========================================
    def rename_column_safe(df, possible_names, new_name):
        """
        Busca entre posibles nombres y renombra la columna a new_name.
        Lanza error si no encuentra ninguna.
        """
        df.columns = df.columns.str.strip()  # eliminar espacios
        for col in possible_names:
            if col in df.columns:
                return df.rename(columns={col: new_name})
        raise ValueError(f"No se encontró ninguna columna entre {possible_names} para renombrar a {new_name}")

    # =========================================
    # 3️⃣ Renombrar columnas clave
    # =========================================
    dim_clientes = rename_column_safe(dim_clientes, ['id_cliente', 'cliente'], 'cliente_id')
    dim_canal = rename_column_safe(dim_canal, ['id_canal'], 'canal_id')
    fact_transacciones = rename_column_safe(fact_transacciones, ['id_cliente', 'cliente'], 'cliente_id')
    fact_transacciones = rename_column_safe(fact_transacciones, ['id_canal', 'canal'], 'canal_id')

    # =========================================
    # 4️⃣ Convertir tipos a enteros
    # =========================================
    dim_clientes['cliente_id'] = dim_clientes['cliente_id'].astype(int)
    dim_canal['canal_id'] = dim_canal['canal_id'].astype(int)
    fact_transacciones['cliente_id'] = fact_transacciones['cliente_id'].astype(int)
    fact_transacciones['canal_id'] = fact_transacciones['canal_id'].astype(int)

    print("Duplicados cliente_id:", dim_clientes['cliente_id'].duplicated().sum())
    print("Nulos cliente_id:", dim_clientes['cliente_id'].isna().sum())



    # =========================================
    # 5️⃣ Crear dimensiones si no existen
    # =========================================
    from sqlalchemy import inspect
    inspector = inspect(engine_dwh)

    for df, table, pk in [(dim_clientes, 'dim_clientes', 'cliente_id'), 
                      (dim_canal, 'dim_canal', 'canal_id')]:

        # 1️⃣ Crear tabla SIEMPRE primero y luego reemplazar
        df.to_sql(table, engine_dwh, schema="dbo", if_exists='replace', index=False)
        
        
        with engine_dwh.begin() as conn:

            # 🔧 Asegurar NOT NULL
            conn.execute(text(f"""
                ALTER TABLE dbo.{table}
                ALTER COLUMN {pk} INT NOT NULL
            """))

            # 🔑 Crear PK
            conn.execute(text(f"""
                ALTER TABLE dbo.{table}
                ADD CONSTRAINT pk_{table} PRIMARY KEY ({pk})
            """))

            logging.info(f"PK creada correctamente en {table} ✅")

            # =========================================
            # 2️⃣ PREPARAR FACT (SURROGATE KEY)   
            # =========================================
            fact_transacciones.reset_index(drop=True, inplace=True) 
            fact_transacciones["id_fact"] = fact_transacciones.index + 1

            # =========================================
            # 3️⃣ CREAR FACT
            # =========================================
            with engine_dwh.begin() as conn:

                fact_transacciones.to_sql(
                    'fact_transacciones',
                    conn,
                    schema="dbo",
                    if_exists='replace',
                index=False
                )

                # 🔧 NOT NULL
                conn.execute(text("""
                    ALTER TABLE dbo.fact_transacciones
                    ALTER COLUMN id_fact INT NOT NULL
                """))

                # 🔑 PK FACT
                conn.execute(text("""
                    ALTER TABLE dbo.fact_transacciones
                    ADD CONSTRAINT pk_fact PRIMARY KEY (id_fact)
                """))

                logging.info("Tabla fact_transacciones creada con PK surrogate ✅")


        # =========================================
        # 4️⃣ CREAR FOREIGN KEYS
        # =========================================
        with engine_dwh.begin() as conn:

        # FK → clientes
            conn.execute(text("""
                ALTER TABLE dbo.fact_transacciones
                ADD CONSTRAINT fk_fact_clientes
                FOREIGN KEY (cliente_id)
                REFERENCES dbo.dim_clientes(cliente_id)
            """))

            logging.info("FK clientes creada ✅")

        # FK → canal
        conn.execute(text("""
            ALTER TABLE dbo.fact_transacciones
            ADD CONSTRAINT fk_fact_canal
            FOREIGN KEY (canal_id)
            REFERENCES dbo.dim_canal(canal_id)
        """))

        logging.info("FK canal creada ✅")


    # =========================================
    # 5️⃣ ÍNDICES PARA BI
    # =========================================
    with engine_dwh.begin() as conn:

        for col in ['cliente_id', 'canal_id', 'fecha_transaccion']:

            idx_name = f"idx_fact_{col}"

            conn.execute(text(f"""
                CREATE NONCLUSTERED INDEX {idx_name}
                ON dbo.fact_transacciones({col})
            """))

            logging.info(f"Índice {idx_name} creado ✅")


    logging.info("ETL completo y seguro para BI ✅")

                   
        #logging.info(f"{table} creada y lista ✅")
    # =========================================
    # 6️⃣ Insertar dimensiones incremental
    # =========================================
    with engine_dwh.connect() as conn:
        for df, table, pk in [(dim_clientes, 'dim_clientes', 'cliente_id'),
                              (dim_canal, 'dim_canal', 'canal_id')]:
            existing = pd.read_sql(f"SELECT {pk} FROM {table}", conn)
            new_rows = df[~df[pk].isin(existing[pk])]
            if not new_rows.empty:
                new_rows.to_sql(table, conn, if_exists='append', index=False)
                logging.info(f"{len(new_rows)} filas nuevas agregadas a {table} ✅")

    # =========================================
    # 7️⃣ Insertar fact_transacciones incremental
    # =========================================
    with engine_dwh.connect() as conn:
        if 'fact_transacciones' not in inspector.get_table_names():
            fact_transacciones.to_sql('fact_transacciones', conn, if_exists='replace', index=False)
            logging.info("Tabla fact_transacciones creada y cargada ✅")
        else:
            if 'transaccion_id' in fact_transacciones.columns:
                existing_fact = pd.read_sql("SELECT transaccion_id FROM fact_transacciones", conn)
                new_fact = fact_transacciones[~fact_transacciones['id_transaccion'].isin(existing_fact['transaccion_id'])]
                if not new_fact.empty:
                    new_fact.to_sql('fact_transacciones', conn, if_exists='append', index=False)
                    logging.info(f"{len(new_fact)} transacciones nuevas agregadas ✅")

    # =========================================
    # 8️⃣ Validar integridad referencial antes de crear FK
    # =========================================
    with engine_dwh.connect() as conn:
        # Verificar que todos los cliente_id de fact existan en dim_clientes
        missing_clientes = pd.read_sql("""
            SELECT DISTINCT f.cliente_id
            FROM fact_transacciones f
            LEFT JOIN dim_clientes d ON f.cliente_id = d.cliente_id
            WHERE d.cliente_id IS NULL
        """, conn)
        if not missing_clientes.empty:
            raise ValueError(f"Error: hay cliente_id en fact que no existen en dim_clientes: {missing_clientes['cliente_id'].tolist()}")

    # Verificar que todos los canal_id de fact existan en dim_canal
        missing_canales = pd.read_sql("""
            SELECT DISTINCT f.canal_id
            FROM fact_transacciones f
            LEFT JOIN dim_canal d ON f.canal_id = d.canal_id
            WHERE d.canal_id IS NULL
        """, conn)
        if not missing_canales.empty:
            raise ValueError(f"Error: hay canal_id en fact que no existen en dim_canal: {missing_canales['canal_id'].tolist()}")

    with engine_dwh.connect() as conn:
        result = conn.execute(text("""
        SELECT kc.name, t.name AS table_name, c.name AS column_name
        FROM sys.key_constraints kc
        JOIN sys.tables t ON kc.parent_object_id = t.object_id
        JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE kc.type = 'PK' AND t.name = 'dim_clientes'
    """)).fetchall()

    print("PK detectada:", result)

    # =========================================
    # Crear FK e índices si no existen
    # =========================================
    
    with engine_dwh.connect() as conn:

        # Asegurar tipos consistentes (PK = FK)
    
        conn.execute(text("""
            ALTER TABLE dbo.fact_transacciones
            ALTER COLUMN cliente_id INT NOT NULL
        """))

        conn.execute(text("""
            ALTER TABLE dbo.dim_clientes
            ALTER COLUMN cliente_id INT NOT NULL
        """))

        conn.execute(text("""
            ALTER TABLE dbo.fact_transacciones
            ALTER COLUMN canal_id INT NOT NULL
        """))

        conn.execute(text("""
            ALTER TABLE dbo.dim_canal
            ALTER COLUMN canal_id INT NOT NULL
        """))
    
        # Crear FK clientes
        fk_exists = conn.execute(text("""
            SELECT COUNT(*) 
            FROM sys.foreign_keys 
            WHERE name = 'fk_fact_clientes'
        """)).scalar()

        if fk_exists == 0:
            conn.execute(text("""
                ALTER TABLE dbo.fact_transacciones
                ADD CONSTRAINT fk_fact_clientes
                FOREIGN KEY (cliente_id) 
                REFERENCES dbo.dim_clientes(cliente_id)
            """))
            logging.info("FK clientes creada ✅")

        # Crear FK canal     
        fk_exists = conn.execute(text("""
            SELECT COUNT(*) 
            FROM sys.foreign_keys 
            WHERE name = 'fk_fact_canal'
        """)).scalar()

        if fk_exists == 0:
            conn.execute(text("""
                ALTER TABLE dbo.fact_transacciones
                ADD CONSTRAINT fk_fact_canal
                FOREIGN KEY (canal_id) 
                REFERENCES dbo.dim_canal(canal_id)
            """))
            logging.info("FK canal creada ✅")  
        
        # Índices para BI    
        for col in ['cliente_id', 'canal_id', 'fecha_transaccion']:
            idx_name = f"idx_fact_{col}"

            idx_exists = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM sys.indexes 
                WHERE name = '{idx_name}'
            """)).scalar()

            if idx_exists == 0:
                conn.execute(text(f"""
                    CREATE NONCLUSTERED INDEX {idx_name} 
                    ON dbo.fact_transacciones({col})
                """))
                logging.info(f"Índice {idx_name} creado ✅")    
    
                logging.info("ETL completo y seguro para BI ✅")
    
    return dataframes_raw, dataframes_std

if __name__ == "__main__":
    main()


logging.info("===========================================================================================================================================")





