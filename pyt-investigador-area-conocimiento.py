# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 18:12:50 2024

@author: Enzo
"""

###############################################################################
# WORKFLOW PARA DETERMINAR INVESTIGADORES ESPECIALIZADOS EN UNA DETERMINADA AREA
# DEL CONOCIMIENTO
###############################################################################

###############################################################################
# SE IMPORTAN LAS LIBRERIAS QUE SERÁN CONSIDERADAS
###############################################################################

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
from itertools import combinations
import re

###############################################################################
# SE CARGAN LOS TABLAS Y VISTAS Y SE CONVIERTEN EN OBJETOS DATAFRAMES
###############################################################################

# Se establecen los parámetros para conectarse a la base de datos de CONCYTECBI

host = ##################
port = "5432"
database = "CONCYTEC-BI"
user = ##################
password = ##############

# Establecer la conexión
conexion = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)

# SE ALMANCENAN LAS DISTINTAS TABLAS Y VISTAS DEL CONCYTEC BI EN DATAFRAME

# La tabla que se ha considerando descargar es sit_consulta_renacyt
nombre_tabla1 = "sit_consulta_renacyt"
# Consulta SQL para seleccionar todos los datos de la tabla
consulta1 = f'SELECT * FROM {nombre_tabla1}'

# Descargar datos en un DataFrame de pandas
renacyt = pd.read_sql_query(consulta1, conexion)

# La tabla que se ha considerado descargar es vw_indicador_b_vs_ws_api_scopus_resumen
nombre_tabla2 = "vw_indicador_b_vs_ws_api_scopus_resumen"
consulta2 = "SELECT * FROM ficha_renacyt.vw_indicador_b_vs_ws_api_scopus_resumen;"

# Descargar datos en un DataFrame de pandas
pub_renacyt = pd.read_sql_query(consulta2, conexion)

nombre_tabla3 = "tbl_scopus_pub"
consulta3 = "SELECT * FROM directorio_cti.tbl_scopus_pub;"

# Descargar datos en un DataFrame de pandas
pub_scopus = pd.read_sql_query(consulta3, conexion)

nombre_tabla4 = "tbl_scopus_pub_aut_afil"
consulta4 = "SELECT * FROM directorio_cti.tbl_scopus_pub_aut_afil;"

codigos_scopus = pd.read_sql_query(consulta4, conexion)

nombre_tabla5 = "tbl_ws_api_scopus_detalle_afiliacion_publicaciones_renacyt"
consulta5 = f'SELECT * FROM {nombre_tabla5}'

scopus_renacyt = pd.read_sql_query(consulta5, conexion)


nombre_tabla6 = "tbl_indicador_c"
consulta6 = "SELECT * FROM ficha_renacyt.tbl_indicador_c;"

base_renacyt_patentes = pd.read_sql_query(consulta6, conexion)


nombre_tabla7 = "tbl_indicador_d"
consulta7 = "SELECT * FROM ficha_renacyt.tbl_indicador_d;"

base_renacyt_libros = pd.read_sql_query(consulta7, conexion)


nombre_tabla8 = "tbl_indicador_e"
consulta8 = "SELECT * FROM ficha_renacyt.tbl_indicador_e;"

base_renacyt_h = pd.read_sql_query(consulta8, conexion)


# La tabla que se ha considerado descargar es vw_indicador_b_vs_ws_api_scopus_resumen
nombre_tabla9 = "produccion_bibliografica_scimago"
consulta9 = "SELECT * FROM scimago.produccion_bibliografica_scimago;"

scimago = pd.read_sql_query(consulta9, conexion)

# Cerrar la conexión
conexion.close()

###############################################################################
# Implementación del desarrollo
###############################################################################

# Se analiza el dataframe renacyt
renacyt.shape
renacyt.columns
renacyt.dtypes


# Se convierten tres columnas en formato string a formato datetime
renacyt["fecha_inicio_vigencia_reglamento_2018"] = pd.to_datetime(renacyt["fecha_inicio_vigencia_reglamento_2018"])
renacyt["fecha_fin_vigencia_reglamento_2018"] = pd.to_datetime(renacyt["fecha_fin_vigencia_reglamento_2018"])
renacyt["fecha_nacimiento"] = pd.to_datetime(renacyt["fecha_nacimiento"])

# Se realiza un esquema para obtener solo a los investigadores RENACYT activos
# El filtro lo que hace es considerar a los investigadores
renacyt_por_validar = renacyt.query('condicion_reglamento_2021 =="Activo" or fecha_fin_vigencia_reglamento_2018 >="2024-12-05"')
a = renacyt_por_validar["codigo_renacyt"].nunique()
print(f"La cantidad de investigadores RENACYT únicos son {a}")

# Se identifica valores duplicados de la columna codigo_renacyt del dataframe renacyt
# y se procede a eliminar estos duplicados
duplicadofila = renacyt_por_validar[renacyt_por_validar.duplicated("codigo_renacyt")]

# Se construye una base de datos que almacena a los investigadores RENACYT únicos
renacyt_validado = renacyt_por_validar.drop_duplicates(subset=["codigo_renacyt"])
renacyt_validado.columns
renacyt_validado.condicion_reglamento_2021.value_counts()

# Se renombra atributos del dataframe renacyt_validado
renacyt_validado.rename(columns=({"Numero_de_documento_de_identidad":"DNI"}), inplace=True)

# Se agregan dos columnas adicionales al dataframe renacyt_validado
renacyt_validado['ficha_Renacyt'] = renacyt_validado['URL CV Público'].str.replace(r'https://ctivitae.concytec.gob.pe/appDirectorioCTI/VerDatosInvestigador.do\?id_investigador=', 'https://servicio-renacyt.concytec.gob.pe/ficha-renacyt/?idInvestigador=', regex=True)
renacyt_validado["Perfil_autor_scopus"] = "https://www.scopus.com/authid/detail.uri?authorId=" + renacyt_validado["id_perfil_scopus"].astype(str)
renacyt_validado.columns

# Se analiza el dataframe renacyt_validado
renacyt_validado["codigo_renacyt"].nunique()
renacyt_validado.dtypes

# Se construye un sub dataframe a partir de renacyt_validado
renacyt_validado1 = renacyt_validado[["id_investigador", "codigo_renacyt", "desc_personal"]]

# Dado que la columna desc_personal presenta filas con valores nulos, se procede a eliminarlos
renacyt_validado1 = renacyt_validado1.dropna(subset=['desc_personal'])

# Ahora bien, se ingresan las areas del conocimiento que quieran ser analizadas
entrada_usuario = input("Ingrese palabras clave separadas por comas: ")
# las areas ingresas se guardan a partir de un list comprenhension
palabras_clave = [palabra.strip() for palabra in entrada_usuario.split(",")]

# Se construye una función para verificar si las palabras clave están presentes en el texto
def buscar_palabras_clave(texto):
    for palabra_clave in palabras_clave:
        if re.search(r'\b{}\b'.format(re.escape(palabra_clave)), texto, flags=re.IGNORECASE):
            return True
    return False


# Aplicar la función a la columna 'Texto' y almacenar el resultado en una nueva columna 'Contiene_Palabras_Clave'
renacyt_validado1['Contiene_Palabras_Clave'] = renacyt_validado1['desc_personal'].apply(buscar_palabras_clave)

# Se crea un dataframe que almacena a los investigadores RENACYT vinculados con esas areas del conocimiento
pedido = renacyt_validado1[renacyt_validado1["Contiene_Palabras_Clave"]==True]
pedido = pedido[["codigo_renacyt"]]

fusion = pd.merge(pedido, renacyt_validado, on="codigo_renacyt", how="left")
fusion.shape

# Se genera un partición que contiene los codigos de los investigadores renacyt
fusion1 = fusion[["codigo_renacyt"]]

# Ahora bien, se analiza el dataframe pub_renacyt
# Este dataframe está compuesto por las publicaciones científicas indizadas en SCOPUS
# que han servido para el proceso de calificación de investigadores

pub_renacyt.shape
pub_renacyt.info()
pub_renacyt.columns


# Se renombra atributos del dataframe pub_renacyt
pub_renacyt.rename(columns=({"codigo_registro":"codigo_renacyt",
                             "api_eid":"eid",
                             "id_perfil_scopus":"codigo_scopus"}), inplace=True)



# Se agrega un atributo a pub_renacyt relacionado con la cantidad de publicaciones calificadas
pub_renacyt["cantidad_pub_calificadas"] = pub_renacyt.groupby("codigo_renacyt")["codigo_renacyt"].transform("size")

# Se realiza una fusion entre fusion1 y pub_renacyt
fusion2 = pd.merge(fusion1, pub_renacyt, on="codigo_renacyt", how="left")


# Se analiza la distribución de las publicaciones científicas
fusion2.desc_tipo_produccion_bibliografica.value_counts()
fusion2["codigo_renacyt"].nunique()
# Se analiza la distribución de publicaciones científicas de la columna cantidad_pub_calificadas
fusion2["cantidad_pub_calificadas"].describe()

# Se consideran solo las publicaciones científicas mayores de 30
fusion2 = fusion2[fusion2["cantidad_pub_calificadas"]>=20]
fusion2["codigo_renacyt"].nunique()
fusion2.columns

# Considerando renacyt validado se construye un sub dataframe
renacyt_validado.columns
autor = renacyt_validado[["Areas|Sub Areas|Disciplinas",
                          "codigo_renacyt", "ficha_Renacyt", "Perfil_autor_scopus", "desc_personal", "Institucion Laboral Principal"]]

# Se fusiona autor con fusion2
fusion3 = pd.merge(autor, fusion2, on="codigo_renacyt")

# El desarrollo se centra en fusion3
fusion3.columns
esquema1 = fusion3[["codigo_renacyt", "codigo_scopus","apellido_paterno", "apellido_materno","nombres","Institucion Laboral Principal","Areas|Sub Areas|Disciplinas", "ficha_Renacyt",
                   "Perfil_autor_scopus", "cantidad_pub_calificadas"]]


esquema1 = esquema1.drop_duplicates(subset=["codigo_renacyt"])
esquema2 = fusion2[["codigo_renacyt", "eid", "id_doi", "titulo", "desc_tipo_produccion_bibliografica", "anio_fecha_produccion"]]

a = "Listado_publicaciones_investigadores_pub_num"
  
with pd.ExcelWriter(f'{a}.xlsx') as writer:
      # Guardar cada DataFrame en una hoja diferente
     esquema1.to_excel(writer, sheet_name="Investigadores", index=False)
     esquema2.to_excel(writer, sheet_name="Publicaciones", index=False)