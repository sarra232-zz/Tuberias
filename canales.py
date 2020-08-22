#!/usr/bin/env python
# coding: utf-8

import sys 
import datetime
import time
import pandas as pd 
import os
import google.cloud
import pyodbc
from google.cloud import storage
import pyodbc
import traceback
import gcsfs
import io
import json
from logger import logger


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/Willson/Downloads/UdeA/Practicas/Bancolombia/Canales/GIT/canales-bancolombia-master/transacciones-bancolombia-a6b73894e33f.json"
client = storage.Client("C:/Users/Willson/Downloads/UdeA/Practicas/Bancolombia/Canales/GIT/canales-bancolombia-master/canales-bancolombia-9dba0ee33646.json")



def load_config_json( conf_path ):
    """Carga un archivo json en un diccionario de python
    recibe como parámetro la ruta de un archivo json
    """
    if conf_path is None or conf_path == '':
        raise RuntimeError('Archivo de Configuración no puede ser Nulo')
    
    with open( conf_path ) as f_in :
        json_str = f_in.read()
        return json.loads( json_str )


def get_configuration(conf_path):
    """Devuelve un diccionario de python a partir de una ruta recibida por parámetros
    Si no se envía un parámetro se genera un error.
    """
    if len( conf_path  ) < 2:
        raise RuntimeError("ERROR en Ejecución debe enviar la dirección del path del archivo de configuración")
    
    return load_config_json(conf_path)


def log_exception(ex,process,log):
    """Función Helper que permite hacer un stack de mensajes especificando el lugar de error en el código
    La información obteniuda se escribe en el log como mensajes de error
    """
    ex_type, ex_value, ex_traceback = sys.exc_info()
    trace_back = traceback.extract_tb(ex_traceback)

    # Format stacktrace
    stack_trace = list()

    for trace in trace_back:
        stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
    
    log.Error('Error en {0}'.format(process))    
    log.Error("Tipo de Excepcion : %s " % ex_type.__name__)
    log.Error("Mensaje : %s" %ex_value)
    log.Error("Stack trace : %s" %stack_trace)


def storage_write(date,bucketName,bucketFolder,table,log):
    try:
        year = date.format('%Y')
        month = date.format('%m')
        day = date.format('%d')
        bucket = client.get_bucket(bucketName)
        filename = "%s/%s" % (str(bucketFolder) + '/2020',str(date) + '.csv')
        bucket.blob(filename).upload_from_string(table.to_csv(index=False))
        log.Info("Escribiendo fecha: " + str(date) + " en: " + bucketName + "/" + bucketFolder)
    except ValueError:
        log.Error("No se completo la escritura en Google Cloud Storage")


# Función para leer el último día en que se subio un archivo en google cloud storage y devuelve el o los días desde la ultima ingesta hasta la actual (t-1, tiempo actual menos un día)
def listDatesfromUltimateIngestion(filename_in_bucket,log):
    #Read from cloud storage
    # get bucket with name
    dth = datetime.date.today()
    delta_ingestion = dth - datetime.timedelta(2)
    delta_yesterday = dth - datetime.timedelta(1)
    last_ingestion = delta_ingestion.strftime('%Y-%m-%d')
    yesterday = delta_yesterday.strftime('%Y-%m-%d')
    bucket = client.get_bucket('canales_bookmarks')
    # get bucket data as blob
    filename = "%s" % (filename_in_bucket + '.csv')
    blob = bucket.get_blob(filename)
    list_dates = pd.read_csv('gs://canales_bookmarks/'+ filename)
    listdates = pd.DataFrame(list_dates, index=None)
    max_date = listdates.max(axis=0)
    if( max_date[0] == last_ingestion ):
        datelist = pd.date_range(start = max_date[0],end = yesterday)
        return datelist[1:]
    elif( max_date[0] < last_ingestion ):
        datelist = pd.date_range(start = max_date[0],end = yesterday)
        return datelist[1:]
    else:
        log.Warning("El flujo de " + filename_in_bucket + " se encuentra actualizado")
        datelist = []
        return datelist


def validarDatos(table,date,filename_for_fileingesttion,log):
    canales_filtro = ['APP','SVP','SVE','SUC','CB','ALM']
    validar_canales = table.isin(canales_filtro).any().any()
    validar_archivo = table.empty
    if not validar_archivo:
        log.Info("El archivo contiene datos")
        if filename_for_fileingesttion != 'canales_np_td' and filename_for_fileingesttion != 'canales_np_tdc':
            if  validar_canales:
                log.Info("El archivo contiene los canales en la lista")
                log.Info("Seleccionando Bucket para escribir")
                storage_write(date,'canales_bancolombia',filename_for_fileingesttion,table,log)
                log.Info("Escribiendo ultima ejecucion")
                writeDatesInFile(date,filename_for_fileingesttion,log)
                return True
                log.Info("Archivo subido a Google Cloud storage")        
            else:
                log.Warning("El archivo no contiene los canales en la lista")
                return False
        else:
            log.Info("Seleccionando Bucket para escribir")
            storage_write(date,'canales_bancolombia',filename_for_fileingesttion,table,log)
            log.Info("Escribiendo ultima ejecucion")
            writeDatesInFile(date,filename_for_fileingesttion,log)
            return True 
    else:
        log.Info("El archivo no contiene datos")
        return False

#Función para cargar el archivo de fechas que suben a google cloud storage
def writeDatesInFile(date,filename_for_fileingestion,log):
    # Se obtiene la lista de las fechas desde la última fecha de carga hasta T-1(día actual -1)
    last_ingestion = date
    #datelist = pd.date_range( start = "2020-01-01" ,end = last_ingestion)
    data_list = pd.DataFrame({'fecha_ultima_ejecucion':last_ingestion},index=[0])
    #Se adiciona la fecha o las fechas a la lista de fechas cargadas y se validan para cada respectivo caso:
 
    # en google cloud storage
    bucket = client.get_bucket('canales_bookmarks')
    filename = "%s" % (filename_for_fileingestion + '.csv') # Nombre de la Carpeta + Nombre del archivo.
    bucket.blob(filename).upload_from_string(data_list.to_csv(index=None))


def writeEjecutionsInFile(filename_for_fileingestion,resultado):
    date = time.strftime('%Y-%m-%d')
    execution_time = time.strftime('%H:%M:%S')
    bucket = client.get_bucket('canales_bookmarks')
    filename = "%s" % (filename_for_fileingestion + '_ejecuciones.csv')
    blob = bucket.get_blob(filename)
    list_ejecutions = pd.read_csv('gs://canales_bookmarks/'+ filename)
    new_ejecution = pd.DataFrame({'fecha_ultima_ejecucion':date,'hora de ejecucion':execution_time,'resultado':resultado},index=[0])
    write_new = list_ejecutions.append(new_ejecution,sort=False)
    bucket.blob(filename).upload_from_string(write_new.to_csv(index=None))


def writeLog(bucketFolder,log,nameLog,logFile):
    print(bucketFolder)
    print(logFile)
    bucket = client.get_bucket('canales_logs')
    filename = bucketFolder + "/" + nameLog + '.txt'
    blob = bucket.blob(filename)
    blob.upload_from_filename(logFile + filename)
    log.Info("Escribiendo log " + filename + " en " + str(bucket))


def retry(log,nameProcess,nameLog,logFile,max_tries=5):
    tic = time.time()
    configFile = get_configuration("configuracion.json")
    connStr = configFile["connStr"]
    for i in range(max_tries):
        log.Info("Intento de conexion #" + str(i) )
        try:
            time.sleep(3) 
            conn_string = connStr
            conn = pyodbc.connect(conn_string, autocommit=True)
            toc = time.time()
            log.Info("Conexion Establecida " + str(round(toc - tic)) + " s")
            return conn
            break
        except pyodbc.Error as e:
            sqlstate = e.args[1]
            log_exception(e , "Conexion con Impala",log)
            writeEjecutionsInFile(nameProcess,"Fallido")
            sys.exit("Programa terminado con Error")
            continue


def main(nameProcess):
    """Función principal del sistema donde  se especifica las funciones a
    ejecutar durante todo el proceso.
        *  Construir parametros
        *  Ejecutar consultas SQL
        *  Otros procesos/métodos
    """
    
    configFile = get_configuration("C:/Users/Willson/Downloads/UdeA/Practicas/Bancolombia/Canales/GIT/canales-bancolombia-master/configuracion.json")
    logFile = configFile["pathLog"]
    nameLog = time.strftime('%Y-%m-%d %H-%m-%S')
    log = logger(pathlog = logFile + nameProcess + "/", logName = nameLog + ".txt")
    tic = time.time()
    
    log.Info("+-+-+-+-+-+-+- Inicio Proceso Principal ! +-+-+-+-+-+-+- ")
    
    cn = retry(log,nameProcess,nameLog,logFile)
    success_list = []
    error_list = []
    log.Info("+-+-+-+-+-+-+- Inicio Proceso " + nameProcess + "! +-+-+-+-+-+-+- ")
    try:
        bucketFolder = nameProcess
        file = open( nameProcess + '.sql', 'r')
        sql = s = " ".join(file.readlines())
        datelist = listDatesfromUltimateIngestion(bucketFolder,log)
        if len(datelist) > 0:
            for date in datelist:
                year = date.strftime('%Y')
                month = date.strftime('%m')
                day = date.strftime('%d')
                date_time = date.strftime('%Y-%m-%d')
                execution_time = time.strftime('%H:%M:%S')
                log.Info("+-+-+-+-+-+-+- Inicio proceso para la fecha: " + date_time + "! +-+-+-+-+-+-+- ")
                log.Info("Ejecutando Query, para la fecha:{0}-{1}-{2}".format(year,month,day))
                query_result = pd.read_sql(sql.format(year,month,day),cn)
                log.Info("Consulta Terminada")
                log.Info('Numero de registros: ' + str(query_result.shape[0]))
                log.Info('Registros Nulos:'+ str( query_result.isnull().sum()).replace('\n','|').replace(' ',' '))
                log.Info("Validando Datos")
                condition = validarDatos( query_result,date_time,bucketFolder,log)
                log.Info("+-+-+-+-+-+-+- Final proceso para la fecha: " + date_time + "! +-+-+-+-+-+-+- ")
                if not condition:
                    log.Error("Ejecucion interrumpida, ha ocurrido un error en la fecha:" + str(date_time))
                    error_list.append(date_time)
                    resultado = "Fallido"
                    log.Info("+-+-+-+-+-+-+- Final proceso para la fecha: " + date_time + "! +-+-+-+-+-+-+- ")
                    log.Info("############################################################################# ")
                    break
                else:
                    success_list.append(date.strftime('%Y-%m-%d'))
                    resultado = "Exitoso"            
        else:
            log.Error("Ejecucion de " + nameProcess + " Terminada")
            resultado = "Actualizado"
            
               
    except Exception as e:
        log_exception(e , "Ejecución de " + nameProcess + " fallido",log)
        resultado = "Fallido"
    
    finally:
        toc = time.time()
        log.Info("Escribiendo log "+nameProcess+'/'+nameLog +'.txt'+ 'en'+ "<Bucket: {0}>".format(bucketFolder))
        log.Info("Ejecuciones exitosas: " +  str(success_list))
        log.Info("Ejecuciones fallidas: " +  str(error_list))
        log.Info("***** Fin de Ejecucion del Proceso.  Duracion Total(s): {0}".format( round(toc - tic) ))
        writeEjecutionsInFile(nameProcess,resultado)
        writeLog(bucketFolder,log,nameLog,logFile)
        cn.close()
    

main('canales_itc')


main('canales_fisicos')


main('canales_np_td')


main('canales_np_tdc')