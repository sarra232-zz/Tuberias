# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 08:50:22 2017

Módulo que permite hacer loggeo de información en una ruta específica

@author: rlarios
"""

import time
import io


class logger():
    def __init__(self , pathlog = 'log/' , logName = "process.log"  ):
        """Inicializa el logger
        
        Argumentos:
        pathlog -- Ruta donde se escribirá el log
        logName -- Nombre del archivo log (el archivo tendrá un sufihjo con la fecha de creación)        
        """
        self.path = pathlog
        self.filename = self.path + logName
        self.flog = io.open(self.filename , 'a' , encoding = 'UTF-8') 
        self.typeMsg = {
                "I" : "[INFO] ",
                "E" : "[ERROR] ",
                "D" : "[DEBUG] ",
                "W" : "[WARNING]"              
                }
        
    def curDate(self):
        """Devuelve la decha en formato YYYYMMDDHHMMSS."""
        time.ctime()
        return  time.strftime('%Y-%m-%d %H-%M-%S')
    
    def curTime(self):
        """Devuelve la decha en formato YYYY-MM-DD HH:MM:SS."""
        time.ctime()
        return  '[' + time.strftime('%Y-%m-%d %H-%M-%S') + '] '
    
    def Info(self , message):
        """Escribe un mensaje de info en el log"""
        self.__writeLog(message , "I")
        
    def Error(self , message):
        """Escribe un mensaje de error en el log"""
        self.__writeLog(message , "E")
        
    def Debug(self , message):
        """Escribe un mensaje de Debug en el log"""
        self.__writeLog(message , "D")

    def Warning(self , message):
        """Escribe un mensaje de Warning en el log"""
        self.__writeLog(message , "W")
    
    def __writeLog(self , message , typeM):
        """Función que escribe el mensaje con el tipo especificado"""
        msx = self.curTime() + self.typeMsg[typeM] + message.strip()
        print( msx ) 
        self.flog.writelines( msx + "\n")
        self.flog.flush()
        
    def close(self):
        """Cierra el archivo de log"""
        self.flog.flush()
        self.flog.close()