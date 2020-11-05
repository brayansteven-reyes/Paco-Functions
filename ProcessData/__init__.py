import logging

import azure.functions as func

import os
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

def getDataReadCSV(url,sep=",",encoding="UTF-8"):
    data = pd.read_csv(url,sep=sep,encoding=encoding)
    return data

def processData(data,masterTable):
    for i in data.columns:
        data = data.rename(columns={i: i.upper()})
    
    data = data.rename(columns={'LOCALIZACI_N': 'LOCALIZACION', 'LIQUIDACI_N': 'LIQUIDACION', 'D_AS_ADICIONADOS': 'DIAS_ADICIONADOS', 'C_DIGO_BPIN': 'CODIGO_BPIN', 'OBLIGACI_N_AMBIENTAL': 'OBLIGACION_AMBIENTAL'})
    data = data.fillna('VALOR NULO')
     
    accents = ['!', '¡', '?', '¿', '@', '#', '=', '&', '+', '*', 'ç']
    def removeAccents(x):
        x = x.replace("Á","A")
        x = x.replace("É","E")
        x = x.replace("Í","I")
        x = x.replace("Ó","O")
        x = x.replace("Ú","U")
        for i in accents:
            x = x.replace(i, " ")
        return x

    a = dict(data.dtypes)
    stringList = []

    for i in data.columns:
        if str(a[i]) == 'object' and i != "URLPROCESO":
            stringList.append(i)

    for i in stringList:
        data[i] = data[i].str.upper()
        data[i] = (data[i].astype(str)).apply(removeAccents)
    
    def changeDate(x):
        if x == "VALOR NULO":
            x = "1900-01-01"
        else:
            x = x[0:10]
        return x

    datesColumns = ['FECHA_DE_FIRMA','FECHA_DE_INICIO_DEL_CONTRATO','FECHA_DE_FIN_DEL_CONTRATO','FECHA_DE_INICIO_DE_EJECUCION','FECHA_DE_FIN_DE_EJECUCION']

    for i in datesColumns:
        data[i] = data[i].apply(changeDate)
        data[i] = data[i].replace('8201-12-22', '2018-12-22')
        data[i] = data[i].replace('8201-12-21', '2018-12-21')

    for i in datesColumns:
        data[i] = pd.to_datetime(data[i])
        
    def replaceIDString(x):
        if not x == "VALOR NULO":
            for n in x:
                if not n.isdigit():
                    if n != "-":
                        x = x.replace(n,"")
        return x

    IDColumns = ['DOCUMENTO_PROVEEDOR', 'NIT_ENTIDAD']

    for i in IDColumns:
        data[i] = (data[i].astype(str)).apply(replaceIDString)

    # Mapping between CLASES DE PROCESO in order to standarized SECOP I and SECOP II Databases
    def clase_proceso(x):
        y = ""
        if x == "LICITACION PUBLICA" or x == "LICITACION PUBLICA ACUERDO MARCO DE PRECIOS":
            y = "LICITACION PUBLICA"
        if x == "SELECCION ABREVIADA DE MENOR CUANTIA SIN MANIFESTACION DE INTERES" or x == "SELECCION ABREVIADA MENOR CUANTIA" or x == "SELECCION ABREVIADA DE MENOR CUANTIA" or x == "SELECCION ABREVIADA MENOR CUANTIA SIN MANIFESTACION INTERES" or x == "SELECCION ABREVIADA DE MENOR CUANTIA (LEY 1150 DE 2007)" or x == "SELECCION ABREVIADA DEL LITERAL H DEL NUMERAL 2 DEL ARTICULO 2 DE LA LEY 1150 DE 2007" or x == "SELECCION ABREVIADA SERVICIOS DE SALUD":
            y = "SELECCION ABREVIADA DE MENOR CUANTIA"
        if x == "LICITACION PUBLICA (OBRA PUBLICA)" or x == "LICITACION PUBLICA OBRA PUBLICA" or x == "LICITACION PUBLICA" or x == "LICITACION OBRA PUBLICA":
            y = "LICITACION OBRA PUBLICA"
        if x == "SELECCION ABREVIADA SUBASTA INVERSA" or x == "ENAJENACION DE BIENES CON SUBASTA" or x == "ENAJENACION DE BIENES CON SOBRE CERRADO" or x == "SUBASTA":
            y = "SUBASTA"
        if x == "CONTRATACION DIRECTA" or x == "CONTRATACION DIRECTA (CON OFERTAS)" or x == "CONTRATACION DIRECTA (LEY 1150 DE 2007)":
            y = "CONTRATACION DIRECTA"
        if x == "MINIMA CUANTIA" or x == "CONTRATACION MINIMA CUANTIA":
            y = "CONTRATACION MINIMA CUANTIA"
        if x == "CONTRATACION REGIMEN ESPECIAL" or x == "CONTRATACION REGIMEN ESPECIAL (CON OFERTAS)" or x == "REGIMEN ESPECIAL":
            y = "REGIMEN ESPECIAL"
        if x == "CONCURSO DE MERITOS CON RECALIFICACION" or x == "CONCURSO DE MERITOS ABIERTO" or x == "CONCURSO DE MERITOS CON LISTA CORTA":
            y = "CONCURSO DE MERITOS"
        if x == "SOLICITUD DE INFORMACION A LOS PROVEEDORES":
            y = "SOLICITUD DE INFORMACION A LOS PROVEEDORES"
        if x == "INVITACION A OFERTAS CORPORATIVAS O ASOCIACIONES DE ENTIDADES TERRITORIALES":
            y = "INVITACION OFERTAS COOPERATIVAS"
        if x == "INICIATIVA PRIVADA SIN RECURSOS PUBLICOS":
            y = "INICIATIVA PRIVADA SIN RECURSOS PUBLICOS"
        if x == "CONTRATOS Y CONVENIOS CON MAS DE DOS PARTES":
            y = "CONTRATOS Y CONVENIOS CON MAS DE DOS PARTES"
        if x == "ASOCIACION PUBLICO PRIVADA":
            y = "ASOCIACION PUBLICO PRIVADA"
        return y


    data['CLASE_PROCESO'] = data['MODALIDAD_DE_CONTRATACION'].apply(clase_proceso)
    data['BASE_DE_DATOS'] = 'SECOP II'

    columnsName = masterTable.columns.tolist()
    for i in columnsName:
        masterTable = masterTable.rename(columns={i: i.upper()})
        
    columnsName = masterTable.columns.tolist()

    for i in columnsName:
        masterTable = masterTable.rename(columns={i: i.replace(" ","_")})
               
    def deleteSpaces(x):
        testA = True
        testB = True
        token = True
        x = str(x)
        while token:
            if x[0] == " ":
                x = x[1:]
            else:
                testA = False
            if x[-1] == " ":
                x = x[:-1]
            else:
                testB = False
            token = testA and testB
        return x

    masterTable['NOMBRE_EN_PLATAFORMA'] = masterTable['NOMBRE_EN_PLATAFORMA'].apply(deleteSpaces)
    masterTable['NOMBRE_ESTANDAR'] = masterTable['NOMBRE_ESTANDAR'].apply(deleteSpaces)
    
    platformName = masterTable['NOMBRE_EN_PLATAFORMA'].tolist()
    standardName = masterTable['NOMBRE_ESTANDAR'].tolist()
    dictNames = dict(zip(platformName, standardName))

    def standardNameFunction(x):
        if x in dictNames:
            return dictNames[x]
        else:
            return x

    data['NOMBRE_ENTIDAD_ESTANDAR'] = data['NOMBRE_ENTIDAD'].apply(standardNameFunction)
    data['DURATION'] = data['FECHA_DE_FIN_DEL_CONTRATO'] - data['FECHA_DE_INICIO_DEL_CONTRATO']
    data['DURATION'] = data['DURATION'] / np.timedelta64(1,'D')
    data['DEPARTAMENTO'] = data['DEPARTAMENTO'].replace('DISTRITO CAPITAL DE BOGOTA', 'BOGOTA')

    # Result data
    columnsNameDict = dict({'REFERENCIA_CONTRATO': 'REFERENCIA_DEL_CONTRATO', 'BASE_DE_DATOS': 'BASE_DE_DATOS', 'MUNICIPIO': 'CIUDAD', 'DEPARTAMENTO': 'DEPARTAMENTO', 'ESTADO_DEL_PROCESO': 'ESTADO_CONTRATO', 'FECHA_CARGUE_SECOP': 'FECHA_DE_FIRMA', 'FECHA_FIRMA_CONTRATO': 'FECHA_DE_FIRMA', 'FECHA_INICIO_CONTRATO': 'FECHA_DE_INICIO_DEL_CONTRATO', 'FECHA_FIN_CONTRATO': 'FECHA_DE_FIN_DEL_CONTRATO', 'CLASE_PROCESO': 'CLASE_PROCESO', 'TIPO_PROCESO': 'TIPO_DE_CONTRATO', 'ID_PROCESO': 'ID_CONTRATO', 'TIPO_CONTRATO': 'MODALIDAD_DE_CONTRATACION', 'NOMBRE_ENTIDAD': 'NOMBRE_ENTIDAD_ESTANDAR','NIT_ENTIDAD': 'NIT_ENTIDAD', 'CODIGO_ENTIDAD': 'NIT_ENTIDAD', 'TIPO_ID_CONTRATISTA': 'TIPODOCPROVEEDOR', 'ID_CONTRATISTA': 'DOCUMENTO_PROVEEDOR','RAZON_SOCIAL_CONTRATISTA': 'PROVEEDOR_ADJUDICADO', 'TIPO_ID_REPRESENTANTE': 'TIPODOCPROVEEDOR', 'ID_REPRESENTANTE': 'DOCUMENTO_PROVEEDOR', 'NOMBRE_REPRESENTANTE': 'PROVEEDOR_ADJUDICADO','VALOR_CONTRATO': 'VALOR_DEL_CONTRATO', 'VALOR_TOTAL_CONTRATO': 'VALOR_DEL_CONTRATO', 'OBJETO_A_CONTRATAR': 'DESCRIPCION_DEL_PROCESO','DETALLE_DEL_OBJETO_A_CONTRATAR': 'DESCRIPCION_DEL_PROCESO', 'URL_PROCESO': 'URLPROCESO', 'JUSTIFICACION_CONTRATACION': 'JUSTIFICACION_MODALIDAD_DE', 'NOMBRE_GRUPO': 'RAMA', 'NOMBRE_FAMILIA': 'SECTOR','NOMBRE_CLASE': 'SECTOR'})

    data2 = pd.DataFrame(columns=list(columnsNameDict))

    for i in columnsNameDict:
        data2[i] = data[columnsNameDict[i]]
        
    data2['VALOR_ADICIONES'] = 0
    data2['CAUSAL'] = "NO APLICA"
    data2['NIT_ENTIDAD'] = data2['NIT_ENTIDAD'].astype(str)
    data2['CODIGO_ENTIDAD'] = data2['CODIGO_ENTIDAD'].astype(str)
    data2 = data2[['REFERENCIA_CONTRATO','BASE_DE_DATOS','MUNICIPIO','DEPARTAMENTO','ESTADO_DEL_PROCESO','FECHA_CARGUE_SECOP','FECHA_FIRMA_CONTRATO','FECHA_INICIO_CONTRATO','FECHA_FIN_CONTRATO','CLASE_PROCESO','TIPO_PROCESO','ID_PROCESO','TIPO_CONTRATO','NOMBRE_ENTIDAD','NIT_ENTIDAD','CODIGO_ENTIDAD','TIPO_ID_CONTRATISTA','ID_CONTRATISTA','RAZON_SOCIAL_CONTRATISTA','TIPO_ID_REPRESENTANTE','ID_REPRESENTANTE','NOMBRE_REPRESENTANTE','VALOR_CONTRATO','VALOR_ADICIONES','VALOR_TOTAL_CONTRATO','OBJETO_A_CONTRATAR','DETALLE_DEL_OBJETO_A_CONTRATAR','URL_PROCESO','CAUSAL','NOMBRE_GRUPO','NOMBRE_FAMILIA','NOMBRE_CLASE','JUSTIFICACION_CONTRATACION']]
    ##########################################
    data2['OBJETO_A_CONTRATAR']=data2['OBJETO_A_CONTRATAR'].str.strip()
    ##########################################
    return data2


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        logging.info("Azure Blob storage v" + __version__ + " - Python quickstart sample")
        # Quick start code goes here
    except Exception as ex:
        logging.info('Exception:')
        logging.info(ex)
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=paco7input7data;AccountKey=PLgrSnqe3y4J3z0AGD7G7ggqo9McGM099M9UhKeg9srGa5WGGWEiWzjFteRIeuyhe5y2k5+zkrq5WkBmg18NHw==;EndpointSuffix=core.windows.net')
    master_path = "/tmp/"
    master_file_name ='Tabla_Maestra_v2.csv'
    master_file_path = os.path.join(master_path, master_file_name)
    # download
    logging.info('Download input files')
    master_blob_client = blob_service_client.get_blob_client(container='input7data', blob=master_file_name)
    # [START download_a_blob]
    with open(master_file_path, "wb") as master_blob:
                download_stream = master_blob_client.download_blob()
                master_blob.write(download_stream.readall())

    logging.info('Archivo '+master_file_path)

    data_path = "/tmp/"
    data_file_name ='SECOPII.csv'
    data_file_path =  os.path.join(data_path, data_file_name)
    # download
    logging.info('Download input files')
    data_blob_client = blob_service_client.get_blob_client(container='input7data', blob=data_file_name)
    # [START download_a_blob]
    with open(data_file_path, "wb") as data_blob:
                download_stream = data_blob_client.download_blob()
                data_blob.write(download_stream.readall())

    logging.info('Archivo '+data_file_path)

    # Create a file in local data directory to upload and download
    local_path = "/tmp/"
    local_file_name ='SECOPII-process.csv'
    upload_file_path = os.path.join(local_path, local_file_name)

    # Write text to the file
    """file = open(upload_file_path, 'w')
    file.write('Hello, World!')
    file.close()"""

    logging.info('Loading file')
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container='input7data', blob=local_file_name)

    logging.info("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

    data = getDataReadCSV(data_file_path)
    master_table = getDataReadCSV(master_file_path,";",'ISO-8859-1')
    result = processData(data,master_table)
    result.to_csv (upload_file_path, index = False, header=True)

    # Upload the created file
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data,overwrite=True)

    os.remove(master_file_path)
    os.remove(data_file_path)
    os.remove(upload_file_path)
    result_str = {'status':'ok'}
    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(
            result_str,
            status_code=200
    )
    
    """name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )"""