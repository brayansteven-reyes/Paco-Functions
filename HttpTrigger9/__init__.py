import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import base64

def checkVariableOrSetNull(req,variable):
    try:
        return req.form[variable]
    except:
        return ''
    
def main(req: func.HttpRequest, sendGridMessage: func.Out[str]) -> func.HttpResponse:
    # Validar todos datos mandatorios
    name = req.form['name']
    department = checkVariableOrSetNull(req,'department')
    municipality = checkVariableOrSetNull(req,'municipality')
    title = checkVariableOrSetNull(req,'title')
    involved = checkVariableOrSetNull(req,'involved')
    facts = checkVariableOrSetNull(req,'facts')
    try:
        user_file = req.files["file"]
    except:
        user_file = ''
    tmp_id = ''
    file_encode = ''
    filename = ''
    attachments = {}
    # Query id 
    query_id = """
            SELECT AUTO_INCREMENT
            FROM  INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'paco'
            AND   TABLE_NAME   = 'complaints';
    """
    query_id_result = connection.query_db(query_id)
    for ids in query_id_result:
        tmp_id = ids['AUTO_INCREMENT']
    
    if user_file != '':    
        filename = str(tmp_id)+"-"+user_file.filename
        filestream = user_file.stream
        attachments = {"attachments": [
                {
                    "content": file_encode,
                    "content_id": "ii_139db99fdb5c3704",
                    "disposition": "inline",
                    "filename": filename,
                    "name": filename
                }
            ]}
        # Save file
        pacoStorage = os.environ["paco7storage7complaint"]
        blob = BlobClient.from_connection_string(conn_str= pacoStorage, container_name="paco", blob_name="complaints/"+filename)
        blob.upload_blob(filestream.read(), blob_type="BlockBlob")
    
        # encode base 64
        file_encode= base64.b64encode(filestream.read()).decode()
        message_bytes = filename.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')

    # Insert database
    query = f"""insert into complaints(name,department,municipality,title,involved,facts,filename) 
                values('{name}','{department}','{municipality}','{title}','{involved}','{facts}','{filename}');"""
    query_result = connection.insert_db(query)

    # Send message
    message = {
    "personalizations": [ {
        "to": [{
        "email": "denunciacorrupción@presidencia.gov.co"
        }
        ]}],
    "subject": "PACO - Denuncia ID "+ str(query_result),
    "content": [{
        "type": "text/plain",
        "value": f"""Una denucia fue realizada con ID:{query_result}
                \n\n Nombre/Entidad: {name}
                \n Departamento: {department}
                \n Municipio: {municipality}
                \n Asunto:{title}
                \n Implicados:{involved}
                \n Descripción hechos:{facts}
                """
        }]}
    message.update(attachments)
    sendGridMessage.set(json.dumps(message))
    
    # Return okay
    return func.HttpResponse(
        "Complaint ID "+ str(query_result),
        status_code=200
    )
