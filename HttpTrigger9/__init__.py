import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import base64

def main(req: func.HttpRequest, sendGridMessage: func.Out[str]) -> func.HttpResponse:
    # Validar todos datos mandatorios
    name = req.form['name']
    department = req.form['department']
    municipality = req.form['municipality']
    title = req.form['title']
    involved = req.form['involved']
    facts = req.form['facts']
    user_file = req.files["file"]

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
    
    filename = str(tmp_id)+"-"+user_file.filename
    filestream = user_file.stream

    # Insert database
    query = f"""insert into complaints(name,department,municipality,title,involved,facts,filename) 
                values('{name}','{department}','{municipality}','{title}','{involved}','{facts}','{filename}');"""
    query_result = connection.insert_db(query)
    # Save file
    pacoStorage = os.environ["pacoStorage"]
    blob = BlobClient.from_connection_string(conn_str= pacoStorage, container_name="paco", blob_name="complaints/"+filename)
    blob.upload_blob(filestream.read(), blob_type="BlockBlob")
    
    # encode base 64
    file_encode= base64.b64encode(filestream.read()).decode()
    message_bytes = filename.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    # Send message
    message = {
    "personalizations": [ {
        "to": [{
        "email": "brayanreyes@presidencia.gov.co"
        }]}],
    "attachments": [
        {
            "content": file_encode,
            "content_id": "ii_139db99fdb5c3704",
            "disposition": "inline",
            "filename": filename,
            "name": filename
        }
    ],
    "subject": "PACO - Denuncia ID "+ str(query_result),
    "content": [{
        "type": "text/plain",
        "value": f"""La siguiente denucia fue realizada {query_result}
                \n\n Nombre: {name}
                \n Departamento: {department}
                \n Municipio: {municipality}
                \n Titulo:{title}
                \n Implicados:{involved}
                \n Descripción hechos:{facts}
                """
        }]}
    
    sendGridMessage.set(json.dumps(message))
    
    # Return okay
    return func.HttpResponse(
        "Complaint wit id "+ str(query_result) +" save satisfactory",
        status_code=200
    )
