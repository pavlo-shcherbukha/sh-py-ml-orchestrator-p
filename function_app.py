import json
from multiprocessing import context
import azure.functions as func
import azure.durable_functions as df
import logging
import os
from datetime import datetime
from datetime import timedelta
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient




app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


#----------------------------------------------------
# Налаштування функції за допомогою декораторів Python V2
# ----------------------------------------------------

@app.queue_trigger(
    arg_name="msg",
    queue_name="video-processed",
    connection="AzureWebJobsStorage"
)
@app.durable_client_input(client_name="client")
async def client_function(msg: func.QueueMessage, client: df.DurableOrchestrationClient):
    #instance_id = "MyTargetOrchestratorInstance" 
    #event_name = "QueueMessageReceived"
    try:
        event_data_str = msg.get_body().decode("utf-8")
        event_data = json.loads(event_data_str)
        instance_id = event_data["instance_id"]
        event_name = "PROCESSING_VIDEO_COMPLITED"
        logging.info(f"Received message: {event_data}")
        await client.raise_event(instance_id, event_name, event_data)
        logging.info(f"Successfully raised external event '{event_name}' for instance '{instance_id}'.")

        return None
    except Exception as e:
        logging.error(f"❌ Помилка  обробки повідомлення з черги video-processed: {e}")
        raise       





# ------------------------------------------------------------------------------
# An HTTP-Triggered що запускає оркестратор відео обробки
# -----------------------------------------------------------------------------
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def video_orchestration_starter(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    body=req.get_json()
    blob_name=body["blobName"]
    blob_url=body["blobURL"]
    if not blob_name:
        return func.HttpResponse(
            json.dumps( {"ok": False, "error_message": "blob_name is not defined"}, ensure_ascii=False),
            mimetype="application/json", 
            status_code=400
        )
    instance_id = await client.start_new(function_name, client_input={"name": blob_name, "bloburl": blob_url })
    response = client.create_check_status_response(req, instance_id)
    return response


# Orchestrator
@app.orchestration_trigger(context_name="context")
def video_orchestrator(context):
    try:
        logging.debug("video_orchestrator - Orchestration started.")
        input_data = context.get_input()
        blob_name = None
        if input_data is None:
            logging.debug("No input data provided")
        else:   
            blob_name = input_data.get('name')
            blob_url = input_data.get('bloburl')

        if not blob_name:
            logging.debug("No blob name provided in input data")

            raise ValueError("blob_name is requered in input data!")

        #result1 = yield context.call_activity("video_orchestration_activity", "Kyiv")
        
        msg_timestamp=datetime.now().isoformat()
        blobMessage = {
            "blobUrl": blob_url,
            "blobName": blob_name,
            "posted": msg_timestamp,
            "instance_id": context.instance_id
        }
        result5 = yield context.call_activity("send_processing_request", blobMessage)
        due_time = context.current_utc_datetime + timedelta(seconds=360)
        durable_timeout_task = context.create_timer(due_time)

        eventName = "PROCESSING_VIDEO_COMPLITED"
        eventBody = context.wait_for_external_event(eventName)
        winning_task = yield context.task_any([eventBody, durable_timeout_task])
        result6 = None 
        if eventBody == winning_task:
            durable_timeout_task.cancel()
            result6 = yield context.call_activity("SendVideoComplited", eventBody.result)
        else:
            result6 = yield context.call_activity("Escalate", "ESCALATE DUE TO TIMEOUT")

        return [ result5, result6 ]
        #return [result1]
    except Exception as e:
        logging.error( f"Orchestration failed with error: {e}")        
        error_r = yield context.call_activity("SendFailureNotification", str(e))
        error_r["error_type"] = type(e).__name__
        return [error_r]

# Activity  
@app.activity_trigger(input_name="city")
def video_orchestration_activity(city: str):
    """
      Проста Activity що повертає привітаня з вказаним містом
    """
    resp_data = {
        "ok": True,
        "activity": "video_orchestration_activity",
        "code": 200,
        "message": "Activity processed successfully",
        "data": "Hello " + city 
    }
    return  resp_data 
    

# Activity
@app.activity_trigger(input_name="error")
def SendFailureNotification(error: str):
    """
        Функція  обробки помилки оркестратора
    """
    resp_data = {
        "ok": False,
        "activity": "SendFailureNotification",
        "code": 404,
        "message": f"video_orchestrator completed with error = {error}"
       
    }
    return resp_data


# Activity
@app.activity_trigger(input_name="blobMessage")
def send_processing_request(  blobMessage: object):
    """Відправити в чергу запит на  обробку відеофайлу"""
    q_srvc_connect = os.getenv("AzureWebJobsStorage","")
    q_name = "video-to-processing"
    json_message = json.dumps(blobMessage)
    logging.debug( f"Відправляю повідомлення в чергу {q_name} запит на обробку відео {json_message}" )
    try:
        request_queue_client = QueueClient.from_connection_string(
            conn_str = q_srvc_connect, 
            queue_name = q_name
        )

        msgsend_result=request_queue_client.send_message(
            json_message
        )
        
        logging.debug(f"✅ Повідомлення про завершення обробки відправлено до черги { q_name}.")
        logging.debug(f" ID повідомлення: {msgsend_result.id}, Попередній перегляд: {msgsend_result.pop_receipt}")

        resp_data = {
            "ok": True,
            "activity": "send_processing_request",
            "code": 200,
            "message": "Activity processed successfully",
            "data":  {"message_id":msgsend_result.id, "blobMessage": blobMessage}
        }
        return  resp_data        

    except Exception as e:
        logging.error(f"❌ Помилка  публікації повідомлення до черги: {e}")
        raise       
    finally:
        request_queue_client .close()
        logging.debug("З'єднання з StorageQueue закрито")    


# Activity  
@app.activity_trigger(input_name="eventData")
def SendVideoComplited(eventData: object):
    """Фіналізує обробку відеофайла в durable функції і формує фінальну відопвідь"""
    resp_data = {
        "ok": True,
        "activity": "SendVideoComplited",
        "code": 200,
        "message": "Activity processed successfully",
        "data": eventData
    }
    return  resp_data 


# Activity
@app.activity_trigger(input_name="error_message")
def Escalate(error_message: str):
    """Фіналізує обробку відеофайла як не успішну по причині простроченого періоду часу"""
   
    resp_data = {
        "ok": True,
        "activity": "Escalate_activity",
        "code": 200,
        "message": "Activity processed successfully",
        "data": error_message
    }
    return  resp_data 
