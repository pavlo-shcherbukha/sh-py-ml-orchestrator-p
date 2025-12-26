# Helper for Hupyter Notebook tests

import os
import sys
import tempfile
import requests
import json
import time
from azure.storage.queue import QueueClient, TextBase64EncodePolicy
from azure.storage.blob import BlobServiceClient





class DurableHelper:
 
    def __init__(self, host_url, storage_conn_str):
        self.host_url = host_url
        self.conn_str = storage_conn_str

    def check_helper(self):
        """Перевірка доступності модуля"""
        print("Модуль Helper доступний")

    def start_orchestrator(self, payload):
        """Запускає функцію та повертає ID інстансу та статус-URL"""
        try:
            starter_url = f"{self.host_url}/api/orchestrators/video_orchestrator"
            print(f"Запуск Orchestrator за адресою: {starter_url}")
            response = requests.post( starter_url, json=payload)
            response.raise_for_status()
            control_urls = response.json()
            return control_urls
        except requests.exceptions.RequestException as e:
            print(f"❌ Помилка при запуску: {e}")
            return None
        
    def check_status(self, status_url):
        """Перевіряє статсус інстанса оркестратора"""
        try:
            response = requests.get(status_url)
            response.raise_for_status()
            status_information = response.json()
            return status_information
        except requests.exceptions.RequestException as e:
            print(f"❌ Помилка при перевірці статусу: {e}")
            return None

    def terminate_instance(self, terminate_url=None, reason="No longer needed",status_url=None):
        """Завершує (перериває) роботу інстанса оркестратора"""
        try:
            response = requests.post( terminate_url.replace("{reason}", reason)) 
            response.raise_for_status()
            time.sleep(3) # Даємо час хосту обробити команду
            status_check = requests.get(status_url).json()
            return status_check
        except requests.exceptions.RequestException as e:
            print(f"❌ Помилка при спробі термінації: {e}") 
            return None           

    def purge_history(self, purge_url):
        """Очищення історії інстанса оркестратора"""
        try:
            response = requests.delete(purge_url)
            response.raise_for_status()
            purge_data = response.json()
            return purge_data
        except requests.exceptions.RequestException as e:
            print(f"❌ Помилка при спробі видалення: {e}")   
            return None                              

    def purge_history_all(self, payload=None):
        """Очищення історії по всіх інстансах оркестратора"""
        try:
            if payload is None:
                payload = {
                    "timeFrom": "2025-10-01T00:00:00Z",
                    "timeTill": "2030-01-01T00:00:00Z",
                    "runtimeStatus": ["Completed", "Failed", "Terminated"]
                }
            purge_all_url = f"{self.host_url}/runtime/webhooks/durabletask/purge-history"
            response = requests.post(purge_all_url, json=payload)
            response.raise_for_status()
            purge_data = response.json()
            return purge_data
        except requests.exceptions.RequestException as e:
            print(f"❌ Помилка при спробі видалення: {e}")   
            return None 

    def send_event_http(self, event_url, event_name, event_payload):
        """Надсилає подію по HTTP  до орекестратора, що сигналізує про закінчення обробки відео"""  
        # В шаблон URL  підставляю реальне найменування події
        event_url_f = event_url.replace('{eventName}', event_name)
        print(f"EventName={event_name}  Відправляю на URL {event_url_f}")
        try:
            if event_payload is None:
                raise ValueError("Paload події не може бути Node")
            print("Відправляю запит")
            response = requests.post( event_url_f, json=event_payload)
            response.raise_for_status() 
            status_code = response.status_code
            status_text = requests.status_codes._codes.get(status_code, ('UNKNOWN',))[0]
            return { "status_code": status_code, "status_text": status_text}
        except requests.exceptions.RequestException as e:
            print(f"❌ Критична помилка при відправці зовнішньої події {event_name} через HTTP: {e}")
            return None

    def send_event_queue(self, q_srvc_connect, q_queue_name, payload):
        """Надсилає подію в Storage Queue trigger через SDK """
        json_message = json.dumps(payload)
        print( f"Відправляю повідомлення в чергу-відповідь про успішну обробку відео {json_message}" )
        try:
            response_queue_client = QueueClient.from_connection_string(
                conn_str=q_srvc_connect, 
                queue_name=q_queue_name,
                message_encode_policy=TextBase64EncodePolicy()
            )
            response_queue_client.send_message(
                json_message
            )
            print(f"✅ Повідомлення про завершення обробки відправлено до черги { q_queue_name }.")
            return True
        except Exception as e:
            print(f"❌ Помилка при відправці повідомлення до черги: {e}")
            return None
        finally:
            response_queue_client.close()
            print("З'єднання  закрито") 


    def load_json_file(self, file_name):
        """Завантажує JSON файл та повертає вміст як словник"""
        try:
            pth = os.path.abspath(os.path.join('../data', '../data'))
            file_path = os.path.join(pth, file_name)

            with open(file_path, 'r', encoding='utf-8') as json_file:
                data = json.load(json_file)
            return data
        except Exception as e:
            print(f"❌ Помилка при завантаженні JSON файлу {file_path}: {e}")
            return None
