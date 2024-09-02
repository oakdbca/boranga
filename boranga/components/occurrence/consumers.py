import json
import logging
import time

# from asgiref.sync import async_to_sync
from channels.generic.websocket import WebsocketConsumer

logger = logging.getLogger(__name__)


class OCRBulkImportTaskConsumer(WebsocketConsumer):
    def connect(self):
        logger.debug("OCR Bulk Import Task Consumer Connected")
        # UPTO: 2024-09-01
        # Got daphne server working. Managed to connect the web socket and send a message to the consumer.
        # which then updates the progress bar on the client side.
        # Next steps: Adding in the following group code leads to a web socket connection error.
        # Not sure why. Need to set up this websocker to be able to recieve messages from django code that
        # is processing the bulk import task. so we can show real time progress on the client side.
        # On top of all of this would have to convince team to use async uvicorn workers instead of guicorn workers.
        # And have the infrastructure team make any required changes to to nginx and other services.
        # async_to_sync(self.channel_layer.group_add)(
        #     "ocr_bulk_import_task_group", self.channel_name
        # )
        self.accept()

    def disconnect(self, close_code):
        logger.debug("OCR Bulk Import Task Consumer Disconnected")
        # async_to_sync(self.channel_layer.group_discard)("chat", self.channel_name)

    def receive(self, text_data):
        text_data_json = json.loads(text_data)
        message = text_data_json["message"]
        logger.debug(message)
        progressPercentage = 0
        big_number = 100
        for i in range(big_number):
            # wait one second
            time.sleep(0.25)
            progressPercentage += 1
            self.send(text_data=json.dumps({"message": progressPercentage}))
