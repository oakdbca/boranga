import json
import time

from channels.generic.websocket import WebsocketConsumer


class OCRBulkImportTaskConsumer(WebsocketConsumer):
    def connect(self):
        self.accept()
        progressPercentage = 0
        big_number = 100
        for i in range(big_number):
            # wait one second
            time.sleep(1)
            progressPercentage += 1
            self.send(
                ocr_bulk_import_task=json.dumps(
                    {"percentage_complete": progressPercentage}
                )
            )

    def disconnect(self, close_code):
        pass

    def receive(self):
        progressPercentage = 0
        big_number = 100
        for i in range(big_number):
            # wait one second
            time.sleep(1)
            progressPercentage += 1
            self.send(
                ocr_bulk_import_task=json.dumps(
                    {"percentage_complete": progressPercentage}
                )
            )
