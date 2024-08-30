from django.urls import re_path

from . import consumers

websocket_urlpatterns = [
    re_path(
        r"ws/occurrence_report_bulk_imports/$",
        consumers.OCRBulkImportTaskConsumer.as_asgi(),
    ),
]
