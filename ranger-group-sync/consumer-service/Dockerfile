# Builds one image used for both the long-running consumer
# (sync_service.py) and the one-shot reconciliation job (reconcile.py) --
# k8s/consumer-deployment.yaml and k8s/reconcile-cronjob.yaml select which
# one runs via the container `args:`.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ranger_client.py sync_service.py reconcile.py ./

ENTRYPOINT ["python"]
CMD ["sync_service.py"]
