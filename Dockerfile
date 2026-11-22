FROM python:3.11-slim
WORKDIR /app
COPY quorumkv ./quorumkv
ENTRYPOINT ["python", "-m", "quorumkv.server"]
