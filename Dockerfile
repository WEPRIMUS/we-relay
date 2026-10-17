FROM python:3.12-slim

WORKDIR /app
COPY server.py cli.py agent_runner.py index.html ./
COPY assets ./assets
COPY agents.example.json ./

# All data (database, Markdown copies, url file) lives in /data. Mount a volume there.
ENV RELAY_HOST=0.0.0.0
ENV RELAY_PORT=8787
ENV RELAY_HOME=/data

EXPOSE 8787

CMD ["python", "server.py", "--host", "0.0.0.0", "--port", "8787"]
