FROM python:3.9-slim

WORKDIR /app

COPY ./app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./config.py .
COPY ./.env.example ./.env
COPY ./demo/architecture.png ./demo/architecture.png
COPY ./app/streamlit_app.py .

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
