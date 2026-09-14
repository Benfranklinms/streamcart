FROM python:3.12-slim

WORKDIR /app
COPY streamlit/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY streamlit /app/streamlit

EXPOSE 8501
CMD ["streamlit", "run", "streamlit/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
