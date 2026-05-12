FROM python:3.11-slim
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install streamlit==1.32.0 pandas==2.2.1 psycopg2-binary==2.9.9 plotly==5.20.0 sqlalchemy
COPY app.py .
EXPOSE 8501
CMD ["python", "-m", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]