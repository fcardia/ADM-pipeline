  FROM python:3.12-slim

  WORKDIR /app

  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt

  COPY sensor_simulator.py .

  RUN mkdir -p /app/data

  ENTRYPOINT ["python3", "sensor_simulator.py"]
  CMD ["--interval", "1", "--csv-path", "data/sensor_data.csv"]