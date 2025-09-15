FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN apt-get update -y 
RUN pip install --no-cache-dir -r requirements.txt

# Luôn upgrade pip và cài lib bản mới nhất
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir fastapi \
    && pip install --no-cache-dir "uvicorn[standard]" \
    && pip install --no-cache-dir requests \
    && pip install --no-cache-dir py_eureka_client

COPY . .

EXPOSE 9002

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9002"]
