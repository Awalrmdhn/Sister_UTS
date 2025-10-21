FROM python:3.11-slim

WORKDIR /app

RUN adduser --disabled-password --gecos '' awals && chown -R awals:awals /app
USER awals

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

EXPOSE 8080

CMD ["python", "-m", "src.main"]
