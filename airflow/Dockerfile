# Usar a imagem base do Airflow
FROM apache/airflow:2.6.0-python3.9

# Definir o diretório de trabalho dentro do container
WORKDIR /opt/airflow

# Copiar o arquivo requirements.txt para dentro do container
COPY requirements.txt .

# Instalar pacotes de dependências
RUN pip install --no-cache-dir -r requirements.txt

# Garantir que o Airflow use o executor LocalExecutor
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor