# Projeto de Sistema de Recomendação de Filmes

Este projeto utiliza **collaborative filtering** para realizar recomendações de filmes com base nas notas e gostos em comum dos usuários. O sistema integra várias ferramentas e tecnologias, como FastAPI, ClickHouse, Streamlit, Airflow e Docker.

## Tecnologias Utilizadas

- **FastAPI**: API para processamento das recomendações.  
- **ClickHouse**: Banco de dados para armazenamento eficiente dos dados.  
- **Streamlit**: Ferramenta para visualização dos dashboards do projeto.  
- **Airflow**: Gerenciamento de fluxos de trabalho (ETL).  
- **Docker**: Containerização para gerenciamento de ambientes.  

## Como Rodar o Projeto

1. Abra o terminal na pasta raiz do projeto e execute o comando abaixo para construir e iniciar os containers:  
   ```bash
   docker compose up -d --build
   ```

2. Caso ocorra algum erro e os containers do Airflow parem imediatamente, execute:  
   ```bash
   docker compose down
   ```  
   Depois, reinicie os containers com o comando anterior.

3. Acesse a interface do Airflow no navegador em:  
   [http://localhost:8080](http://localhost:8080)

4. Inicialize a DAG responsável pelo tratamento dos dados.

5. Para visualizar o dashboard do projeto no Streamlit, acesse:  
   [http://localhost:8501](http://localhost:8501)

## Dataset

O dataset utilizado neste projeto foi obtido do site [MovieLens | GroupLens](https://grouplens.org/datasets/movielens/).

---

### Observações

- Certifique-se de ter **Docker** e **Docker Compose** instalados e configurados corretamente antes de iniciar o projeto.
- Verifique se as portas necessárias estão disponíveis no seu sistema.
