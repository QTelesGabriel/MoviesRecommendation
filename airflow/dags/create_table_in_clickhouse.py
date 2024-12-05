def create_table_in_clickhouse():
    # Conectar ao ClickHouse
    from clickhouse_driver import Client
    import os
    import re

    # Função para limpar caracteres especiais
    def limpar_nome(nome):
        # Substitui qualquer caractere que não seja letra, número ou espaço por um underscore
        nome_limpo = re.sub(r'[^a-zA-Z0-9]', '_', nome)
        return nome_limpo

    # Caminho para o diretório onde estão os arquivos CSV
    diretorio = 'data/ratings_genre/'

    # Listar os arquivos no diretório
    arquivos = os.listdir(diretorio)

    # Criar uma lista com os nomes dos arquivos sem a extensão '.csv'
    nomes_arquivos_sem_csv = [arquivo.replace('.csv', '') for arquivo in arquivos if arquivo.endswith('.csv')]

    # Aplicar a função a cada item da lista
    nomes_tabelas = [limpar_nome(genero) for genero in nomes_arquivos_sem_csv]

    client = Client(host='172.18.0.3', user='default', password='default', database='recommendation')

    # Criar tabela movies
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS movies
    (
        movieId Int64,
        title String,
        genres String
    ) ENGINE = MergeTree()
    ORDER BY movieId;
    """
    # Executar o comando SQL
    client.execute(create_table_sql)

    # Criar tabela ratings
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ratings
    (
        userId Int64,
        movieId Int64,
        rating Float64,
        timestamp Int64
    ) ENGINE = MergeTree()
    ORDER BY (userId, movieId);
    """
    # Executar o comando SQL
    client.execute(create_table_sql)

    for nome in nomes_tabelas:
        # Definir o comando SQL para criar a tabela
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {nome}
        (
            userId Int64,
            movieId Int64,
            rating Float64,
            timestamp Int64
        ) ENGINE = MergeTree()
        ORDER BY (userId, movieId);
        """
        # Executar o comando SQL
        client.execute(create_table_sql)
