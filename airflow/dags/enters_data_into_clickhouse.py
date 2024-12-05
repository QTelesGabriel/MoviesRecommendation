def enters_data_into_clickhouse_ratings():
    from clickhouse_driver import Client
    import os
    import csv

    # Conectar ao ClickHouse
    client = Client(host='172.18.0.3', 
                    user='default', 
                    password='default', 
                    database='recommendation',
                    send_receive_timeout=1000,
                    connect_timeout=1000
                    )

    # Teste de conexão
    try:
        response = client.execute('SHOW TABLES')
        print("Conexão bem-sucedida:", response)
    except Exception as e:
        print(f"Erro ao conectar ao ClickHouse: {e}")

    # Função para importar um CSV para uma tabela ClickHouse
    def import_csv_to_clickhouse(csv_file, table_name, columns, types):
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Ignora a primeira linha com os nomes das colunas

            # Definir o tamanho do lote para inserção
            batch_size = 50000
            batch = []

            for row in reader:
                # Converte os dados para os tipos especificados
                converted_row = [types[i](row[i]) for i in range(len(row))]
                batch.append(converted_row)

                # Quando o batch atinge o tamanho máximo, insere no ClickHouse
                if len(batch) >= batch_size:
                    try:
                        # Construa a query literal para inserção rápida
                        values = ",".join([f"({','.join(map(str, item))})" for item in batch])
                        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES {values}"
                        client.execute(query)
                        print(f'{len(batch)} dados inseridos na tabela {table_name}')
                    except Exception as e:
                        print(f"Erro ao inserir lote: {e}")
                    batch.clear()  # Limpa o lote após inserção

            # Se ainda houver dados restantes no batch
            if batch:
                try:
                    # Construa a query literal para o último lote
                    values = ",".join([f"({','.join(map(str, item))})" for item in batch])
                    query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES {values}"
                    client.execute(query)
                    print(f'{len(batch)} dados inseridos na tabela {table_name}')
                except Exception as e:
                    print(f"Erro ao inserir último lote: {e}")

    # Caminhos das pastas onde os arquivos estão localizados
    folder_1 = 'data/original/'

    # Importar `ratings.csv`
    import_csv_to_clickhouse(
        os.path.join(folder_1, 'ratings.csv'),
        'ratings',
        ['userId', 'movieId', 'rating', 'timestamp'],
        [int, int, float, int]
    )

def enters_data_into_clickhouse_movies():
    from clickhouse_driver import Client
    import os
    import csv

    # Conectar ao ClickHouse
    client = Client(
        host='172.18.0.3',
        user='default',
        password='default',
        database='recommendation',
        send_receive_timeout=1000,
        connect_timeout=1000
    )

    # Teste de conexão
    try:
        response = client.execute('SHOW TABLES')
        print("Conexão bem-sucedida:", response)
    except Exception as e:
        print(f"Erro ao conectar ao ClickHouse: {e}")

    # Função para importar um CSV para uma tabela ClickHouse
    def import_csv_to_clickhouse(csv_file, table_name, columns, types):
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Ignora a primeira linha com os nomes das colunas

            # Definir o tamanho do lote para inserção
            batch_size = 1000
            batch = []

            for row in reader:
                # Converte os dados para os tipos especificados e escapa strings
                converted_row = []
                for i in range(len(row)):
                    value = row[i]

                    # Escapa aspas simples e adiciona aspas simples ao redor de strings
                    if types[i] == str:
                        value = value.replace("'", "''")  # Escapa aspas simples
                        value = f"'{value}'"  # Adiciona aspas simples
                    converted_row.append(value)

                batch.append(converted_row)

                # Quando o batch atinge o tamanho máximo, insere no ClickHouse
                if len(batch) >= batch_size:
                    try:
                        # Constrói a query literal para inserção rápida
                        values = ",".join([f"({','.join(row)})" for row in batch])
                        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES {values}"
                        client.execute(query)
                        print(f'{len(batch)} dados inseridos na tabela {table_name}')
                    except Exception as e:
                        print(f"Erro ao inserir lote: {e}")
                    batch.clear()  # Limpa o lote após inserção

            # Insere o lote restante, se houver
            if batch:
                try:
                    values = ",".join([f"({','.join(row)})" for row in batch])
                    query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES {values}"
                    client.execute(query)
                    print(f'{len(batch)} dados inseridos na tabela {table_name}')
                except Exception as e:
                    print(f"Erro ao inserir último lote: {e}")

    # Caminho do arquivo
    folder_1 = 'data/original/'

    # Importar `movies.csv`
    import_csv_to_clickhouse(
        os.path.join(folder_1, 'movies.csv'),
        'movies',
        ['movieId', 'title', 'genres'],
        [int, str, str]
    )


def enters_data_into_clickhouse_ratings_genre():
    from clickhouse_driver import Client
    import os
    import csv
    import re

    # Conectar ao ClickHouse
    client = Client(host='172.18.0.3', 
                    user='default', 
                    password='default', 
                    database='recommendation',
                    send_receive_timeout=1000,
                    connect_timeout=1000
                    )

    # Teste de conexão
    try:
        response = client.execute('SHOW TABLES')
        print("Conexão bem-sucedida:", response)
    except Exception as e:
        print(f"Erro ao conectar ao ClickHouse: {e}")

    # Função para limpar caracteres especiais no nome da tabela
    def limpar_nome(nome):
        return re.sub(r'[^a-zA-Z0-9]', '_', nome)

    # Função para importar um CSV para uma tabela ClickHouse
    def import_csv_to_clickhouse(csv_file, table_name, columns, types):
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Ignora a primeira linha com os nomes das colunas

            # Definir o tamanho do lote para inserção
            batch_size = 50000
            batch = []

            for row in reader:
                # Converte os dados para os tipos especificados
                converted_row = [types[i](row[i]) for i in range(len(row))]
                batch.append(converted_row)

                # Quando o batch atinge o tamanho máximo, insere no ClickHouse
                if len(batch) >= batch_size:
                    try:
                        # Construa a query literal para inserção rápida
                        values = ",".join([f"({','.join(map(str, item))})" for item in batch])
                        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES {values}"
                        client.execute(query)
                        print(f'{len(batch)} dados inseridos na tabela {table_name}')
                    except Exception as e:
                        print(f"Erro ao inserir lote: {e}")
                    batch.clear()  # Limpa o lote após inserção

            # Se ainda houver dados restantes no batch
            if batch:
                try:
                    # Construa a query literal para o último lote
                    values = ",".join([f"({','.join(map(str, item))})" for item in batch])
                    query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES {values}"
                    client.execute(query)
                    print(f'{len(batch)} dados inseridos na tabela {table_name}')
                except Exception as e:
                    print(f"Erro ao inserir último lote: {e}")

    # Caminho da pasta onde os arquivos estão localizados
    folder_2 = 'data/ratings_genre/'

    # Importar arquivos de `ratings_genre`
    for filename in os.listdir(folder_2):
        if filename.endswith('.csv'):
            table_name = limpar_nome(os.path.splitext(filename)[0])
            import_csv_to_clickhouse(
                os.path.join(folder_2, filename),
                table_name,
                ['userId', 'movieId', 'rating', 'timestamp'],
                [int, int, float, int]
            )
