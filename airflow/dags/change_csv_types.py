def change_csv_types():
    import os
    import pandas as pd
    
    def adjust_csv_types_in_place(csv_file):
        # Carregar o CSV
        df = pd.read_csv(csv_file)
        
        # Verificar as colunas do DataFrame para garantir que existem
        print(f"Colunas no CSV: {df.columns.tolist()}")

        # Ajustar tipos de dados conforme necessário
        if 'movieId' in df.columns:
            df['movieId'] = pd.to_numeric(df['movieId'], errors='coerce').astype('Int64')  # Convertendo para Int64

        if 'tagId' in df.columns:
            df['tagId'] = pd.to_numeric(df['tagId'], errors='coerce').astype('Int64')  # Convertendo para Int64

        if 'rating' in df.columns:
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce').astype('Float32')  # Convertendo para Float32

        if 'title' in df.columns:
            df['title'] = df['title'].astype('string')  # Convertendo para String

        if 'genres' in df.columns:
            df['genres'] = df['genres'].astype('string')  # Convertendo para String

        # Verificar se as colunas essenciais existem antes de tentar remover NaNs
        if 'movieId' in df.columns and 'tagId' in df.columns:
            # Remover linhas que possam ter valores nulos nas colunas essenciais
            df = df.dropna(subset=['movieId', 'tagId'], how='any')
        else:
            print("As colunas 'movieId' e 'tagId' não foram encontradas no arquivo CSV.")
            return

        # Sobrescrever o arquivo CSV original com os dados ajustados
        df.to_csv(csv_file, index=False)

    csv_files = os.listdir('data/original')
    for csv_file in csv_files:
        adjust_csv_types_in_place(f'data/original/{csv_file}')