def separate_genre_ratings():
    import pandas as pd
    import os
    import re

    # Carregar os datasets
    movies = pd.read_csv('data/original/movies.csv')  # Arquivo com movieId, title e genres
    ratings = pd.read_csv('data/original/ratings.csv')  # Arquivo com userId, movieId, rating e timestamp

    # Garantir que movieId seja do tipo Int64 ao exportar
    movies['movieId'] = pd.to_numeric(movies['movieId'], downcast='integer')
    ratings['movieId'] = pd.to_numeric(ratings['movieId'], downcast='integer')

    # Criar um conjunto de gêneros únicos a partir do campo 'genres' em movies.csv
    unique_genres = set(
        genre
        for genres in movies['genres'].dropna()  # Remover NaN se houver
        for genre in genres.split('|')  # Separar gêneros por '|'
    )

    # Definir o diretório para salvar os arquivos
    output_dir = 'data/ratings_genre'

    # Criar o diretório se não existir
    os.makedirs(output_dir, exist_ok=True)

    def clean_filename(name):
        # Substitui qualquer caractere que não seja letra, número ou espaço por um underscore
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
        return clean_name

    # Iterar sobre cada gênero único e salvar os ratings correspondentes
    for genre in unique_genres:
        # Selecionar os filmes que pertencem ao gênero atual
        genre_movies = movies[movies['genres'].str.contains(genre, na=False)]
        
        # Filtrar os ratings apenas para os filmes do gênero atual
        genre_ratings = ratings[ratings['movieId'].isin(genre_movies['movieId'])]
        
        clean_genre = clean_filename(genre)

        # Caminho completo do arquivo
        file_path = os.path.join(output_dir, f'{clean_genre}.csv')
        
        # Salvar o arquivo CSV contendo apenas os ratings do gênero
        genre_ratings.to_csv(file_path, index=False)
                    