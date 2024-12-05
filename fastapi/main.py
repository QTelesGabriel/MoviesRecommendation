from fastapi import FastAPI, HTTPException
from clickhouse_driver import Client
import pandas as pd
import json

app = FastAPI()

client = Client(
        host='172.18.0.3',
        user='default',
        password='default',
        database='recommendation'
        )

@app.get("/teste_conexao")
def teste_conexao():
    try:
        validate_query = "SELECT 1 FROM system.tables WHERE name = 'ratings'"
        table_exists = client.execute(validate_query)
        if table_exists:
            print("Tabela existe.")
        else:
            print("Tabela não encontrada.")
    except Exception as e:
        print(f"Erro ao verificar a tabela: {e}")

@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: int):
    try:
        # 1. Obter as avaliações do usuário atual
        user_query = f"""
        SELECT movieId, rating FROM ratings WHERE userId = {user_id}
        """
        user_ratings = client.execute(user_query)

        if not user_ratings:
            raise HTTPException(status_code=404, detail="Usuário não encontrado ou sem avaliações.")

        # Converter para DataFrame
        user_df = pd.DataFrame(user_ratings, columns=["movieId", "rating"])

        # 2. Obter avaliações de outros usuários
        ratings_query = """
        SELECT userId, movieId, rating FROM ratings
        """
        all_ratings = client.execute(ratings_query)
        ratings_df = pd.DataFrame(all_ratings, columns=["userId", "movieId", "rating"])

        # 3. Criar matriz de usuários x filmes
        user_movie_matrix = ratings_df.pivot(index="userId", columns="movieId", values="rating")
        print(user_movie_matrix)

        # 4. Calcular similaridade com outros usuários usando correlação de Pearson
        user_similarity = user_movie_matrix.corrwith(user_movie_matrix.loc[user_id], axis=1, drop=False)

        # 5. Selecionar usuários mais similares (top 5)
        similar_users = user_similarity.dropna().sort_values(ascending=False).head(5).index.tolist()

        # 6. Obter recomendações com base nos usuários similares
        similar_ratings_query = f"""
        SELECT m.movieId, m.title, AVG(r.rating) as avg_rating 
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId IN ({",".join(map(str, similar_users))})
        AND r.movieId NOT IN ({",".join(map(str, user_df['movieId'].tolist()))})
        GROUP BY m.movieId, m.title
        ORDER BY avg_rating DESC
        LIMIT 5
        """
        recommendations = client.execute(similar_ratings_query)

        return {"recommendations": [{"movieId": row[0], "avg_rating": row[1]} for row in recommendations]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendations_by_genre/{user_id}/{genre}")
def get_recommendations_by_genre(user_id: int, genre: str):
    try:
        # 1. Validar se a tabela do gênero existe
        genre_table = genre
        validate_query = f"SELECT count() FROM system.tables WHERE name = '{genre_table}'"
        table_exists = client.execute(validate_query)

        if not table_exists[0][0]:
            raise HTTPException(status_code=404, detail=f"Tabela para o gênero '{genre}' não encontrada.")

        # 2. Obter as avaliações do usuário para o gênero específico
        user_query = f"""
        SELECT movieId, rating FROM {genre_table} WHERE userId = {user_id}
        """
        user_ratings = client.execute(user_query)

        if not user_ratings:
            raise HTTPException(status_code=404, detail="Usuário não avaliou nenhum filme neste gênero.")

        user_df = pd.DataFrame(user_ratings, columns=["movieId", "rating"])

        # 3. Obter avaliações de outros usuários no mesmo gênero
        ratings_query = ratings_query = """
        SELECT userId, movieId, rating FROM ratings
        """
        all_ratings = client.execute(ratings_query)
        ratings_df = pd.DataFrame(all_ratings, columns=["userId", "movieId", "rating"])

        # 4. Criar matriz de usuário x filme para o gênero específico
        user_movie_matrix = ratings_df.pivot(index="userId", columns="movieId", values="rating")

        # 5. Calcular similaridade entre usuários com Pearson
        user_similarity = user_movie_matrix.corrwith(user_movie_matrix.loc[user_id], axis=1, drop=False)

        # 6. Selecionar os usuários mais semelhantes (top 5)
        similar_users = user_similarity.dropna().sort_values(ascending=False).head(5).index.tolist()

        # 7. Obter recomendações dos usuários semelhantes
        similar_ratings_query = f"""
        SELECT m.movieId, m.title, AVG(r.rating) as avg_rating 
        FROM {genre_table} r
        JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId IN ({",".join(map(str, similar_users))})
        AND r.movieId NOT IN ({",".join(map(str, user_df['movieId'].tolist()))})
        GROUP BY m.movieId, m.title
        ORDER BY avg_rating DESC
        LIMIT 5
        """
        recommendations = client.execute(similar_ratings_query)

        return {"recommendations": [{"movieId": row[0], "avg_rating": row[1]} for row in recommendations]}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recommendations/distribution")
def get_recommendation_distribution(movie_ids: list[int]):
    try:
        # Consultar a distribuição de ratings por filme
        query = f"""
        SELECT movieId, rating, COUNT(*) as count
        FROM ratings
        WHERE movieId IN ({','.join(map(str, movie_ids))})
        GROUP BY movieId, rating
        ORDER BY movieId, rating
        """
        data = client.execute(query)
        if not data:
            raise HTTPException(status_code=404, detail="Nenhuma avaliação encontrada para os filmes recomendados.")
        result = [{"movieId": row[0], "rating": row[1], "count": row[2]} for row in data]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
@app.post("/recommendations/average")
def get_recommendation_average(movie_ids: list[int]):
    try:
        query = f"""
        SELECT movieId, AVG(rating) as avg_rating
        FROM ratings
        WHERE movieId IN ({','.join(map(str, movie_ids))})
        GROUP BY movieId
        ORDER BY avg_rating DESC
        """
        data = client.execute(query)
        if not data:
            raise HTTPException(status_code=404, detail="Usuário não encontrado ou sem avaliações.")
        return [{"movieId": row[0], "avg_rating": row[1]} for row in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recommendations/count")
def get_recommendation_count(movie_ids: list[int]):
    try:
        query = f"""
        SELECT movieId, COUNT(*) as count
        FROM ratings
        WHERE movieId IN ({','.join(map(str, movie_ids))})
        GROUP BY movieId
        ORDER BY count DESC
        """
        data = client.execute(query)
        if not data:
            raise HTTPException(status_code=404, detail="Usuário não encontrado ou sem avaliações.")
        return [{"movieId": row[0], "count": row[1]} for row in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/user_ratings/{user_id}")
def get_user_ratings(user_id: int):
    try:
        query = f"SELECT movieId, rating FROM ratings WHERE userId = {user_id}"
        data = client.execute(query)
        if not data:
            raise HTTPException(status_code=404, detail="Usuário não encontrado ou sem avaliações.")
        return {"user_id": user_id, "ratings": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/all_ratings")
def get_all_ratings():
    """
    Retorna todas as avaliações realizadas pelos usuários.
    """
    try:
        query = "SELECT userId, movieId, rating FROM ratings"
        data = client.execute(query)
        return {"ratings": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/user_movie_matrix/{user_id}")
def get_user_movie_matrix(user_id: int):
    """
    Gera a matriz de usuários x filmes.
    """
    try:
        query = "SELECT userId, movieId, rating FROM ratings"
        data = client.execute(query)
        ratings_df = pd.DataFrame(data, columns=["userId", "movieId", "rating"])
        user_movie_matrix = ratings_df.pivot(index="userId", columns="movieId", values="rating")
        return json.loads(user_movie_matrix.to_json())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/user_similarity/{user_id}")
def get_user_similarity(user_id: int):
    """
    Calcula a similaridade entre o usuário e outros usuários.
    """
    try:
        query = "SELECT userId, movieId, rating FROM ratings"
        data = client.execute(query)
        ratings_df = pd.DataFrame(data, columns=["userId", "movieId", "rating"])
        user_movie_matrix = ratings_df.pivot(index="userId", columns="movieId", values="rating")
        
        # Verificar se o usuário está presente
        if user_id not in user_movie_matrix.index:
            raise HTTPException(status_code=404, detail="Usuário não encontrado na matriz de dados.")
        
        # Calcular similaridade
        user_similarity = user_movie_matrix.corrwith(user_movie_matrix.loc[user_id], axis=1, drop=False).dropna()

        # Verificar se a similaridade foi calculada corretamente
        if user_similarity.empty:
            raise HTTPException(status_code=404, detail="Nenhum usuário similar encontrado.")
        
        similarity_df = user_similarity.reset_index()
        similarity_df.columns = ["userId", "similarity"]
        
        # Excluir o próprio usuário da lista
        similarity_df = similarity_df[similarity_df['userId'] != user_id]
        
        # Ordenar pela similaridade
        similarity_df = similarity_df.sort_values(by="similarity", ascending=False)
        
        return similarity_df.to_dict(orient="records")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
@app.post("/movies_rated_by_similar_users")
def get_movies_rated_by_similar_users(user_ids: list[int]):
    """
    Retorna os filmes avaliados pelos usuários mais similares.
    """
    try:
        # Construir a consulta para pegar os filmes avaliados pelos usuários fornecidos, incluindo a tabela de filmes
        user_ids_str = ','.join(map(str, user_ids))
        query = f"""
        SELECT r.userId, r.movieId, m.title, r.rating
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId IN ({user_ids_str})
        """
        data = client.execute(query)
        if not data:
            raise HTTPException(status_code=404, detail="Nenhum filme encontrado para os usuários fornecidos.")
        # Converter os dados retornados em um DataFrame
        movies_df = pd.DataFrame(data, columns=["userId", "movieId", "movieTitle", "rating"])
        # Retornar os dados como lista de dicionários
        return movies_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))