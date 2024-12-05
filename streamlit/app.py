import streamlit as st
import pandas as pd
import requests
import altair as alt

# Base URL da API FastAPI
API_BASE_URL = "http://172.18.0.5:8000"

# Configuração inicial da página
st.set_page_config(
    page_title="Sistema de Recomendação e Análise",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Funções para consumir os endpoints
@st.cache_data
def fetch_recommendations(user_id):
    response = requests.get(f"{API_BASE_URL}/recommendations/{user_id}")
    if response.status_code == 200:
        return pd.DataFrame(response.json()["recommendations"])
    else:
        st.error(response.json()["detail"])
        return pd.DataFrame()

@st.cache_data
def fetch_recommendations_by_genre(user_id, genre):
    response = requests.get(f"{API_BASE_URL}/recommendations_by_genre/{user_id}/{genre}")
    if response.status_code == 200:
        return pd.DataFrame(response.json()["recommendations"])
    else:
        st.error(response.json()["detail"])
        return pd.DataFrame()

@st.cache_data
def fetch_recommendation_distribution(movie_ids):
    # Garantir que movie_ids seja uma lista de inteiros
    if not isinstance(movie_ids, list):
        st.error("Os movie_ids devem ser uma lista de inteiros.")
        return pd.DataFrame()

    if not all(isinstance(i, int) for i in movie_ids):
        st.error("Todos os elementos de movie_ids devem ser inteiros.")
        return pd.DataFrame()

    response = requests.post(f"{API_BASE_URL}/recommendations/distribution", json=movie_ids)
    
    if response.status_code == 200:
        dist_data = pd.DataFrame(response.json())
        if 'movieId' not in dist_data.columns or 'rating' not in dist_data.columns or 'count' not in dist_data.columns:
            st.error("Dados retornados da API estão incompletos.")
            return pd.DataFrame()
        return dist_data
    else:
        st.error("Erro ao buscar distribuição das notas para filmes recomendados.")
        return pd.DataFrame()


@st.cache_data
def fetch_recommendation_average(movie_ids):
    response = requests.post(f"{API_BASE_URL}/recommendations/average", json=movie_ids)
    if response.status_code == 200:
        return pd.DataFrame(response.json())  # movieId, avg_rating
    else:
        st.error("Erro ao buscar médias das avaliações para filmes recomendados.")
        return pd.DataFrame()

@st.cache_data
def fetch_recommendation_count(movie_ids):
    response = requests.post(f"{API_BASE_URL}/recommendations/count", json=movie_ids)
    if response.status_code == 200:
        return pd.DataFrame(response.json())  # movieId, count
    else:
        st.error("Erro ao buscar contagem de avaliações para filmes recomendados.")
        return pd.DataFrame()
    
@st.cache_data
def fetch_user_ratings(user_id):
    response = requests.get(f"{API_BASE_URL}/user_ratings/{user_id}")
    if response.status_code == 200:
        return pd.DataFrame(response.json()["ratings"], columns=["movieId", "rating"])
    else:
        st.error(response.json()["detail"])
        return pd.DataFrame()

@st.cache_data
def fetch_user_movie_matrix(user_id):
    response = requests.get(f"{API_BASE_URL}/user_movie_matrix/{user_id}")
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        st.error("Erro ao buscar matriz de usuários x filmes.")
        return pd.DataFrame()

@st.cache_data
def fetch_user_similarity(user_id):
    response = requests.get(f"{API_BASE_URL}/user_similarity/{user_id}")
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        st.error("Erro ao calcular similaridade de usuários.")
        return pd.DataFrame()
    
# Função para buscar filmes avaliados pelos usuários mais similares
@st.cache_data
def fetch_movies_rated_by_similar_users(user_id, user_similarity):
    # Filtrar os usuários com maior similaridade (por exemplo, similaridade > 0.6)
    similar_users = user_similarity[user_similarity['similarity'] > 0.6]
    
    # Remover o próprio usuário da lista
    similar_users = similar_users[similar_users['userId'] != user_id]

    # Ordenar os usuários pela similaridade em ordem decrescente
    similar_users = similar_users.sort_values(by='similarity', ascending=False)
    
    # Limitar a 5 usuários mais similares
    top_similar_users = similar_users.head(5)
    
    # Extrair os IDs dos usuários mais similares
    user_ids = top_similar_users['userId'].tolist()

    if not user_ids:
        st.warning("Nenhum usuário com alta similaridade encontrado.")
        return pd.DataFrame()

    # Fazer a requisição para o endpoint com os IDs dos usuários mais similares
    response = requests.post(f"{API_BASE_URL}/movies_rated_by_similar_users", json=user_ids)
    
    if response.status_code == 200:
        return pd.DataFrame(response.json())  # rating, count, movieId, movieTitle
    else:
        st.error("Erro ao buscar filmes avaliados pelos usuários mais similares.")
        st.error(f"Resposta do servidor: {response.text}")  # Exibe o corpo da resposta de erro
        return pd.DataFrame()
    
# Função para exibir gráficos com rolagem lateral
def display_graphs_with_scroll(data):
    # Organizar os gráficos em uma área com rolagem horizontal
    container = st.container()
    with container:
        # Criar uma barra de rolagem horizontal com a opção de exibir cada gráfico por filme
        col1, col2, col3 = st.columns([1, 1, 1])  # 3 colunas
        columns = [col1, col2, col3]

        # Exibir um gráfico por coluna
        for i, movie_id in enumerate(data['movieId'].unique()):
            movie_data = data[data['movieId'] == movie_id]

            chart = alt.Chart(movie_data).mark_bar().encode(
                x=alt.X(
                    'rating:Q',
                    bin=alt.Bin(maxbins=10),
                    title='Nota de Avaliação',
                    scale=alt.Scale(domain=[0, 5], nice=True)
                ),
                y=alt.Y('count:Q', title='Número de Avaliações'),
                color='rating:Q',
                tooltip=['rating:Q', 'count:Q']
            ).properties(
                title=f'Distribuição das Notas - Filme ID: {movie_id}'
            )

            # Exibir o gráfico na coluna correspondente
            columns[i % len(columns)].altair_chart(chart, use_container_width=True)

# Layout Principal
st.title("🎥 Sistema de Recomendação e Análise")
st.sidebar.header("📊 Configurações do Usuário")
user_id = st.sidebar.number_input("🔢 ID do Usuário", min_value=1, value=1)
# Lista de gêneros disponíveis
genres = [
    'Action', 'Adventure', 'Animation', 'Children',
    'Comedy', 'Crime', 'Documentary', 'Drama',
    'Fantasy', 'Film_Noir', 'Horror', 'IMAX',
    'Musical', 'Mystery', 'Romance', 'Sci_Fi',
    'Thriller', 'War', 'Western', '_no_genres_listed_'
]

# Alterando para um selectbox
genre = st.sidebar.selectbox("🎭 Escolha o Gênero (Opcional)", ["Nenhum"] + genres)

# Verificação de seleção
if genre == "Nenhum":
    genre = None

# Botões de Ação
st.sidebar.subheader("⚙️ Ações")
recomendacao_geral = st.sidebar.button("🔍 Obter Recomendações Gerais")
recomendacao_genero = st.sidebar.button("🎭 Obter Recomendações por Gênero")
mostrar_analise = st.sidebar.checkbox("Mostrar Análise Detalhada")

# Layout por Seções
if recomendacao_geral:
    st.header("🎬 Recomendações Gerais")
    recommendations = fetch_recommendations(user_id)
    if not recommendations.empty:
        st.subheader("📋 Tabela de Recomendações")
        st.dataframe(recommendations)
        st.subheader("📊 Gráficos sobre as Recomendações")
        
        # Obtemos os movieIds recomendados
        recommended_movie_ids = recommendations["movieId"].tolist()

        # **Gráfico 1: Distribuição das Notas**
        st.subheader("🔎 Distribuição das Notas dos Filmes Recomendados")
        dist_data = fetch_recommendation_distribution(recommended_movie_ids)
        if not dist_data.empty:
            display_graphs_with_scroll(dist_data)
        else:
            st.warning("Nenhuma distribuição disponível para filmes recomendados.")

        # **Gráfico 2: Média das Avaliações**
        st.subheader("🎥 Média das Avaliações por Filme Recomendado")
        avg_data = fetch_recommendation_average(recommended_movie_ids)
        if not avg_data.empty:
            avg_chart = alt.Chart(avg_data).mark_bar().encode(
                x=alt.X('movieId:O', title='ID do Filme'),
                y=alt.Y('avg_rating:Q', title='Média das Avaliações'),
                tooltip=['movieId', 'avg_rating']
            ).properties(title='Média das Avaliações dos Filmes Recomendados')
            st.altair_chart(avg_chart, use_container_width=True)
        else:
            st.warning("Nenhuma média disponível para filmes recomendados.")

        # **Gráfico 3: Contagem de Avaliações**
        st.subheader("📈 Número de Avaliações por Filme Recomendado")
        count_data = fetch_recommendation_count(recommended_movie_ids)
        if not count_data.empty:
            count_chart = alt.Chart(count_data).mark_bar().encode(
                x=alt.X('movieId:O', title='ID do Filme'),
                y=alt.Y('count:Q', title='Número de Avaliações'),
                tooltip=['movieId', 'count:Q']
            ).properties(title='Número de Avaliações dos Filmes Recomendados')
            st.altair_chart(count_chart, use_container_width=True)
        else:
            st.warning("Nenhuma contagem disponível para filmes recomendados.")
    else:
        st.warning("Nenhuma recomendação disponível para este usuário.")

if recomendacao_genero and genre:
    st.header(f"🎭 Recomendações no Gênero: {genre.capitalize()}")
    recommendations = fetch_recommendations_by_genre(user_id, genre)
    if not recommendations.empty:
        st.subheader("📋 Tabela de Recomendações por Gênero")
        st.dataframe(recommendations)
        st.subheader("📊 Gráfico de Recomendações por Gênero")
        
        # Obtemos os movieIds recomendados
        recommended_movie_ids = recommendations["movieId"].tolist()

        # **Gráfico 1: Distribuição das Notas**
        st.subheader("🔎 Distribuição das Notas dos Filmes Recomendados")
        dist_data = fetch_recommendation_distribution(recommended_movie_ids)
        if not dist_data.empty:
            display_graphs_with_scroll(dist_data)
        else:
            st.warning("Nenhuma distribuição disponível para filmes recomendados.")

        # **Gráfico 2: Média das Avaliações**
        st.subheader("🎥 Média das Avaliações por Filme Recomendado")
        avg_data = fetch_recommendation_average(recommended_movie_ids)
        if not avg_data.empty:
            avg_chart = alt.Chart(avg_data).mark_bar().encode(
                x=alt.X('movieId:O', title='ID do Filme'),
                y=alt.Y('avg_rating:Q', title='Média das Avaliações'),
                tooltip=['movieId', 'avg_rating']
            ).properties(title='Média das Avaliações dos Filmes Recomendados')
            st.altair_chart(avg_chart, use_container_width=True)
        else:
            st.warning("Nenhuma média disponível para filmes recomendados.")

        # **Gráfico 3: Contagem de Avaliações**
        st.subheader("📈 Número de Avaliações por Filme Recomendado")
        count_data = fetch_recommendation_count(recommended_movie_ids)
        if not count_data.empty:
            count_chart = alt.Chart(count_data).mark_bar().encode(
                x=alt.X('movieId:O', title='ID do Filme'),
                y=alt.Y('count:Q', title='Número de Avaliações'),
                tooltip=['movieId', 'count:Q']
            ).properties(title='Número de Avaliações dos Filmes Recomendados')
            st.altair_chart(count_chart, use_container_width=True)
        else:
            st.warning("Nenhuma contagem disponível para filmes recomendados.")
    else:
        st.warning("Nenhuma recomendação disponível para este usuário.")

if mostrar_analise:
    st.header("📊 Análise Detalhada")

    # Seção 1: Avaliações do Usuário
    st.subheader("🔎 Avaliações do Usuário")
    st.markdown("<br>", unsafe_allow_html=True)
    user_ratings = fetch_user_ratings(user_id)
    if not user_ratings.empty:

        col1, col2, col3 = st.columns([3, 1, 15])

        with col1:
            st.write("As avaliações fornecidas pelo usuário são exibidas abaixo.")
            st.dataframe(user_ratings)

        with col2:
            st.markdown("<br>", unsafe_allow_html=True)

        with col3:
            chart = alt.Chart(user_ratings).mark_bar().encode(
                x=alt.X("movieId:O", title="ID do Filme"),
                y=alt.Y("rating:Q", title="Avaliação"),
                tooltip=["movieId", "rating"]
            ).properties(title="Avaliações do Usuário")
            st.altair_chart(chart, use_container_width=True)

    # Seção 2: Similaridade de Usuários
    st.subheader("🤝 Similaridade de Usuários")
    st.markdown("<br>", unsafe_allow_html=True)
    user_similarity = fetch_user_similarity(user_id)
    if not user_similarity.empty:
        
        col1, col2, col3 = st.columns([3, 1, 15])

        with col1:
            st.write("Os usuários mais similares ao atual:")
            st.dataframe(user_similarity)

        with col2:
            st.markdown("<br>", unsafe_allow_html=True)

        with col3:
            chart = alt.Chart(user_similarity).mark_bar().encode(
                x=alt.X("userId:O", title="ID do Usuário"),
                y=alt.Y("similarity:Q", title="Similaridade"),
                tooltip=["userId", "similarity"]
            ).properties(title="Usuários mais Similares")
            st.altair_chart(chart, use_container_width=True)
    
    st.markdown("<br>", unsafe_allow_html=True)

    # Exibir filmes avaliados por usuários similares
    similar_users_movies = fetch_movies_rated_by_similar_users(user_id, user_similarity)
    st.subheader("🎬 Filmes Avaliados pelos Usuários Similares")
    st.write("Aqui estão alguns filmes avaliados pelos usuários mais semelhantes ao seu:")
    st.dataframe(similar_users_movies)

    st.markdown("<br>", unsafe_allow_html=True)

    # Seção 3: Matriz Usuário x Filmes
    st.subheader("📋 Matriz Usuário x Filmes")
    user_movie_matrix = fetch_user_movie_matrix(user_id)
    if not user_movie_matrix.empty:
        st.write("Matriz que mostra as avaliações entre usuários e filmes.")
        st.dataframe(user_movie_matrix)
