import os
import warnings
import matplotlib
matplotlib.use('Agg')   # Backend no-interactivo: obligatorio en contenedores sin pantalla
import matplotlib.pyplot as plt
import seaborn as sns
# ==============================================================================
# MÓDULO DE VISUALIZACIÓN (VISUALIZE) — Adaptado para Apache Airflow
# Cada función genera y exporta un gráfico PNG independiente a output/.
# ==============================================================================
OUTPUT_DIR = "output"

def plot_top_ratings(df) -> None:
    """Top 10 clasificaciones de edad más frecuentes (barras)."""
    plt.figure(figsize=(10, 6))
    top_ratings = df['rating'].value_counts().head(10).index
    ax = sns.countplot(
        data=df[df['rating'].isin(top_ratings)],
        x='rating', hue='rating', palette='Set2',
        legend=False, order=top_ratings, dodge=False
    )
    plt.title('Top 10 Clasificaciones (Ratings) más comunes')
    plt.xlabel('Clasificación (Rating)')
    plt.ylabel('Cantidad de Títulos')
    for container in ax.containers:
        ax.bar_label(container, padding=3)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/1_top_ratings.png')
    plt.close()
    print("  ✔ Gráfico 1 generado: 1_top_ratings.png")

def plot_type_proportions(df) -> None:
    """Proporción de Películas vs Series (pie chart)."""
    plt.figure(figsize=(8, 8))
    counts = df['type'].value_counts()
    plt.pie(
        counts, labels=counts.index, autopct='%1.1f%%',
        colors=['skyblue', 'lightcoral'], startangle=90
    )
    plt.title('Proporción de Películas vs Series')
    plt.legend(counts.index, title="Tipo de Contenido", loc="upper left", bbox_to_anchor=(1, 0.9))
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/2_proporcion_peliculas_series.png')
    plt.close()
    print("  ✔ Gráfico 2 generado: 2_proporcion_peliculas_series.png")

def plot_violin_releases(df) -> None:
    """Distribución de años de estreno (violin plot)."""
    plt.figure(figsize=(10, 6))
    sns.violinplot(
        data=df[df['release_year'] > 1990],
        x='type', y='release_year',
        hue='type', palette='muted', legend=True, dodge=False
    )
    plt.title('Distribución de Lanzamientos (Violin)')
    plt.xlabel('Tipo de Contenido')
    plt.ylabel('Año de Lanzamiento Original')
    plt.legend(title="Clasificación")
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/3_distribucion_lanzamientos_violin.png')
    plt.close()
    print("  ✔ Gráfico 3 generado: 3_distribucion_lanzamientos_violin.png")

def plot_correlation_heatmap(df) -> None:
    """Matriz de correlación entre variables numéricas (heatmap)."""
    plt.figure(figsize=(8, 6))
    corr_cols = ['release_year', 'year_added', 'duration_num']
    corr_matrix = df[corr_cols].corr()
    ax = sns.heatmap(
        corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1,
        cbar_kws={'label': 'Grado de correlación'}
    )
    labels = ['Año Lanzamiento', 'Año añadido a Netflix', 'Duración (minutos)']
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_yticklabels(labels, rotation=0)
    plt.title('Matriz de Correlación')
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/4_matriz_correlacion.png')
    plt.close()
    print("  ✔ Gráfico 4 generado: 4_matriz_correlacion.png")

def plot_gauss_duration(movies_df) -> None:
    """Campana de Gauss de duración en minutos (solo películas)."""
    plt.figure(figsize=(10, 6))
    sns.histplot(data=movies_df, x='duration_num', kde=True, color='purple', label='Películas')
    plt.title('Campana de Gauss: Distribución de Duración (Películas)')
    plt.xlabel('Duración (minutos)')
    plt.ylabel('Frecuencia / Cantidad de Títulos')
    plt.legend(title="Tipo")
    plt.xlim(0, 250)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/5_campana_gauss_duracion.png')
    plt.close()
    print("  ✔ Gráfico 5 generado: 5_campana_gauss_duracion.png")

def plot_scatter_duration(movies_df) -> None:
    """Dispersión: Año de lanzamiento vs Duración (solo películas)."""
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=movies_df[movies_df['release_year'] > 1980],
        x='release_year', y='duration_num',
        alpha=0.3, color='green', label='Duración min.'
    )
    plt.title('Dispersión: Año de Lanzamiento vs Duración')
    plt.xlabel('Año de Lanzamiento')
    plt.ylabel('Duración (minutos)')
    plt.legend(title="Valor Muestral")
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/6_dispersion_duracion.png')
    plt.close()
    print("  ✔ Gráfico 6 generado: 6_dispersion_duracion.png")

def run_visualize(source_table: str, sql_tools) -> None:
    """
    [Patrón Fachada] Orquestador de la fase Visualize.

    Lee los datos con features desde SQLite y genera los 6 gráficos PNG en output/.

    Parámetros
    ----------
    source_table : str
        Tabla SQLite con los datos caracterizados (salida de Characterize).
    sql_tools : module
        Módulo con load_df_from_sql.
    """
    warnings.simplefilter(action='ignore', category=FutureWarning)

    print("\n" + "=" * 40)
    print("INICIANDO FASE DE VISUALIZACIÓN (VISUALIZE)")
    print("=" * 40)

    # 1. Leer datos desde SQLite
    df = sql_tools.load_df_from_sql(source_table)
    print(f"Registros disponibles para graficar: {len(df)}")

    # La columna categórica en SQLite se guarda como texto; la reconvertimos
    df['type'] = df['type'].astype(str)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sns.set_theme(style="whitegrid")

    # 2. Subconjunto solo de películas
    movies_df = df[df['type'] == 'Movie']

    # 3. Generar los 6 gráficos
    print("Generando gráficos...")
    plot_top_ratings(df)
    plot_type_proportions(df)
    plot_violin_releases(df)
    plot_correlation_heatmap(df)
    plot_gauss_duration(movies_df)
    plot_scatter_duration(movies_df)

    print("=" * 40)
    print(f"VISUALIZACIÓN COMPLETADA — 6 PNGs en '{OUTPUT_DIR}/'")
    print("=" * 40 + "\n")
