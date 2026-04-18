import os
import warnings

import matplotlib
matplotlib.use('Agg')   # Backend no-interactivo: obligatorio en contenedores sin pantalla
import matplotlib.pyplot as plt
import seaborn as sns

OUTPUT_DIR = "output"

def plot_top_ratings(df) -> None:
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