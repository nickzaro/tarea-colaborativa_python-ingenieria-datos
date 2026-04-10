import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

def plot_top_ratings(df):
    plt.figure(figsize=(10, 6))
    top_ratings = df['rating'].value_counts().head(10).index
    
    ax = sns.countplot(data=df[df['rating'].isin(top_ratings)], 
                       x='rating', hue='rating', palette='Set2', legend=False,
                       order=top_ratings, dodge=False)
    
    plt.title('Top 10 Clasificaciones (Ratings) más comunes')
    plt.xlabel('Clasificación (Rating)')
    plt.ylabel('Cantidad de Títulos')
    
    for container in ax.containers:
        ax.bar_label(container, padding=3)
        
    plt.tight_layout()
    plt.savefig('output/1_top_ratings.png')
    plt.close()


def plot_type_proportions(df):
    plt.figure(figsize=(8, 8))
    counts = df['type'].value_counts()
    plt.pie(counts, labels=counts.index, autopct='%1.1f%%', colors=['skyblue', 'lightcoral'], startangle=90)
    plt.title('Proporción de Películas vs Series')
    plt.legend(counts.index, title="Tipo de Contenido", loc="upper left", bbox_to_anchor=(1, 0.9))
    plt.tight_layout()
    plt.savefig('output/2_proporcion_peliculas_series.png')
    plt.close()


def plot_violin_releases(df):
    plt.figure(figsize=(10, 6))
    sns.violinplot(data=df[df['release_year'] > 1990], x='type', y='release_year', 
                   hue='type', palette='muted', legend=True, dodge=False)
    plt.title('Distribución de Lanzamientos (Violin)')
    plt.xlabel('Tipo de Contenido')
    plt.ylabel('Año de Lanzamiento Original')
    plt.legend(title="Clasificación")
    plt.tight_layout()
    plt.savefig('output/3_distribucion_lanzamientos_violin.png')
    plt.close()


def plot_correlation_heatmap(df):
    plt.figure(figsize=(8, 6))
    corr_cols = ['release_year', 'year_added', 'duration_num']
    corr_matrix = df[corr_cols].corr()
    
    ax = sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, 
                     cbar_kws={'label': 'Grado de correlación'})
    
    labels = ['Año Lanzamiento', 'Año añadido a Netflix', 'Duración (minutos)']
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_yticklabels(labels, rotation=0)
    
    plt.title('Matriz de Correlación')
    plt.tight_layout()
    plt.savefig('output/4_matriz_correlacion.png')
    plt.close()


def plot_gauss_duration(movies_df):
    plt.figure(figsize=(10, 6))
    sns.histplot(data=movies_df, x='duration_num', kde=True, color='purple', label='Películas')
    plt.title('Campana de Gauss: Distribución de Duración (Películas)')
    plt.xlabel('Duración (minutos)')
    plt.ylabel('Frecuencia / Cantidad de Títulos')
    plt.legend(title="Tipo")
    plt.xlim(0, 250)
    plt.tight_layout()
    plt.savefig('output/5_campana_gauss_duracion.png')
    plt.close()


def plot_scatter_duration(movies_df):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=movies_df[movies_df['release_year'] > 1980], 
                    x='release_year', y='duration_num', alpha=0.3, color='green', label='Duración min.')
    plt.title('Dispersión: Año de Lanzamiento vs Duración')
    plt.xlabel('Año de Lanzamiento')
    plt.ylabel('Duración (minutos)')
    plt.legend(title="Valor Muestral")
    plt.tight_layout()
    plt.savefig('output/6_dispersion_duracion.png')
    plt.close()


def plot_content_distribution(final_df):
    print("Iniciando generación individual de los gráficos...")
    sns.set_theme(style="whitegrid")
    
    os.makedirs("output", exist_ok=True)
    movies_df = final_df[final_df['type'].str.upper() == 'MOVIE']
    
    plot_top_ratings(final_df)
    plot_type_proportions(final_df)
    plot_violin_releases(final_df)
    plot_correlation_heatmap(final_df)
    plot_gauss_duration(movies_df)
    plot_scatter_duration(movies_df)
    
    print("¡Proceso exitoso! Se han generado 6 archivos PNG independientes en la carpeta 'output/'.")
