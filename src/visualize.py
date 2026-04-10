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

