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
