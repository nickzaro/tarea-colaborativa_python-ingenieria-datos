import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# ==============================================================================
# MÓDULO DE VISUALIZACIÓN DE DATOS (DATA VISUALIZATION MODULE)
# Principio de Responsabilidad Única (SRP): Cada función tiene la única tarea de
# renderizar y exportar un gráfico estadístico independiente para el Dashboard.
# ==============================================================================

def plot_top_ratings(df):
    """
    Genera un diagrama de barras con el Top 10 de las restricciones de edad más frecuentes.
    
    ¿Por qué se realizó?:
    Para identificar demográficamente el público objetivo dominante en Netflix.
    
    ¿Por qué debe estar en el reporte?:
    Muestra de un vistazo si el catálogo apunta más a nichos adultos (TV-MA) 
    o audiencias familiares (PG-13, TV-PG), sirviendo como punto de partida 
    para tomar decisiones sobre adquisición de futuras licencias.
    """
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
    """
    Genera un gráfico circular (Pie Chart) para comparar el volumen de Series vs Películas.
    
    ¿Por qué se realizó?:
    Para entender la distribución macro del modelo de negocio de la plataforma.
    
    ¿Por qué debe estar en el reporte?:
    Cualquier inversor o analista debe conocer primero la naturaleza del producto 
    que ofrece la plataforma. Este gráfico demuestra claramente que Netflix sigue
    siendo primariamente una plataforma de largometrajes, fundamentando su estrategia base.
    """
    plt.figure(figsize=(8, 8))
    counts = df['type'].value_counts()
    plt.pie(counts, labels=counts.index, autopct='%1.1f%%', colors=['skyblue', 'lightcoral'], startangle=90)
    plt.title('Proporción de Películas vs Series')
    plt.legend(counts.index, title="Tipo de Contenido", loc="upper left", bbox_to_anchor=(1, 0.9))
    plt.tight_layout()
    plt.savefig('output/2_proporcion_peliculas_series.png')
    plt.close()


def plot_violin_releases(df):
    """
    Genera un gráfico de Violín distribuyendo los años de estreno originales de los contenidos.
    
    ¿Por qué se realizó?:
    Para analizar la antigüedad de los contenidos dentro de ambas categorías (Series y Películas).
    
    ¿Por qué debe estar en el reporte?:
    Reemplaza al clásico Boxplot ofreciendo la "densidad". Nos relata visualmente
    que las Series tienden a ser un mercado exclusivamente moderno (la barriga del violín está arriba),
    mientras que las Películas tienen una larga cola de productos clásicos/antiguos en la plataforma.
    """
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
    """
    Genera una Matriz de Correlación lineal entre las variables continuas numéricas.
    
    ¿Por qué se realizó?:
    Para descartar o confirmar dependencias matemáticas directas entre las métricas del dataset.
    
    ¿Por qué debe estar en el reporte?:
    Es el centro del análisis predictivo. Permite inferir si "los títulos más modernos
    son más cortos" o "los títulos se agregan poco después de estrenarse", lo cual
    descifra las políticas de compra de Netflix y sus patrones de adquisición en la década.
    """
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
    """
    Genera un histograma con estimación de densidad de kernel (Campana de Gauss).
    
    ¿Por qué se realizó?:
    Para encontrar matemáticamente la duración "ideal" o esperada estadística de una película.
    
    ¿Por qué debe estar en el reporte?:
    Demuestra la conformación a la tendencia normal en producciones cinematográficas.
    Sirve a los equipos productivos para saber el "punto dulce" (estándar de la industria) 
    para aprobar guiones: usualmente el pico absoluto de la campana (90 a 100 minutos).
    """
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
    """
    Genera un diagrama de dispersión (Scatter Plot) correlacionando Año estático vs Duración.
    
    ¿Por qué se realizó?:
    Para rastrear "outliers" (valores atípicos) y tendencias culturales cinematográficas históricas.
    
    ¿Por qué debe estar en el reporte?:
    Cubre el defecto de la correlación lineal básica revelando si, independientemente de 
    todo el catálogo global, existen porciones o grupos de años en el pasado donde 
    hubiera la costumbre de hacer películas enormemente largas (outliers esparcidos).
    """
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
    """
    ============================================================================
    Orquestador / Facade Pattern (Patrón Fachada según Clean Code).
    
    ¿Por qué se realizó?:
    Para aislar la lógica de instanciación del cliente externo (job.py).
    Cumple con aislar las sub-rutinas en funciones declarativas independientes.
    ============================================================================
    """
    print("Iniciando generación individual de los gráficos...")
    sns.set_theme(style="whitegrid")
    
    os.makedirs("output", exist_ok=True)
    movies_df = final_df[final_df['type'] == 'MOVIE']
    
    plot_top_ratings(final_df)
    plot_type_proportions(final_df)
    plot_violin_releases(final_df)
    plot_correlation_heatmap(final_df)
    plot_gauss_duration(movies_df)
    plot_scatter_duration(movies_df)
    
    print("¡Proceso exitoso! Se han generado 6 archivos PNG independientes en la carpeta 'output/'.")
