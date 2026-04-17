#!/bin/bash

# Función para mostrar ayuda
show_help() {
  echo "Uso: ./run.sh [comando]"
  echo ""
  echo "Comandos disponibles:"
  echo "  start   - Levanta Airflow en segundo plano"
  echo "  stop    - Detiene todos los contenedores"
  echo "  restart - Reinicia los servicios"
  echo "  init    - Prepara la base de datos (solo la primera vez)"
  echo "  logs    - Muestra los logs en tiempo real"
  echo "  status  - Muestra el estado de los contenedores"
}

case $1 in
  init)
    echo "⚙️ Configurando UID en .env..."
    echo "AIRFLOW_UID=$(id -u)" > .env
    echo "🗄️ Inicializando base de datos..."
    docker compose up airflow-init
    ;;
  start)
    echo "🚀 Levantando Airflow..."
    docker compose up -d
    echo "✅ Airflow listo en http://localhost:8080"
    ;;
  stop)
    echo "🛑 Deteniendo servicios..."
    docker compose down
    ;;
  restart)
    echo "🔄 Reiniciando..."
    docker compose restart
    ;;
  logs)
    docker compose logs -f
    ;;
  status)
    docker compose ps
    ;;
  *)
    show_help
    ;;
esac