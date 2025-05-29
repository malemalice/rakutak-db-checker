#!/bin/bash

case "$1" in
    "run")
        python main.py --run
        ;;
    "shell")
        /bin/bash
        ;;
    *)
        echo "Usage:"
        echo "  docker compose exec app /app/run.sh run    - Run the validation scheduler"
        echo "  docker compose exec app /app/run.sh shell  - Start an interactive shell"
        exit 1
        ;;
esac 