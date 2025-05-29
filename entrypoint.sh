#!/bin/bash

# If no arguments are provided, show help
if [ $# -eq 0 ]; then
    echo "Usage:"
    echo "  docker compose exec app run    - Run the validation scheduler"
    echo "  docker compose exec app shell  - Start an interactive shell"
    exit 1
fi

# Handle different commands
case "$1" in
    "run")
        /usr/local/bin/python /app/main.py --run
        ;;
    "shell")
        exec /bin/bash
        ;;
    *)
        # Pass through any other commands
        exec "$@"
        ;;
esac 