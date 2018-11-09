#!/bin/sh

set -e

export COMPOSE_CONVERT_WINDOWS_PATHS=1
if [ -n "$1" ]; then
    if [ $1 = "create" ]; then
        if [ -z "$2" ]; then
            $0 "$1" "dev" "$2" "$3" "$3"
            exit $?
        fi
        if [ $2 = "dev" ]; then        
            ENV=dev
            POSTGRES_USERNAME="postgres"
            POSTGRES_PASSWORD="temporary"
            POSTGRES_READONLY_USERNAME="readonly"
            POSTGRES_READONLY_PASSWORD="KM8Rd9cJ4724nbRW"   
        elif [ $2 = "prod" ]; then
            ENV=prod        
            if [ -z "$POSTGRES_USERNAME" ]; then
                echo '$POSTGRES_USERNAME is missing'
                exit 1
            fi
            if [ -z "$POSTGRES_PASSWORD" ]; then
                echo '$POSTGRES_PASSWORD is missing'
                exit 1
            fi
            if [ -z "$POSTGRES_READONLY_USERNAME" ]; then
                echo '$POSTGRES_READONLY_USERNAME is missing'            
                exit 1
            fi
            if [ -z "$POSTGRES_READONLY_PASSWORD" ]; then
                echo '$POSTGRES_READONLY_PASSWORD is missing'            
                exit 1
            fi
        else
            echo "Environment is missing"
            exit 1
        fi    

        if [ -n "$SCHEDULE" ]; then    
            SCHEDULE=--label=cron.schedule="$SCHEDULE"
        fi

        NAME=$3
        shift    
        shift  
        shift            

        docker run -d --network stack_internal_network "$SCHEDULE" --name $NAME -e CONNECTION_STRING=postgresql://"$POSTGRES_USERNAME":"$POSTGRES_PASSWORD"@postgres:5432/cvmData -e CONFIG.READONLY_USERNAME="$POSTGRES_READONLY_USERNAME" -e CONFIG.READONLY_PASSWORD="$POSTGRES_READONLY_PASSWORD" conradoqg/cvm-fund-explorer-workers $@
    elif [ $1 = "rm" ]; then
        docker service rm cvmFundExplorer
    fi
else
    echo "Usage: run ACTION ENVIRONMENT NAME WORKER_ARGS"
    echo ""
    echo "ACTION = create/rm"
    echo "ENVIRONMENT = dev/prod"
    echo "NAME = container's name"
    echo "WORKER_ARGS = worker's args"
fi