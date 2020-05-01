#!/bin/bash -e

# load the library
. bashopts.sh # For more information check it out https://gitlab.mbedsys.org/mbedsys/bashopts

# Enable backtrace display on error
#trap 'bashopts_exit_handle' ERR

# Initialize the library
bashopts_setup -n $0 -d "Run a worker" -u "$0 [options and commands] [-- [extra args]]\n\nCOMMANDS:\n    List of arguments to be passed to the container" -x

# Declare the options
bashopts_declare -n ACTION -l action -o a -d "Action" -t enum -e 'create' -e 'remove' -r
bashopts_declare -n NAME -l name -o n -d "Name" -t string -r
bashopts_declare -n SCHEDULE -l schedule -o s -d "Schedule, a crontab pattern to determine a scheduled execution (e.g.: \"0 * * * * ?\")" -t string
bashopts_declare -n DEBUG -l debug -o d -d "Debug by exposing 9229 port and not daemonizing" -t boolean -v false
bashopts_declare -n POSTGRES_USERNAME -l postgres-username -d "Postgres username" -t string -v postgres
bashopts_declare -n POSTGRES_PASSWORD -l postgres-password -d "Postgres password" -t string -v temporary
bashopts_declare -n POSTGRES_READONLY_USERNAME -l postgres-readonly-username -d "Postgres readonly username" -t string -v readonly
bashopts_declare -n POSTGRES_READONLY_PASSWORD -l postgres-readonly-password -d "Postgres readonly password" -t string -v KM8Rd9cJ4724nbRW
bashopts_declare -n WTD_TOKEN -l wtd-token -d "WorldTradingData.com token" -t string -v 123
bashopts_declare -n EOD_TOKEN -l eod-token -d "eodhistoricaldata.com token" -t string -v 123

# Parse arguments
bashopts_parse_args "$@"

# Process options
bashopts_process_opts

if [ -v ${bashopts_commands[0]} ]; then
    echo "[ERRO] Command is missing"   
    exit 1 
fi

echo "Settings"
echo "NAME: $NAME"
echo "SCHEDULE: $SCHEDULE"
echo "DEBUG: $DEBUG"
echo "POSTGRES_USERNAME: $POSTGRES_USERNAME"
echo "POSTGRES_PASSWORD: $POSTGRES_PASSWORD"
echo "POSTGRES_READONLY_USERNAME: $POSTGRES_READONLY_USERNAME"
echo "POSTGRES_READONLY_PASSWORD: $POSTGRES_READONLY_PASSWORD"
echo "WTD_TOKEN: $WTD_TOKEN"
echo "EOD_TOKEN: $EOD_TOKEN"

echo "Deploying $ENV environment stack"

if [ $ACTION = "create" ]; then    
    if ! $DEBUG; then
        DEBUG_ARGS="-d"
    else
        DEBUG_ARGS="-p 9229:9229"
    fi

    if [ -n "$SCHEDULE" ]; then        
        docker run --network stack_internal_network $DEBUG_ARGS --label=cron.schedule="$SCHEDULE" $EXPOSE_ARGS --name $NAME --cap-add=SYS_ADMIN  -v worker_volume_$NAME:/cvm-fund-explorer-workers/db -e CONNECTION_STRING=postgresql://"$POSTGRES_USERNAME":"$POSTGRES_PASSWORD"@postgres:5432/cvmData -e READONLY_USERNAME="$POSTGRES_READONLY_USERNAME" -e READONLY_PASSWORD="$POSTGRES_READONLY_PASSWORD" -e EOD_TOKEN="$EOD_TOKEN" conradoqg/cvm-fund-explorer-workers ${bashopts_commands[@]}
    else
        docker run --network stack_internal_network $DEBUG_ARGS $EXPOSE_ARGS --name $NAME --cap-add=SYS_ADMIN  -v worker_volume_$NAME:/cvm-fund-explorer-workers/db -e CONNECTION_STRING=postgresql://"$POSTGRES_USERNAME":"$POSTGRES_PASSWORD"@postgres:5432/cvmData -e READONLY_USERNAME="$POSTGRES_READONLY_USERNAME" -e READONLY_PASSWORD="$POSTGRES_READONLY_PASSWORD" -e WTD_TOKEN="$WTD_TOKEN" -e EOD_TOKEN="$EOD_TOKEN" conradoqg/cvm-fund-explorer-workers ${bashopts_commands[@]}
    fi    
elif [ $ACTION = "remove" ]; then
    docker service rm cvmFundExplorer
fi