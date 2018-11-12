# Copyright 2017-2018 Emeric Verschuur <emeric@mbedsys.org>
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

BASHOPTS_VERSION=2.1.1

bashopts_exit_handle() {
  local err=$?
  set +o xtrace
  local code="${1:-1}"
  echo "Error in ${BASH_SOURCE[1]}:${BASH_LINENO[0]}. '${BASH_COMMAND}' exited with status $err"
  # Print out the stack trace described by $function_stack
  if [ ${#FUNCNAME[@]} -gt 2 ]
  then
    echo "Call tree:"
    for ((i=1;i<${#FUNCNAME[@]}-1;i++))
    do
      echo " $i: ${BASH_SOURCE[$i+1]}:${BASH_LINENO[$i]} ${FUNCNAME[$i]}(...)"
    done
  fi
  echo "Exiting with status ${code}"
  exit "${code}"
}

# trap ERR to provide an error handler whenever a command exits nonzero
#  this is a more verbose version of set -o errexit
trap 'bashopts_exit_handle' ERR
# setting errtrace allows our ERR trap handler to be propagated to functions,
#  expansions and subshells
set -o errtrace

# display a error (fatal)
bashopts_log() {
    if [ -n "$bashopts_log_handler" ]; then
        $bashopts_log_handler "$@"
        return;
    fi
    local level=$1;
    shift || bashopts_log C "Usage bashopts_log <level> message"
    case "${level,,}" in
        c|critical)
            >&2 printf "[CRIT] %s\n" "$@"
            exit 1
            ;;
        e|error)
            >&2 printf "[ERRO] %s\n" "$@"
            ;;
        w|warning)
            >&2 printf "[WARN] %s\n" "$@"
            ;;
        *)
            bashopts_log C "Invalid log level: $level"
            ;;
    esac
}

if [ ! "${BASH_VERSINFO[0]}" -ge 4 ]; then
    bashopts_log C "bashopts require BASH version 4 or greater"
fi

# extract the value part of a declaration ("the value")
bashopts_get_def() {
    declare | grep "^$1=" | sed -E 's/^[^=]+=//g'
    # NOTE: alternative but not working in some case...:
    # declare -p $1 | sed -E "s/^declare\\s[^=]*=//g"
}

# extract the full declaration (name="the value")
bashopts_get_def_full() {
    declare | grep "^$1="
    # NOTE: alternative but not working in some case...:
    # declare -p $1 | sed -E "s/^declare\\s[^=]*=/$1=/g"
}

# check and format an option name value
bashopts_check_opt_name() {
    if [[ "$1" =~ ^[a-zA-Z0-9_]+$ ]]; then
        echo $1
        return 0
    fi
    bashopts_log E "'$1' is not a valid variable name"
    return 1
}

# check and format a number value
bashopts_check_number() {
    if [ -z "$1" ]; then
        echo 0
        return 0
    elif [[ "$1" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
        echo $1
        return 0
    fi
    bashopts_log E "Option $op: '$1' is not a valid number"
    return 1
}

# check and format a boolean value
bashopts_check_boolean() {
    case "${1,,}" in
        ''|f|false|0)
            echo "false"
            return 0
            ;;
        t|true|1)
            echo "true"
            return 0
            ;;
        *)
            bashopts_log E "Option $op: '$1' is not a valid boolean value"
            return 1
            ;;
    esac
}

# check and format a string value
bashopts_check_string() {
    echo "$1"
    return 0
}

# check and format a enumeration value
bashopts_check_enumeration() {
    local line expr values
    while read -r line; do
        expr="^($line)\$"
        if [[ "$1" =~ $expr ]]; then
            echo "${line##*|}"
            return 0
        fi
        values+=("'${line##*|}'")
    done <<< "${2:-${bashopts_optprop_enum_values[$op]}}"
    bashopts_log E "Option $op: Invalid value '$1' (accepted values are: ${values[*]})"
    return 1
}

# check nothing
bashopts_check_nothing() {
    echo "$1"
    return 0
}

# declare the options property arrays
for f in name default expression short_opt long_opt description type enum_values method check setting interactive req_value; do
    eval declare -x -A bashopts_optprop_$f
done

# declare the associative array: arg name => option name
declare -x -A bashopts_arg2op
# option list in the declaration order
bashopts_optlist=()
# commands list (from global tool_name [args] [commands] [-- optional extra args])
bashopts_commands=()
# extra arguments list (from global tool_name [args] [commands] [-- optional extra args])
bashopts_extra_args=()
# tool name (from global tool_name [args] [commands] [-- optional extra args])
bashopts_tool_name=$0

# STEP 1: setup
bashopts_setup() {
    local arg arglist no_default_opts non_interactive disable_interactive
    if ! arglist=$(getopt -o "n:d:u:s:yxp" -n "$0 " -- "$@"); then
        bashopts_log C "Usage bashopts_setup:" \
            "        -n <val>  Tool name" \
            "        -d <val>  Tool description" \
            "        -u <val>  Tool usage description" \
            "        -s <val>  setting file path" \
            "        -y        Set non interactive mode as the default mode" \
            "        -x        Disable entirely interactive mode" \
            "        -p        Force value storage even if the value is equal to the default one"
    fi
    eval set -- "$arglist";
    # Store the global bashopts properties
    while true; do
        arg=$1
        shift
        case "$arg" in
            -n) bashopts_tool_name=$1;           shift;;
            -d) bashopts_tool_description=$1;    shift;;
            -u) bashopts_tool_usage=$1;          shift;;
            -s) bashopts_tool_settings_path=$1;  shift;;
            -y) non_interactive="true";;
            -x) disable_interactive="true";;
            -p) bashopts_tool_settings_force_write="true";;
            --) break;;
            *)  bashopts_log C "Fatal error";;
        esac
    done
    if [ -z "$bashopts_tool_name" ]; then
        bashopts_log C "Undefined tool name"
    fi
    if [ -z "$bashopts_tool_description" ]; then
        bashopts_log C "Undefined tool description"
    fi
    bashopts_tool_usage=${bashopts_tool_usage:-"$bashopts_tool_name [options and commands] [-- [extra args]]"}
    # add the default options
    bashopts_declare -n __BASHOPTS_DISPLAY_HELP__ -l help -o h -d "Display this help"
    if [ "$disable_interactive" == "true" ]; then
        BASHOPTS_INTERACTIVE="false"
    else
        if [ "$non_interactive" == "true" ]; then
            bashopts_declare -n BASHOPTS_INTERACTIVE -l interactive -o i -d "Interactive mode"
        else
            bashopts_declare -n BASHOPTS_NON_INTERACTIVE -l non-interactive -o n -d "Non interactive mode"
        fi
    fi
}

# STEP 2: add options
bashopts_declare() {
    local arg arglist options options_enum_values
    if ! arglist=$(getopt -o "n:v:x:o:l:d:t:e:m:k:rsi" -n "$0 " -- "$@"); then
        bashopts_log C "Usage bashopts_declare:" \
            "        -n <val>  Name" \
            "        -v <val>  Default value" \
            "        -x <val>  Bash expression: like default but this expression is computed and can contain variables and other bash expression" \
            "        -o <val>  Short option" \
            "        -l <val>  Long option" \
            "        -d <val>  Description" \
            "        -t <val>  Value type: string, enumeration, number, boolean (default)" \
            "        -e <val>  Enum element: restrict accepted values with a list of '-e <element>' options (you have to set one '-e <val>' by elements)" \
            "        -m <val>  Method: set (DEFAULT: simple value), add (list with several values)" \
            "        -k <val>  Custom check method (bash function)" \
            "        -r        Value required" \
            "        -i        Enable interactive edition" \
            "        -s        Store in setting"
    fi
    eval set -- "$arglist";
    declare -A options
    # parse all the parameters
    while true; do
        arg=$1
        shift
        case "$arg" in
            -n) options[name]=$(bashopts_check_opt_name $1 || exit 1); shift;;
            -v) options[default]=$1;        shift;;
            -x) options[expression]=$1;     shift;;
            -o) options[short_opt]=$1;      shift;;
            -l) options[long_opt]=$1;       shift;;
            -d) options[description]=$1;    shift;;
            -t) options[type]=$1;           shift;;
            -e) options_enum_values+=("$1");shift;;
            -m) options[method]=$1;         shift;;
            -k) options[check]=$1;          shift;;
            -s) options[setting]="true";;
            -i) options[interactive]="true";;
            -r) options[req_value]="true";;
            --) break;;
            *)  bashopts_log C "Fatal error";;
        esac
    done
    # Check incompatible -v and -r options
    if [ -n "${options[default]}" ] && [ "${options[req_value]}" == "true" ]; then
        bashopts_log C "bashopts_declare: -r and -v options cannot be activated at the same time"
    fi
    # format the type and check/format the default value
    case "${options[type],,}" in
        ''|b|bool|boolean)
            options[type]="boolean"
            ;;
        e|enum|enumeration)
            options[type]="enumeration"
            if [ ${#options_enum_values[@]} -lt 2 ]; then
                bashopts_log C "bashopts_declare: ${options[name]} enumeration need at least two elements (two '-e <val>' calls at least)"
            fi
            options[enum_values]="$(printf "%s\n" "${options_enum_values[@]}")"
            ;;
        s|str|string)
            options[type]="string"
            ;;
        n|num|number)
            options[type]="number"
            ;;
        *)
            bashopts_log C "Invalid type ${options[type]}"
            ;;
    esac
    # Check for incompatibility with old version (-e opt moved to -x)
    if [ "${options[type]}" != "enumeration" ] && [ ${#options_enum_values[@]} -gt 0 ]; then
        bashopts_log C "bashopts_declare: The former '-e' option is now moved to '-x'" \
            " => the new '-e' is reserved for enumeration elements"
    fi
    # Setup check value method
    if ! [[ -v options[check] ]]; then
        options[check]="bashopts_check_${options[type]}"
    fi
    # format the option method
    case "${options[method],,}" in
        ''|s|set)
            # default: simple value - override
            options[method]="set"
            if [ "${options[type]}" != "string" ] || [[ -v options[default] ]]; then
                # Check the default value format
                if  [ ! -v options[req_value] ]; then
                    if ! options[default]="$(${options[check]} "${options[default]}" "${options[enum_values]}")"; then
                        bashopts_log W "Invalid default value for ${options[name]} option, this value will stay unset"
                        unset options[default]
                    fi
                fi
            fi
            ;;
        a|add)
            # array value - add
            options[method]="add"
            ;;
        *)
            bashopts_log C "Invalid method ${options[method]}"
            ;;
    esac
    # Check option name
    if [[ -v bashopts_optprop_name[${options[name]}] ]]; then
        bashopts_log C "Dupplicate option name '${options[name]}'"
    fi
    # check the short option
    if [[ -v options[short_opt] ]]; then
        if ! [[ ${options[short_opt]} =~ ^[a-zA-Z0-9_-]$ ]]; then
            bashopts_log C "Invalid short option ${options[short_opt]}"
        fi
        if [[ -v bashopts_arg2op[-${options[short_opt]}] ]]; then
            bashopts_log C "Dupplicate short option '${options[short_opt]}'"
        fi
        bashopts_arg2op[-${options[short_opt]}]=${options[name]}
    fi
    # check the long option
    if [[ -v options[long_opt] ]]; then
        if ! [[ ${options[long_opt]} =~ ^[a-zA-Z0-9_-]{2,}$ ]]; then
            bashopts_log C "Invalid long option ${options[long_opt]}"
        fi
        if [[ -v bashopts_arg2op[--${options[long_opt]}] ]]; then
            bashopts_log C "Dupplicate long option '${options[long_opt]}'"
        fi
        bashopts_arg2op[--${options[long_opt]}]=${options[name]}
    fi
    # store the option properties
    for f in ${!options[@]}; do
        eval "bashopts_optprop_$f[${options[name]}]='${options[$f]//\'/\'\\\'\'}'"
    done
    bashopts_optlist+=(${options[name]})
}

bashopts_get_valid_value_list() {
    local op
    case "$1" in
        -*)
            op=${bashopts_arg2op[$1]}
            ;;
        *)
            op=$1
            ;;
    esac
    case "${bashopts_optprop_type[$op]}" in
        boolean)
            echo -e "true\nfalse"
            ;;
        enumeration)
            while read -r line; do
                echo "\"${line##*|}\""
            done <<< "${bashopts_optprop_enum_values[$op]}"
            ;;
    esac
}

# maximum of two values
bashopts_math_max() {
    echo $(($1>$2?$1:$2))
}

# minimum of two values
bashopts_math_min() {
    echo $(($1<$2?$1:$2))
}

# join array element
bashopts_join_by() {
    local sep="$1"
    shift || bashopts_log C "Usage: bashopts_join_by <separator> [elt1 [elt2...]]"
    printf "%s" "$1"
    test $# -gt 1 || return 0
    shift
    printf "$sep%s" "$@"
}

# dump an option value by its name
bashopts_dump_value() {
    local op=$1
    shift || bashopts_log C "Usage: bashopts_dump_value op_name"
    [[ -v "$op" ]] || return 0
    if [ "${bashopts_optprop_method[$op]}" == "set" ]; then
        if [ "${bashopts_optprop_type[$op]}" == "string" ]; then
            echo -n "\"${!op//\"/\\\"}\""
        else
            echo -n "${!op}"
        fi
        return 0
    fi
    eval set -- \"\${${op}[@]}\"
    echo -n "["
    if [ "${bashopts_optprop_type[$op]}" == "string" ]; then
        echo -n "\"${1//\"/\\\"}\""
    else
        echo -n "${1}"
    fi
    shift
    while [ -n "$1" ]; do
        if [ "${bashopts_optprop_type[$op]}" == "string" ]; then
            echo -n ", \"${1//\"/\\\"}\""
        else
            echo -n ", ${1}"
        fi
        shift
    done
    echo -n "]"
}

# display the formated help
bashopts_display_help() {
    local elts optargs_max_len=8 val ncol line
    declare -A optargs
    if tput cols &> /dev/null; then
        ncol=$(tput cols)
    else
        ncol=${COLUMNS:-160}
    fi
    local value_max_len=$(( $ncol / 4 ))
    # compute the good arguments column size
    for op in "${bashopts_optlist[@]}"; do
        elts=()
        unset val
        if ! [[ $op =~ ^__.*__$ ]] && [[ -v $op ]]; then
            val=" $(bashopts_dump_value $op | tr -d '\n')"
        fi
        if [[ -v bashopts_optprop_short_opt[$op] ]]; then elts+=("-${bashopts_optprop_short_opt[$op]}"); fi
        if [[ -v bashopts_optprop_long_opt[$op] ]]; then elts+=("--${bashopts_optprop_long_opt[$op]}"); fi
        optargs[$op]="$(bashopts_join_by , ${elts[@]})${val:0:${value_max_len}}"
        optargs_max_len=$(bashopts_math_max $optargs_max_len ${#optargs[$op]})
    done
    optargs_max_len=$(bashopts_math_min $optargs_max_len $(( $ncol / 3 )) )
    # display global info
    echo
    echo "NAME:"
    echo "    $bashopts_tool_name - $bashopts_tool_description"
    echo
    echo "USAGE:"
    echo -e "    $bashopts_tool_usage"
    echo
    echo "OPTIONS:"    
    for op in "${bashopts_optlist[@]}"; do
        # display arguments, value if available, description, and additional info if available
        printf "    %-${optargs_max_len}s    ${bashopts_optprop_description[$op]}" "${optargs[$op]}"
        if ! [[ $op =~ ^__.*__$ ]]; then
            # display additional information the each properties
            # discarding special options like --help
            echo -n " - [\$$op] (type: ${bashopts_optprop_type[$op]}"
            if [[ -v bashopts_optprop_expression[$op] ]]; then
                printf ", default: \"%.${value_max_len}s\"" "$(tr -d '\n' <<< "${bashopts_optprop_expression[$op]//\"/\\\"}")"
            elif [[ -v bashopts_optprop_default[$op] ]]; then
                if [[ "${bashopts_optprop_type[$op]}" =~ ^(string|enumeration)$ ]]; then
                    printf ", default: \"%.${value_max_len}s\"" "$(tr -d '\n' <<< "${bashopts_optprop_default[$op]//\"/\\\"}")"
                else
                    printf ", default: %.${value_max_len}s" "$(tr -d '\n' <<< "${bashopts_optprop_default[$op]}")"
                fi                
            elif [[ -v bashopts_optprop_req_value[$op] ]]; then
                printf ", required: ${bashopts_optprop_req_value[$op]}"                
            else
                elts=")"
            fi
            if [ "${bashopts_optprop_type[$op]}" == "enumeration" ]; then
                echo -n ", accepted values:$(
                    while read -r line; do
                        echo -n " '${line##*|}'"
                    done <<< "${bashopts_optprop_enum_values[$op]}"
                )"
            fi
            echo ")"
        else
            echo ""
        fi
    done
    test "$1" != "-e" || exit $2
}

# Enable help display on option process
bashopts_display_help_delayed() {
    __BASHOPTS_DISPLAY_HELP__="true"
}

# display all otions values and properties
bashopts_display_summary() {
    local elts desc_max_len=0 val dval ncol
    if tput cols &> /dev/null; then
        ncol=$(tput cols)
    else
        ncol=${COLUMNS:-160}
    fi
    local value_max_len=$(( $ncol / 4 ))
    declare -A optargs
    for op in "${bashopts_optlist[@]}"; do
        desc_max_len=$(bashopts_math_max $desc_max_len ${#bashopts_optprop_description[$op]})
    done
    for op in "${bashopts_optlist[@]}"; do
        if ! [[ $op =~ ^__.*__$ ]]; then
            printf "* %-${desc_max_len}.${value_max_len}s : $(bashopts_dump_value $op | tr -d '\n')\n" "${bashopts_optprop_description[$op]}"
        fi
    done
}

# STEP 3: parse arg
bashopts_parse_args() {
    local op arg val args is_arg short_opts long_opts

    # split argument into two arrays: normal and extra arguments
    is_arg=1
    args=()
    for arg in "$@"; do
        if [ $is_arg -eq 1 ]; then
            if [ "$arg" == "--" ]; then is_arg=0; continue; fi
            args+=("$arg")
        else
            bashopts_extra_args+=("$arg")
        fi
    done

    # build the long and short getopt option list from the options
    short_opts=""
    long_opts=()
    for op in "${bashopts_optlist[@]}"; do
        if [[ -v bashopts_optprop_short_opt[$op] ]]; then
            short_opts="${short_opts}${bashopts_optprop_short_opt[$op]}:$(test "${bashopts_optprop_type[$op]}" != "boolean" || echo ":")"
        fi
        if [[ -v bashopts_optprop_long_opt[$op] ]]; then
            long_opts+=("${bashopts_optprop_long_opt[$op]}:$(test "${bashopts_optprop_type[$op]}" != "boolean" || echo ":")")
        fi
    done
    long_opts=$(bashopts_join_by , ${long_opts[@]})

    # call the getopt
    if ! args=$(getopt -o $short_opts -l "$long_opts" -n "$bashopts_tool_name" -- "${args[@]}"); then
        >&2 bashopts_display_help
        exit 1
    fi
    eval set -- "$args";

    # store the arguments value part
    while true; do
        arg=$1
        shift
        case $arg in
            --)
                # end of the argument part
                break
                ;;
            -*)
                val="$1"
                shift
                op=${bashopts_arg2op[$arg]}
                if [ -z "$val" ]; then
                    if [ "${bashopts_optprop_type[$op]}" == "boolean" ]; then
                        # boolean argument with no value is considered as true
                        val="true"
                    else
                        # empty value tell to unset the value or clear the array
                        unset $op
                        continue
                    fi
                fi
                val="$(${bashopts_optprop_check[$op]} "$val")" || exit 1
                case "${bashopts_optprop_method[$op]}" in
                    set)
                        # normal case: override the value
                        eval "$op=$(declare -p val | sed -E 's/^declare\s[^=]*=//g')"
                        ;;
                    add)
                        # array case: add the value
                        eval "$op+=($(declare -p val | sed -E 's/^declare\s[^=]*=//g'))"
                        ;;
                esac
                ;;
            *)
                bashopts_log C "Fatal error: args"
                ;;
        esac
    done

    # store the command part
    bashopts_commands=("$@")
}

# display an array: [val1, val2, ...]
bashopts_dump_array() {
    local type=$1
    shift || bashopts_log C "Usage: bashopts_dump_array type elt1 [elt2...]"
    echo -n "["
    if [ "$type" == "string" ]; then
        echo -n "\"${1//\"/\\\"}\""
    else
        echo -n "${1}"
    fi
    shift || true
    while [ -n "$1" ]; do
        if [ "$type" == "string" ]; then
            echo -n ", \"${1//\"/\\\"}\""
        else
            echo -n ", ${1}"
        fi
        shift
    done
    echo -n "]"
}

bashopts_read_json_array() {
    local line
    while read -r line; do
        eval "$1+=($line)"
    done <<< "$(jq '.[]' <<< "$2")" && return 0 || \
    bashopts_log E "Invalid JSON array"
    return 1
}

# Process a specified option
bashopts_process_option() {
    local dval tval ival op arg arglist check val_req edit_req
    if ! arglist=$(getopt -o "n:k:r" -n "bashopts_process_option " -- "$@"); then
        bashopts_log C "Usage bashopts_process_opt" \
            "        -n <val>  property name" \
            "        -k <val>  override value check function" \
            "        -r        At least one value required"
    fi
    eval set -- "$arglist";
    # parse all the parameters
    while true; do
        arg=$1
        shift
        case "$arg" in
            -n) op=$1; shift;;
            -k) check=$1; shift;;
            -r) val_req="true";;
            --) break;;
            *)  bashopts_log C "Fatal error";;
        esac
    done
    test -n "$op" || \
        bashopts_log C "bashopts_process_option: missing -n option"
    if [ -z "$check" ]; then
        check="${bashopts_optprop_check[$op]}"
    fi
    if [ "${bashopts_optprop_req_value[$op]}" == "true" ]; then
        val_req="true"
    fi

    # eval or get default value
    if [[ -v bashopts_optprop_expression[$op] ]]; then
        eval "dval=${bashopts_optprop_expression[$op]}"
    elif [ "${bashopts_optprop_method[$op]}" == "add" ]; then
        dval=()
    else
        dval="${bashopts_optprop_default[$op]}"
    fi
    # Init edit_req
    edit_req=${bashopts_optprop_interactive[$op]}
    if [[ -v $op ]]; then
        # Extract value from option name
        eval "tval=$(bashopts_get_def $op)"
        # Edition no more really required if already defined
        edit_req="false"
    elif [ "${bashopts_optprop_setting[$op]}" == "true" ] \
        && [ -f "$(readlink -m "$bashopts_tool_settings_path")" ] \
        && grep -E -q "^$op=" $bashopts_tool_settings_path; then
        eval "tval=$(grep -E "^${op}=" $bashopts_tool_settings_path | sed -E "s/^[^=]+=//g")"
    fi
    if [[ -v tval ]]; then
        # Check current value(s)
        for (( i=0; i<${#tval[@]}; i++)); do
            if ! $check "${tval[$i]}" > /dev/null; then
                if [ "$BASHOPTS_INTERACTIVE" != "true" ]; then
                    bashopts_log C "Non interactive mode: Exit due to one or more error"
                fi
                # (re)enable edition
                edit_req="true"
                break
            fi
        done
    elif [ "$val_req" == "true" ] && [ "$__BASHOPTS_DISPLAY_HELP__" != "true" ]; then
        bashopts_log E "At least one value required"
        if [ "$BASHOPTS_INTERACTIVE" != "true" ]; then
            exit 1
            #bashopts_log C "Non interactive mode: Exit due to one or more error"
        fi
        # (re)enable edition
        edit_req="true"
    fi
    if ! [[ -v tval ]] || [ "$edit_req" == "true" ]; then
        if [[ ! -v tval ]] && [ -n "$dval" ]; then
            # set default value
            eval "tval=$(bashopts_get_def dval)"
        fi
        if [ "$edit_req" == "true" ]; then
            if [ "$BASHOPTS_INTERACTIVE" == "true" ]; then
                # interactive edition
                while true; do
                    # Display the property description
                    echo "* ${bashopts_optprop_description[$op]}$(
                        # Add possible value list for enumeration type
                        if [ "${bashopts_optprop_type[$op]}" == "enumeration" ]; then
                            echo -n " (accepted values:$(
                                while read -r line; do
                                    echo -n " '${line##*|}'"
                                done <<< "${bashopts_optprop_enum_values[$op]}"
                            )"
                            echo -n ")"
                        fi
                    )"
                    # Add info for array properties
                    if [ "${bashopts_optprop_method[$op]}" == "add" ]; then
                        echo " -> List property format: 'single val.' or BASH array '(v1 v2 v3)' or JSON array '[v1, v2, v3]'"
                    fi
                    echo -n "    $(bashopts_dump_array {bashopts_optprop_type[$op]} "${tval[@]}"): "
                    read ival || bashopts_log C "Unexpected error, aborting..."
                    if [ -n "$ival" ]; then
                        if [ "${bashopts_optprop_method[$op]}" == "add" ]; then
                            # array value
                            tval=()
                            case "${ival:0:1}" in
                                '[')
                                    bashopts_read_json_array tval "$ival" || continue
                                    ;;
                                '(')
                                    if ! eval "tval=$ival" 2>/dev/null; then
                                        bashopts_log E "Invalid BASH array"
                                        continue
                                    fi
                                    ;;
                                *)
                                    tval+=("$ival")
                                    ;;
                            esac
                        else
                            # non array/normal value
                            tval=$ival
                        fi
                    elif [ "${bashopts_optprop_method[$op]}" == "add" ] && ! [[ -v tval ]]; then
                        tval=()
                    fi
                    # check format
                    if [ "${#tval[@]}" -eq 0 ] && [ "$val_req" == "true" ]; then
                        bashopts_log E "At least one value required"
                        unset tval
                        continue
                    fi
                    if [ "${bashopts_optprop_method[$op]}" == "add" ]; then
                        # array value
                        for (( i=0; i<${#tval[@]}; i++)); do
                            if ! tval[$i]="$($check "${tval[$i]}")"; then
                                unset tval
                                break
                            fi
                        done
                    else
                        # non array/normal value
                        if ! tval="$($check "$tval")"; then
                            unset tval
                        fi
                    fi
                    if declare -p tval > /dev/null 2>&1; then
                        # edit OK, break
                        break
                    fi
                    # otherwise, loop...
                done
            fi
        fi
    fi
    if [[ -v tval ]]; then
        eval "$op=$(bashopts_get_def tval)"
    fi
    if [ "${bashopts_optprop_setting[$op]}" == "true" ]; then
        if [ -n "$bashopts_tool_settings_path" ]; then
            # vrite the value to the setting file
            (
                test -d "$(dirname $bashopts_tool_settings_path)" || \
                    mkdir -p "$(dirname $bashopts_tool_settings_path)"
                if [ -f "$bashopts_tool_settings_path" ]; then
                    # remove old value
                    sed -i "/^$op=/d" $bashopts_tool_settings_path
                fi
                if [ "$bashopts_tool_settings_force_write" == "true" ] \
                    || [ "$(bashopts_get_def $op)" != "$(bashopts_get_def dval)" ]; then
                    # append the new value to the file if the value is not the default or 
                    # force_write is true
                    echo "$(bashopts_get_def_full $op)" >> $bashopts_tool_settings_path
                fi
            ) || bashopts_log W "Please check the settings file"
        else
            bashopts_log W "No settings file specified"
        fi
    fi
    if [ "$op" == "BASHOPTS_NON_INTERACTIVE" ] && ! [[ -v BASHOPTS_INTERACTIVE ]]; then
        if [ "$BASHOPTS_NON_INTERACTIVE" == "true" ]; then
            BASHOPTS_INTERACTIVE="false"
        else
            BASHOPTS_INTERACTIVE="true"
        fi
    fi
}

# STEP 4: process arg
bashopts_process_opts() {
    local op
    if [ "$__BASHOPTS_DISPLAY_HELP__" == "true" ]; then
        BASHOPTS_INTERACTIVE="false"
    fi
    for op in "${bashopts_optlist[@]}"; do
        bashopts_process_option -n $op
    done
    if [ "$__BASHOPTS_DISPLAY_HELP__" == "true" ]; then
        bashopts_display_help
        exit 0
    fi
}

# Export all option variables
bashopts_export_opts() {
    for op in "${bashopts_optlist[@]}"; do
        if [[ -v $op ]]; then
            export $op
        fi
    done
}
