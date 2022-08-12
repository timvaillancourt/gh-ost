#!/bin/bash

mysql_host=${1:-replica}
defaults_file="$(mktemp).cnf"

trap "rm -f $defaults_file 2>/dev/null" EXIT SIGINT SIGTERM

# write defaults file
cat <<EOF >$defaults_file
[client]
user=root
password=${MYSQL_ROOT_PASSWORD}
EOF
chmod 600 $defaults_file

tries=1
while [ $tries -lt 60 ]; do
  res=$(mysql --defaults-file=${defaults_file} -h${mysql_host} -e 'show slave status\G' -Bss)

  sql_running=0
  io_running=0
  sbm_ok=0
  if [ "$(echo "$res" | awk '/Slave_SQL_Running:/{print $2}')" = "Yes" ]; then
    sql_running=1
  fi
  if [ "$(echo "$res" | awk '/Slave_IO_Running:/{print $2}')" = "Yes" ]; then
    io_running=1
  fi
  if [ "$(echo "$res" | awk '/Seconds_Behind_Master:/{print $2}')" = "0" ]; then
    sbm_ok=1
  fi

  if [ $sql_running -eq 1 ] && [ $io_running -eq 1 ] && [ $sbm_ok -eq 1 ]; then
    master_host=$(echo "$res" | awk '/Master_Host:/{print $2}')
    master_port=$(echo "$res" | awk '/Master_Port:/{print $2}')
    echo "# Replica '${mysql_host}' is ready, replicating from '${master_host}:${master_port}'"
    exit 0
  fi

  tries=$(($tries + 1))
  sleep 2
done
