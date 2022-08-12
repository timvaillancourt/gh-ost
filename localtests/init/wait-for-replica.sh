#!/bin/bash

MYSQL_HOST=${1:-replica}

tries=0
while [ $tries -lt 60 ]; do
  tries=$(($tries + 1))
  res=$(mysql -h${MYSQL_HOST} -uroot -p${MYSQL_ROOT_PASSWORD} -e 'show slave status\G' -Bss)

  sql_running=0
  io_running=0
  sbm_ok=0
  master_host=$(echo "$res" | awk '/Master_Host:/{print $2}')
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
    echo "# Replica is ready, replicating from ${master_host}"
    break
  else
    sleep 1
  fi
done
