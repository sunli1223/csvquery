#!/bin/bash
f=$(sed -nr "/^\[server\]/ { :l /^f[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" "/etc/csvquery/csvquery.ini")
password=$(sed -nr "/^\[server\]/ { :l /^password[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" "/etc/csvquery/csvquery.ini")
files=$(echo $f|awk -F ',' '{for(i=1;i<=NF;i++){print "-f "$i " ";}}')

/usr/bin/csvquery server --dbname csv $files -u root -p $password
