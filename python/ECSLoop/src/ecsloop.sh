#!/bin/bash

count=0

while :
do
	count=$(expr $count + 1)
	echo "$(date +%Y-%m-%dT%H:%M:%S) ${count}回目のループ"
	sleep 5
done
