#!/bin/bash

# git repo directory
REPODIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$REPODIR"

while true;
do
	# until forever: update, cleanup and start slave
	git pull
	docker system prune -f
	./valkyrie-slave.py
	sleep 1 # avoid busy-loop
done
