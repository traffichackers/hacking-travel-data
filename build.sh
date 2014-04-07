#!/bin/sh
sudo apt-get update
sudo apt-get install nodejs -y
sudo ln -s /usr/bin/nodejs /usr/bin/node
sudo apt-get install npm -y
sudo apt-get install postgresql -y
sudo apt-get install pgadmin3 -y
sudo apt-get install libpq-dev -y
psql -c 'CREATE DATABASE hackingTravel'
sudo npm install -g coffee-script
npm install xml2js
npm install pg
npm install ftp
npm install async
npm install csv
npm install better-require
