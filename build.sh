#!/bin/sh
sudo apt-get update
sudo apt-get install nodejs -y
sudo ln -s /usr/bin/nodejs /usr/bin/node
sudo apt-get install npm -y
sudo apt-get install postgresql -y
sudo apt-get install pgadmin3 -y
sudo apt-get install libpq-dev -y
sudo -u postgres createuser hackingtravel
sudo -u postgres createdb hackingtravel
sudo -u postgres psql -c "alter user hackingtravel with password '82jl9t';"
npm install
sudo npm install -g coffee-script
