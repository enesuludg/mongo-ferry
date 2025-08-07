sudo apt-get remove mongo-tools
wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2004-x86_64-100.9.4.tgz
tar -xvzf mongodb-database-tools-ubuntu2004-x86_64-100.9.4.tgz
sudo mv mongodb-database-tools-*/bin/* /usr/local/bin/
/usr/local/bin/mongorestore --version
echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
mongorestore --version
