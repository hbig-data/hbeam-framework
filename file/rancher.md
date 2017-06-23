#  Docker Rancher 安装
========================================

## 1.安装

docker run rancher/server --help

docker run -d --restart=unless-stopped -p 18080:8080 rancher/server

## Rancher 使用MySQL 数据库

> CREATE DATABASE IF NOT EXISTS cattle COLLATE = 'utf8_general_ci' CHARACTER SET = 'utf8';
> GRANT ALL ON cattle.* TO 'cattle'@'%' IDENTIFIED BY 'cattle';
> GRANT ALL ON cattle.* TO 'cattle'@'localhost' IDENTIFIED BY 'cattle';


 docker run -d --restart=always -p 18080:8080 rancher/server --db-host 192.168.1.8 --db-port 2849 --db-user ndmp --db-pass 123456 --db-name cattle