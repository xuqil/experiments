#/bin/sh

# 备份数据库
mysqldump -u root -p --databases test > test.sql