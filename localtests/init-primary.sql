create user 'gh-ost'@'%' identified by 'gh-ost';
grant all on *.* to 'gh-ost'@'%';
create database if not exists `test`;
