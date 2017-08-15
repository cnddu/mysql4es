# mysql4es
sync mysql data with es

DROP TABLE IF EXISTS `synces_user`;
CREATE TABLE `synces_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
   recordid int(11),
  `timestamp` int unsigned,
  `opcode` tinyint,
  `username` text,
  `nickname` text,
  `avatar` text,
  `description` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

opcode:
1 - insert;
2 - update;
3 - delete;

insert into user(username,nickname,avatar,description,user.key,user.level,reg_time,key_time,position,company,homepage,city,star_num,view_num,docs_num,stars_num,following_num,followers_num,booklist_num,followbooklist_num) values ("test1","nick1","httpabc.com","this is a ts is a test","random",0,0,0,0,"ltd","hp","bj",0,0,0,0,0,0,0,0);

update user set nickname=3 where username='test2';

delete from user where username='test2';
