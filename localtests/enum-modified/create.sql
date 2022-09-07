drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  e enum('red', 'green', 'blue', 'orange') not null,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test values (null, 7, 'red');

/*
drop event if exists gh_ost_test;
delimiter ;;
create event gh_ost_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into gh_ost_test values (null, 11, 'red');
  insert into gh_ost_test values (null, 13, 'green');
  insert into gh_ost_test values (null, 17, 'blue');
  set @last_insert_id := last_insert_id();
  update gh_ost_test set e='orange' where id = @last_insert_id;
end ;;
*/
