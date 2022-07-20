drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  e enum('Pass', 'Fail') not null collate 'utf8_bin',
  primary key(id)
) auto_increment=1;

insert into gh_ost_test values (null, 7, 'Pass');

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
  insert into gh_ost_test values (null, 11, 'Fail');
  insert into gh_ost_test values (null, 13, 'Pass');
  insert into gh_ost_test values (null, 17, 'Fail');
  set @last_insert_id := last_insert_id();
  update gh_ost_test set e='Fail' where id = @last_insert_id;
end ;;
