create sequence artserial;

create table artists(
id integer PRIMARY KEY DEFAULT nextval('artserial'),
name varchar(100) NOT NULL,
dynasty varchar(30),
art_field varchar(100), --诗、词、曲
description text,
create_date timestamp with time zone DEFAULT current_timestamp
);

ALTER TABLE artists ADD additional_name_info1 text;
ALTER TABLE artists ADD additional_name_info2 text;
ALTER TABLE artists ADD additional_info1 text;
ALTER TABLE artists ADD additional_info2 text;
ALTER TABLE artists ADD additional_info3 text;

insert into artists(name, dynasty, art_field, description)
values ('testname', '唐', '诗', '用于测试的人物');


create sequence article_serial;

create table articles(
id bigint PRIMARY KEY DEFAULT nextval('article_serial'),
artist_id integer REFERENCES artists(id),
title text not null,
content text not null,
article_type varchar(10), --诗、词、曲...
publish_date date,
create_date timestamp with time zone DEFAULT current_timestamp,
has_comment_flag boolean default FALSE,
rate smallint default 0, --0,1,2,3,4,5
additional_info1 text,
additional_info2 text,
additional_info3 text
);

ALTER TABLE articles ADD title text not null;

ALTER TABLE articles ADD additional_title_info1 text;
ALTER TABLE articles ADD additional_title_info2 text;
ALTER TABLE articles ADD additional_content_info1 text;
ALTER TABLE articles ADD additional_content_info2 text;

create sequence comment_serial;

create table comments(
id bigint primary key default nextval('comment_serial'),
article_id bigint references articles(id),
critic_id integer references artists(id),
publish_date date,
title text,
content text not null,
create_date timestamp with time zone DEFAULT current_timestamp,
article_rate smallint default 0, --0,1,2,3,4,5
comment_rate smallint default 0, --0,1,2,3,4,5
additional_info1 text,
additional_info2 text,
additional_info3 text
);



select title, substring(title,1,position('（' in title)-1),
  substring(title,position('（' in title)+1, position('）' in title)-position('（' in title) -1)
from articles where title like '%（%）';

update articles
set title = substring(title,1,position('（' in title)-1),
  additional_title_info1 = substring(title,position('（' in title)+1, position('）' in title)-position('（' in title) -1)
where title like '%（%）';

