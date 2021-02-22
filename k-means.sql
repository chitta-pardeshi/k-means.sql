-- install: https://www.sqlite.org/
-- command: sqlite3 < k-means.sql

.mode column
.header ON

-- step 01: create table rdata
--          generate random table
--          r = 1..100
--          c = 1..5
--          v = between 0 and 9999

        create table rdata (r int, c int, v real) ;

        insert into rdata(r, c, v)
        with recursive 
        row_nos(r) as ( select 1 union all select r+1 from row_nos limit 100),
        col_nos(c) as ( select 1 union all select c+1 from col_nos limit 5 )
        select r, c, abs(random() % 1000) v from row_nos, col_nos
        ;

-- step 02: inspect few rows generated data

        select  r,
                max(case c when 1 then v end) v1,
                max(case c when 2 then v end) v2,
                max(case c when 3 then v end) v3,
                max(case c when 4 then v end) v4,
                max(case c when 4 then v end) v5
        from    rdata
        group by r
        order by r
        limit 5
        ;

-- step 03: create view rstat

        create view rstat as 
        select  c, 
                min(v) as min_v,
                avg(v) as avg_v,
                max(v) as max_v
        from    rdata
        group by c
        ;

-- step 04: create table kdata with normalized values
        create table kdata (r int, c int, v real) ;
        create index kdata_i1 on kdata(c, r, v);

        insert into kdata(r, c, v)
        select rdata.r as r,
        rdata.c as c,
        (rdata.v - rstat.avg_v) / (rstat.max_v - rstat.min_v) as v
        from    rdata
        join    rstat 
        on      rstat.c = rdata.c
        ;


-- step 04: inspect few rows of normalized data

        select  r,
                max(case c when 1 then v end) v1,
                max(case c when 2 then v end) v2,
                max(case c when 3 then v end) v3,
                max(case c when 4 then v end) v4,
                max(case c when 4 then v end) v5
        from    kdata
        group by r
        order by r
        limit 5
        ;

-- step 05: create table krows by randomly allocating cluster number to each row

        create table krows (r int not null primary key, k int, l int, d real, i int default 0);

        insert into krows (r, k)
        select  r, 
                abs(random() % 10) k
        from    kdata 
        group by r ;

-- step 06: create view kmean to calculate new cluster means

        create view kmean as
                select
                        ki.k,
                        kd.c,
                        avg(kd.v) v
                from kdata kd
                join krows ki on ki.r = kd.r
                group by ki.k, 
                        kd.c
        ;

-- step 07: create view nextkrows to calculate new nearest cluster
        create view nextkrows as 
        with kpick as (
                select  km.k,
                        sum( (kd.v - km.v) * (kd.v - km.v) ) d
                from kdata kd
                join kmean km on km.c = kd.c
                where kd.r = krows.r
                group by km.k
                order by sum( (kd.v - km.v) * (kd.v - km.v) ) asc
                limit 1
        ) 
        select  r, 
                k as l,
                (select k from kpick) as k,
                (select d from kpick) as d
        from krows
        ;

-- step 08: create instead of trigger to iterate reallocation

        create view iterate_reallocation as select 0 as max_iterations ;

        create trigger iterate_reallocation_trigger
        instead of insert on iterate_reallocation
        begin

        update krows
        set (k, l, d, i) = (
                select k, l, d, krows.i + 1
                from nextkrows
                where r = krows.r
        )
        ;

        insert into iterate_reallocation (max_iterations)
        select new.max_iterations - 1 
        where new.max_iterations != 0
        and exists (
                select 1
                from krows
                where l != k
        );
        end;

        PRAGMA recursive_triggers = ON;


-- step 09: iterate k-means

        insert into iterate_reallocation (max_iterations) values (0) ;

        select  max(i) as iteration,
                count(distinct k) as number_of_clusters,
                sum (case l when k then 0 else 1 end) as reallocated,
                sum(d) as sum_distance_square,
                max(d) as max_distance
        from    krows ;

.header off


--        : recalculate couple of times

        insert into iterate_reallocation (max_iterations) values (2) ;

        select  max(i) as iteration,
                count(distinct k) as number_of_clusters,
                sum (case l when k then 0 else 1 end) as reallocated,
                sum(d) as sum_distance_square,
                max(d) as max_distance
        from    krows ;

--        : recalculate till no reallocation happens

        insert into iterate_reallocation (max_iterations) values (-1) ;

        select  max(i) as iteration,
                count(distinct k) as number_of_clusters,
                sum (case l when k then 0 else 1 end) as reallocated,
                sum(d) as sum_distance_square,
                max(d) as max_distance
        from    krows ;

--         : inspect normalized k-means

.header on
        select  k,
                max(case c when 1 then v end) v1,
                max(case c when 2 then v end) v2,
                max(case c when 3 then v end) v3,
                max(case c when 4 then v end) v4,
                max(case c when 4 then v end) v5
        from    kmean
        group by k
        order by k
        ;

--         : inspect rescaled k-means

        create view rmean as
        select  kmean.k as k,
                kmean.c as c,
                kmean.v * (rstat.max_v - rstat.min_v) + rstat.avg_v as v
        from    kmean
        join    rstat on rstat.c = kmean.c
        ;

.header on
        select  k,
                max(case c when 1 then v end) v1,
                max(case c when 2 then v end) v2,
                max(case c when 3 then v end) v3,
                max(case c when 4 then v end) v4,
                max(case c when 4 then v end) v5
        from    rmean
        group by k
        order by k
        ;
-- done
