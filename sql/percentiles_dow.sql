/* Define Percentile Functions */
begin;
CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(
    SELECT $1[s.i] AS "foo"
    FROM
        generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)
    ORDER BY foo
);
$$;

CREATE OR REPLACE FUNCTION percentile_cont(myarray real[], percentile real)
RETURNS real AS
$$

DECLARE
  ary_cnt INTEGER;
  row_num real;
  crn real;
  frn real;
  calc_result real;
  new_array real[];
BEGIN
  ary_cnt = array_length(myarray,1);
  row_num = 1 + ( percentile * ( ary_cnt - 1 ));
  new_array = array_sort(myarray);

  crn = ceiling(row_num);
  frn = floor(row_num);

  if crn = frn and frn = row_num then
    calc_result = new_array[row_num];
  else
    calc_result = (crn - row_num) * new_array[frn] 
            + (row_num - frn) * new_array[crn];
  end if;

  RETURN calc_result;
END;
$$
  LANGUAGE 'plpgsql' IMMUTABLE;
end;

/* Select Percentiles */
drop table if exists percentiles_dow_temp;

create table percentiles_dow_temp ( pairId integer, dow integer, lastUpdated time, p10 real, p30 real, p50 real, p70 real, p90 real, recordCount bigint);

insert into percentiles_dow_temp (pairId, dow, lastUpdated, p10, p30, p50, p70, p90, recordCount)
select
  pairId,
  extract(dow from lastUpdated) as dow,
  lastUpdated::timestamp::time,
  percentile_cont(cast(array_agg(travelTime) as real[]), cast(0.10 as real)) as p10,
  percentile_cont(cast(array_agg(travelTime) as real[]), cast(0.30 as real)) as p30,
  percentile_cont(cast(array_agg(travelTime) as real[]), cast(0.50 as real)) as p50,
  percentile_cont(cast(array_agg(travelTime) as real[]), cast(0.70 as real)) as p70,
  percentile_cont(cast(array_agg(travelTime) as real[]), cast(0.90 as real)) as p90,
  count(travelTime) as recordCount
from history
group by pairId, extract(dow from lastUpdated), lastUpdated::timestamp::time;

drop table if exists percentiles_dow;
alter table percentiles_dow_temp rename to percentiles_dow;
