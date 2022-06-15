
drop table if exists "EDWH_PROD"."WS_SKYWARDS_PROD".ruch_ga_cpm_log_befr_search_c07;
create table "EDWH_PROD"."WS_SKYWARDS_PROD".ruch_ga_cpm_log_befr_search_c07 as

select 
all_logged_in_search.visitid,
all_logged_in_search.date_part,
all_logged_in_search.channelgrouping,
all_logged_in_search.FULLVISITORID,
case when all_logged_in_search.tier = 'W' then 'Without Profile'
else all_logged_in_search.tier end as log_in_tier,
all_logged_in_search.person_id,
all_logged_in_search.device,
min(case when lower(vm.value:eventInfo:eventAction::string) =  'search type' then vm.value:hitNumber::int end) as min_search_hit_no,
min(case when ve.value:index::string = '28' and lower(ve.value:value::string) in ('logged in', 'partially logged in') then vm.value:hitNumber::int end  ) as min_log_in_hit_no
from



					(
						select -- everybody who logged in and searched for a flight
						--a2.customdimensions,
						a2.hits,
						a2.date_part,
						a2.visitid,
						a2.channelgrouping,
						a2.FULLVISITORID,
						REGEXP_SUBSTR(cast(array_agg(distinct sess_cd.value:value::string) as string), '[A-Z]+' ) as tier,
						REGEXP_SUBSTR(cast(array_agg(distinct sess_cd.value:value::string) as string), '[0-9]+' ) as person_id,
						-- cast(array_agg(distinct sess_cd.value:value::string) as string) as person_id,
						parse_json(a2.DEVICE):deviceCategory::string as device
						from 
						"EDWH_PROD"."WS_MDP_PROD"."EXT_GA_RAW_DATA" as a2,
						lateral flatten( input => parse_json(a2.CUSTOMDIMENSIONS) ) as sess_cd
						where 
						to_date(a2.date_part) between '2021-05-08' and '2021-05-08' and
						(
						sess_cd.value:index::string = '53' or
						sess_cd.value:index::string = '33' 
						)  and
						lower(regexp_replace(cast(a2.hits as string),'"', '')) like ('%logged in%') and
						lower(regexp_replace(a2.hits,'"', '')) like ('%search type%') and -- searched for a a flight-- FULLVISITORID =  '7225682929779536801'  and
						1 = 1 
						group by
						a2.hits,
						a2.date_part,
						a2.visitid,
						a2.channelgrouping,
						a2.FULLVISITORID,
						parse_json(a2.DEVICE):deviceCategory::string
						-- order by random()
						-- limit 100
						
					) as all_logged_in_search,
						lateral flatten( input => parse_json(all_logged_in_search.hits) ) as vm,
						lateral flatten( input => vm.value:customDimensions ) as ve
				
group by
all_logged_in_search.visitid,
all_logged_in_search.date_part,
all_logged_in_search.channelgrouping,
all_logged_in_search.FULLVISITORID,
case when all_logged_in_search.tier = 'W' then 'Without Profile'
else all_logged_in_search.tier end,
all_logged_in_search.person_id,
all_logged_in_search.device
;