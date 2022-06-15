
drop table if exists "EDWH_PROD"."WS_SKYWARDS_PROD".RUCH_GA_CPM_CONVERSION_C12_1_2;
create table "EDWH_PROD"."WS_SKYWARDS_PROD".RUCH_GA_CPM_CONVERSION_C12_1_2 as

select
log_in_guys.visitid, -- session,
log_in_guys.date_part,
-- log_in_guys.clientid, -- unique user for a session
log_in_guys.channelgrouping,
log_in_guys.FULLVISITORID, -- unique user
log_in_guys.person_id , --skywards id
log_in_guys.device,


count(distinct log_in_guys.FULLVISITORID) as log_in_search_count,
count(distinct log_in_seen.FULLVISITORID) as promo_seen_count,
count(distinct look_book_no_inpth.FULLVISITORID) as no_inpath_bookers,
count(distinct look_book_and_inpth.FULLVISITORID) as inpath_bookers,
sum (look_book_no_inpth.usr_count_reg_cpm) as usr_count_reg_cpm,
sum (look_book_no_inpth.usr_count_comm_flt) as usr_count_comm_flt,
sum (look_book_no_inpth.usr_count_reward) as usr_count_reward,

sum (look_book_no_inpth.reg_cpm_rev ) as reg_cpm_rev ,
sum (look_book_no_inpth.comm_tktrev) as comm_tkt_rev,
(sum (look_book_and_inpth.in_path_rev)/1000000) as in_path_rev


	from
		(
			select -- everybody who logged in and searched for a flight
			
			a56.visitid,
			a56.date_part,
			-- a56.clientid,
			a56.channelgrouping,
			a56.FULLVISITORID,
			sess_cd.value:value::string as person_id,
			parse_json(a56.DEVICE):deviceCategory::string as device
			from 
			"EDWH_PROD"."WS_MDP_PROD"."EXT_GA_RAW_DATA" as a56,
			lateral flatten( input => parse_json(a56.CUSTOMDIMENSIONS) ) as sess_cd
			where 
			to_date(a56.date_part) between '2021-07-11' and '2021-07-30' and
			lower(regexp_replace(a56.hits,'[":]', '')) like ('%logged in%')
			and
			lower(regexp_replace(a56.hits,'[":]', '')) like ('%search type%') and -- searched for a a flight
			sess_cd.value:index::string = '53' and
			1 = 1
			-- limit 100
		
		) as log_in_guys
		
			left join
				(
				
					select -- everybody who logged in ,searched and saw an offer
					a1.visitid,
					-- a1.clientid,
					-- a1.channelgrouping,
					a1.FULLVISITORID
					from 
					"EDWH_PROD"."WS_MDP_PROD"."EXT_GA_RAW_DATA" as a1

					where 
					to_date(date_part) between '2021-07-11' and '2021-07-30' and
					lower(regexp_replace(a1.hits,'[":]', '')) like ('%logged in%') and
					lower(regexp_replace(a1.hits,'[":]', '')) like ('%search type%') and
					lower(regexp_replace(a1.hits,'[":]', '')) like ('%promotion shown%') and
					lower(regexp_replace(a1.hits,'[":]', '')) like ('%eventlabelemirates~cash plus miles|%') and

					-- saw the offer
					-- lower(regexp_replace(a1.hits,'"', '')) like ('%eventlabel:emirates~cash plus miles|%') and -- saw the offer
					1 = 1 
					-- limit 100
				
				) as log_in_seen
				
				on	log_in_guys.visitid = log_in_seen.visitid and log_in_guys.FULLVISITORID = log_in_seen.FULLVISITORID




				left join

						(
						
                            select -- everybody who logged in ,searched and saw an offer and booked but not an inpath booking
                            ss1.FULLVISITORID,
                            -- ss1.hits,
							ss1.visitid ,                            
							-- ss1.totals,
							-- ss1.CUSTOMDIMENSIONS,
							sum (distinct case when lower(ve.value:value::string) like '%cash plus miles%' then ss1.revenue end )/1000000 as reg_cpm_rev,
							count( distinct case when lower(ve.value:value::string) like '%cash plus miles%' then ss1.FULLVISITORID end ) as usr_count_reg_cpm,
							count (distinct case when lower(ve.value:value::string) like any ('%redemption%', '%reward%') then ss1.FULLVISITORID end ) as usr_count_reward,
                            
                            sum (distinct case when lower(ve.value:value::string) like ('%revenue%') then ss1.revenue end )/1000000 as comm_tktrev,
							
							count (distinct case when lower(ve.value:value::string) like ('%revenue%') then ss1.FULLVISITORID end ) as usr_count_comm_flt

								from
									(
									
									select 
										a2.TOTALS,
                                      -- a2.date_part,
										a2.CUSTOMDIMENSIONS,
										a2.visitid,
										-- a2.clientid,
										-- channelgrouping,
										a2.FULLVISITORID,
										a2.hits,
										parse_json(a2.TOTALS):transactionRevenue::int as revenue
										-- parse_json(DEVICE):deviceCategory::string as device
										from 
										"EDWH_PROD"."WS_MDP_PROD"."EXT_GA_RAW_DATA" as a2
										
										where 
										
										to_date(a2.date_part) between '2021-07-11' and '2021-07-30' and
										-- a2.CLIENTID in ('1892303178.1630482088', '1912199833.1623931931') and
										-- a2.FULLVISITORID in ('8127380265257348776' , '8212835747775593499')  and
										-- a2.VISITID in ('1630508696', '1630442166') and
										lower(regexp_replace(a2.hits,'[":]', '')) like ('%promotion shown%') and
										lower(regexp_replace(a2.hits,'[":]', '')) like ('%eventlabelemirates~cash plus miles|%') and
										lower(regexp_replace(a2.hits,'[":]', '')) like ('%typetransaction%') and
										lower(regexp_replace(a2.hits,'[":,]', '')) not like ('%index75valueemirates~cash plus miles|%') and
										--a2.device like ('%desktop%') and 
										1 = 1 
										-- limit 50
									
									 ) as ss1,
									lateral flatten( input => parse_json(ss1.hits) ) as vm,
									lateral flatten( input => vm.value:customDimensions ) as ve

							where
							ve.value:index::string = '14' -- to differentiate the type of booking

							group by
                            ss1.FULLVISITORID,
                            ss1.hits,
							ss1.visitid                           
							-- ss1.totals,
							-- ss1.CUSTOMDIMENSIONS
                            
								
						) as look_book_no_inpth 
						
						on log_in_guys.visitid = look_book_no_inpth.visitid and log_in_guys.FULLVISITORID = look_book_no_inpth.FULLVISITORID
				
				
								left join
									(
										select  -- everybody who logged in ,searched and saw an offer and bought an in path cpm ticket
										visitid,
										-- clientid,
										FULLVISITORID,
										-- hits,
										parse_json(TOTALS):transactionRevenue::int as in_path_rev
										-- parse_json(DEVICE):deviceCategory::string as device
										from 
										"EDWH_PROD"."WS_MDP_PROD"."EXT_GA_RAW_DATA" as a3
										
										where 
										to_date(a3.date_part) between '2021-07-11' and '2021-07-30' and
										lower(regexp_replace(a3.hits,'[":]', '')) like ('%promotion shown%') and
										lower(regexp_replace(a3.hits,'[":]', '')) like ('%eventlabelemirates~cash plus miles|%') and
										lower(regexp_replace(a3.hits,'[":]', '')) like ('%typetransaction%') and
										lower(regexp_replace(a3.hits,'[":,]', '')) like ('%index75valueemirates~cash plus miles|%') and
										1 = 1 
										-- limit 100
									
									) as look_book_and_inpth
									
									on log_in_guys.visitid = look_book_and_inpth.visitid and log_in_guys.FULLVISITORID = look_book_and_inpth.FULLVISITORID
group by
log_in_guys.visitid,
log_in_guys.date_part,
-- log_in_guys.clientid,
log_in_guys.channelgrouping,
log_in_guys.FULLVISITORID,
log_in_guys.person_id ,
log_in_guys.device;