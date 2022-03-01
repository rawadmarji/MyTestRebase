with d as (
 select  timeentry_id ,hashvalue , count(*) from stage.expenditure_files where hashvalue is not null 
 group by timeentry_id ,hashvalue  order by timeentry_id desc)
 
 select * from d left join ods_data.expenditure e on (e.timeentry_id=d.timeentry_id and e.hashvalue=d.hashvalue)
 where e.hashvalue  is not  null order by e.timeentry_id  



select count(*) from (
select timeentry_id ,* from stage.expenditure_files where batchid ='20220211'  and isvalid =1 
and claimid <> '99999' and delta =1
order by batchid  desc) as d
select  count(*),
consnum,planid,outcomenumber,servicenumber,dateofservice,procedurecode,
serviceunit,unitcost,providername,totalserviceunitcost,unittype,servicedeliverydesc,panumber,authstart,
authend,fein,providerphysicaladdress,providerphysicalcity,providerphysicalstate,providerphysicalzip,
providermailingaddress,providermailingcity,providermailingstate,providermailingzip,providercontact,
provideremail,datepaid,paidmethod,paiddescription,claimid,originalclaimid,wages,isvalid,
hashvalue,recordtype,fis_consumer_full_path,delta,source_system,timeentry_id
from stage.expenditure_files ef  where timeentry_id ='5506338854808' and delta=1 and isvalid =1 
group by 
consnum,planid,outcomenumber,servicenumber,dateofservice,procedurecode,
serviceunit,unitcost,providername,totalserviceunitcost,unittype,servicedeliverydesc,panumber,authstart,
authend,fein,providerphysicaladdress,providerphysicalcity,providerphysicalstate,providerphysicalzip,
providermailingaddress,providermailingcity,providermailingstate,providermailingzip,providercontact,
provideremail,datepaid,paidmethod,paiddescription,claimid,originalclaimid,wages,isvalid,
hashvalue,recordtype,fis_consumer_full_path,delta,source_system,timeentry_id

select ef.hashvalue, e.hashvalue ,
                case when e.hashvalue is null then 1 else 0 end as delta,* 
           from stage.expenditure_files ef
           left join ods_data.expenditure e 
              on ef.hashvalue = e.hashvalue where ef.batchid ='20220211'
              
 select hashvalue ,* from ods_data.expenditure e  where timeentry_id ='5506338854808' order by batchid  desc
 select timeentry_id , hashvalue ,* from ods_data.expenditure e2  
 where hashvalue  in ('xf75625807881a5e486736d0c265be68b','xb19a35330af6d5345f69d9fb851d43d1','x4ec64fc85d3e39727d36341017676ff7')
 select * from stage.expenditure_files s
    left join ods_data.expenditure o 
    	on  s.timeentry_id = o.timeentry_id
	where	s.timeentry_id ='5506338854808' and
			s.isvalid = 1 and
			o.timeentry_id is null
			
select s.batchid, s.filename, s.processingdate, n.consnum, n.planid, n.outcomenumber, 
n.servicenumber, n.dateofservice, n.procedurecode, n.serviceunit, n.unitcost, n.providername, 
n.totalserviceunitcost, n.unittype, n.servicedeliverydesc, n.panumber, n.authstart, n.authend, n.fein, 
n.providerphysicaladdress, n.providerphysicalcity, n.providerphysicalstate, n.providerphysicalzip, n.providermailingaddress, 
n.providermailingcity, n.providermailingstate, n.providermailingzip, n.providercontact, n.provideremail, n.datepaid, n.paidmethod, 
n.paiddescription, n.claimid, n.originalclaimid, n.filecreationdate, n.wages, n.timeentry_id, n.hashvalue
		from 
			(
				SELECT 
			     s.consnum
				,cast(nullif(s.planid ,'') as decimal(8, 2)) as planid
				,cast(nullif(s.outcomenumber ,'') as int ) as outcomenumber
				,cast(nullif(s.servicenumber ,'') as int) as servicenumber
				,s.dateofservice
				,s.procedurecode
				,cast(cast(nullif(s.serviceunit ,'') as decimal(8,2)) as int) as serviceunit
				,cast(nullif(s.unitcost ,'') as decimal(8,2)) as unitcost
				,s.providername
				,cast(nullif(s.totalserviceunitcost ,'') as decimal(8,2)) as totalserviceunitcost
				,cast(cast(nullif(s.unittype,'') as decimal(8,2)) as int) as unittype
				,s.servicedeliverydesc
				,s.panumber
				,s.authstart
				,s.authend
				,s.fein
				,s.providerphysicaladdress
				,s.providerphysicalcity
				,s.providerphysicalstate
				,s.providerphysicalzip
				,s.providermailingaddress
				,s.providermailingcity
				,s.providermailingstate
				,s.providermailingzip
				,s.providercontact
				,s.provideremail
				,s.datepaid
				,s.paidmethod
				,s.paiddescription
				,s.claimid
				,s.originalclaimid
				,s.filecreationdate
				,cast(nullif(s.wages ,'') as decimal(8,2)) as wages
				,s.timeentry_id
				,s.hashvalue
			  FROM stage.expenditure_files as s
			  where isvalid = 1 
			    and batchid = '20220211'
			    and timeentry_id ='5507588196738'
			 except  
			SELECT consnum, planid, outcomenumber, servicenumber, dateofservice, procedurecode, serviceunit, unitcost, providername
			, totalserviceunitcost, unittype, servicedeliverydesc, panumber, authstart, authend, fein, providerphysicaladdress, providerphysicalcity
			, providerphysicalstate, providerphysicalzip, providermailingaddress, providermailingcity, providermailingstate, providermailingzip
			, providercontact, provideremail, datepaid, paidmethod, paiddescription, claimid, originalclaimid, filecreationdate, wages, timeentry_id
			, hashvalue
			FROM ods_data.expenditure
			 ) n
			 join stage.expenditure_files s
			   on s.hashvalue = n.hashvalue
			  and s.isvalid = 1
where s.timeentry_id ='5506338854808'
select hashvalue ,* from ods_data.expenditure e  where timeentry_id ='5507588196738'
--xf1c6d563b727e33380527895f365c020
select hashvalue ,delta ,isvalid,* from stage.expenditure_files ef   where timeentry_id ='5507588196738'
and isvalid =1 and delta=1  
order by processingdate  desc
select ef.hashvalue, 
                case when e.hashvalue is null then 1 else 0 end as delta ,ef.*
           from stage.expenditure_files ef
           left join ods_data.expenditure e 
              on ef.hashvalue = e.hashvalue where ef.timeentry_id ='5507588196738'
              
              update  stage.expenditure_files exfiles
   set  delta = subquery.delta
  from  (select ef.hashvalue, ef.timeentry_id ,
                case when e.hashvalue is null then 1 else 0 end as delta 
           from stage.expenditure_files ef
           left join ods_data.expenditure e 
              on ef.hashvalue = e.hashvalue 
         ) subquery
 where exfiles.hashvalue = subquery.hashvalue
     and exfiles.batchid ='20220128' and exfiles.timeentry_id ='5507588196738'
     select hashvalue ,delta,isvalid  ,timeentry_id,*  from stage.expenditure_files where   timeentry_id ='5507588196738' order by processingdate desc limit 50
     union
     select hashvalue  ,  timeentry_id,*  from ods_data.expenditure  where timeentry_id ='5507588196738' 
     select timesheet_end, * from stage.expenditure_bp where timeentry_id ='5507588196738'  order by cast(batchid as int)  desc
     
     select pay_date,pay_period_end ,* from stage.payroll_bp where employee_id=811677 and batchid ='20220211'
     
     select employee_id ,pay_period_end,batchid ,count(*) from stage.payroll_bp group by employee_id ,pay_period_end,batchid
     having count(*)>1 order by cast(batchid as int) desc