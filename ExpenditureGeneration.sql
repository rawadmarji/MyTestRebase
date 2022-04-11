with y as (      select distinct on(timeentry_id) timeentry_id  as p ,ef2.* from stage.expenditure_bp ef2  
  order by timeentry_id  ,batchid  desc)


  select distinct  e.prior_auth__start_date ,e.prior_auth__end_date , e.timesheet_end , e.date,p.pay_period_end , to_char(to_date(case when v.numberc is null then p.pay_date else v.pay_date end, 'mm/dd/yy'), 'mmddyyyy')
         as date_paid,
        p.payment_type
        as paid_method, e.timeentry_id ,v.numberc  as originalclaimid  ,p.numberc  as claimid ,e.employee_id into public.new_expenditure_v3
        from y e
 left join stage.voided_bp v 
 on (e.employee_id = v.employee_id
 and e.date between v.pay_period_start and v.pay_period_end and (v.type = 'Retro' or v.type = 'retro')) -- records that fall under retro pay
 left join stage.payroll_bp p on
  (  e.employee_id = p.employee_id
   
  and to_date(e.timesheet_end, 'mm/dd/yyyy') =
      to_date(p.pay_period_end, 'mm/dd/yyyy') )    where e.istimeoff<>'Y' and coalesce(v.numberc,'')<>coalesce(p.numberc,'')
	  
	with f as( select  prior_auth__start_date ,prior_auth__end_date ,timesheet_end,"date",pay_period_end,max(date_paid) as date_paid ,
  max(paid_method) as paid_method,timeentry_id,replace(originalclaimid,'-','') as originalclaimid  ,
  replace(max(claimid),'-','') as claimid ,employee_id
    from public.new_expenditure_v3   group by 
    timesheet_end,"date",pay_period_end,timeentry_id,originalclaimid,employee_id,prior_auth__start_date ,prior_auth__end_date),
 pu as(select * from f where coalesce (claimid,'') <>coalesce (originalclaimid ,'') 
 and timeentry_id not in ('3310683317618','3310685734334','3310683317619','3310685734333'))/*,
 newd as (select distinct on(timeentry_id) timeentry_id  as p ,ef2.* from stage.expenditure_files ef2 
  order by timeentry_id  ,batchid  desc)*/
 --select * from pu where timeentry_id ='4407374921658'
select  		max(consnum) Cons_Num,	max(planid)  Plan_ID,	max(outcomenumber) Outcome_Number,	max(servicenumber) Service_Number,	
max(to_char(to_date(dateofservice, 'mm/dd/yyyy'), 'YYYY-MM-DD')) as Date_Of_Service,
	procedurecode Procedure_Code,max(serviceunit) Service_unit,max(cast(unitcost as numeric(36,2))) as Unit_Cost,max(providername) Provider_Name,	
	max(cast(totalserviceunitcost as numeric(36,2))) as Total_Service_Unit_Cost,
	max(unittype) Unit_Type,	max(servicedeliverydesc) Service_Delivery_Desc,	max(panumber) PA_Number,	
	max(to_char(to_date(prior_auth__start_date, 'mm/dd/yyyy'), 'YYYY-MM-DD')) as AuthStart,	
	max(to_char(to_date(prior_auth__end_date, 'mm/dd/yyyy'), 'YYYY-MM-DD')) as AuthEnd,	max(fein) FEIN,	providerphysicaladdress Provider_Physical_Address,	
	providerphysicalcity Provider_Physical_City ,providerphysicalstate Provider_Physical_State ,	providerphysicalzip Provider_Physical_Zip,	providermailingaddress Provider_Mailing_Address,	providermailingcity Provider_Mailing_City,
	providermailingstate Provider_Mailing_State,providermailingzip Provider_Mailing_Zip ,providercontact Provider_Contact,provideremail Provider_Email,
	max(to_char(	
	case  when source_system = 'BP' then to_date(datepaid, 'mmddyyyy')		 
	when source_system = 'MIP' then to_date(datepaid, 'mm/dd/yyyy') end,'YYYY-MM-DD'))as Date_Paid,	
	max(paidmethod) as Paid_Method,	max(paiddescription) Paid_Description,	max(pu.claimid) as Claim_ID ,	max(pu.originalclaimid) as Original_Claim_ID,	
	max(cast(wages as numeric(36,2))) as Wages,	recordtype,	pu.timeentry_id TimeEntry_ID into   public.finalexpenditue_v2
	from pu left join stage.expenditure_files ef on  ef.timeentry_id =pu.timeentry_id 

and ef.fein=pu.employee_id::varchar 
--and ef.claimid =pu.claimid and pu.date_paid =ef.datepaid 
where (isvalid=1 or ef.timeentry_id in ('4407701051759','4407701051761','4407701051762','4407701051760','4407700980244')) --and outcomenumber is not null and servicenumber is not null
and pu.timeentry_id::bigint>=4407097979939 --and pu.timeentry_id ='5509789295996'

group by procedurecode

,	providerphysicaladdress,	
	providerphysicalcity,providerphysicalstate,	providerphysicalzip,	providermailingaddress,	providermailingcity,
	providermailingstate,providermailingzip,providercontact,provideremail,
	pu.claimid,pu.originalclaimid,recordtype,pu.timeentry_id,source_system
order by timeentry_id

SELECT query_to_xml('select * from public.finalExpenditue_v2', true, false, '')
