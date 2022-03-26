 select distinct timeentry_id , hashvalue ,claimid ,originalclaimid  from stage.expenditure_files ef 
where batchid ='20220318' and claimid =originalclaimid  order by timeentry_id ,hashvalue  desc

select distinct timeentry_id , hashvalue ,claimid ,originalclaimid from stage.expenditure_files ef  where timeentry_id ='5510192818554'
and  batchid ='20220318'

  select distinct   e.timesheet_end , e.date,p.pay_period_end , to_char(to_date(case when v.numberc is null then p.pay_date else v.pay_date end, 'mm/dd/yy'), 'mmddyyyy')
         as date_paid,
        p.payment_type
        as paid_method, e.timeentry_id ,v.numberc  as originalclaimid  ,p.numberc  as claimid ,e.employee_id  from stage.expenditure_bp e
 left join stage.voided_bp v 
 on (e.employee_id = v.employee_id
 and e.date between v.pay_period_start and v.pay_period_end and (v.type = 'Retro' or v.type = 'retro')) -- records that fall under retro pay
 left join stage.payroll_bp p on
  (  e.employee_id = p.employee_id
   
  and to_date(e.timesheet_end, 'mm/dd/yyyy') =
      to_date(p.pay_period_end, 'mm/dd/yyyy') )  where timeentry_id ='5509672111711'
      
      select * from public.expenditure_new_d where timeentry_id ='5506514443693' -- claimid <>originalclaimid order by timeentry_id 
      select timeentry_id ,count(*) from public.expenditure_new_d group by timeentry_id having count(*)>1 

 --select timeentry_id ,datepaid ,claimid  from stage.expenditure_files ef2  where timeentry_id like '%-%'
/*select count(*)  from (*/SELECT query_to_xml('
with f as( select timesheet_end,"date",pay_period_end,max(date_paid) as date_paid ,
  max(paid_method) as paid_method,timeentry_id,replace(originalclaimid,''-'','''') as originalclaimid  ,
  replace(max(claimid),''-'','''') as claimid ,employee_id
    from public.expenditure_new_d   group by 
    timesheet_end,"date",pay_period_end,timeentry_id,originalclaimid,employee_id),
 pu as(select * from f where coalesce (claimid,'') <>coalesce (originalclaimid ,'') 
 and timeentry_id not in (''3310683317618'',''3310685734334'',''3310683317619'',''3310685734333''))
select distinct 		max(consnum) Cons_Num,	max(planid)  Plan_ID,	max(outcomenumber) Outcome_Number,	max(servicenumber) Service_Number,	
max(to_char(to_date(dateofservice, ''mm/dd/yyyy''), ''YYYY-MM-DD'')) as Date_Of_Service,
	procedurecode Procedure_Code,max(serviceunit) Service_unit,max(cast(unitcost as numeric(36,2))) as Unit_Cost,max(providername) Provider_Name,	
	max(cast(totalserviceunitcost as numeric(36,2))) as Total_Service_Unit_Cost,
	max(unittype) Unit_Type,	max(servicedeliverydesc) Service_Delivery_Desc,	max(panumber) PA_Number,	max(to_char(to_date(authstart, ''mm/dd/yyyy''), ''YYYY-MM-DD'')) as AuthStart,	
	max(to_char(to_date(authend, ''mm/dd/yyyy''), ''YYYY-MM-DD'')) as AuthEnd,	fein FEIN,	providerphysicaladdress Provider_Physical_Address,	
	providerphysicalcity Provider_Physical_City ,providerphysicalstate Provider_Physical_State ,	providerphysicalzip Provider_Physical_Zip,	providermailingaddress Provider_Mailing_Address,	providermailingcity Provider_Mailing_City,
	providermailingstate Provider_Mailing_State,providermailingzip Provider_Mailing_Zip ,providercontact Provider_Contact,provideremail Provider_Email,
	max(to_char(	
	case  when source_system = ''BP'' then to_date(datepaid, ''mmddyyyy'')		 
	when source_system = ''MIP'' then to_date(datepaid, ''mm/dd/yyyy'') end,''YYYY-MM-DD''))as Date_Paid,	
	max(paidmethod) as Paid_Method,	paiddescription Paid_Description,	max(pu.claimid) as Claim_ID ,	max(pu.originalclaimid) as Original_Claim_ID,	
	max(cast(wages as numeric(36,2))) as Wages,	recordtype,	pu.timeentry_id TimeEntry_ID
	from pu left join stage.expenditure_files ef on  ef.timeentry_id =pu.timeentry_id 

and ef.fein=pu.employee_id::varchar 
and ef.claimid =pu.claimid and pu.date_paid =ef.datepaid 
where isvalid=1 and outcomenumber is not null and servicenumber is not null
and pu.timeentry_id::bigint>=4407097979939 
group by procedurecode

,fein,	providerphysicaladdress,	
	providerphysicalcity,providerphysicalstate,	providerphysicalzip,	providermailingaddress,	providermailingcity,
	providermailingstate,providermailingzip,providercontact,provideremail,paiddescription,
	pu.claimid,pu.originalclaimid,recordtype,pu.timeentry_id,source_system
order by timeentry_id', true, false, '' ) as l group by timeentry_id having count(*)>1
--where  pu.employee_id like '%47-2305520%' or pu.employee_id  like '%-%'
--where pu.timeentry_id ='5509829046249'  and pu.claimid ='770319' and fein='814762'

with f as( select timesheet_end,"date",pay_period_end,max(date_paid) as date_paid ,
  max(paid_method) as paid_method,timeentry_id,replace(originalclaimid,'-','') as originalclaimid  ,
  replace(max(claimid),'-','') as claimid ,employee_id
    from public.expenditure_new_d   group by 
    timesheet_end,"date",pay_period_end,timeentry_id,originalclaimid,employee_id),
 pu as(select * from f where coalesce (claimid,'') <>coalesce (originalclaimid ,'') 
 and timeentry_id not in ('3310683317618','3310685734334','3310683317619','3310685734333'))
select distinct 		max(consnum) Cons_Num,	max(planid)  Plan_ID,	max(outcomenumber) Outcome_Number,	max(servicenumber) Service_Number,	
max(to_char(to_date(dateofservice, 'mm/dd/yyyy'), 'YYYY-MM-DD')) as Date_Of_Service,
	procedurecode Procedure_Code,max(serviceunit) Service_unit,max(cast(unitcost as numeric(36,2))) as Unit_Cost,max(providername) Provider_Name,	
	max(cast(totalserviceunitcost as numeric(36,2))) as Total_Service_Unit_Cost,
	max(unittype) Unit_Type,	max(servicedeliverydesc) Service_Delivery_Desc,	max(panumber) PA_Number,	max(to_char(to_date(authstart, 'mm/dd/yyyy'), 'YYYY-MM-DD')) as AuthStart,	
	max(to_char(to_date(authend, 'mm/dd/yyyy'), 'YYYY-MM-DD')) as AuthEnd,	fein FEIN,	providerphysicaladdress Provider_Physical_Address,	
	providerphysicalcity Provider_Physical_City ,providerphysicalstate Provider_Physical_State ,	providerphysicalzip Provider_Physical_Zip,	providermailingaddress Provider_Mailing_Address,	providermailingcity Provider_Mailing_City,
	providermailingstate Provider_Mailing_State,providermailingzip Provider_Mailing_Zip ,providercontact Provider_Contact,provideremail Provider_Email,
	max(to_char(	
	case  when source_system = 'BP' then to_date(datepaid, 'mmddyyyy')		 
	when source_system = 'MIP' then to_date(datepaid, 'mm/dd/yyyy') end,'YYYY-MM-DD'))as Date_Paid,	
	max(paidmethod) as Paid_Method,	paiddescription Paid_Description,	max(pu.claimid) as Claim_ID ,	max(pu.originalclaimid) as Original_Claim_ID,	
	max(cast(wages as numeric(36,2))) as Wages,	recordtype,	pu.timeentry_id TimeEntry_ID into public.finalExpenditue
	from pu left join stage.expenditure_files ef on  ef.timeentry_id =pu.timeentry_id 

and ef.fein=pu.employee_id::varchar 
and ef.claimid =pu.claimid and pu.date_paid =ef.datepaid 
where isvalid=1 and outcomenumber is not null and servicenumber is not null
and pu.timeentry_id::bigint>=4407097979939 
group by procedurecode

,fein,	providerphysicaladdress,	
	providerphysicalcity,providerphysicalstate,	providerphysicalzip,	providermailingaddress,	providermailingcity,
	providermailingstate,providermailingzip,providercontact,provideremail,paiddescription,
	pu.claimid,pu.originalclaimid,recordtype,pu.timeentry_id,source_system
order by timeentry_id
SELECT query_to_xml('select * from public.finalExpenditue', true, false, '')
select * from public.finalExpenditue where original_claim_id =claim_id 



