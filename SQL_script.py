nvg_data_gr_loans_np = """
create table cdm.nvg_data_gr_loans_np stored as parquet as
with t1 as (
select mt.applicationuid,
    'IKAR' as rep_type,
    '' as second_name, 
    '' as first_name, 
    '' as third_name,
    '' as birth_dt,
    mt.applicantid,
    mt.applicationuid as cre_natural_loan_request_id,
    mt.hjid as name_id,
    mt.hjid as report_id,
    l.hjid as loan_id,
    to_timestamp(substr(mt.reportdatetime, 1, 8), 'ddMMyyyy') as report_dt,
    case
		when cast(l.relationship as int) <= 4 then 'D'
		when cast(l.relationship as int) = 5 then 'G'
		else null
	end as class_type,
    l.isown as is_bank,
	to_timestamp(l.opendate, 'ddMMyyyy') as date_start,
	to_timestamp(l.finalpmtdate, 'ddMMyyyy') as date_end_plan,
	to_timestamp(l.factclosedate, 'ddMMyyyy') as date_end_fact,
	l.creditlimit as credit_amount,
	l.outstanding as current_debt_rur,
	l.delqbalance as delinquent_debt_rur,
    case
		when l.type_ = '1' then 'АК'
		when l.type_ = '6' then 'ИК'
		when l.type_ = '7' then 'КК'
		when l.type_ = '8' then 'КК'
		when l.type_ = '21' then 'МЗ'
		when l.type_ = '9' then 'ПК'
		else 'Прочее'
	  end as product,
	l.currency,
	l.interestrate/ 100/ 12 as interest_rate_month,
	case
		when cast(l.relationship as int) <= 4 then 1
		else 0
	end as is_debtor,
	case
		when cast(l.relationship as int) = 5 then 1
		else 0
	end as is_guarantor,
	l.pmtstring84m,
	to_timestamp(l.pmtstringstart, 'ddMMyyyy') as pmtstringstart,
	to_timestamp(l.Infconfirmdate, 'ddMMyyyy') as  infconfirmdate,
	cast(l.currentdelq as int) as currentdelq, 
    ov.inquiry1week as inquiry1week,
	ov.inquiry1month as inquiry1month,
	ov.inquiry3month as inquiry3month,
	ov.inquiry6month as inquiry6month,
    l.nextpmt as next_pmt,
	coalesce(l.businesscategory,1) as business_category,
	l.status,
	l.relationship,
	l.type_, 
	l.uuid,
    l.creditcostrate as credit_cost_rate,
	'01.01.06' as programcode, 
	l.principal_past_due,
    l.collateralcode as collateralcode,
	l.principal_outstanding,
	rank() over 
	    (partition by mt.applicantid
           order by 
           to_timestamp(mt.reportdatetime, 'ddMMyyyyHHmmss') desc,
           mt.hjid desc
           ) as rn
from stage.cre_sf_singleformattype sft 
inner join stage.cre_sf_maintype mt on mt.hjid = sft.main
inner join stage.cre_sf_loanstype l on l.loanstypes_loan_hjid = sft.loans
left join stage.cre_sf_loansoverviewtype ov on ov.hjid = sft.loansoverview
where 1=1
	and cast(l.relationship as int) <= 5
	and l.TYPE_ not in (
	'17' ,
	'18' ,
	'19' 
	)
    and length(coalesce(replace(regexp_replace(L.pmtstring84m,'X+$',''),'9','5'),'')) <= 2000
)
select distinct * from t1 where rn = 1
""";
                                        
nvg_cre_date = "create table cdm.nvg_cre_date stored as parquet as select to_timestamp('2023-03-01','yyyy-MM-dd') as application_date";

nvg_data_gr_month_pmt = """
create table cdm.nvg_data_gr_month_pmt stored as parquet
select 
cl.applicationuid,
    cl.rep_type,
    cl.second_name,
    cl.first_name,
    cl.third_name,
    cl.birth_dt,
    cl.applicantid,
    cl.name_id,
    cl.report_id,
	cl.report_dt,
    cl.loan_id,
    cl.class_type,
    cl.is_bank,
    cl.date_start,
    cl.date_end_plan,
    cl.date_end_fact,
    cl.credit_amount,
    cl.current_debt_rur,
    cl.delinquent_debt_rur,
    cl.product,
    cl.currency,
    cl.interest_rate_month,
    cl.is_debtor,
    cl.is_guarantor,
    cl.pmtstring84m,
	cl.pmtstringstart,
	cl.infconfirmdate,
    cl.currentdelq,
    cl.inquiry1week,
    cl.inquiry1month,
    cl.inquiry3month,
    cl.inquiry6month,
    cl.next_pmt,
    cl.business_category,
    cl.status,
    cl.relationship,
    cl.type_,
	cl.uuid,
	--cl.worstpmtpat,
	--cl.nextpmt,
	cl.collateralcode,
	cl.next_pmt as month_pmt,
	substring(cast(cast(now() as timestamp) as string), 1, 19) as t_changed_dttm 
	from cdm.nvg_data_gr_loans_np as cl
    """;
                                         
nvg_data_gr_cre_mart = """
create table cdm.nvg_data_gr_cre_mart stored as parquet as
select
    applicationuid,
	sct.application_date,
	rep_type,
	second_name,
	first_name,
	third_name,
	birth_dt,
	applicantid,
	cast(name_id as decimal(19,0)) as name_id,
	concat(rep_type, '_', cast(report_id as string)) as report_id,
	report_dt as report_dt,
	cast(loan_id as decimal(19,0)) as loan_id,
	class_type,
	cast(is_bank as integer) as is_bank,
	date_start,
	date_end_plan,
	date_end_fact,
	cast(credit_amount as decimal(19,2)) as credit_amount,
	cast(current_debt_rur as decimal(19,2)) as current_debt_rur,
	cast(delinquent_debt_rur as decimal(19,2)) as delinquent_debt_rur,
	product,
	currency,
	cast(interest_rate_month as decimal(38,6)) as interest_rate_month,
	is_debtor,
	is_guarantor,
	pmtstring84m,
	pmtstringstart,
	infconfirmdate,
	currentdelq,
	cast(inquiry1week as decimal(10,0)) as inquiry1week,
	cast(inquiry1month as decimal(10,0)) as inquiry1month,
	cast(inquiry3month as decimal(10,0)) as inquiry3month,
	cast(inquiry6month as decimal(10,0)) as inquiry6month,
	cast(next_pmt as decimal(19,2)) as next_pmt,
	cast(business_category as decimal(15,0)) as business_category,
	status,
	relationship,
	type_,
	cast(month_pmt as decimal(38,6)) as month_pmt,
	uuid,
	--worstpmtpat,
	--nextpmt,
	collateralcode,
	--COLLATERALCODE,
	now() as t_changed_dttm,
	cast(0 as tinyint ) as t_deleted_flg,
	cast(null as integer) as t_process_task_id,
	cast(1 as tinyint ) as t_active_flg
from cdm.nvg_data_gr_month_pmt
cross join cdm.nvg_cre_date as sct
""";

nvg_data_gr_cre_dlq_t2 = """
create table cdm.nvg_data_gr_cre_dlq_t2 stored as parquet as
select distinct
	application_date,
    report_id, 
    report_dt as report_date, 
    applicantid, 
    loan_id, 
    pmtstringstart, 
    --обстригаем правый "хвост", если он весь состоит из 'X'
    replace(regexp_replace(pmtstring84m,'X+$',''),'9','5') as pmtstring84m,
    reverse(replace(regexp_replace(pmtstring84m,'X+$',''),'9','5')) as pmtstring84m_rev,
    length(regexp_replace(pmtstring84m,'X+$','')) as len,
	0 as l_val,
	0 as worst_status,
	0 as  count_day
FROM cdm.nvg_data_gr_cre_mart
""";

nvg_data_gr_cre_dlq_t4 = """
create table cdm.nvg_data_gr_cre_dlq_t4 stored as parquet as
select 
	application_date,
	report_id, 
	report_date, 
	applicantid, 
	loan_id, 
	pmtstringstart, 
	pmtstring84m,
	pmtstring84m_rev as st,
	pmtstring84m_rev, 
	len, 
	l_val, 
	worst_status,
	count_day
from cdm.nvg_data_gr_cre_dlq_t2 
where len > 1
""";

new_nvg_data_gr_cre_dlq_t1 = """
insert into cdm.nvg_data_gr_cre_dlq_t1 
(
    application_date,
    report_id,
    report_date,
    applicantid,
    loan_id,
    dlq_end_dt,
    worst_status,
    count_day
)
select
	application_date,
	report_id, 
	report_date, 
	applicantid, 
	loan_id, 
	pmtstringstart as dlq_end_dt, 	
	case 
		when l_val = 1.5 then l_val
		else greatest(l_val, worst_status) 
	end as worst_status,
	count_day
from cdm.nvg_data_gr_cre_dlq_t3
where l_val between 1.5 and 5
""";

nvg_data_gr_cre_dlq_t5 = """
create table cdm.nvg_data_gr_cre_dlq_t5 stored as parquet as
select 
	application_date,
	report_id, 
	report_date, 
	applicantid, 
	loan_id, 
	pmtstringstart, 
	pmtstring84m, 
	pmtstring84m_rev, 
--записываем единственный статус как худший
	case 
		when substr(pmtstring84m_rev, 1, 1) = 'X' then 1 
		when substr(pmtstring84m_rev, 1, 1) = 'A' then 1.5 
		when substr(pmtstring84m_rev, 1, 1) in ('0', '1', '2', '3', '4', '5', '7', '8', '9') then cast(substr(pmtstring84m_rev, 1, 1) as int) 
		else 1 
	end as worst_status,
	case when substr(pmtstring84m_rev, 1, 1) = '5' then 1 else 0 end count_day
FROM cdm.nvg_data_gr_cre_dlq_t2
where len = 1
""";

new2_nvg_data_gr_cre_dlq_t1 = """
insert into cdm.nvg_data_gr_cre_dlq_t1 
(
    application_date,
    report_id, 
    report_date,
    applicantid,
    loan_id,
    dlq_end_dt,
    worst_status,
    count_day
)
select
	application_date,
	report_id, 
	report_date, 
	applicantid, 
	loan_id,
	pmtstringstart as dlq_end_dt,
	worst_status,
	count_day
from cdm.nvg_data_gr_cre_dlq_t5
where worst_status between 1.5 and 5
""";

nvg_data_gr_cre_dlq = """
create table cdm.nvg_data_gr_cre_dlq stored as parquet as
select 
	application_date,
    report_id,
    report_dt,
    applicantid,
    loan_id,
    dlq_end_dt,
    max(delinq_length) as delinq_length,
    is_active
from 
(
   select 
		application_date,
		report_id,
		report_date as report_dt,
		applicantid,
		loan_id,
		dlq_end_dt,
		case 
			when worst_status<>5 
			then case
					when worst_status = 1.5 then 29
					when worst_status = 2 then 59
					when worst_status = 3 then 89
					when worst_status = 4 then 119
				end 
			when worst_status=5
			then datediff(dlq_end_dt, ((dlq_end_dt - interval '121 day') - interval '1 month' * (count_day-1)) )
		end as delinq_length,
		case
		when report_date between 
				(dlq_end_dt - interval '1 day' * (case
				when worst_status = 1.5 then 29
				when worst_status = 2 then 59
				when worst_status = 3 then 89
				when worst_status = 4 then 119
				else datediff(dlq_end_dt,  ((dlq_end_dt - interval '121 day') - interval '1 month' * (count_day-1)) )
				end)) and ((dlq_end_dt + interval '1 month') - interval '1 day')
		  then 1
		  else 0
		end as is_active
    from cdm.nvg_data_gr_cre_dlq_t1 t  
    union all
	select
        ist.application_date,
        ist.report_id,
        ist.report_dt,
        ist.applicantid,
        ist.loan_id,
        coalesce(scr.dlq_end_dt, ist.Infconfirmdate) as dlq_end_dt,
        currentdelq as delinq_length,
        case when report_dt between 
                 (coalesce(scr.dlq_end_dt, ist.Infconfirmdate) - interval '1 day'*(ist.currentdelq)) and ((coalesce(scr.dlq_end_dt, ist.Infconfirmdate) + interval '1 month') - interval '1 day') then 1 else 0 end as is_active
    from cdm.nvg_data_gr_cre_mart as ist
		left join cdm.nvg_data_gr_cre_dlq_t1 as scr
			on ist.applicantid = scr.applicantid
			and ist.loan_id = scr.loan_id
			and cast(datediff(ist.Infconfirmdate, scr.dlq_end_dt) as int) between 1 and 30
    where currentdelq > 0
)x
group by application_date, report_id, report_dt, applicantid, loan_id, dlq_end_dt, is_active
""";

nvg_data_gr_agr_all = """
create table cdm.nvg_data_gr_agr_all stored as parquet as
SELECT  
    a.applicationuid,
    a.application_date,
    a.applicantid,
    a.report_dt,
    a.loan_id,
    a.class_type, 
    a.is_bank,
    a.date_start, --Дата открытия договора 
    a.date_end_plan, --Дата финального платежа (плановая)
    case
        when a.date_end_fact is null and months_between(application_date, a.report_dt) > 1 and application_date > a.date_end_plan then a.date_end_plan
        else a.date_end_fact
    end as date_end_fact,--Дата закрытия счета (фактическая)
    a.credit_amount,
    a.credit_amount AS card_limit, 
    case 
        when A.STATUS = '14' then 0 
        when a.product = 'КК' and months_between(application_date, a.report_dt) > 1 then 0
        when months_between(application_date, a.report_dt) > 1 then greatest(a.current_debt_rur - nvl(A.MONTH_PMT,0) * months_between(application_date, a.report_dt), 0)
        else a.current_debt_rur
    end as current_debt_rur,
    nvl(A.MONTH_PMT, 0) as next_pmt,  
    case 
        when A.STATUS = '14' then 0
        when a.product = 'КК' and months_between(application_date, a.report_dt) > 1 then 0
        else a.delinquent_debt_rur
    end as delinquent_debt_rur,
    a.product,  
    0 AS is_diff,
	0 as is_individual_pp,  -- Пикмулов. Изменения ЕПОКИ3.1-1. - Индивидуальный график платежей SPMPLN-4162
    a.currency, 
    a.interest_rate_month,
    a.is_debtor,
    a.is_guarantor,
	case 
		when a.collateralcode is null
		then 1 else 0 
	end coll_not_flg, /* АМ: если 1, то у кредита нет залога */
	case 
		when  a.collateralcode in ('11', '12')
		then 1 else 0 
	end coll_real_flg, /* АМ: если 1, то у кредита залог недвижимость */
	case 
		when a.collateralcode in ('1', '01') 
		then 1 else 0 
	end coll_auto_flg /*АМ: если 1, то у кредита залог авто */

FROM cdm.nvg_data_gr_cre_mart a
""";

nvg_data_gr_bki_t1 = """
create table cdm.nvg_data_gr_bki_t1 stored as parquet as
SELECT  
    a.applicationuid,
    a.application_date,
    a.applicantid,
    a.report_dt,
    a.loan_id,
    a.class_type,
    a.is_bank,
    a.date_start,
    a.date_end_plan,
    a.date_end_fact,
    a.credit_amount,
    a.card_limit,
    a.current_debt_rur,
    a.next_pmt,
    a.delinquent_debt_rur,
    a.product,
    a.is_diff,
	a.is_individual_pp,  -- Изменения ЕПОКИ3.1-1. - Индивидуальный график платежей SPMPLN-4162
    a.currency,
    a.interest_rate_month,
    a.is_debtor,
    a.is_guarantor,
    -- по курсу на текущую дату
    a.credit_amount AS credit_amount_rur,
    a.card_limit AS card_limit_rur,
    -- по курсу на дату договора
    a.credit_amount AS credit_amount_rur_0,
    a.card_limit AS card_limit_rur_0,
    -- по курсу на дату последних доступных остатков
    a.credit_amount AS credit_amount_rur_b,
    a.card_limit AS card_limit_rur_b,
    -- оставшийся срок в месяцах
    datediff(a.date_end_plan, a.application_date) / (365.25 / 12) AS remain_term,
    -- продукт 
    CASE WHEN a.product = 'ИК' THEN 1 ELSE 0 END AS is_ik, -- ипотека
    CASE WHEN a.product = 'ПК' THEN 1 ELSE 0 END AS is_pk, -- потреб
    CASE WHEN a.product = 'АК' THEN 1 ELSE 0 END AS is_ak, -- авто
    CASE WHEN a.product = 'КК' THEN 1 ELSE 0 END AS is_kk, -- карты
    CASE WHEN a.product = 'МЗ' THEN 1 ELSE 0 END AS is_mk, -- микрозаймы
    CASE WHEN a.product = 'Прочее' THEN 1 ELSE 0 END AS is_ok, -- другие
	a.coll_not_flg,
	a.coll_real_flg,
	a.coll_auto_flg,
    -- досрочное погашение
    CASE WHEN a.date_end_plan <> a.date_start AND datediff(a.date_end_fact, a.date_start)/datediff(a.date_end_plan, a.date_start) <= 0.9 THEN 1 ELSE 0 END AS is_advanced,
    -- досрочное погашение на 25%
    CASE WHEN a.date_end_plan <> a.date_start AND datediff(a.date_end_fact, a.date_start)/datediff(a.date_end_plan, a.date_start) <= 0.75 THEN 1 ELSE 0 END AS is_advanced_25,

    -- для расчёта длины кредитной истории: 
    -- сортируем все договоры клиента по дате договора,
    -- берём из предыдущих договоров максимальную дату фактического закрытия,
    -- открытые кредиты ограничиваются датой оценки (заявки);
    -- эта дата необходима для того, чтобы убрать периоды пересечения из последующих договоров
    MAX(NVL(a.date_end_fact, a.application_date)) OVER (PARTITION BY a.applicationuid, a.application_date, a.applicantid, a.class_type, a.is_bank ORDER BY a.date_start, date_end_fact asc nulls last, loan_id desc  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_date_end_fact__bank,
    MAX(NVL(a.date_end_fact, a.application_date)) OVER (PARTITION BY a.applicationuid, a.application_date, a.applicantid, a.class_type            ORDER BY a.date_start, date_end_fact asc nulls last, loan_id desc  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_date_end_fact
	 
FROM cdm.nvg_data_gr_agr_all a
""";

nvg_data_gr_bki_t2 = """
create table cdm.nvg_data_gr_bki_t2 stored as parquet as
SELECT 
    a.applicationuid,
    a.application_date,
    a.applicantid,
    a.report_dt,
    a.loan_id,
    a.class_type,
    a.is_bank,
    a.date_start,
    a.date_end_plan,
    a.date_end_fact,
    a.credit_amount,
    a.card_limit,
    a.current_debt_rur,
    a.next_pmt,
    a.delinquent_debt_rur,
    a.product,
    a.is_diff,
	a.is_individual_pp,  --. Изменения ЕПОКИ3.1-1. - Индивидуальный график платежей SPMPLN-4162
    a.currency,
    a.interest_rate_month,
    a.is_debtor,
    a.is_guarantor,
    a.credit_amount_rur,
    a.card_limit_rur,
    a.credit_amount_rur_0,
    a.card_limit_rur_0,
    a.credit_amount_rur_b,
    a.card_limit_rur_b,
    a.remain_term,
    a.is_ik,
    a.is_pk,
    a.is_ak,
    a.is_kk,
    a.is_mk,
    a.is_ok,
    a.is_advanced,
    a.is_advanced_25,
    a.prev_date_end_fact__bank,
    a.prev_date_end_fact,
	a.coll_not_flg,
	a.coll_real_flg,
	a.coll_auto_flg,
    CASE 
        WHEN 
            a.date_end_fact IS NOT NULL OR
            a.is_kk = 0 AND NVL(a.current_debt_rur, 0) + NVL(a.delinquent_debt_rur, 0) = 0 THEN 0
        ELSE 1
    END AS is_open,
    -- для расчёта длины кредитной истории:
    -- корректируем дату договора так, чтобы она начиналась не раньше фактической даты закрытия предыдущих договоров,
    -- чтобы исключить пересечения; если предыдущего договора нет, то оставляем дату как есть 
    GREATEST(a.date_start, NVL((a.prev_date_end_fact__bank + interval '1 day'), a.date_start)) AS for_length__date_start__bank,
    GREATEST(a.date_start, NVL((a.prev_date_end_fact + interval '1 day'), a.date_start)) AS for_length__date_start,
    -- для расчёта длины кредитной истории:
    -- если договор открыт, то нужно использовать дату оценки в качестве даты фактического закрытия договора 
    NVL(a.date_end_fact, a.application_date) AS for_length__date_end_fact
FROM cdm.nvg_data_gr_bki_t1 a
""";

nvg_data_gr_bki_t3 = """
create table cdm.nvg_data_gr_bki_t3 stored as parquet as
SELECT 
    a.applicationuid,
    a.application_date,
    a.applicantid,
    a.report_dt,
    a.loan_id,
    a.class_type,
    a.is_bank,
    a.date_start,
    a.date_end_plan,
    a.date_end_fact,
    a.credit_amount,
    a.card_limit,
    a.current_debt_rur,
    a.next_pmt,
    a.delinquent_debt_rur,
    a.product,
    a.is_diff,
	a.is_individual_pp,  -- . Изменения ЕПОКИ3.1-1. - Индивидуальный график платежей SPMPLN-4162
    a.currency,
    a.interest_rate_month,
    a.is_debtor,
    a.is_guarantor,
    a.credit_amount_rur,
    a.card_limit_rur,
    a.credit_amount_rur_0,
    a.card_limit_rur_0,
    a.credit_amount_rur_b,
    a.card_limit_rur_b,
    a.remain_term,
    a.is_ik,
    a.is_pk,
    a.is_ak,
    a.is_kk,
    a.is_mk,
    a.is_ok,
    a.is_advanced,
    a.is_advanced_25,
    a.prev_date_end_fact__bank,
    a.prev_date_end_fact,
    a.is_open,
    a.for_length__date_start__bank,
    a.for_length__date_start,
    a.for_length__date_end_fact,
	a.coll_not_flg,
	a.coll_real_flg,
	a.coll_auto_flg,

    -- обязательство рассчитывается только по заёмщикам/созаёмщикам и поручителям; в остальных случаях - 0
    CASE 
        -- закрытые
        WHEN a.is_open = 0 THEN 0
        -- карты
        WHEN a.is_kk = 1 THEN 0.05 * a.card_limit_rur
        -- дифференцированные
        WHEN a.remain_term <= 0 THEN 0 -- деление на 0
        WHEN a.is_diff = 1 THEN ((NVL(a.current_debt_rur, 0)  * (1 / a.remain_term + a.interest_rate_month))+ NVL(a.delinquent_debt_rur, 0))
        -- прочие (аннуитетные) 
        WHEN POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0
        WHEN 1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0
        ELSE ((NVL(a.current_debt_rur, 0)  * a.interest_rate_month / (1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) )) + NVL(a.delinquent_debt_rur, 0))
    END *
    -- если поручительство, то коэффициент
    CASE WHEN a.class_type = 'G' THEN 0.2 ELSE 1 END AS liability_rur, 
    CASE 
        -- закрытые
        WHEN a.is_open = 0 THEN 0
        -- БКИ (по алгоритму РКК)
        WHEN a.is_bank = 0 THEN a.next_pmt
        -- карты
        WHEN a.is_kk = 1 THEN 0.05 * a.card_limit_rur
        -- дифференцированные
        WHEN a.remain_term <= 0 THEN 0 -- деление на 0
        WHEN a.is_diff = 1 THEN ((NVL(a.current_debt_rur, 0)  * (1 / a.remain_term + a.interest_rate_month))+ NVL(a.delinquent_debt_rur, 0))
        -- прочие (аннуитетные) 
        WHEN POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0
        WHEN 1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) = 0 THEN 0 -- деление на 0

        ELSE ((NVL(a.current_debt_rur, 0)  * a.interest_rate_month / (1 - 1 / POWER(1 + a.interest_rate_month, a.remain_term) )) + NVL(a.delinquent_debt_rur, 0))
    END *
    -- если поручительство, то коэффициент
    CASE WHEN a.class_type = 'G' THEN 0.2 ELSE 1 END AS liability_rur_rkk,   
    -- Для определения не карт, среди открытых/закрытых, по дате открытия
    row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_kk, a.is_bank order by a.date_start desc, abs(loan_id) desc) as start_desc_num,
    row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_kk, a.is_bank order by a.date_start asc, abs(loan_id) asc) as start_asc_num,
    -- Для определения не карт, среди открытых/закрытых, по дате закрытия
    row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_kk, a.is_bank order by a.date_end_fact desc, abs(loan_id) desc) as end_desc_num,
    -- Для определения ПК среди всех (открытых, закрытых)
    row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_pk, a.is_bank order by a.date_start desc, abs(loan_id) desc) as start_pk_desc_num,
    row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_pk, a.is_bank order by a.date_start asc, abs(loan_id) asc) as start_pk_asc_num                 
FROM cdm.nvg_data_gr_bki_t2 a
""";

nvg_data_gr_bki_t4 = """
create table cdm.nvg_data_gr_bki_t4 stored as parquet as
SELECT 
    a.applicationuid,
    a.application_date,
    a.applicantid,
    a.report_dt,
    a.loan_id,
    a.class_type,
    a.is_bank,
    a.date_start,
    a.date_end_plan,
    a.date_end_fact,
    a.credit_amount,
    a.card_limit,
    a.current_debt_rur,
    a.next_pmt,
    a.delinquent_debt_rur,
    a.product,
    a.is_diff,
	a.is_individual_pp,  --. Изменения ЕПОКИ3.1-1. - Индивидуальный график платежей SPMPLN-4162
    a.currency,
    a.interest_rate_month,
    a.is_debtor,
    a.is_guarantor,
    a.credit_amount_rur,
    a.card_limit_rur,
    a.credit_amount_rur_0,
    a.card_limit_rur_0,
    a.credit_amount_rur_b,
    a.card_limit_rur_b,
    a.remain_term,
    a.is_ik,
    a.is_pk,
    a.is_ak,
    a.is_kk,
    a.is_mk,
    a.is_ok,
    a.is_advanced,
    a.is_advanced_25,
    a.prev_date_end_fact__bank,
    a.prev_date_end_fact,
    a.is_open,
    a.for_length__date_start__bank,
    a.for_length__date_start,
    a.for_length__date_end_fact,
    -- обязательство рассчитывается только по заёмщикам/созаёмщикам и поручителям; в остальных случаях - 0
    liability_rur,
    liability_rur_rkk,   
    -- Для определения не карт, среди открытых/закрытых, по дате открытия
    start_desc_num,
    start_asc_num,
    -- Для определения не карт, среди открытых/закрытых, по дате закрытия
    end_desc_num,
    -- Для определения ПК среди всех (открытых, закрытых)
    start_pk_desc_num,
    start_pk_asc_num,
    row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_pk, a.is_bank order by a.liability_rur_rkk desc nulls last, abs(loan_id) desc) as liab_pk_desc_num,
    row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_ik, a.is_bank order by a.liability_rur_rkk desc nulls last, abs(loan_id) desc) as liab_ik_desc_num,
	row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_kk, a.is_bank order by a.liability_rur_rkk desc nulls last, abs(loan_id) desc) as liab_kk_desc_num,  ---ТОП-АП
	row_number () over (partition by a.applicationuid, a.applicantid, a.application_date, a.is_debtor, a.is_open, a.is_ak, a.is_bank order by a.liability_rur_rkk desc nulls last, abs(loan_id) desc) as liab_ak_desc_num   
from cdm.nvg_data_gr_bki_t3 a
""";

nvg_data_gr_bki_t5 = """
create table cdm.nvg_data_gr_bki_t5 stored as parquet as
SELECT 
    applicationuid,
    application_date, 
    loan_id, 
    is_bank,
    applicantid, 
    -- количество любых просрочек
    COUNT(CASE WHEN delinq_cutoff_date < application_date THEN 1 END) AS count_any,
    -- количество любых просрочек за 5 лет
    COUNT(CASE WHEN ADD_MONTHS(application_date, -5*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date THEN 1 END) AS count_any_5y,
    -- длительность текущей просрочки
    MAX(CASE WHEN is_active = 1 THEN delinq_length ELSE 0 END) AS active_delinq_length,
    -- просрочки за 5 лет
    COUNT(CASE WHEN ADD_MONTHS(application_date, -5*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS count_1_29_5y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -5*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS count_30_59_5y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -5*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS count_60_89_5y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -5*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS count_90_119_5y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -5*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 120 <= delinq_length THEN 1 END) AS count_120_5y, 
    -- просрочки за 3 года
    COUNT(CASE WHEN ADD_MONTHS(application_date, -3*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS count_1_29_3y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -3*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS count_30_59_3y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -3*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS count_60_89_3y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -3*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS count_90_119_3y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -3*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 120 <= delinq_length THEN 1 END) AS count_120_3y, 
    -- просрочки за 1 год
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS count_1_29_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS count_30_59_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS count_60_89_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS count_90_119_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 120 <= delinq_length THEN 1 END) AS count_120_1y 
FROM
    (
        -- просрочки по внешним договорам
        SELECT  
            b.applicationuid,
            a.loan_id,
            b.is_bank,
            b.application_date,
            a.applicantid,
            a.dlq_end_dt AS delinq_cutoff_date,
            a.delinq_length,
            CASE 
                WHEN months_between(b.application_date, a.report_dt) > 1 THEN 0 
                ELSE a.is_active 
            END as is_active
        FROM cdm.nvg_data_gr_cre_dlq a
        INNER join cdm.nvg_data_gr_cre_mart b
            on b.applicantid = a.applicantid 
            and b.report_id = a.report_id 
            and b.loan_id = a.loan_id
			and a.application_date = b.application_date
    ) aa
GROUP BY applicationuid, application_date, applicantid, loan_id, is_bank
""";

nvg_data_gr_bki_t6 = """
create table cdm.nvg_data_gr_bki_t6 stored as parquet as
SELECT  
    b.applicationuid,
    a.loan_id,
    is_bank,
    b.application_date,
    a.applicantid,
    b.date_start, 
    --дата выхода на просрочку
    --тк по БКИ приходит группа просрочки, то считая дату начала возможна ситуация когда дата_договора>дата начала просрочки
    case when (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start 
            then b.date_start 
         else (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length))   
    end as delinquency_start_dt, 
    --длительность до истечения 6 месяцев
    datediff((LEAST(a.dlq_end_dt, ADD_MONTHS(b.date_start, 6)) + interval '1 day'), 
    case when (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start then b.date_start else (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) end) AS delinq_length6m,
    --длительность до истечения 12 месяцев
    datediff((LEAST(a.dlq_end_dt, ADD_MONTHS(b.date_start, 12)) + interval '1 day'), 
    case when (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start then b.date_start else (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) end) AS delinq_length12m,
    --общая длительность просрочки 
    a.delinq_length,
    case when months_between(case when (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start then b.date_start else (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) end, b.date_start) < 2 
            and a.delinq_length > 90 
            then 1 
         else 0 
    end first_payment_default,
    case when months_between(case when (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start then b.date_start else (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) end, b.date_start) > 2 
            and months_between(case when (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) < b.date_start then b.date_start else  (a.dlq_end_dt - interval '1 day' * (-1 + a.delinq_length)) end, date_start) < 3 
            and a.delinq_length > 90 
            then 1 
        else 0 
    end second_payment_default
from cdm.nvg_data_gr_cre_dlq a
inner join cdm.nvg_data_gr_cre_mart  b 
    on b.applicantid = a.applicantid
    and b.report_id = a.report_id
    and b.loan_id = a.loan_id
	and a.application_date = b.application_date
""";

nvg_data_gr_bki_t7 = """
create table cdm.nvg_data_gr_bki_t7 stored as parquet as
SELECT 
    applicationuid,
    application_date, 
    loan_id, 
    is_bank,
    applicantid, 
    --6 месяцев BANK
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 0 THEN 1 END) AS bank_count_first_6m_1, 
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 30 THEN 1 END) AS bank_count_first_6m_30,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 60 THEN 1 END) AS bank_count_first_6m_60,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 90 THEN 1 END) AS bank_count_first_6m_90,
    --12 месяцев BANK
	--!!!!Здесь будут расхождения, т.к. исправлена опечатка в формировании delinq_length12m
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 0 THEN 1 END) AS bank_count_first_12m_1,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 30 THEN 1 END) AS bank_count_first_12m_30,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 60 THEN 1 END) AS bank_count_first_12m_60,
    COUNT(CASE WHEN is_bank = 1 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 90 THEN 1 END) AS bank_count_first_12m_90,
    --6 месяцев BKI
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 0 THEN 1 END) AS bki_count_first_6m_1, 
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 30 THEN 1 END) AS bki_count_first_6m_30,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 60 THEN 1 END) AS bki_count_first_6m_60,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 6) AND delinquency_start_dt < application_date AND delinq_length6m > 90 THEN 1 END) AS bki_count_first_6m_90,
    --12 месяцев BKI
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 0 THEN 1 END) AS bki_count_first_12m_1,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 30 THEN 1 END) AS bki_count_first_12m_30,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 60 THEN 1 END) AS bki_count_first_12m_60,
    COUNT(CASE WHEN is_bank = 0 AND delinquency_start_dt <= ADD_MONTHS(date_start, 12) AND delinquency_start_dt < application_date AND delinq_length12m > 90 THEN 1 END) AS bki_count_first_12m_90,
    ---флаг выхода на просрочку с первого платежа
    max(first_payment_default)  first_payment_default,
    ---флаг выхода на просрочку со второго платежа 
    max(second_payment_default) second_payment_default   
from cdm.nvg_data_gr_bki_t6
group by 
    applicationuid,
    application_date, 
    loan_id, 
    is_bank,
    applicantid
""";

nvg_data_gr_bki_payment = """
create table cdm.nvg_data_gr_bki_payment stored as parquet as
SELECT 
    applicationuid,
    application_date, 
    loan_id, 
    is_bank,
    applicantid, 
	is_ik,  --ипотека
	is_pk,  --потреб
	is_kk,
	liability_rur_rkk ,
	liab_pk_desc_num as liab_pk_desc_any_num,
	liab_ik_desc_num as liab_ik_desc_any_num,
	liab_kk_desc_num as liab_kk_desc_any_num,
    -- просрочки за 1 год
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 1 <= delinq_length AND delinq_length < 30 THEN 1 END) AS bank_count_1_29_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 30 <= delinq_length AND delinq_length < 60 THEN 1 END) AS bank_count_30_59_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 60 <= delinq_length AND delinq_length < 90 THEN 1 END) AS bank_count_60_89_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 90 <= delinq_length AND delinq_length < 120 THEN 1 END) AS bank_count_90_119_1y, 
    COUNT(CASE WHEN ADD_MONTHS(application_date, -1*12) <= delinq_cutoff_date AND delinq_cutoff_date < application_date AND 120 <= delinq_length THEN 1 END) AS bank_count_120_1y 
	 
 
FROM
    (
        SELECT  
            b.applicationuid,
            a.loan_id,
            b.is_bank,
			t4.is_debtor, 
			t4.is_open,
            b.application_date,
            a.applicantid,
			t4.is_ik,  --ипотека
			t4.is_pk,  --потреб
			t4.is_kk,  --кредитные карты 
			t4.liability_rur_rkk ,
			t4.liab_pk_desc_num,
			t4.liab_ik_desc_num,
			t4.liab_kk_desc_num ,
            a.dlq_end_dt AS delinq_cutoff_date,
            a.delinq_length  
        FROM cdm.nvg_data_gr_cre_dlq a
        INNER join cdm.nvg_data_gr_cre_mart b
            on b.applicantid = a.applicantid 
            and b.report_id = a.report_id 
            and b.loan_id = a.loan_id
			and a.application_date = b.application_date
		INNER join cdm.nvg_data_gr_bki_t4 t4
			on  b.applicantid = t4.applicantid 
            and b.loan_id = t4.loan_id
			and a.application_date = t4.application_date
		where t4.is_bank = 0
 
    ) a
GROUP BY applicationuid, 
		application_date, 
		applicantid, 
		loan_id, 
		is_bank,
		is_ik,   
		is_pk,  
		is_kk,
		liability_rur_rkk ,
		liab_pk_desc_num,
		liab_ik_desc_num,
		liab_kk_desc_num 
""";

nvg_data_gr_bki_aggr = """
create table cdm.nvg_data_gr_bki_aggr stored as parquet as
SELECT  
	a.applicationuid,
	a.application_date, 
	a.applicantid,
	------------------------------------------------------------------------
	-- количество открытых
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 THEN 1 END) as int) AS cnt_opened, -- Количество открытых договоров, где обязательство имеет статус "открыто" (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_ik = 1 THEN 1 END) as int) AS mrtg_open, -- Количество открытых ипотечных кредитов (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_pk = 1 THEN 1 END) as int) AS cl_open, -- Количество открытых потребительских кредитов (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_ak = 1 THEN 1 END) as int) AS auto_open, --  Количество открытых автокредитов (заёмщик) 
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_kk = 1 THEN 1 END) as int) AS card_open, --  Количество открытых кредитных карт (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_mk = 1 THEN 1 END) as int) AS micro_open, --  Количество открытых микрозаймов (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND a.is_ok = 1 THEN 1 END) as int) AS other_open, --  Количество других открытых кредитов (не авто, не ипотека, не карта, не потребительский кредит) (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND ADD_MONTHS(a.application_date, -6)   <= a.date_start THEN 1 END)as int) AS cnt_opened_6m, -- Определяется количество договоров, открытых не более полугода назад (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 AND ADD_MONTHS(a.application_date, -12)  <= a.date_start THEN 1 END)as int) AS cnt_opened_1y, --  Определяется количество договоров, открытых не более года назад (заёмщик)
	------------------------------------------------------------------------
	-- количество закрытых
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 THEN 1 END) as int) AS cnt_closed, --  Суммарное количество закрытых кредитов (заёмщик)         
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ik = 1 THEN 1 END) as int) AS mrtg_closed, --  Количество закрытых ипотечных кредитов (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 THEN 1 END) as int) AS cl_closed, --  Количество закрытых потребительских кредитов (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ak = 1 THEN 1 END) as int) AS auto_closed, --  Количество закрытых автокредитов (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 1 THEN 1 END) as int) AS card_closed, --  Количество закрытых кредитных карт (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_mk = 1 THEN 1 END) as int) AS micro_closed, -- Количество закрытых микрозаймов (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ok = 1 THEN 1 END) as int) AS other_closed, -- Количество других закрытых кредитов (не авто, не ипотека, не карта, не потребительский кредит) (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) as int) AS cl_closed_gr500k, -- Количество закрытых потребительских кредитов с суммой обязательств >=500 000 рублей (заёмщик) 
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) as int) AS cl_closed_gr100k, -- Количество закрытых потребительских кредитов с суммой обязательств >=100 000 рублей (заёмщик) 
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000  THEN 1 END) as int) AS cl_closed_gr50k, -- Количество закрытых потребительских кредитов с суммой обязательств >=50 000 рублей (заёмщик) 
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000   THEN 1 END) as int) AS cl_closed_ls50k, -- Количество закрытых потребительских кредитов с суммой обязательств <50 000 рублей (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND ADD_MONTHS(a.application_date, -6)    <= a.date_end_fact THEN 1 END) as int) AS cnt_agr_closed_last6m, -- Количество закрытых договоров (не КК), дата закрытия которых наступила не позднее 6-ти месяцев с даты запроса
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND ADD_MONTHS(a.application_date, -12)   <= a.date_end_fact THEN 1 END) as int) AS cnt_agr_closed_last1y, -- Количество закрытых договоров (не КК), дата закрытия которых наступила не позднее 1-го года с даты запроса
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND ADD_MONTHS(a.application_date, -12*3) <= a.date_end_fact THEN 1 END) as int) AS cnt_agr_closed_last3y, --  Количество закрытых договоров (не КК), дата закрытия которых наступила не позднее 3-х лет с даты запроса
	--Классический вариант--
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND NVL(delinq.count_any, 0) = 0    THEN 1 END) as int) AS cnt_closed_no_del, -- Суммарное количество закрытых без просрочек кредитов (заёмщик)       
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND NVL(delinq.count_any, 0) = 0    THEN 1 END) as int) AS cl_closed_no_del, --  Количество закрытых без просрочек потребительских кредитов       
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ik = 1 AND NVL(delinq.count_any, 0) = 0    THEN 1 END) as int) AS mrtg_closed_no_del, --  Количество закрытых без просрочек ипотек (заёмщик)        
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_ok = 1 AND NVL(delinq.count_any, 0) = 0    THEN 1 END) as int) AS other_closed_no_del, --  Суммарное количество закрытых без просрочек других кредитов (не авто, не ипотека, не карта, не потребительский кредит) (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND NVL(delinq.count_any, 0) = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) as int) AS cnt_closed_gr500k, --  Количество закрытых без просрочек кредитов (не КК) с суммой обязательств >=500 000 рублей (заёмщик)       
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND NVL(delinq.count_any, 0) = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) as int) AS cnt_closed_gr100k, -- Количество закрытых без просрочек кредитов (не КК) с суммой обязательств >=100 000 рублей (заёмщик)       
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND NVL(delinq.count_any, 0) = 0 AND a.credit_amount_rur_0 >= 50000  THEN 1 END) as int) AS cnt_closed_gr50k, --  Количество закрытых без просрочек кредитов (не КК) с суммой обязательств >=50 000 рублей (заёмщик)               
	------------------------------------------------------------------------
	-- количество закрытых досрочно
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_ik = 1    THEN 1 END) as int) AS mrtg_adv_closed, -- Количество закрытых досрочно ипотек (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1    THEN 1 END) as int) AS cl_adv_closed, -- Количество закрытых досрочно потребительских кредитов
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_ok = 1    THEN 1 END) as int) AS other_adv_closed, -- Суммарное количество закрытых досрочно других кредитов (не авто, не ипотека, не карта, не потребительский кредит) (заёмщик)
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0    THEN 1 END) as int) AS cnt_adv_repayment, -- Количество договоров погашенных раньше даты планового завершения на 10 или более %
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) as int) AS cnt_adv_repayment_gr_500k, -- Количество договоров погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 500 000 рублей
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) as int) AS cnt_adv_repayment_gr_100k, -- Количество договоров погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 100 000 рублей
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000  THEN 1 END) as int) AS cnt_adv_repayment_gr_50k, -- Количество договоров погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 50 000 рублей
	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000   THEN 1 END) as int) AS cnt_adv_repayment_ls_50k, -- Количество договоров погашенных раньше даты планового завершения на 10 или более % с суммой выдачи НЕ более 50 000 рублей
	------------------------------------------------------------------------
	--Доп.агрегаты для витрины досрочного погашение

	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 THEN 1 END) 
	END as decimal(38,16)) AS ratio_adv_repayment, --Доля дострочно закрытых кредитов среди всех закрытых договоров
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_adv_repayment_adv_gr_500k,      --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 500 000 рублей среди всех закрытых договоров с суммой выдачи более 500 000 рублей
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_adv_repayment_adv_gr_100k,       --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 100 000 рублей среди всех закрытых договоров с суммой выдачи более 100 000 рублей
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_adv_repayment_adv_gr_50k,       --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи более 50 000 рублей среди всех закрытых договоров с суммой выдачи более 50 000 рублей
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_kk = 0 AND a.credit_amount_rur_0 < 50000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_adv_repayment_adv_ls_50k,       --Доля договоров, погашенных раньше даты планового завершения на 10 или более % с суммой выдачи не более 50 000 рублей среди всех закрытых договоров с суммой выдачи не более 50 000 рублей   
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 THEN 1 END) 
	END as decimal(38,16)) AS ratio_cl_closed,     --Доля досрочно закрытых потребительских кредитов среди всех закрытых потребительских кредитов
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 500000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_cl_closed_gr500k,      --Доля потребительских кредитов, погашенных раньше даты планового завершения на 10 или более %, с суммой обязательств более 500 000 рублей среди всех закрытых потребительских кредитов с суммой обязательств более 500 000 рублей
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 100000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_cl_closed_gr100k,       --Доля потребительских кредитов, погашенных раньше даты планового завершения на 10 или более %, с суммой обязательств более 100 000 рублей среди всех закрытых потребительских кредитов с суммой обязательств более 100 000 рублей
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 >= 50000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_cl_closed_gr50k,       --Доля потребительских кредитов, погашенных раньше даты планового завершения на 10 или более %, с суммой обязательств более 50 000 рублей среди всех закрытых потребительских кредитов с суммой обязательств более 50 000 рублей
	cast(CASE 
		WHEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000 THEN 1 END) > 0 
		THEN COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced = 1 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000 THEN 1 END) / 
			COUNT(CASE WHEN a.is_debtor = 1 AND a.is_open = 0 AND a.is_pk = 1 AND a.credit_amount_rur_0 < 50000 THEN 1 END) 
	END as decimal(38,16)) AS ratio_cl_closed_ls50k,       --Доля потребительских кредитов, погашенных раньше даты планового завершения на 10 или более %, с суммой обязательств не более 50 000 рублей среди всех закрытых потребительских кредитов с суммой обязательств не более 50 000 рублей

	cast(NVL(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced = 1 THEN a.credit_amount_rur_0 END), 0) as decimal(38,16)) AS avg_liab_sum_total_adv_agr, --Средняя переоценённая в рубли сумма обязательства по договорам, закрытым раньше даты планового погашения на 10 или более %
	cast(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN (datediff(a.date_end_plan, a.date_end_fact))/(365.25/ 12) END) as decimal(38,16)) AS avg_diff_plan_fact_closed, --Среднее значение количества месяцев между датами планового и фактического погашения по всем закрытым договорам
		   
	cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bank_adv_agr, --Общая переоценённая в рубли сумма обязательства по кредитам, закрытым досрочно на 10 или более %, по данным Банка
	cast(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bki_adv_agr, --Общая переоценённая в рубли сумма обязательства по кредитам, закрытым досрочно на 10 или более %, по данным БКИ

	cast(COUNT(CASE WHEN a.is_debtor = 1 AND a.is_advanced_25 = 1 AND a.is_kk = 0 THEN 1 END) as int) AS cnt_adv25_closed,    --Количество договоров погашенных раньше даты планового завершения на 25 или более %
	cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced_25 = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bank_adv25_agr,    --Общая переоценённая в рубли сумма обязательства по кредитам, закрытым досрочно на 25 или более %, по данным Банка
	cast(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_advanced_25 = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bki_adv25_agr,    --Общая переоценённая в рубли сумма обязательства по кредитам, закрытым досрочно на 25 или более %, по данным БКИ

	-- количество на просрочке
	--Классический вариант-----
	cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_0plus_bank, -- Рассчитывается только для договоров, открытых в Банке, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 0 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_0plus_bki, -- Рассчитывается только для договоров, открытых в в других банках, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 0 дней.
	cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 30 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_30plus_bank, -- Рассчитывается только для договоров, открытых в Банке без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 30 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 30 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_30plus_bki, -- Рассчитывается только для договоров, открытых в в других банках, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 30 дней.
	cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 60 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_60plus_bank, -- Рассчитывается только для договоров, открытых в Банке без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 60 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length >= 60 AND a.delinquent_debt_rur >= 500              THEN 1 END) as int) AS curdel_60plus_bki, -- Рассчитывается только для договоров, открытых в в других банках, без учёта договоров поручительства. Количество договоров с текущей просрочкой более 500 руб. и длительностью просрочки более 60 дней.
	cast(COUNT(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND 0 < a.delinquent_debt_rur AND a.delinquent_debt_rur < 500 THEN 1 END) as int) AS curdel_tech_bank, -- Рассчитывается только для договоров, открытых в Банке без учёта договоров поручительства. Определяется количество договоров с текущей просрочкой >0 и <500 руб. и длительностью просрочки более 0 дней.
	cast(COUNT(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0   AND 0 < a.delinquent_debt_rur AND a.delinquent_debt_rur < 500 THEN 1 END) as int) AS curdel_tech_bki, -- Рассчитывается только для договоров, открытых в в других банках без учёта договоров поручительства. Определяется количество договоров с текущей просрочкой >0 и <500 руб. и длительностью просрочки более 0 дней.
	------------------------------------------------------------------------
	-- просрочки
	--Классический вариант-----
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_1_29_5y   END), 0) as int) AS bank_1_29_5y_debtor, -- Общее число просрочек до 30 дней в Банке
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_30_59_5y  END), 0) as int) AS bank_30_59_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_60_89_5y  END), 0) as int) AS bank_60_89_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_90_119_5y END), 0) as int) AS bank_90_119_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_120_5y    END), 0) as int) AS bank_120plus_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_1_29_3y   END), 0) as int) AS bank_1_29_3y_debtor,
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_30_59_3y  END), 0) as int) AS bank_30_59_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_60_89_3y  END), 0) as int) AS bank_60_89_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_90_119_3y END), 0) as int) AS bank_90_119_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_120_3y    END), 0) as int) AS bank_120plus_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_1_29_1y   END), 0) as int) AS bank_1_29_1y_debtor,
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_30_59_1y  END), 0) as int) AS bank_30_59_1y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_60_89_1y  END), 0) as int) AS bank_60_89_1y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_90_119_1y END), 0) as int) AS bank_90_119_1y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN delinq.count_120_1y    END), 0) as int) AS bank_120plus_1y_debtor,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_1_29_5y   END), 0) as int) AS bki_1_29_5y_debtor, -- Общее число просрочек до 30 дней по данным БКИ
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_30_59_5y  END), 0) as int) AS bki_30_59_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_60_89_5y  END), 0) as int) AS bki_60_89_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_90_119_5y END), 0) as int) AS bki_90_119_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_120_5y    END), 0) as int) AS bki_120plus_5y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_1_29_3y   END), 0) as int) AS bki_1_29_3y_debtor,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_30_59_3y  END), 0) as int) AS bki_30_59_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_60_89_3y  END), 0) as int) AS bki_60_89_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_90_119_3y END), 0) as int) AS bki_90_119_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_120_3y    END), 0) as int) AS bki_120plus_3y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_1_29_1y   END), 0) as int) AS bki_1_29_1y_debtor,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_30_59_1y  END), 0) as int) AS bki_30_59_1y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_60_89_1y  END), 0) as int) AS bki_60_89_1y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_90_119_1y END), 0) as int) AS bki_90_119_1y_debtor, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN delinq.count_120_1y    END), 0) as int) AS bki_120plus_1y_debtor,
	
	------------------------------------------------------------------------
	-- текущая просрочка
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0 THEN delinq.active_delinq_length ELSE 0 END) as int) AS maxcurdel_bank, -- Максимальная длительность текущей просрочки в Банке
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND delinq.active_delinq_length > 0 THEN delinq.active_delinq_length ELSE 0 END) as int) AS maxcurdel_bki, -- Максимальная длительность текущей просрочки в БКИ
	------------------------------------------------------------------------
	-- срок, NULL будет означать отсутствие соответствующих договоров
	cast(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0   THEN datediff((a.date_end_fact + interval '1 day'), a.date_start)/ (365.25/ 12) END) as decimal(38,16)) AS avg_term_fact_closed, -- Среднее значение количества месяцев между датой фактического погашения и датой договора по закрытым договорам
	cast(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN datediff((a.date_end_plan + interval '1 day'), a.date_start)/ (365.25/ 12) END) as decimal(38,16)) AS avg_term_plan_closed, -- Среднее значение количества месяцев между датой планового погашения и датой договора по всем закрытым договорам
	cast(AVG(CASE WHEN a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN datediff((a.date_end_plan + interval '1 day'), a.date_start)/ (365.25/ 12) END) as decimal(38,16)) AS avg_term_plan_open, -- Среднее значение количества месяцев между датой планового погашения и датой договора по всем открытым договорам
	------------------------------------------------------------------------
	-- длина истории с самого ранеего договора
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN datediff((a.application_date + interval '1 day'), a.date_start)/ (365.25/ 12) ELSE 0 END) as decimal(38,16)) AS length_bank, -- Длительность кредитной истории в Банке (мес.) - от даты открытия самого раннего договора до даты отчёта
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN datediff((a.application_date + interval '1 day'), a.date_start)/ (365.25/ 12) ELSE 0 END) as decimal(38,16)) AS length_bki, -- Длительность кредитной истории в БКИ (мес.) - от даты открытия самого раннего договора до даты отчёта
	-- длина истории по-честному с перерывами
	 
	cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.for_length__date_end_fact >= for_length__date_start__bank THEN datediff((for_length__date_end_fact + interval '1 day'), for_length__date_start__bank) ELSE 0 END) / (365.25/ 12) as decimal(38,16)) AS length_bank_total, -- Совокупная длительность кредитной истории в Банке: по закрытым договорам - от даты открытия до даты факт. закрытия (мес.), по открытым - от открытия до даты отчёта
	cast(SUM(CASE WHEN   a.is_debtor = 1 AND a.for_length__date_end_fact >= for_length__date_start       THEN datediff((for_length__date_end_fact + interval '1 day'), for_length__date_start)       ELSE 0 END) / (365.25/ 12) as decimal(38,16)) AS length_total, -- Совокупная длительность кредитной истории в БКИ: по закрытым договорам - от даты открытия до даты факт. закрытия (мес.), по открытым - от открытия до даты отчёта
	------------------------------------------------------------------------
	-- флаги ипотечных кредитов
	--Классический вариант------------
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 1 AND NVL(delinq.count_any_5y, 0) = 0 AND a.date_start < ADD_MONTHS(a.application_date, -6) AND NVL(delinq.active_delinq_length, 0) = 0 AND NVL(a.delinquent_debt_rur, 0) = 0 THEN 1 ELSE 0 END) as tinyint) AS good_mortgage_bank_open, -- Если у клиента есть открытый ипотечный кредит в Банке,  где обязательство не является поручительством, удовлетворяющий условиям:  нет исторических просрочек длительностью более 7 дней И  нет текущей просрочки И дата открытия не ранее 6 месяцев относительно даты обращения в Банк за кредитом,  то 1  Иначе 0
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 1 AND NVL(delinq.count_any_5y, 0) = 0 AND a.date_start < ADD_MONTHS(a.application_date, -6) AND NVL(delinq.active_delinq_length, 0) = 0 AND NVL(a.delinquent_debt_rur, 0) = 0 THEN 1 ELSE 0 END) as tinyint) AS good_mortgage_bki_open, --  Если у клиента есть открытый ипотечный кредит по информации БКИ,  где обязательство не является поручительством, удовлетворяющий условиям:  нет исторических просрочек длительностью более 7 дней И  нет текущей просрочки И дата открытия не ранее 6 месяцев относительно даты обращения в Банк за кредитом,  то 1  Иначе 0
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 0 AND NVL(delinq.count_any, 0)    = 0 THEN 1 ELSE 0 END) as tinyint) AS good_mortgage_bank_closed, -- Если у клиента есть закрытый ипотечный кредит в Банке,  где обязательство не является поручительством, по которому нет исторических просрочек длительностью более 7 дней то 1 Иначе 0
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_ik = 1 AND a.is_open = 0 AND NVL(delinq.count_any, 0)    = 0 THEN 1 ELSE 0 END) as tinyint) AS good_mortgage_bki_closed, -- Если у клиента есть закрытый ипотечный кредит по информации БКИ,  где обязательство не является поручительством, по которому нет исторических просрочек длительностью более 7 дней то 1 Иначе 0
	------------------------------------------------------------------------
	-- сумма кредита
	cast(NVL(AVG(CASE WHEN   a.is_debtor = 1 AND a.is_kk = 0   THEN a.credit_amount_rur_0        END), 0) as decimal(38,16)) AS avg_liab_sum_total, -- Средняя переоцененная в рубли сумма обязательства по всем  договорам по данным БКИ и Банка
	cast(NVL(AVG(CASE WHEN   a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0        END), 0) as decimal(38,16)) AS avg_liab_sum_total_open_agr, -- Средняя переоцененная в рубли сумма обязательства по открытым договорам по данным БКИ и Банка
	cast(NVL(AVG(CASE WHEN   a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0        END), 0) as decimal(38,16)) AS avg_liab_sum_total_closed_agr, -- Средняя переоцененная в рубли сумма обязательства по закрытым договорам по данным БКИ и Банка
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bank_open, --  Максимальная переоцененная в рубли сумма обязательства по открытым договорам по данным Банка
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bki_open, -- Максимальная переоцененная в рубли сумма обязательства по открытым договорам по данным БКИ
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bank_closed, -- Максимальная переоцененная в рубли сумма обязательства по закрытым договорам по данным Банка
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS max_liab_sum_bki_closed, -- Максимальная переоцененная в рубли сумма обязательства по закрытым договорам по данным БКИ
	cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bank_open_agr, -- Общая переоцененная в рубли сумма обязательства по открытым договорам по данным Банка
	cast(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bki_open_agr, -- Общая переоцененная в рубли сумма обязательства по открытым договорам по данным БКИ
	cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bank_closed_agr, -- Общая переоцененная в рубли сумма обязательства по закрытым договорам по данным Банка
	cast(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 THEN a.credit_amount_rur_0 ELSE 0 END) as decimal(28,6)) AS total_liab_sum_bki_closed_agr, -- Общая переоцененная в рубли сумма обязательства по закрытым договорам по данным БКИ
	------------------------------------------------------------------------
	-- текущая задолженнсть; отдельные агрегаты сделаны намерено
	cast(SUM(CASE WHEN a.is_debtor = 1   THEN NVL(a.current_debt_rur, 0) + NVL(a.delinquent_debt_rur, 0) ELSE 0 END) as decimal(38,16)) AS outstanding, --  Суммарная оставшаяся непогашенная задолженность
	cast(SUM(CASE WHEN a.is_debtor = 1 AND a.is_open = 1 THEN NVL(a.current_debt_rur, 0) + NVL(a.delinquent_debt_rur, 0) ELSE 0 END) as decimal(38,16)) AS outstanding_open, -- Суммарная оставшаяся непогашенная задолженность по открытым счетам
	-- ПРИМЕЧАНИЕ. Здесь есть нестыковка остатков ежемесячных и просрочек ежедневных
	cast(NVL(AVG(CASE WHEN a.is_debtor = 1 AND a.delinquent_debt_rur > 0 THEN a.delinquent_debt_rur        END), 0) as decimal(38,16)) AS avg_delinquent_debt_total, -- Среднее значение переоцененной в рубли текущей просроченной задолженности по всем обязательствам
	cast(SUM(CASE WHEN a.is_debtor = 1               THEN a.delinquent_debt_rur ELSE 0 END) as decimal(28,6)) AS curr_arrear_rur, -- CURR_ARREAR_RUR — Сумма текущей просрочки в рублях на дату запроса (кроме договоров поручительства)      
	------------------------------------------------------------------------
	-- Утилизация: сумма задолженности / сумма лимитов (по курсу на последнюю отчётную дату; в текущей ситуации - конец месяца)
	-- NULL будет означать отсутствие карт
	-- по открытым картам
	cast(CASE 
		WHEN SUM(CASE WHEN a.is_debtor = 1 AND a.is_kk = 1 AND a.is_open = 1 THEN a.card_limit_rur_b END) > 0 THEN   
			SUM(CASE WHEN a.is_debtor = 1 AND a.is_kk = 1 AND a.is_open = 1 THEN NVL(a.current_debt_rur, 0) + NVL(a.delinquent_debt_rur, 0) END) / 
			SUM(CASE WHEN a.is_debtor = 1 AND a.is_kk = 1 AND a.is_open = 1 THEN a.card_limit_rur_b END) 
	END as decimal(38,16)) AS utilization_avg, -- Средняя утилизация по всем картам
	------------------------------------------------------------------------
	-- считаем количество разных видов обязательств: 
	-- ипотека, потреб, авто, карта, поручительство, в БКИ - микро, прочее
	cast(NVL(MAX(a.is_guarantor), 0) +
		NVL(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_ik END), 0) +
		NVL(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_pk END), 0) +
		NVL(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_ak END), 0) +
		NVL(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_kk END), 0) + 
		NVL(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_mk END), 0) +
		NVL(MAX(CASE WHEN a.is_debtor = 1 THEN a.is_ok END), 0) as int) AS cnt_liability_types, -- CNT_LIABILITY_TYPES — Количество разных типов обязательств
	------------------------------------------------------------------------
	-- платёжная нагрузка считается по всем договорам (внутренним, внешним), поручительствам
	cast(NVL(SUM(a.liability_rur), 0) as decimal(38,16)) AS total_curr_payment, --  Суммарная текущая платежная нагрузка
	------------------------------------------------------------------------
	-- если все обязательства - это поручительства
	cast(CASE WHEN COUNT(CASE WHEN a.is_guarantor = 1 THEN 1 END) = COUNT(*) THEN 1 ELSE 0 END as tinyint) AS only_guarantees, -- Если все обязательства клиента являются договорами поручительства, то 1, иначе 0
	------------------------------------------------------------------------
	-- общее количество запросов в БКИ берётся из данных CRE
	-- если данные в CRE есть, но они NULL, то заменям на 0
	-- если данные в CRE отсутствуют, то NULL
	cast(MAX(cre.inquiry1week) as int) AS cnt_inquiry_last1w, -- Общее число запросов кредитной истории за последнюю неделю
	cast(MAX(cre.inquiry1month) as int) AS cnt_inquiry_last1m, --  Общее число запросов кредитной истории за последний месяц
	cast(MAX(cre.inquiry3month) as int) AS cnt_inquiry_last3m, -- Общее число запросов кредитной истории за последние 3 месяца
	cast(MAX(cre.inquiry6month) as int) AS cnt_inquiry_last6m, -- Общее число запросов кредитной истории за последние 6 месяцев
	------------------------------------------------------------------------
	-- Агрегаты для ПКБ-----------------------------------------------------
	------------------------------------------------------------------------
	cast(MAX(a.report_dt) as string) AS CRE_DATE, -- Дата ответа БКИ
	cast(NVL(SUM(a.liability_rur_rkk), 0) as decimal(38,16)) AS total_curr_payment_rkk, -- Суммарная текущая платежная нагрузка, где для внешних кредитов расчет по алгоритму  в РКК
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 THEN a.liability_rur END), 0)  as decimal(38,16)) AS BANK_LIAB_PAYM, -- Суммарная текущая платежная нагрузка в банке
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 THEN a.liability_rur_rkk END), 0)  as decimal(38,16)) AS BKI_LIAB_PAYM, --Суммарная текущая платежная нагрузка по данным БКИ (по алгоритму РКК)
	-- Количество месяцев с момента закрытия последнего выданного кредита в БКИ (среди закрытых, не карт, последний - по дате закрытия)
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 AND a.end_desc_num = 1 THEN datediff(a.application_date, a.date_end_fact) / (365.25/ 12) END)  as decimal(38,16)) AS bki_loan_lst_close_m_term,
	-- Количество месяцев до планового погашения последнего выданного кредита в БКИ (среди открытых, не карт, последний - по дате открытия)
	cast(MAX(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 AND a.start_desc_num = 1 THEN a.remain_term END)  as decimal(38,16)) AS bki_loan_till_lst_close_m_term,
	-- Количество месяцев с момента закрытия последнего выданного кредита в ГПБ (среди закрытых, не карт, последний - по дате закрытия)
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 0 AND a.end_desc_num = 1 THEN datediff(a.application_date, a.date_end_fact) / (365.25/ 12) END)  as decimal(38,16)) AS bank_loan_lst_close_m_term,
	-- Количество месяцев до планового погашения последнего выданного кредита в ГПБ (среди открытых, не карт, последний - по дате открытия)
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_kk = 0 AND a.is_open = 1 AND a.start_desc_num = 1 THEN a.remain_term END)  as decimal(38,16)) AS bank_loan_till_lst_close_m_term,
	-- Процентная ставка по последнему потребительскому кредиту в ГПБ (по дате открытия)
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.start_pk_desc_num = 1 THEN a.interest_rate_month * 100 * 12 END)  as decimal(28,6)) AS bank_loan_lst_rate,
	-- Процентная ставка по первому потребительскому кредиту в ГПБ (по дате открытия)
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.start_pk_asc_num = 1 THEN a.interest_rate_month * 100 * 12 END)  as decimal(28,6)) AS bank_loan_fst_rate,
	cast(CASE WHEN COUNT(CASE WHEN a.is_bank = 1 THEN 1 END) > 0 THEN 1 ELSE 0 END as tinyint) AS bank_ki_flg, -- флаг наличия внутренней КИ
	cast(CASE WHEN COUNT(CASE WHEN a.is_bank = 0 THEN 1 END) > 0 THEN 1 ELSE 0 END as tinyint) AS bki_app_flg, -- флаг наличия внешней КИ
	------------------------------------------------------------------------
	
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 THEN a.liability_rur_rkk END)  as decimal(38,16)) AS max_paym_bank, -- Максимальный ежемесячный платеж (потребы)
	cast(MIN(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 THEN a.liability_rur_rkk END)  as decimal(38,16)) AS min_paym_bank, -- Минимальный ежемесячный платеж (потребы)
	cast(MAX(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.is_open = 1 THEN a.liability_rur_rkk END)  as decimal(38,16)) AS max_paym_bank_act, -- Максимальный ежемесячный платеж среди активных
	cast(MIN(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 AND a.is_pk = 1 AND a.is_open = 1 THEN a.liability_rur_rkk END)  as decimal(38,16)) AS min_paym_bank_act, -- Минимальный ежемесячный платеж среди активных
		
	cast(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN a.delinquent_debt_rur ELSE 0 END) as decimal(28,6)) AS current_overdue_bank_amt,

	--Количество выходов на просрочку 0+/30+/60+/90+ по данным БКИ/Банка за первые 6/12 месяцев с даты выдачи кредита.
	
	--BANK 6m/12m
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_1  END), 0) as int) AS bank_count_first_6m_1, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_30  END), 0) as int) AS bank_count_first_6m_30,
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_60  END), 0) as int) AS bank_count_first_6m_60,
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_6m_90  END), 0) as int) AS bank_count_first_6m_90,
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_1  END), 0) as int) AS bank_count_first_12m_1, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_30  END), 0) as int) AS bank_count_first_12m_30,
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_60  END), 0) as int) AS bank_count_first_12m_60,
	cast(NVL(SUM(CASE WHEN a.is_bank = 1 AND a.is_debtor = 1 THEN del_first.bank_count_first_12m_90  END), 0) as int) AS bank_count_first_12m_90,
	--BKI 6m/12m
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_1 END), 0) as int) AS bki_count_first_6m_1,               
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_30 END), 0) as int) AS bki_count_first_6m_30,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_60 END), 0) as int) AS bki_count_first_6m_60,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_6m_90 END), 0) as int) AS bki_count_first_6m_90, 
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_1 END), 0) as int) AS bki_count_first_12m_1,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_30 END), 0) as int) AS bki_count_first_12m_30,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_60 END), 0) as int) AS bki_count_first_12m_60,
	cast(NVL(SUM(CASE WHEN a.is_bank = 0 AND a.is_debtor = 1 THEN del_first.bki_count_first_12m_90 END), 0) as int) AS bki_count_first_12m_90,
	----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	----------Флаг выхода на просрочку с первого/второго платежа. 
	----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	cast(NVL(max(first_payment_default), 0) as tinyint) AS first_payment_default,     
	cast(NVL(max(case when first_payment_default = 0 then second_payment_default end), 0) as tinyint) AS second_payment_default,  
	substring(cast(cast(now() as timestamp) as string), 1, 19) as t_changed_dttm,
	cast(0 as tinyint) as t_deleted_flg,
	cast(1 as tinyint) as t_active_flg
FROM cdm.nvg_data_gr_bki_t4 a
-- просрочки
left join cdm.nvg_data_gr_bki_t5 delinq
    on delinq.application_date = a.application_date
    and delinq.loan_id = a.loan_id
    and delinq.is_bank = a.is_bank
    and delinq.applicantid = a.applicantid
    and delinq.applicationuid = a.applicationuid
 --количество выходов на просрочку за 6/12 с даты выдачи кредита.
left join cdm.nvg_data_gr_bki_t7 del_first 
    on del_first.application_date = a.application_date
    and del_first.loan_id = a.loan_id
    and del_first.is_bank = a.is_bank
    and del_first.applicantid = a.applicantid
    and del_first.applicationuid = a.applicationuid   
-- количество запросов в БКИ
left join  
(
    -- таблица сформирована в разрезе договоров, которые привязались к заявке из CRE;
    -- поэтому схлопываем строки, т. к. интересуемые поля относятся к заявке в целом, а не к конкретному договору
    select distinct 
        application_date,
        applicantid,
        -- пропуски заполняем нулями, чтобы отличать эти данные (которые получены из CRE)
        -- от строк, к оторым данные из CRE не привязались (там будет NULL)
        nvl(inquiry1week, 0) as inquiry1week, 
        nvl(inquiry1month, 0) as inquiry1month, 
        nvl(inquiry3month, 0) as inquiry3month,
        nvl(inquiry6month, 0) as inquiry6month 
    from cdm.nvg_data_gr_cre_mart
) cre 
    on cre.application_date = a.application_date 
    and cre.applicantid = a.applicantid

left join cdm.nvg_data_gr_bki_payment as pay
	on pay.application_date = a.application_date
    and pay.loan_id = a.loan_id
    and pay.is_bank = a.is_bank
    and pay.applicantid = a.applicantid
    and pay.applicationuid = a.applicationuid
group by
    a.applicationuid,
    a.application_date,
    a.applicantid
""";

nvg_data_gr_cre_dlq_t1 = """
create table cdm.nvg_data_gr_cre_dlq_t1 (
    application_date timestamp, 
    report_id string, 
    report_date timestamp, 
    applicantid string, 
    loan_id decimal(19,0), 
    dlq_end_dt timestamp, 
    worst_status decimal (19,1),
    count_day bigint
            )
stored as parquet 
""";

nvg_data_gr_cre_dlq_t3 = """
create table cdm.nvg_data_gr_cre_dlq_t3 (
    application_date timestamp,	
    report_id string,
    report_date timestamp,
    applicantid string,
    loan_id decimal(19,0),
    pmtstringstart timestamp,
    pmtstring84m string,
    pmtstring84m_rev string,
    len int,
    l_val decimal(11,1),
    worst_status decimal(11,1),
    count_day bigint
)
stored as parquet
"""
