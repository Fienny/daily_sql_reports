from datetime import date, timedelta

today_str = date.today().strftime("%Y%m%d")
today_plus_21_str = (date.today() + timedelta(days=21)).strftime("%Y%m%d")
yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
two_weeks_ago = (date.today() - timedelta(days=14)).strftime("%Y%m%d")


# checking if current snapshot is up do date
current_snapshot = f"""
    select
        case
            when exists (
                select 1
                from main_lakehouse.dbo.udf_prediction
                where snapshot_date = '{today_str}'
                and diag_id = ''
                and system_forecast <> 0
            ) then 1 else 0
        end as current_snapshot_exists;
"""

# system antipeaks
system_antipeaks = f"""
    WITH past_raw AS (
        SELECT
            material,
            forecast_date,
            CASE WHEN OFR_ID <> '' THEN 1 ELSE 0 END AS promo_flag,
            system_forecast
        FROM udf_prediction
        WHERE
            diag_id = ''
            AND store NOT LIKE '%L%'
            AND snapshot_date = forecast_date
            AND forecast_date BETWEEN '{two_weeks_ago}' AND '{yesterday}'
    ),
    future_raw AS (
        SELECT
            material,
            forecast_date,
            CASE WHEN OFR_ID <> '' THEN 1 ELSE 0 END AS promo_flag,
            system_forecast
        FROM udf_prediction
        WHERE
            diag_id = ''
            AND store NOT LIKE '%L%'
            AND snapshot_date = '{today_str}'
            AND forecast_date BETWEEN '{today_str}' AND '{today_plus_21_str}'
    ),
    past AS (
        SELECT
            material,
            forecast_date,
            SUM(system_forecast) AS total_forecast,
            MAX(promo_flag) AS promo_exists
        FROM past_raw
        GROUP BY material, forecast_date
    ),

    future AS (
        SELECT
            material,
            forecast_date,
            SUM(system_forecast) AS total_forecast,
            MAX(promo_flag) AS promo_exists
        FROM future_raw
        GROUP BY material, forecast_date
    ),

    all_select AS (
        SELECT * FROM past
        UNION ALL
        SELECT * FROM future
    ),

    sku_ppm_zavod AS (
        SELECT
            MATERIAL,
            ISNULL(DISMM, 'NO_PPM') AS ppm_type,
            COUNT(DISTINCT PLANT) AS num_of_stores
        FROM blob_oos
        WHERE CALDATE = '2026-01-19'
        GROUP BY
            MATERIAL,
            ISNULL(DISMM, 'NO_PPM')
    ),


    sku_ppm_pivot AS (
        SELECT
            MATERIAL,
            ISNULL([PD], 0)     AS PD,
            ISNULL([ND], 0)     AS ND,
            ISNULL([ZK], 0)     AS ZK,
            ISNULL([ZR], 0)     AS ZR,
            ISNULL([ZC], 0)     AS ZC,
            ISNULL([ZD], 0)     AS ZD,
            ISNULL([NO_PPM], 0) AS NO_PPM
        FROM sku_ppm_zavod
        PIVOT (
            SUM(num_of_stores)
            FOR ppm_type IN ([PD], [ND], [ZK], [ZR], [ZC], [ZD], [NO_PPM])
        ) p
    ),

    sku_info_added AS (
        SELECT
            pd.[Категорийный_менеджм],
            pd.[Название],
            pd.[Узел_2],
            pd.[Описание_2],
            pd.[Узел_3],
            pd.[Описание_3],
            pd.[Узел_4],
            pd.[Описание_4],
            pd.[Узел_5],
            pd.[Описание_5],
            s.*,
            spp.PD, 
            spp.ND,
            spp.ZK,
            spp.ZR,
            spp.ZC,
            spp.ZD,
            spp.NO_PPM
        FROM all_select s
        JOIN product_dictionary_s4 pd
            ON pd.[Материал] = s.material
        LEFT JOIN sku_ppm_pivot spp
            ON s.material = spp.MATERIAL
    )
    SELECT *
    FROM sku_info_added;
"""

old_peaks = f"""
    DECLARE @today DATE = CAST(GETDATE() AS DATE);
WITH forecasts AS (
    SELECT
        material AS sku,
        store,
        forecast_date,
        system_forecast,
        -- Next day
        LEAD(forecast_date) OVER (
            PARTITION BY material, store
            ORDER BY forecast_date
        ) AS next_date,
        LEAD(system_forecast) OVER (
            PARTITION BY material, store
            ORDER BY forecast_date
        ) AS next_forecast,

        -- Previous day f(d–1)
        LAG(forecast_date) OVER (
            PARTITION BY material, store
            ORDER BY forecast_date
        ) AS prev_date,
        LAG(system_forecast) OVER (
            PARTITION BY material, store
            ORDER BY forecast_date
        ) AS previous_day_forecast,

        -- NEW: Two days ago f(d–2)
        LAG(system_forecast, 2) OVER (
            PARTITION BY material, store
            ORDER BY forecast_date
        ) AS forecast_two_days_ago

    FROM udf_prediction
    WHERE snapshot_date = '{today_str}'
      AND forecast_date BETWEEN '{today_str}' AND {today_plus_21_str}
      AND store NOT IN ('L001', 'L002', 'L003', 'L005', 'L006')
      and promo_flag = 0
      and diag_id = ''
),
violations AS (
    SELECT
        sku,
        store,
        next_date AS peak_day,       -- the spike day
		next_forecast AS peak_value, -- the spike value
		forecast_date AS peak_day_minus_one_date,
		system_forecast AS peak_day_minus_one_value,
        CASE
		    WHEN next_forecast IS NULL THEN 0
		    -- If f(d–1) < 2 * f(d–2), use f(d–2)
		    WHEN previous_day_forecast < 2 * forecast_two_days_ago
		         THEN CASE 
		                 WHEN next_forecast > previous_day_forecast * 1.8 THEN 1
		                 ELSE 0
		              END
		
		    -- Otherwise use f(d–1)
		    ELSE CASE
		             WHEN next_forecast > system_forecast * 2 THEN 1 -- тут изменил 1.8 на 2
		             ELSE 0
		         END
		END AS fails_rule
    FROM forecasts
    WHERE system_forecast > 100 
      AND next_forecast > 100
),
selecting AS (
    SELECT
        v.sku,
        v.store,
        v.peak_day_minus_one_value,
        v.peak_day,
        v.peak_value
    FROM (
        SELECT
            v.*,
            ROW_NUMBER() OVER (
                PARTITION BY v.sku, v.store
                ORDER BY v.peak_day_minus_one_date
            ) AS rn
        FROM violations v
        WHERE v.fails_rule = 1
    ) v
    WHERE v.rn = 1
),
PrevYearSales AS (
    SELECT
        s.sku,
        s.store,
        s.peak_day_minus_one_value,
        bo.SC_QUANTITY AS prev_year_peak_day_sales,
        s.peak_day,
        s.peak_value
    FROM selecting s
    JOIN blob_oos bo
        ON s.sku = bo.MATERIAL 
       AND s.store = bo.PLANT
    WHERE bo.CALDATE = DATEADD(YEAR, -1, s.peak_day)
      AND bo.ISPROMO = 0
),
LastCheck as (
	SELECT
        s.sku,
        s.store,
        s.peak_day_minus_one_value,
        s.prev_year_peak_day_sales,
        s.peak_day,
        s.peak_value,
        bo.DISMM as ppm_type,
        avg(SC_QUANTITY) as avg_21_d_sales
    FROM PrevYearSales s
    JOIN blob_oos bo
    	ON s.sku = bo.MATERIAL
       AND s.store = bo.PLANT
    where bo.CALDATE = '2026-01-19' -- between dateadd(day, -15, @today) and dateadd(day, -1, @today) 
    	and bo.ISPROMO = 0
    group by
    	s.sku,
        s.store,
        s.peak_day_minus_one_value,
        s.prev_year_peak_day_sales,
        s.peak_day,
        s.peak_value,
        bo.DISMM
--    having avg(SC_QUANTITY) < s.peak_value / 3
)
SELECT
    pd.[Категорийный_менеджм],
    pd.[Название],
    v.sku,
    v.store,
    pd.[Узел_2],
    pd.[Описание_2],
    pd.[Узел_3],
    pd.[Описание_3],
    pd.[Узел_4],
    pd.[Описание_4],
    pd.[Узел_5],
    pd.[Описание_5],
    v.ppm_type as [Тип ППМ],
    v.peak_day_minus_one_value as [Прогноз_за_день_до_пика],
    v.prev_year_peak_day_sales as [Продажи_прошлого_года_в_день_пика],
    v.peak_day as [День_пика],
    v.peak_value as [Значение_пика],
    v.avg_21_d_sales as [Ср_продажи_за_21_день],
    case
    	when datename(weekday, v.peak_day) in ('Saturday', 'Sunday')
    	then 1
    	else 0
    end as [Пик_в_выходной],
   	CASE WHEN u.s4_adjustment != 0 THEN 1 ELSE 0 END AS [есть_с4_корректировка],
	CASE WHEN u.car_adjustment != 0 THEN 1 ELSE 0 END AS [есть_кар_корректировка],
	u.approved_forecast as [финальный_прогноз]
FROM LastCheck v
JOIN product_dictionary_s4 pd
    ON pd.[Материал] = v.sku
join udf_prediction u
	ON v.sku = u.material
   AND v.store = u.store
   AND u.forecast_date = v.peak_day
   and u.system_forecast = v.peak_value
   and snapshot_date = '{today_str}'
WHERE 1 = 1
  AND v.peak_value - v.peak_day_minus_one_value > 10
  AND v.peak_value - v.prev_year_peak_day_sales > v.peak_value * 0.5
	and u.diag_id = '';
"""

new_peaks = f"""
with data_raw as (
	select
		material,
		store,
		forecast_date,
		system_forecast
	from main_lakehouse.dbo.udf_prediction
	where 1=1 
		and snapshot_date = '{today_str}' 
		and diag_id = '' 
		and forecast_date between '{today_str}' and {today_plus_21_str}
		and system_forecast > 0
),
detecting_max as (
	select
		material,
		store,
		max(system_forecast) as max_forecast
	from data_raw
	group by material, store
),
detecting_min as (
	select
		material,
		store,
		min(system_forecast) as min_forecast
	from data_raw
	group by material, store
),
checking as (
	select
		dmax.material,
		dmax.store,
		dmax.max_forecast,
		dmin.min_forecast,
		dmax.max_forecast / dmin.min_forecast as ratio
	from detecting_max dmax
	join detecting_min dmin
		on dmax.material = dmin.material
		and dmax.store = dmin.store
	WHERE 1=1
		and dmax.max_forecast / dmin.min_forecast > 20 -- criteria
		and dmax.max_forecast > 10
		and dmin.min_forecast > 5
)
select
	pd.[Категорийный_менеджм],
    pd.[Название],
    pd.[Узел_2],
    pd.[Описание_2],
    pd.[Узел_3],
    pd.[Описание_3],
    pd.[Узел_4],
    pd.[Описание_4],
    pd.[Узел_5],
    pd.[Описание_5],
    zi.az_type,
    c.*
from checking c
join main_lakehouse.dbo.product_dictionary_s4 pd
	on c.material = pd.[Материал]
join main_lakehouse.dbo.zassart_info zi
	on zi.plu = pd.[Материал]
	and c.store = zi.zavod;
"""

antipeaks = f"""
    DECLARE @today DATE = CAST(GETDATE() AS DATE);
WITH bo_dedup AS (
    SELECT DISTINCT plu, zavod, az_type
    FROM zassart_info
    WHERE az_type not in ('ND')
),
udf_filt AS (
    SELECT
        ud.*
    FROM udf_prediction ud
    JOIN Market_Dictionary md
      ON ud.store = md.[Номер]
    WHERE ud.snapshot_date = @today
    	and ud.system_forecast > 0
      AND ud.system_forecast IS NOT NULL
      AND CONVERT(date, ud.forecast_date, 112) BETWEEN @today AND dateadd(day, 21, @today)
      AND COALESCE(ud.diag_id, '') = ''
      AND md.[Тип_CF] IN ('Mahalla', 'Supermarket')
),

base AS (
    -- ВАЖНО: здесь НЕ фильтруем по отрицательным корректировкам!
    SELECT
        u.forecast_date,
        u.material,
        u.store,
        b.az_type,
        u.system_forecast,
        COALESCE(u.car_adjustment, 0) AS car_forecast,
        COALESCE(u.s4_adjustment, 0) AS s4_forecast
    FROM udf_filt u
    JOIN bo_dedup b
      ON u.material = b.plu
     AND u.store   = b.zavod
),
agg AS (
    SELECT
        forecast_date,
        material,
        COUNT(DISTINCT store) AS zavod_num,
        SUM(system_forecast) AS system_forecast,
        SUM(car_forecast)    AS car_forecast,
        SUM(s4_forecast)     AS s4_forecast
    FROM base
    GROUP BY forecast_date, material
    having SUM(system_forecast) > 100
),
calc AS (
    SELECT
        *,
        CASE 
            WHEN car_forecast != 0 
                 THEN ((system_forecast + car_forecast) / system_forecast - 1) * 100
            ELSE 0
        END AS car_delta,
        CASE 
            WHEN s4_forecast != 0 
                 THEN ((system_forecast + s4_forecast) / system_forecast - 1) * 100
            ELSE 0
        END AS s4_delta
    FROM agg
)
SELECT
    pd.[Категорийный_менеджм],
    pd.[Название],
    c.material,
    pd.[Узел_2],
    pd.[Описание_2],
    pd.[Узел_3],
    pd.[Описание_3],
    pd.[Узел_4],
    pd.[Описание_4],
    pd.[Узел_5],
    pd.[Описание_5],
    c.forecast_date,
    c.zavod_num,
    c.system_forecast,
    c.car_forecast,
    c.s4_forecast,
    c.car_delta,
    c.s4_delta
FROM calc c
JOIN product_dictionary_s4 pd
  ON pd.[Материал] = c.material
WHERE 1=1
  AND c.zavod_num > 40                         -- (2)
  AND (
        (c.car_forecast < 0 AND c.car_delta < -50) OR
        (c.s4_forecast < 0 AND c.s4_delta < -50)
      )                                        -- (4)
ORDER BY c.forecast_date, c.material;
"""

dbd_total = f"""
    with temp as (
	select
		bw.calc_date,
		EXTRACT(ISODOW FROM bw.calc_date) as day_num,
		ul."user",
		pc.km,
		pc.ui_5,
		pc.ui_5_name,
		sum(bw.sc_quantity) as sc_quantity
	from supply_chains.bw_oos as bw
	left join supply_chains.plu_cat as pc
		on pc.plu=bw.plu
	left join nps.user_login as ul
		on ul.category_manager = pc.km
	where bw.calc_date between current_date - interval '14 days' and  current_date - interval '1 days'
		and bw.zavod not like 'L%'
		and bw.zavod not like 'R%'
		and bw.zavod not like 'B%'
	group by 
		bw.calc_date,
		day_num,
		ul."user",
		pc.km,
		pc.ui_5,
		pc.ui_5_name
),
total as (
	select 
		"user",
		ui_5,
		ui_5_name,
		round(sum(case when calc_date = current_date - interval '14 days' then sc_quantity else 0 end),1) as "14 days ago",
		round(sum(case when calc_date = current_date - interval '13 days' then sc_quantity else 0 end),1) as "13 days ago",
		round(sum(case when calc_date = current_date - interval '12 days' then sc_quantity else 0 end),1) as "12 days ago",
		round(sum(case when calc_date = current_date - interval '11 days' then sc_quantity else 0 end),1) as "11 days ago",
		round(sum(case when calc_date = current_date - interval '10 days' then sc_quantity else 0 end),1) as "10 days ago",
		round(sum(case when calc_date = current_date - interval '9 days' then sc_quantity else 0 end),1) as "9 days ago",
		round(sum(case when calc_date = current_date - interval '8 days' then sc_quantity else 0 end),1) as "8 days ago",
		round(sum(case when calc_date = current_date - interval '7 days' then sc_quantity else 0 end),1) as "7 days ago",
		round(sum(case when calc_date = current_date - interval '6 days' then sc_quantity else 0 end),1) as "6 days ago",
		round(sum(case when calc_date = current_date - interval '5 days' then sc_quantity else 0 end),1) as "5 days ago",
		round(sum(case when calc_date = current_date - interval '4 days' then sc_quantity else 0 end),1) as "4 days ago",
		round(sum(case when calc_date = current_date - interval '3 days' then sc_quantity else 0 end),1) as "3 days ago",
		round(sum(case when calc_date = current_date - interval '2 days' then sc_quantity else 0 end),1) as "2 days ago",
		round(sum(case when calc_date = current_date - interval '1 day' then sc_quantity else 0 end),1) as "1 day ago",
		round(((sum(case when calc_date = current_date - interval '7 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '14 days' then sc_quantity else 0 end),0)-1)*100),1) as "7_рост%",
		round(((sum(case when calc_date = current_date - interval '6 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '13 days' then sc_quantity else 0 end),0)-1)*100),1) as "6_рост%",
		round(((sum(case when calc_date = current_date - interval '5 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '12 days' then sc_quantity else 0 end),0)-1)*100),1) as "5_рост%",
		round(((sum(case when calc_date = current_date - interval '4 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '11 days' then sc_quantity else 0 end),0)-1)*100),1) as "4_рост%",
		round(((sum(case when calc_date = current_date - interval '3 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '10 days' then sc_quantity else 0 end),0)-1)*100),1) as "3_рост%",
		round(((sum(case when calc_date = current_date - interval '2 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '9 days' then sc_quantity else 0 end),0)-1)*100),1) as "2_рост%",
		round(((sum(case when calc_date = current_date - interval '1 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '8 days' then sc_quantity else 0 end),0)-1)*100),1) as "1_рост%",
		round(((sum(case when calc_date = current_date - interval '1 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '2 days' then sc_quantity else 0 end),0)-1)*100),1) as "Рост в прошлому дню %",
		round(((sum(case when calc_date in(current_date - interval '7 day',current_date - interval '6 day',current_date - interval '5 day',current_date - interval '4 day',current_date - interval '3 day',current_date - interval '2 day',current_date - interval '1 day') then sc_quantity else 0 end)/
		nullif(sum(case when calc_date in (current_date - interval '14 days',current_date - interval '13 day',current_date - interval '12 day',current_date - interval '11 day',current_date - interval '10 day',current_date - interval '9 day',current_date - interval '8 day') then sc_quantity else 0 end),0)-1)*100),1) as "Недельный_рост %"
	from temp
	group by
		"user",
		ui_5,
		ui_5_name
)
select 
	'Итог' as "user",
	'Итог' as ui_5,
	'Итог' as ui_5_name,
	sum("14 days ago") as "14 days ago",
	sum("13 days ago") as "13 days ago",
	sum("12 days ago") as "12 days ago",
	sum("11 days ago") as "11 days ago",
	sum("10 days ago") as "10 days ago",
	sum("9 days ago") as "9 days ago",
	sum("8 days ago") as "8 days ago",
	sum("7 days ago") as "7 days ago",
	sum("6 days ago") as "6 days ago",
	sum("5 days ago") as "5 days ago",
	sum("4 days ago") as "4 days ago",
	sum("3 days ago") as "3 days ago",
	sum("2 days ago") as "2 days ago",
	sum("1 day ago") as "1 day ago",
	round(((sum("7 days ago")/sum("14 days ago")-1)*100),1) as "7_Рост %",
	round(((sum("6 days ago")/sum("13 days ago")-1)*100),1) as "6_Рост %",
	round(((sum("5 days ago")/sum("12 days ago")-1)*100),1) as "5_Рост %",
	round(((sum("4 days ago")/sum("11 days ago")-1)*100),1) as "4_Рост %",
	round(((sum("3 days ago")/sum("10 days ago")-1)*100),1) as "3_Рост %",
	round(((sum("2 days ago")/sum("9 days ago")-1)*100),1) as "2_Рост %",
	round(((sum("1 day ago")/sum("8 days ago")-1)*100),1) as "1_Рост %",
	round(((sum("1 day ago")/sum("2 days ago")-1)*100),1) as "Рост в прошлому дню %",
	null as "Недельный рост %" 
from total
union all
select * from total
order by
	"1 day ago" desc;
"""

dbd_reg = f"""
    with temp as (
	select
		bw.calc_date,
		EXTRACT(ISODOW FROM bw.calc_date) as day_num,
		ul."user",
		pc.km,
		pc.ui_5,
		pc.ui_5_name,
		SUM(bw.sc_quantity_not_anypromo) as sc_quantity -- now it is ony reg
	from supply_chains.bw_oos as bw
	left join supply_chains.plu_cat as pc
		on pc.plu=bw.plu
	left join nps.user_login as ul
		on ul.category_manager = pc.km
	where bw.calc_date between current_date - interval '14 days' and  current_date - interval '1 days'
		and bw.zavod not like 'L%'
		and bw.zavod not like 'R%'
		and bw.zavod not like 'B%'
	group by 
		bw.calc_date,
		day_num,
		ul."user",
		pc.km,
		pc.ui_5,
		pc.ui_5_name
),
total as (
	select 
		"user",
		ui_5,
		ui_5_name,
		round(sum(case when calc_date = current_date - interval '14 days' then sc_quantity else 0 end),1) as "14 days ago",
		round(sum(case when calc_date = current_date - interval '13 days' then sc_quantity else 0 end),1) as "13 days ago",
		round(sum(case when calc_date = current_date - interval '12 days' then sc_quantity else 0 end),1) as "12 days ago",
		round(sum(case when calc_date = current_date - interval '11 days' then sc_quantity else 0 end),1) as "11 days ago",
		round(sum(case when calc_date = current_date - interval '10 days' then sc_quantity else 0 end),1) as "10 days ago",
		round(sum(case when calc_date = current_date - interval '9 days' then sc_quantity else 0 end),1) as "9 days ago",
		round(sum(case when calc_date = current_date - interval '8 days' then sc_quantity else 0 end),1) as "8 days ago",
		round(sum(case when calc_date = current_date - interval '7 days' then sc_quantity else 0 end),1) as "7 days ago",
		round(sum(case when calc_date = current_date - interval '6 days' then sc_quantity else 0 end),1) as "6 days ago",
		round(sum(case when calc_date = current_date - interval '5 days' then sc_quantity else 0 end),1) as "5 days ago",
		round(sum(case when calc_date = current_date - interval '4 days' then sc_quantity else 0 end),1) as "4 days ago",
		round(sum(case when calc_date = current_date - interval '3 days' then sc_quantity else 0 end),1) as "3 days ago",
		round(sum(case when calc_date = current_date - interval '2 days' then sc_quantity else 0 end),1) as "2 days ago",
		round(sum(case when calc_date = current_date - interval '1 day' then sc_quantity else 0 end),1) as "1 day ago",
		round(((sum(case when calc_date = current_date - interval '7 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '14 days' then sc_quantity else 0 end),0)-1)*100),1) as "7_рост%",
		round(((sum(case when calc_date = current_date - interval '6 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '13 days' then sc_quantity else 0 end),0)-1)*100),1) as "6_рост%",
		round(((sum(case when calc_date = current_date - interval '5 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '12 days' then sc_quantity else 0 end),0)-1)*100),1) as "5_рост%",
		round(((sum(case when calc_date = current_date - interval '4 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '11 days' then sc_quantity else 0 end),0)-1)*100),1) as "4_рост%",
		round(((sum(case when calc_date = current_date - interval '3 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '10 days' then sc_quantity else 0 end),0)-1)*100),1) as "3_рост%",
		round(((sum(case when calc_date = current_date - interval '2 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '9 days' then sc_quantity else 0 end),0)-1)*100),1) as "2_рост%",
		round(((sum(case when calc_date = current_date - interval '1 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '8 days' then sc_quantity else 0 end),0)-1)*100),1) as "1_рост%",
		round(((sum(case when calc_date = current_date - interval '1 day' then sc_quantity else 0 end)/
		nullif(sum(case when calc_date = current_date - interval '2 days' then sc_quantity else 0 end),0)-1)*100),1) as "Рост в прошлому дню %",
		round(((sum(case when calc_date in(current_date - interval '7 day',current_date - interval '6 day',current_date - interval '5 day',current_date - interval '4 day',current_date - interval '3 day',current_date - interval '2 day',current_date - interval '1 day') then sc_quantity else 0 end)/
		nullif(sum(case when calc_date in (current_date - interval '14 days',current_date - interval '13 day',current_date - interval '12 day',current_date - interval '11 day',current_date - interval '10 day',current_date - interval '9 day',current_date - interval '8 day') then sc_quantity else 0 end),0)-1)*100),1) as "Недельный_рост %"
	from temp
	group by
		"user",
		ui_5,
		ui_5_name
)
select 
	'Итог' as "user",
	'Итог' as ui_5,
	'Итог' as ui_5_name,
	sum("14 days ago") as "14 days ago",
	sum("13 days ago") as "13 days ago",
	sum("12 days ago") as "12 days ago",
	sum("11 days ago") as "11 days ago",
	sum("10 days ago") as "10 days ago",
	sum("9 days ago") as "9 days ago",
	sum("8 days ago") as "8 days ago",
	sum("7 days ago") as "7 days ago",
	sum("6 days ago") as "6 days ago",
	sum("5 days ago") as "5 days ago",
	sum("4 days ago") as "4 days ago",
	sum("3 days ago") as "3 days ago",
	sum("2 days ago") as "2 days ago",
	sum("1 day ago") as "1 day ago",
	round(((sum("7 days ago")/sum("14 days ago")-1)*100),1) as "7_Рост %",
	round(((sum("6 days ago")/sum("13 days ago")-1)*100),1) as "6_Рост %",
	round(((sum("5 days ago")/sum("12 days ago")-1)*100),1) as "5_Рост %",
	round(((sum("4 days ago")/sum("11 days ago")-1)*100),1) as "4_Рост %",
	round(((sum("3 days ago")/sum("10 days ago")-1)*100),1) as "3_Рост %",
	round(((sum("2 days ago")/sum("9 days ago")-1)*100),1) as "2_Рост %",
	round(((sum("1 day ago")/sum("8 days ago")-1)*100),1) as "1_Рост %",
	round(((sum("1 day ago")/sum("2 days ago")-1)*100),1) as "Рост в прошлому дню %",
	null as "Недельный рост %" 
from total
union all
select * from total
order by
	"1 day ago" desc;
"""