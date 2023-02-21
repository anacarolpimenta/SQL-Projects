-- CTE to retrieve all orders of all clients and create a flag to count the orders that contain exclusive brands to count later.
with
   raw_orders as (
	select 
		dc.cpf
		, fpf.order_date
		, fpf.order_num
	 	, fpf.flg_first_order
                , dcv.channel
		, max(
			  case
				when dp.exclusive_brand = 'Yes'
				then 1
			   else 0
			  end
			 ) as fl_count_eb
	from dw.ft_sales fpf
		join dw.dim_client dc
			on dc.chv_client = fpf.chv_client
		join dw.dim_product dp 	
			on dp.chv_product = fpf.chv_product
                join dw.dim_channel dcv
                        on dcv.chv_channel = fpf.chv_channel
        group by 1,2,3,4,5
)
-- CTE to Rank all orders of each client, based on CPF and ordered by order_date
, base_order_sequence	as (
        select distinct
		cpf
		  , order_date
		  , order_num
                  , channel
	          , fl_count_eb
	          , flg_first_order -- this variable is used only for validation, it's not necessary at the end.
		  , rank() over (partition by cpf order by order_date asc) as sequence_order_client
         from raw_orders	
)
-- CTE to get the first order that contains Exclusive brands products, so we get the first order number partitioned by CPF (client) and ordered by order date.
, base_first_order_eb as (
	select distinct 
		cpf,
    	first_value(order_num) over(partition by cpf order by order_date asc 
    	                                                      rows between unbounded preceding and unbounded following) first_order_eb
	from raw_orders
	where fl_count_eb=1
)
-- CTE to join the two bases and bring the total numbers of clients in a certain period and the total converted clients in the same period.
, base as (
	select
		bos.cpf
		, bos.order_date
		, bos.order_num
                , bos.channel
		, bos.sequence_order_client
		, case 
			when bfoeb.first_order_eb is not null -- if not null means that the first exclusive brand order exists, so we count them.
			then 1 
			else 0 
		  end as fl_first_order_eb
	from base_order_sequence bos
		left join base_first_order_eb bfoeb
			on bos.order_num=bfoeb.first_order_eb	
)
-- the final base that where we count the numbers of client that ordered in a certain period, the sequence of the order and the number of converted clients, so that we can calculate our convertion rate in the report.
        select 
		order_date
		, channel
		, case
		   when sequence_order_client > 36
		   then 37
		   else sequence_order_client
	          end as num_sequence_order
	       , case
		  when sequence_order_client > 36
		  then concat('N','+')
		  else concat('N',sequence_order_client)
		 end as nm_sequence_order
	       , count(distinct cpf) as total_clients_with_orders
	       , count(distinct 
			case 
				when fl_first_order_eb = 1
				then cpf 
			end) as convert_clients_eb
        from base 
        where date_diff(current_date, order_date,month) <= 25 -- we want to query only the las 25 months of data to minimize the volume and make the dataset and the report more performatic.
        group by 1,2,3,4
