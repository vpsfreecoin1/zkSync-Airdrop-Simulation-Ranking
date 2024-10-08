select date_trunc('month', block_date) as block_date,
    version,
    count(*) as transaction_count,
    count(distinct user_address) as user_count,
    sum(amount_usd) filter (where amount_usd > 0) as bridged_amount_usd,
    sum(amount_eth) filter (where amount_eth > 0) as bridged_amount_eth,
    sum(amount_usd) as balance_amount_usd,
    sum(amount_eth) as balance_amount_eth
from query_2501864
where block_date >= date('2022-01-01')
group by 1, 2
order by 1, 2
