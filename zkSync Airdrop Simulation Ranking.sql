with user_summary as (
    select user_address, 
        count(*) as transaction_count,
        count(*) filter (where action_type = 'Deposit') as deposit_transaction_count,
        -- count(*) filter (where action_type = 'Withdraw') as withdraw_transaction_count,
        count(*) filter (where action_type = 'Deposit' and version = 'Era') as era_deposit_transaction_count,
        count(*) filter (where action_type = 'Deposit' and version = 'Lite') as lite_deposit_transaction_count,
        -- count(*) filter (where action_type = 'Withdraw' and version = 'Era') as era_withdraw_transaction_count,
        -- count(*) filter (where action_type = 'Withdraw' and version = 'Lite') as lite_withdraw_transaction_count,
        min(block_date) as initial_block_date,
        -- max(block_date) as last_block_date,
        date_diff('day', min(block_date), now()) as zksync_age_days,
        count(distinct block_date) as active_days_count,
        count(distinct date_trunc('week', block_date)) as active_weeks_count,
        count(distinct date_trunc('month', block_date)) as active_months_count,
        sum(amount_usd) filter (where amount_usd > 0) as bridged_amount_usd,
        sum(amount_eth) filter (where amount_eth > 0) as bridged_amount_eth,
        sum(amount_usd) filter (where amount_usd > 0 and version = 'Era') as bridged_amount_usd_era,
        sum(amount_usd) filter (where amount_usd > 0 and version = 'Lite') as bridged_amount_usd_lite,
        sum(amount_usd) as balance_amount_usd,
        sum(amount_eth) as balance_amount_eth,
        count(distinct l2_contract_address) filter (where l2_contract_address <> 0x0000000000000000000000000000000000000000) as l2_contract_count
    from query_2501864 d -- ZKsync Era & Lite deposit / withdraw details
    group by 1
),

user_summary_with_rank as (
    select user_address,
        transaction_count,
        deposit_transaction_count,
        -- withdraw_transaction_count,
        era_deposit_transaction_count,
        lite_deposit_transaction_count,
        -- era_withdraw_transaction_count,
        -- lite_withdraw_transaction_count,
        -- initial_block_date,
        -- last_block_date,
        zksync_age_days,
        active_days_count,
        active_weeks_count,
        active_months_count,
        round(bridged_amount_usd, 2) as bridged_amount_usd,
        -- bridged_amount_eth,
        -- bridged_amount_usd_era,
        -- bridged_amount_usd_lite,
        round(balance_amount_usd, 2) as balance_amount_usd,
        -- balance_amount_eth,
        -- l2_contract_count,
        (
        -- Bridged token to Zksync Era
        if(bridged_amount_usd_era > 0, 1, 0)  -- Bridged to ZKsync Era
        
        -- Deposit Transactions to ZKsync Era
        + if(era_deposit_transaction_count >= 5, 1, 0)  -- Conducted more than 5 deposit transactions on ZKsync Era
        + if(era_deposit_transaction_count >= 10, 1, 0)  -- Conducted more than 10 deposit transactions on ZKsync Era
        + if(era_deposit_transaction_count >= 50, 1, 0)  -- Conducted more than 50 deposit transactions on ZKsync Era
        + if(era_deposit_transaction_count >= 100, 1, 0)  -- Conducted more than 100 deposit transactions on ZKsync Era
        
        -- Bridged token to Zksync Lite
        + if(bridged_amount_usd_lite > 0, 1, 0)  -- Bridged to ZKsync Lite
        
        -- Deposit Transactions to ZKsync Lite
        + if(lite_deposit_transaction_count >= 5, 1, 0)  -- Conducted more than 5 deposit transactions on ZKsync Lite
        + if(lite_deposit_transaction_count >= 10, 1, 0)  -- Conducted more than 10 deposit transactions on ZKsync Lite
        + if(lite_deposit_transaction_count >= 50, 1, 0)  -- Conducted more than 50 deposit transactions on ZKsync Lite
        + if(lite_deposit_transaction_count >= 100, 1, 0)  -- Conducted more than 100 deposit transactions on ZKsync Lite
        
        -- Interacted L2 Contracts Count on ZkSync Era
        + if(l2_contract_count >= 2, 1, 0)  -- Interacted more than 2 contracts on ZKsync Era
        + if(l2_contract_count >= 5, 1, 0)  -- Interacted more than 5 contracts on ZKsync Era
        + if(l2_contract_count >= 10, 1, 0)  -- Interacted more than 10 contracts on ZKsync Era

        -- Unique Active Months
        + if(active_months_count >= 2, 1, 0)  -- Conducted transactions during 2 distinct months
        + if(active_months_count >= 6 , 1, 0) -- Conducted transactions during 6 distinct months
        + if(active_months_count >= 9, 1, 0)  -- Conducted transactions during 9 distinct months
        + if(active_months_count >= 12, 1, 0)  -- Conducted transactions during 12 distinct months
        
        -- Unique Active Weeks
        + if(active_weeks_count >= 10, 1, 0)  -- Conducted transactions during 10 distinct weeks
        + if(active_weeks_count >= 20, 1, 0)  -- Conducted transactions during 20 distinct weeks
        + if(active_weeks_count >= 50, 1, 0)  -- Conducted transactions during 50 distinct weeks
        + if(active_weeks_count >= 100, 1, 0)  -- Conducted transactions during 100 distinct weeks
        
        -- Unique Active Days
        + if(active_days_count >= 50, 1, 0)  -- Conducted transactions during 50 distinct days
        + if(active_days_count >= 100, 1, 0)  -- Conducted transactions during 100 distinct days
        + if(active_days_count >= 200, 1, 0)  -- Conducted transactions during 200 distinct days
        + if(active_days_count >= 500, 1, 0)  -- Conducted transactions during 500 distinct days
        
        -- Age in days
        + if(zksync_age_days >= 100, 1, 0)  -- Started using ZKsync before 100 days
        + if(zksync_age_days >= 200, 1, 0)  -- Started using ZKsync before 200 days
        + if(zksync_age_days >= 500, 1, 0)  -- Started using ZKsync before 500 days

        -- Bridged Amount
        + if(bridged_amount_usd > 1000, 1, 0) -- Bridged more than $1,000 of assets through ZKsync
        + if(bridged_amount_usd > 10000, 1, 0) -- Bridged more than $10,000 of assets through ZKsync
        + if(bridged_amount_usd > 50000, 1, 0) -- Bridged more than $50,000 of assets through ZKsync
        + if(bridged_amount_usd > 250000, 1, 0) -- Bridged more than $250,000 of assets through ZKsync
        + if(bridged_amount_usd > 1000000, 1, 0) -- Bridged more than $1,000,000 of assets through ZKsync
        
        -- Balance Amount
        + if(balance_amount_usd > 10000, 1, 0) -- Balance Amount (Deposit - Withdraw) more than $10,000 of assets on ZKsync
        + if(balance_amount_usd > 100000, 1, 0) -- Balance Amount (Deposit - Withdraw) more than $100,000 of assets on ZKsync
        + if(balance_amount_usd > 250000, 1, 0) -- Balance Amount (Deposit - Withdraw) more than $250,000 of assets on ZKsync
        + if(balance_amount_usd > 1000000, 1, 0) -- Balance Amount (Deposit - Withdraw) more than $1,000,000  of assets on ZKsync
        ) as rank_score
    from user_summary
)

select row_number() over (order by rank_score desc, bridged_amount_usd desc, transaction_count desc) as rk,
    user_address as ua,
    rank_score as rs,
    transaction_count as tc,
    deposit_transaction_count as dtc,
    -- withdraw_transaction_count as wtc,
    era_deposit_transaction_count as edtc,
    lite_deposit_transaction_count as ldtc,
    -- era_withdraw_transaction_count as ewtc,
    -- lite_withdraw_transaction_count as lwtc,
    -- initial_block_date as ibd,
    -- last_block_date as lbd,
    zksync_age_days as zad,
    active_days_count as dc,
    active_weeks_count as wc,
    active_months_count as mc,
    bridged_amount_usd as bau,
    -- bridged_amount_eth as bae,
    -- bridged_amount_usd_era as baue,
    -- bridged_amount_usd_lite as baul,
    balance_amount_usd as bau2
    -- ,
    -- balance_amount_eth as bbae,
    -- l2_contract_count as cc
from user_summary_with_rank
order by rank_score desc, bridged_amount_usd desc, transaction_count desc
-- limit 1000

