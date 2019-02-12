with subscriptions as (

    select * from {{ref('subscriptions')}}

),

months as (

    select * from {{ref('calendar_months')}}

),

-- date spine and amortize (total net_amount/periods)
amortized as (

    select

        {{ dbt_utils.surrogate_key('months.date_month','unique_invoice_id','site') }} as id,
        source_table,
        unique_account_id,
        original_account_id,
        account_name,
        site,
        invoice_issuer,
        plan_code,
        plan_name,
        net_amount_paid/periods as revenue,
        currency,
        sales_date,
        start_date,
        end_date,
        grace_period_end,
        months.date_month

    from months
    join subscriptions
        on date_trunc('month', subscriptions.start_date)::date <= months.date_month
        and date_trunc('month', subscriptions.end_date)::date > months.date_month
    where net_amount_paid > 0

)

select * from amortized
