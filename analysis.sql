WITH store_metrics AS (
    -- Calculate basic store performance metrics
    SELECT 
        bbs.store_number,
        COUNT(*) AS weeks_counted,
        SUM(bbs.weekly_sales) AS total_sales,
        AVG(bbs.weekly_sales) AS avg_weekly_sales,
        MIN(bbs.weekly_sales) AS min_weekly_sales,
        MAX(bbs.weekly_sales) AS max_weekly_sales
    FROM retail_db.best_buy_sales bbs
    GROUP BY bbs.store_number
),
seasonal_performance AS (
    -- Analyze festive vs non-festive week performance
    SELECT 
        bbs.store_number,
        bbs.festive_week,
        AVG(bbs.weekly_sales) AS avg_sales,
        -- Calculate sales variation from store's overall average
        AVG(bbs.weekly_sales) - store_avg.avg_weekly_sales AS sales_variation
    FROM retail_db.best_buy_sales bbs
    JOIN (
        SELECT bbs_inner.store_number, AVG(bbs_inner.weekly_sales) AS avg_weekly_sales
        FROM retail_db.best_buy_sales bbs_inner
        GROUP BY bbs_inner.store_number
    ) store_avg
    ON bbs.store_number = store_avg.store_number
    GROUP BY bbs.store_number, bbs.festive_week
),
economic_impact AS (
    -- Analyze impact of economic factors
    SELECT 
        bbs.store_number,
        CASE 
            WHEN bbs.inflation > 2.5 THEN 'High Inflation'
            ELSE 'Normal Inflation'
        END AS inflation_category,
        AVG(bbs.weekly_sales) AS avg_sales,
        -- Calculate correlations using manual computation if required
        ROUND(COALESCE(SUM(bbs.weekly_sales * bbs.fuel_cost) - SUM(bbs.weekly_sales) * SUM(bbs.fuel_cost) / COUNT(*), 0) 
            / SQRT(
                (SUM(bbs.weekly_sales * bbs.weekly_sales) - SUM(bbs.weekly_sales) * SUM(bbs.weekly_sales) / COUNT(*)) 
                * (SUM(bbs.fuel_cost * bbs.fuel_cost) - SUM(bbs.fuel_cost) * SUM(bbs.fuel_cost) / COUNT(*))
            ), 3) AS fuel_cost_correlation,
        ROUND(COALESCE(SUM(bbs.weekly_sales * bbs.jobless_rate) - SUM(bbs.weekly_sales) * SUM(bbs.jobless_rate) / COUNT(*), 0) 
            / SQRT(
                (SUM(bbs.weekly_sales * bbs.weekly_sales) - SUM(bbs.weekly_sales) * SUM(bbs.weekly_sales) / COUNT(*)) 
                * (SUM(bbs.jobless_rate * bbs.jobless_rate) - SUM(bbs.jobless_rate) * SUM(bbs.jobless_rate) / COUNT(*))
            ), 3) AS unemployment_correlation
    FROM retail_db.best_buy_sales bbs
    GROUP BY 
        bbs.store_number,
        CASE 
            WHEN bbs.inflation > 2.5 THEN 'High Inflation'
            ELSE 'Normal Inflation'
        END
)

SELECT 
    sm.store_number,
    -- Store Performance Classification
    CASE
        WHEN sm.avg_weekly_sales > store_metrics_summary.upper_threshold THEN 'Top Performer'
        WHEN sm.avg_weekly_sales < store_metrics_summary.lower_threshold THEN 'Needs Improvement'
        ELSE 'Average Performer'
    END AS performance_category,
    
    -- Sales Metrics
    ROUND(sm.total_sales, 2) AS total_sales,
    ROUND(sm.avg_weekly_sales, 2) AS avg_weekly_sales,
    ROUND((sm.max_weekly_sales - sm.min_weekly_sales) / NULLIF(sm.min_weekly_sales, 0) * 100, 2) AS sales_volatility_pct,
    
    -- Festive Impact
    ROUND((
        SELECT sp.sales_variation 
        FROM seasonal_performance sp 
        WHERE sp.store_number = sm.store_number 
        AND sp.festive_week = 1
    ), 2) AS festive_sales_lift,
    
    -- Economic Impact
    ei.inflation_category,
    ei.fuel_cost_correlation,
    ei.unemployment_correlation,
    
    -- Store Ranking
    RANK() OVER (ORDER BY sm.avg_weekly_sales DESC) AS sales_rank,
    
    -- Percentage of Total Company Sales
    ROUND(sm.total_sales * 100 / store_metrics_summary.total_company_sales, 2) AS pct_of_total_sales
FROM 
    store_metrics sm
    JOIN economic_impact ei ON sm.store_number = ei.store_number
    CROSS JOIN (
        SELECT 
            AVG(sm_inner.avg_weekly_sales) AS company_avg_sales,
            STDDEV(sm_inner.avg_weekly_sales) AS sales_stddev,
            AVG(sm_inner.avg_weekly_sales) + STDDEV(sm_inner.avg_weekly_sales) AS upper_threshold,
            AVG(sm_inner.avg_weekly_sales) - STDDEV(sm_inner.avg_weekly_sales) AS lower_threshold,
            SUM(sm_inner.total_sales) AS total_company_sales
        FROM store_metrics sm_inner
    ) store_metrics_summary
ORDER BY 
    sales_rank;
    
    
    


