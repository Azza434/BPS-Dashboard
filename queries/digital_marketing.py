sql_query = """
        WITH DateRange AS (
            SELECT
                '{start_date}'::date AS start_date,
                '{end_date}'::date AS end_date
        ),
        GoogleSpendTotals AS (
            SELECT
                SUM(cs.cost_micros / 1000000.0) AS total_cost,
                SUM(cs.conversions) AS total_conversions,
                SUM(cs.conversions_value) AS total_revenue,
                CASE
                    WHEN SUM(cs.cost_micros) > 0 THEN SUM(cs.conversions_value) / (SUM(cs.cost_micros) / 1000000.0)
                    ELSE 0
                END AS total_roas
            FROM
                google_ads.campaign_stats cs
            JOIN
                (
                    SELECT
                        id,
                        name,
                        start_date,
                        ROW_NUMBER() OVER(PARTITION BY id ORDER BY start_date DESC) as rn
                    FROM google_ads.campaign_history
                ) ch ON cs.id = ch.id AND ch.rn = 1
            WHERE
                cs.date >= (SELECT start_date FROM DateRange)
                AND cs.date <= (SELECT end_date FROM DateRange)
                AND cs.customer_id = '2026374428'
        ),
        GA4BaseMetrics AS (
            SELECT
                SUM(sessions) AS total_ga4_sessions,
                SUM(total_users) AS total_ga4_total_users
            FROM
                google_analytics_4.traffic_acquisition_session_default_channel_grouping_report
            WHERE
                date >= (SELECT start_date FROM DateRange)
                AND date <= (SELECT end_date FROM DateRange)
                AND session_default_channel_grouping IN ('Organic Search', 'Organic Shopping')
        ),
        GA4PurchaseMetrics AS (
            SELECT
                SUM(event_count) AS total_seo_sales,
                SUM(total_revenue) AS total_ga4_seo_revenue
            FROM
                google_analytics_4.traffic_acquisition_by_channel_and_event
            WHERE
                date >= (SELECT start_date FROM DateRange)
                AND date <= (SELECT end_date FROM DateRange)
                AND session_default_channel_group IN ('Organic Shopping', 'Organic Search')
                AND event_name = 'purchase'
        )
        SELECT
            gst.total_cost AS "Google Ads Total Cost",
            gst.total_conversions AS "Google Ads Total Conversions",
            gst.total_revenue AS "Google Ads Total Revenue",
            gst.total_roas AS "Google Ads Total ROAS",
            CASE
                WHEN gst.total_conversions > 0 THEN gst.total_cost / gst.total_conversions
                ELSE 0.0
            END AS "Google Ads Cost / Conversion",
            CASE
                WHEN gst.total_cost > 0 THEN gst.total_conversions::NUMERIC / gst.total_cost
                ELSE 0.0
            END AS "Conv. Rate",
            gbm.total_ga4_sessions AS "GA4 Total Sessions",
            gbm.total_ga4_total_users AS "GA4 Total Users",
            COALESCE(gpm.total_seo_sales, 0) AS "Total SEO Sales",
            COALESCE(gpm.total_ga4_seo_revenue, 0) AS "Total GA4 SEO Revenue",
            CASE
                WHEN gbm.total_ga4_sessions > 0 THEN COALESCE(gpm.total_seo_sales, 0)::NUMERIC / gbm.total_ga4_sessions
                ELSE 0.0
            END AS "Overall GA4 SEO Conversion Rate",
            CASE
                WHEN COALESCE(gpm.total_seo_sales, 0) > 0 THEN 2050.0 / COALESCE(gpm.total_seo_sales, 0)
                ELSE 0.0
            END AS "Overall SEO Cost Per Sale"
        FROM
            GoogleSpendTotals gst
        CROSS JOIN
            GA4BaseMetrics gbm
        CROSS JOIN
            GA4PurchaseMetrics gpm;
        """