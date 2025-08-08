# app.py
from flask import Flask, render_template, jsonify, request
import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta
import calendar
import os

# Initialize the Flask application
app = Flask(__name__)

# Database connection parameters
DB_CONFIG = {
    "dbname": "fivetran",
    "user": "fivetran",
    "password": "R00frack12AM",
    "host": "35.234.144.173",
    "port": "5432"
}

@app.route('/')
def index():
    """
    Renders the main dashboard page (BPS Google Ads Dashboard),
    fetching data directly from the database and passing it to the template.
    """
    # Get start and end dates from request, or default to last month
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            # Fallback to last month if dates are invalid
            today = datetime.now().date()
            first_day_of_current_month = today.replace(day=1)
            end_date = first_day_of_current_month - timedelta(days=1)
            start_date = end_date.replace(day=1)
    else:
        # Default to last month
        today = datetime.now().date()
        first_day_of_current_month = today.replace(day=1)
        end_date = first_day_of_current_month - timedelta(days=1)
        start_date = end_date.replace(day=1)

    conn = None
    cur = None
    dashboard_metrics = {}
    error_message = None

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # SQL query for BPS Google Ads Dashboard
        sql_query = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
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

        cur.execute(sql_query)
        result = cur.fetchone()
        column_names = [desc[0] for desc in cur.description]

        if result:
            dashboard_metrics = dict(zip(column_names, result))
        else:
            dashboard_metrics = {
                "Google Ads Total Cost": 0.0, "Google Ads Total Conversions": 0.0,
                "Google Ads Total Revenue": 0.0, "Google Ads Total ROAS": 0.0,
                "Google Ads Cost / Conversion": 0.0, "Conv. Rate": 0.0,
                "GA4 Total Sessions": 0, "GA4 Total Users": 0,
                "Total SEO Sales": 0, "Total GA4 SEO Revenue": 0.0,
                "Overall GA4 SEO Conversion Rate": 0.0, "Overall SEO Cost Per Sale": 0.0
            }

    except Error as e:
        print(f"Error connecting to PostgreSQL or executing query: {e}")
        error_message = f"Failed to load dashboard data: {e}"
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return render_template('index.html', metrics=dashboard_metrics,
                           start_date=start_date, end_date=end_date,
                           error_message=error_message)

@app.route('/google_ads_dashboard')
def google_ads_dashboard():
    """
    Renders the Google Ads Dashboard page, fetching data directly from the database
    and passing it to the template.
    """
    # Get start and end dates from request, or default to last month
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            # Fallback to last month if dates are invalid
            today = datetime.now().date()
            first_day_of_current_month = today.replace(day=1)
            end_date = first_day_of_current_month - timedelta(days=1)
            start_date = end_date.replace(day=1)
    else:
        # Default to last month
        today = datetime.now().date()
        first_day_of_current_month = today.replace(day=1)
        end_date = first_day_of_current_month - timedelta(days=1)
        start_date = end_date.replace(day=1)

    end_date_year = datetime.now().date() - timedelta(days=1)
    start_date_year = end_date_year - timedelta(days=365)

    conn = None
    cur = None
    dashboard_metrics = {}
    daily_metrics_data = []
    campaign_conversions_data = []
    campaign_performance_data = []
    error_message = None

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # SQL query for Google Ads Dashboard (monthly totals)
        monthly_sql_query = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
        )
        SELECT
            SUM(cs.impressions) AS impressions,
            SUM(cs.clicks) AS clicks,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.clicks) ELSE 0 END AS cpc,
            SUM(cs.cost_micros / 1000000.0) AS cost,
            CASE WHEN SUM(cs.conversions) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.conversions) ELSE 0 END AS conversion_cost,
            SUM(cs.conversions) AS conversions,
            SUM(cs.conversions_value) AS conversion_value,
            CASE WHEN SUM(cs.cost_micros / 1000000.0) > 0 THEN SUM(cs.conversions_value) / SUM(cs.cost_micros / 1000000.0) ELSE 0 END AS roas,
            CASE WHEN SUM(cs.impressions) > 0 THEN SUM(cs.clicks)::NUMERIC / SUM(cs.impressions) ELSE 0 END AS ctr,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.conversions)::NUMERIC / SUM(cs.clicks) ELSE 0 END AS conv_rate
        FROM
            google_ads.campaign_stats AS cs
        WHERE
            cs.customer_id = '2026374428'
            AND cs.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange);
        """

        cur.execute(monthly_sql_query)
        result = cur.fetchone()
        column_names = [desc[0] for desc in cur.description]

        if result:
            dashboard_metrics = dict(zip(column_names, result))
        else:
            dashboard_metrics = {
                "impressions": 0, "clicks": 0, "cpc": 0.0, "cost": 0.0,
                "conversion_cost": 0.0, "conversions": 0, "conversion_value": 0.0,
                "roas": 0.0, "ctr": 0.0, "conv_rate": 0.0
            }

        # SQL query for Google Ads Dashboard (monthly totals)
        monthly_sql_query_shopping = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
        ),
        LatestCampaignHistory AS (
            SELECT
                id,
                name,
                ROW_NUMBER() OVER(PARTITION BY id ORDER BY start_date DESC) as rn
            FROM google_ads.campaign_history
        )
        SELECT
            SUM(cs.impressions) AS impressions,
            SUM(cs.clicks) AS clicks,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.clicks) ELSE 0 END AS cpc,
            SUM(cs.cost_micros / 1000000.0) AS cost,
            CASE WHEN SUM(cs.conversions) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.conversions) ELSE 0 END AS conversion_cost,
            SUM(cs.conversions) AS conversions,
            SUM(cs.conversions_value) AS conversion_value,
            CASE WHEN SUM(cs.cost_micros / 1000000.0) > 0 THEN SUM(cs.conversions_value) / SUM(cs.cost_micros / 1000000.0) ELSE 0 END AS roas,
            CASE WHEN SUM(cs.impressions) > 0 THEN SUM(cs.clicks)::NUMERIC / SUM(cs.impressions) ELSE 0 END AS ctr,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.conversions)::NUMERIC / SUM(cs.clicks) ELSE 0 END AS conv_rate
        FROM
            google_ads.campaign_stats AS cs
        JOIN
            LatestCampaignHistory AS ch ON cs.id = ch.id AND ch.rn = 1
        WHERE
            cs.customer_id = '2026374428'
            AND cs.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
            AND ch.name LIKE '%Shopping%';
        """

        cur.execute(monthly_sql_query_shopping)
        result = cur.fetchone()
        column_names = [desc[0] for desc in cur.description]

        if result:
            dashboard_metrics_shopping = dict(zip(column_names, result))
        else:
            dashboard_metrics_shopping = {
                "impressions": 0, "clicks": 0, "cpc": 0.0, "cost": 0.0,
                "conversion_cost": 0.0, "conversions": 0, "conversion_value": 0.0,
                "roas": 0.0, "ctr": 0.0, "conv_rate": 0.0
            }

        # SQL query for Google Ads Dashboard (monthly totals)
        monthly_sql_query_search = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
        ),
        LatestCampaignHistory AS (
            SELECT
                id,
                name,
                ROW_NUMBER() OVER(PARTITION BY id ORDER BY start_date DESC) as rn
            FROM google_ads.campaign_history
        )
        SELECT
            SUM(cs.impressions) AS impressions,
            SUM(cs.clicks) AS clicks,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.clicks) ELSE 0 END AS cpc,
            SUM(cs.cost_micros / 1000000.0) AS cost,
            CASE WHEN SUM(cs.conversions) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.conversions) ELSE 0 END AS conversion_cost,
            SUM(cs.conversions) AS conversions,
            SUM(cs.conversions_value) AS conversion_value,
            CASE WHEN SUM(cs.cost_micros / 1000000.0) > 0 THEN SUM(cs.conversions_value) / SUM(cs.cost_micros / 1000000.0) ELSE 0 END AS roas,
            CASE WHEN SUM(cs.impressions) > 0 THEN SUM(cs.clicks)::NUMERIC / SUM(cs.impressions) ELSE 0 END AS ctr,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.conversions)::NUMERIC / SUM(cs.clicks) ELSE 0 END AS conv_rate
        FROM
            google_ads.campaign_stats AS cs
        JOIN
            LatestCampaignHistory AS ch ON cs.id = ch.id AND ch.rn = 1
        WHERE
            cs.customer_id = '2026374428'
            AND cs.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
            AND ch.name NOT LIKE '%Shopping%';
        """

        cur.execute(monthly_sql_query_search)
        result = cur.fetchone()
        column_names = [desc[0] for desc in cur.description]

        if result:
            dashboard_metrics_search = dict(zip(column_names, result))
        else:
            dashboard_metrics_search = {
                "impressions": 0, "clicks": 0, "cpc": 0.0, "cost": 0.0,
                "conversion_cost": 0.0, "conversions": 0, "conversion_value": 0.0,
                "roas": 0.0, "ctr": 0.0, "conv_rate": 0.0
            }

        # SQL query for Google Ads Dashboard (monthly totals)
        yearly_sql_query = f"""
        WITH DateRange AS (
            SELECT
                '{start_date_year.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date_year.strftime('%Y-%m-%d')}'::date AS end_date
        ),
        LatestCampaignHistory AS (
            SELECT
                id,
                name,
                ROW_NUMBER() OVER(PARTITION BY id ORDER BY start_date DESC) as rn
            FROM google_ads.campaign_history
        )
        SELECT
            SUM(cs.impressions) AS impressions,
            SUM(cs.clicks) AS clicks,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.clicks) ELSE 0 END AS cpc,
            SUM(cs.cost_micros / 1000000.0) AS cost,
            CASE WHEN SUM(cs.conversions) > 0 THEN SUM(cs.cost_micros / 1000000.0) / SUM(cs.conversions) ELSE 0 END AS conversion_cost,
            SUM(cs.conversions) AS conversions,
            SUM(cs.conversions_value) AS conversion_value,
            CASE WHEN SUM(cs.cost_micros / 1000000.0) > 0 THEN SUM(cs.conversions_value) / SUM(cs.cost_micros / 1000000.0) ELSE 0 END AS roas,
            CASE WHEN SUM(cs.impressions) > 0 THEN SUM(cs.clicks)::NUMERIC / SUM(cs.impressions) ELSE 0 END AS ctr,
            CASE WHEN SUM(cs.clicks) > 0 THEN SUM(cs.conversions)::NUMERIC / SUM(cs.clicks) ELSE 0 END AS conv_rate
        FROM
            google_ads.campaign_stats AS cs
        JOIN
            LatestCampaignHistory AS ch ON cs.id = ch.id AND ch.rn = 1
        WHERE
            cs.customer_id = '2026374428'
            AND cs.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
            AND ch.name NOT LIKE '%Germany%'
            AND ch.name NOT LIKE '%Netherlands%';
        """

        cur.execute(yearly_sql_query)
        result = cur.fetchone()
        column_names = [desc[0] for desc in cur.description]

        if result:
            dashboard_metrics_yearly = dict(zip(column_names, result))
        else:
            dashboard_metrics_yearly = {
                "impressions": 0, "clicks": 0, "cpc": 0.0, "cost": 0.0,
                "conversion_cost": 0.0, "conversions": 0, "conversion_value": 0.0,
                "roas": 0.0, "ctr": 0.0, "conv_rate": 0.0
            }

        # SQL query for daily metrics for charts
        daily_sql_query = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
        )
        SELECT
            DATE(cs.date) AS day,
            SUM(cs.impressions) AS impressions,
            SUM(cs.clicks) AS clicks,
            SUM(cs.conversions) AS conversions,
            CASE WHEN SUM(cs.conversions) > 0 THEN SUM(cs.cost_micros) / 1000000.0 / SUM(cs.conversions) ELSE 0 END AS cost_per_conversion,
            SUM(cs.conversions_value) AS conversion_value
        FROM
            google_ads.campaign_stats AS cs
        WHERE
            cs.customer_id = '2026374428'
            AND cs.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
        GROUP BY
            DATE(cs.date)
        ORDER BY
            day;
        """
        cur.execute(daily_sql_query)
        daily_results = cur.fetchall()
        daily_column_names = [desc[0] for desc in cur.description]

        for row in daily_results:
            daily_metrics_data.append(dict(zip(daily_column_names, row)))

        # New SQL query for Conversions By Campaign
        campaign_conversions_query = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
        )
        SELECT
            ch.name AS campaign,
            SUM(cs.conversions) AS conversions
        FROM
            google_ads.campaign_stats AS cs
        INNER JOIN
            google_ads.campaign_history AS ch
        ON
            cs.id = ch.id
        WHERE
            cs.customer_id = '2026374428'
            AND cs.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
        GROUP BY
            ch.name
        ORDER BY
            campaign;
        """
        cur.execute(campaign_conversions_query)
        campaign_conversions_results = cur.fetchall()
        campaign_conversions_column_names = [desc[0] for desc in cur.description]

        for row in campaign_conversions_results:
            campaign_conversions_data.append(dict(zip(campaign_conversions_column_names, row)))

        campaign_performance_query = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
        ),
        LatestCampaignHistory AS (
            SELECT
                id,
                name,
                ROW_NUMBER() OVER(PARTITION BY id ORDER BY start_date DESC) as rn
            FROM google_ads.campaign_history
        )
        SELECT
            ch.name AS campaign_name,
            SUM(cs.cost_micros / 1000000.0) AS cost,
            SUM(cs.conversions) AS conversions,
            CASE
                WHEN SUM(cs.clicks) > 0 THEN SUM(cs.conversions)::NUMERIC / SUM(cs.clicks)
                ELSE 0
            END AS conversion_rate,
            CASE
                WHEN SUM(cs.conversions) > 0 THEN SUM(cs.cost_micros) / 1000000.0 / SUM(cs.conversions)
                ELSE 0
            END AS cost_per_conversion,
            SUM(cs.conversions_value) AS conversion_value,
            CASE
                WHEN SUM(cs.cost_micros) > 0 THEN SUM(cs.conversions_value) / (SUM(cs.cost_micros) / 1000000.0)
                ELSE 0
            END AS conversion_value_cost_ratio
        FROM
            google_ads.campaign_stats AS cs
        JOIN
            LatestCampaignHistory AS ch ON cs.id = ch.id AND ch.rn = 1
        WHERE
            cs.customer_id = '2026374428'
            AND cs.date BETWEEN (SELECT start_date FROM DateRange) AND (SELECT end_date FROM DateRange)
        GROUP BY
            ch.name
        ORDER BY
            cost DESC;
        """
        cur.execute(campaign_performance_query)
        campaign_performance_results = cur.fetchall()
        campaign_performance_column_names = [desc[0] for desc in cur.description]

        for row in campaign_performance_results:
            campaign_performance_data.append(dict(zip(campaign_performance_column_names, row)))


    except Error as e:
        print(f"Error connecting to PostgreSQL or executing query: {e}")
        error_message = f"Failed to load Google Ads dashboard data: {e}"
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return render_template('google_ads_dashboard.html', metrics=dashboard_metrics,
                            daily_metrics=daily_metrics_data,
                            shopping_metrics=dashboard_metrics_shopping,
                            search_metrics=dashboard_metrics_search,
                            yearly_metrics=dashboard_metrics_yearly,
                            campaign_conversions=campaign_conversions_data,
                            campaign_performance=campaign_performance_data,
                            start_date=start_date, end_date=end_date,
                            error_message=error_message)

@app.route('/client_seo_report')
def client_seo_report():
    """
    Renders the Client SEO Report page, fetching data directly from the database
    and passing it to the template.
    """
    # Get start and end dates from request, or default to last month
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            # Fallback to last month if dates are invalid
            today = datetime.now().date()
            first_day_of_current_month = today.replace(day=1)
            end_date = first_day_of_current_month - timedelta(days=1)
            start_date = end_date.replace(day=1)
    else:
        # Default to last month
        today = datetime.now().date()
        first_day_of_current_month = today.replace(day=1)
        end_date = first_day_of_current_month - timedelta(days=1)
        start_date = end_date.replace(day=1)

    # Calculate previous period dates
    delta = end_date - start_date
    prev_end_date = start_date - timedelta(days=1)
    prev_start_date = prev_end_date - delta

    # Calculate last 12 months dates
    end_date_12_months = datetime.now().date()
    start_date_12_months = end_date_12_months - timedelta(days=365)


    conn = None
    cur = None
    seo_metrics = {}
    daily_seo_metrics_data = []
    monthly_seo_metrics_data = [] # New list for monthly data
    error_message = None

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # SQL query for Client SEO Report (monthly totals)
        seo_summary_query = f"""
        WITH DateRange AS (
            SELECT
                '{start_date.strftime('%Y-%m-%d')}'::date AS start_date,
                '{end_date.strftime('%Y-%m-%d')}'::date AS end_date
        ),
        GA4SessionMetrics AS (
            SELECT
                SUM(sessions) AS "GA4 Sessions"
            FROM
                google_analytics_4.traffic_acquisition_session_default_channel_grouping_report
            WHERE
                date >= (SELECT start_date FROM DateRange)
                AND date <= (SELECT end_date FROM DateRange)
                AND session_default_channel_grouping IN ('Organic Search', 'Organic Shopping')
        ),
        GA4EventMetrics AS (
            SELECT
                SUM(new_users) AS "GA4 New Users",
                SUM(total_revenue) AS "GA4 Total Revenue",
                SUM(event_count) AS "Key Events"
            FROM
                google_analytics_4.traffic_acquisition_by_channel_and_event
            WHERE
                date >= (SELECT start_date FROM DateRange)
                AND date <= (SELECT end_date FROM DateRange)
                AND session_default_channel_group IN ('Organic Search', 'Organic Shopping')
        )
        SELECT
            gsm."GA4 Sessions",
            COALESCE(gem."GA4 New Users", 0) AS "GA4 New Users",
            COALESCE(gem."GA4 Total Revenue", 0) AS "GA4 Total Revenue",
            COALESCE(gem."Key Events", 0) AS "Key Events"
        FROM
            GA4SessionMetrics gsm
        CROSS JOIN
            GA4EventMetrics gem;
        """

        cur.execute(seo_summary_query)
        result = cur.fetchone()
        column_names = [desc[0] for desc in cur.description]

        if result:
            seo_metrics = dict(zip(column_names, result))
        else:
            seo_metrics = {
                "GA4 Sessions": 0,
                "GA4 New Users": 0,
                "GA4 Total Revenue": 0.0,
                "Key Events": 0
            }

        # SQL query for daily SEO metrics with previous period comparison
        seo_daily_query = f"""
        WITH CurrentPeriod AS (
            SELECT
                date,
                SUM(sessions) AS "GA4 Sessions",
                SUM(new_users) AS "GA4 New Users",
                SUM(total_revenue) AS "GA4 Total Revenue",
                SUM(event_count) AS "Key Events"
            FROM
                google_analytics_4.traffic_acquisition_by_channel_and_event
            WHERE
                date >= '{start_date.strftime('%Y-%m-%d')}'::date
                AND date <= '{end_date.strftime('%Y-%m-%d')}'::date
                AND session_default_channel_group IN ('Organic Search', 'Organic Shopping')
            GROUP BY
                date
        ),
        PreviousPeriod AS (
            SELECT
                date + INTERVAL '{delta.days} days' AS date, -- Shift previous period dates to align with current
                SUM(sessions) AS "GA4 Sessions Previous Period",
                SUM(new_users) AS "GA4 New Users Previous Period",
                SUM(total_revenue) AS "GA4 Total Revenue Previous Period",
                SUM(event_count) AS "Key Events Previous Period"
            FROM
                google_analytics_4.traffic_acquisition_by_channel_and_event
            WHERE
                date >= '{prev_start_date.strftime('%Y-%m-%d')}'::date
                AND date <= '{prev_end_date.strftime('%Y-%m-%d')}'::date
                AND session_default_channel_group IN ('Organic Search', 'Organic Shopping')
            GROUP BY
                date
        )
        SELECT
            COALESCE(cp.date, pp.date) AS "Date",
            COALESCE(cp."GA4 Sessions", 0) AS "GA4 Sessions",
            COALESCE(cp."GA4 New Users", 0) AS "GA4 New Users",
            COALESCE(cp."GA4 Total Revenue", 0) AS "GA4 Total Revenue",
            COALESCE(cp."Key Events", 0) AS "Key Events",
            COALESCE(pp."GA4 Sessions Previous Period", 0) AS "GA4 Sessions Previous Period",
            COALESCE(pp."GA4 New Users Previous Period", 0) AS "GA4 New Users Previous Period",
            COALESCE(pp."GA4 Total Revenue Previous Period", 0) AS "GA4 Total Revenue Previous Period",
            COALESCE(pp."Key Events Previous Period", 0) AS "Key Events Previous Period"
        FROM
            CurrentPeriod cp
        FULL OUTER JOIN
            PreviousPeriod pp ON cp.date = pp.date
        ORDER BY
            "Date";
        """
        cur.execute(seo_daily_query)
        daily_results = cur.fetchall()
        daily_column_names = [desc[0] for desc in cur.description]

        for row in daily_results:
            daily_seo_metrics_data.append(dict(zip(daily_column_names, row)))

        # SQL query for monthly SEO metrics for the last 12 months
        seo_monthly_query = f"""
        WITH MonthlyData AS (
            SELECT
                DATE_TRUNC('month', date) AS month,
                SUM(sessions) AS "GA4 Sessions",
                SUM(new_users) AS "GA4 New Users",
                SUM(total_revenue) AS "GA4 Total Revenue",
                SUM(event_count) AS "Key Events"
            FROM
                google_analytics_4.traffic_acquisition_by_channel_and_event
            WHERE
                date >= '{start_date_12_months.strftime('%Y-%m-%d')}'::date
                AND date <= '{end_date_12_months.strftime('%Y-%m-%d')}'::date
                AND session_default_channel_group IN ('Organic Search', 'Organic Shopping')
            GROUP BY
                month
        )
        SELECT
            month AS "Month",
            "GA4 Sessions",
            "GA4 New Users",
            "GA4 Total Revenue",
            "Key Events"
        FROM
            MonthlyData
        ORDER BY
            "Month";
        """
        cur.execute(seo_monthly_query)
        monthly_results = cur.fetchall()
        monthly_column_names = [desc[0] for desc in cur.description]

        for row in monthly_results:
            monthly_seo_metrics_data.append(dict(zip(monthly_column_names, row)))


    except Error as e:
        print(f"Error connecting to PostgreSQL or executing query: {e}")
        error_message = f"Failed to load SEO report data: {e}"
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return render_template('client_seo_report.html', metrics=seo_metrics,
                           daily_seo_metrics=daily_seo_metrics_data,
                           monthly_seo_metrics=monthly_seo_metrics_data, # Pass monthly data
                           start_date=start_date, end_date=end_date,
                           error_message=error_message)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)
