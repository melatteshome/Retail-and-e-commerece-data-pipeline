# Retail-and-e-commerece-data-pipeline
Modern retail and e-commerce teams need to understand what shoppers are doing right now—which products they view, promotions they click, and items they add to cart—so they can react with relevant offers, replenishment decisions, and personalized experiences. Today, however, this information is scattered across raw click-stream logs, product catalogs, promo lists, and inventory systems that update at different speeds and in different formats. Analysts and data products often wait hours (or days) for slow batch jobs to stitch everything together, making insights stale and actions mistimed.

This project solves that gap by building a near-real-time purchase data pipeline that:

Ingests live web events from storefronts via Kafka.

Enriches each event on the fly with product, promotion, and inventory details using Spark.

Tracks history cleanly with Slowly Changing Dimension Type 2 (SCD-2) tables.

Lands query-ready fact tables in a cloud warehouse databend within minutes.

With this pipeline in place, downstream dashboards, recommendation engines, and BI tools get fresh, complete, and trustworthy data—empowering the business to spot trends instantly, target customers accurately, and keep shelves stocked.
