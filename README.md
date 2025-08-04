# Near-Real-Time Purchase Pipeline  
*(Retail & eCommerce Data Engineering Project)*

## 📚 Problem Statement  
Retail and e-commerce teams need to understand what shoppers are doing **right now**—which products they view, promotions they click, and items they add to cart—so they can respond with relevant offers and keep shelves stocked.  
Today, this information lives in separate systems that refresh at different speeds. Analysts may wait hours (or days) for overnight batch jobs to stitch everything together, making insights stale and actions mistimed.

**Goal:** Build a **near-real-time data pipeline** that ingests live web events, enriches them with product, promo, and inventory details, and lands curated fact tables in a cloud warehouse within minutes.

---

## 🎯 Project Objectives
1. **Ingest live click-stream events** from the storefront utilizing Kafka .
2. **Enrich events on the fly** with reference data using Spark.
3. **Track history** using Slowly Changing Dimension Type-2 (SCD-2) tables.
4. **Deliver query-ready facts** in a cloud warehouse using databend.

---


