WITH category AS (
    SELECT 
    id AS category_id,
    name AS category_name
    FROM growsari_prod_merge.category 
    WHERE name IN (
                    'Alcohol',
                    'Cigarettes',
                    'Milk',
                    'Coffee',
                    'Noodles',
                    'Laundry Products',
                    'Ready to Drink',
                    'Seasonings and Mixes',
                    'Bread and Biscuits',
                    'Chips'
                  )
), warehouse AS (

    SELECT 
    id AS warehouse_id,
    code AS warehouse
    FROM
    growsari_prod_merge.warehouse 
    WHERE code IN (
                      'IO',
                      'PN',
                      'TL',
                      'BU',
                      'DO',
                      'LU',
                      'TC',
                      'BC',
                      'ST',
                      'NG',
                      'LP',
                      'DT'
    
                     )
), sku_cat_wh AS (
    SELECT
    main.warehouse,
    main.productid,
    main.itemcode,
    c.category_name,
    wp.is_available,
    main.SKU,
    main.warehouse_id,
	wp.is_deleted
    FROM 
    (
        SELECT  w.warehouse,
                w.warehouse_id,
              CASE 
                  WHEN wp.single_item_id > 0 THEN wp1.id ELSE wp.id END AS productid,
              CASE
                  WHEN wp.single_item_id > 0 THEN wp1.item_code ELSE wp.item_code END itemcode,
              CASE
                  WHEN wp.single_item_id > 0 THEN wp1.super8_name ELSE wp.super8_name END SKU
        FROM warehouse AS w
        LEFT JOIN growsari_prod_merge.warehouse_product wp ON w.warehouse_id = wp.warehouse_id
        LEFT JOIN growsari_prod_merge.warehouse_product AS wp1 ON wp1.id = wp.single_item_id AND wp.single_item_id > 0
            and w.warehouse_id NOT IN (1,2)
        GROUP BY 1,2,3,4,5
    ) AS main
    JOIN growsari_prod_merge.warehouse_product AS wp ON wp.id = main.productid
    JOIN growsari_prod_merge.mother_product AS mp ON mp.id = wp.mother_product_id
    JOIN category AS c ON c.category_id = mp.category_id
    WHERE wp.is_available <> 0 
    AND wp.is_available IS NOT NULL -- item is ON 
    
    
), days_covered AS (

  WITH inventory_rate_f AS (
      SELECT
        skw.productid,
        ps.cal_wh_quantity::int AS gs_inventory,
        NVL(rr.runrate_based_demand_qty,0)::int runrate_qty
        FROM sku_cat_wh AS skw 
        JOIN growsari_prod_merge.product_super8_price_history AS ps ON ps.product_id = skw.productid
        LEFT JOIN supply_planning.run_rate AS rr ON rr.productid = skw.productid
        WHERE DATE(ps.date) = CURRENT_DATE
            AND skw.warehouse_id NOT IN (1,2)
            AND skw.is_deleted = 0
  )
  
  SELECT
  productid,
  gs_inventory,
  runrate_qty,
  FLOOR(CASE 
              WHEN gs_inventory = 0 THEN 0
              WHEN runrate_qty = 0 AND gs_inventory = 0 THEN 0
              WHEN runrate_qty = 0 OR runrate_qty IS NULL THEN NULL
              ELSE gs_inventory / runrate_qty
          END) AS DaysCover
  FROM inventory_rate_f

), penetration_rank AS (
    
    WITH pr AS (
      SELECT
      single_item_code,
      super8_name,
      warehouse_code,
      brand,
      category,
      sub_category,
      penetration_by_sku,
      RANK() OVER (ORDER BY penetration_by_sku ASC) AS penetration_rank
      FROM
      (
        SELECT
        d.single_item_code,
        d.super8_name,
        d.warehouse_code,
        d.brand,
        d.category,
        d.sub_category,
        (((SUM(d.placed_active_stores::real) OVER (PARTITION BY d.super8_name)*1.0)/(SUM(gs.active_stores::real) OVER (PARTITION BY d.super8_name)) )*100) AS penetration_by_sku
        FROM
        (
                SELECT
                yearweek,
                single_item_code,
                super8_name,
                warehouse_code,
                brand,
                category,
                sub_category,
                SUM(placed_active_stores) AS placed_active_stores
                FROM  (
                    SELECT 
                    GS_YEARWEEK(o.delivered_by::date) AS yearweek,
                    (CASE WHEN wp.single_item_id > 0 THEN wp1.item_code ELSE wp.item_code END) AS single_item_code,
                    (CASE WHEN wp.single_item_id > 0 THEN wp1.super8_name ELSE wp.super8_name END) AS super8_name,
                    w.warehouse AS warehouse_code,
                    b.name AS brand,
                    CASE WHEN cat.name IN ('Bar & Liquid Detergents', 'Bleach & Fabric Conditioners', 'Powder Detergents') THEN 'Laundry Products'  ELSE cat.name END AS category,
                    sc.name AS sub_category,
                    COUNT(DISTINCT o.associate_id) AS placed_active_stores
                    FROM growsari_prod_merge.order AS o
                    JOIN growsari_prod_merge.order_item AS oi ON oi.order_id = o.id
                    									AND oi.is_deleted != 1
                    									and o.current_order_status != 'cancelled'
                    LEFT JOIN growsari_prod_merge.order_issue AS ois ON ois.item_id = oi.id
                    			  AND ois.is_deleted = 0
                    			  AND ois.product_id = 0
                    			  AND (ois.is_pullout = 0
                    					OR (ois.is_pullout = 1
                    						AND (ois.reason = 'Not enough money.'
                    						  OR ois.reason = 'Item damaged'
                    						  OR ois.reason = 'Wrong item/product')))
                    JOIN growsari_prod_merge.warehouse_product wp ON wp.id = oi.product_id
                    JOIN growsari_prod_merge.mother_product AS mp ON mp.id = wp.mother_product_id
                    LEFT JOIN growsari_prod_merge.warehouse_product AS wp1 ON wp1.id = wp.single_item_id 
                    				AND wp.single_item_id > 0 
                    JOIN growsari_prod_merge.brand AS b ON b.id = mp.brand_id
                    JOIN growsari_prod_merge.category AS cat ON cat.id = mp.category_id
                          AND cat.name IN (
                                          'Alcohol',
                                          'Cigarettes',
                                          'Milk',
                                          'Coffee',
                                          'Noodles',
                                          'Laundry Products',
                                          'Ready to Drink',
                                          'Seasonings and Mixes',
                                          'Bread and Biscuits',
                                          'Chips'
                                        )
                    LEFT JOIN growsari_prod_merge.sub_category AS sc ON sc.id = mp.sub_category_id
                    JOIN growsari_prod_merge.store_warehouse_shipper sws ON sws.id = o.associate_id
                    JOIN growsari_prod_merge.store s ON s.id = sws.store_id
                    JOIN growsari_prod_merge.account_warehouse aw ON aw.account_id  = s.account_id
                    JOIN warehouse w ON w.warehouse_id = aw.warehouse_id
                    JOIN sku_cat_wh AS scw ON w.warehouse = scw.warehouse AND (wp.super8_name = scw.SKU OR wp1.super8_name = scw.SKU) AND (wp.item_code = scw.itemcode OR wp1.item_code = scw.itemcode) 
                    WHERE GS_YEARWEEK(o.delivered_by::date) BETWEEN GS_YEARWEEK(DATE(DATEADD(WEEK, -4, DATEADD(HOUR, 8, GETDATE())))) AND GS_YEARWEEK(DATEADD(HOUR, 8, GETDATE()))
                    GROUP BY 
                    1,2,3,4,5,6,7
                  )
                GROUP BY yearweek, single_item_code, super8_name, warehouse_code, brand, category, sub_category
            ) AS d
            LEFT JOIN (
            SELECT  GS_YEARWEEK(o.delivered_by::date) AS yearweek,
                            w.warehouse,
                            COUNT(DISTINCT o.associate_id) AS active_stores
                    FROM growsari_prod_merge.order AS o
                    JOIN growsari_prod_merge.store_warehouse_shipper AS sws ON sws.id = o.associate_id
                    JOIN growsari_prod_merge.store AS s ON s.id = sws.store_id
                    JOIN growsari_prod_merge.account_warehouse AS aw ON aw.account_id = s.account_id
                    JOIN warehouse w ON w.warehouse_id  = aw.warehouse_id
                    WHERE o.current_order_status != 'cancelled'
                          AND GS_YEARWEEK(o.delivered_by::date) BETWEEN GS_YEARWEEK(DATE(DATEADD(WEEK, -4, DATEADD(HOUR, 8, GETDATE())))) AND GS_YEARWEEK(DATEADD(HOUR, 8, GETDATE())) -- p4w
                          AND w.warehouse != 'Random'
                    GROUP BY yearweek, warehouse
            ) AS gs
            ON d.yearweek = gs.yearweek AND gs.warehouse = d.warehouse_code
            ORDER BY d.yearweek, d.single_item_code, d.warehouse_code
        )
      GROUP BY 
      single_item_code,
      super8_name,
      warehouse_code,
      brand,
      category,
      sub_category,
      penetration_by_sku
     )
     
   
   SELECT * FROM pr
)


    SELECT 
    DISTINCT
    scw.warehouse,
    scw.category_name AS category,
    pr.sub_category,
    pr.brand,
    scw.SKU AS itemdesc,
    CASE 
        WHEN scw.is_available = 0 THEN 'Off'
        ELSE 'On'
    END AS availability,
    dc.DaysCover AS days_cover,
    pr.penetration_rank AS p4w_penetration
    FROM sku_cat_wh AS scw
    JOIN penetration_rank AS pr
        ON pr.single_item_code = scw.itemcode 
        AND pr.warehouse_code = scw.warehouse 
        AND pr.super8_name = scw.SKU
    JOIN days_covered AS dc ON dc.productid = scw.productid
                            AND dc.DaysCover >= 4
    ORDER BY pr.penetration_rank DESC

