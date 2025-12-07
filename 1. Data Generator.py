# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# =========================
# CONFIG
# =========================
BASE = "/Volumes/dev/raw/olist/cdc"
PATHS = {
    "customers": f"{BASE}/customers",
    "orders": f"{BASE}/orders",
    "order_items": f"{BASE}/order_items",
    "order_payments": f"{BASE}/order_payments",
}

def now_tag():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(path):
    try:
        dbutils.fs.mkdirs(path)
    except:
        pass

def write_single_csv_to_volume(df, target_dir: str, final_name: str):
    """Escreve CSV único no Volume sem usar DBFS root"""
    ensure_dir(target_dir)
    staging = f"{target_dir}/_staging_{uuid.uuid4().hex}"
    
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", "true")
       .csv(staging))
    
    part_files = [f.path for f in dbutils.fs.ls(staging) if f.path.endswith(".csv")]
    if not part_files:
        raise RuntimeError(f"Arquivo part não encontrado em {staging}")
    
    dest = f"{target_dir}/{final_name}"
    try:
        dbutils.fs.rm(dest)
    except:
        pass
    
    dbutils.fs.mv(part_files[0], dest)
    dbutils.fs.rm(staging, recurse=True)

def with_sequence(df, start_seq: int, order_cols: list):
    """Adiciona sequenceNum sem warning"""
    w = Window.partitionBy(F.lit(1)).orderBy(*[F.col(c) for c in order_cols])
    return (df
        .withColumn("_rn", F.row_number().over(w))
        .withColumn("sequenceNum", (F.lit(start_seq) + F.col("_rn") - 1).cast("long"))
        .drop("_rn"))

def load_ids_from_table(table_name: str, id_col: str, limit: int = 5000):
    """Carrega IDs reais de uma tabela bronze"""
    try:
        df = spark.table(table_name).select(id_col).where(F.col(id_col).isNotNull()).distinct()
        ids = [r[id_col] for r in df.limit(limit).collect()]
        if not ids:
            raise RuntimeError(f"Nenhum ID encontrado em {table_name}.{id_col}")
        print(f"✅ Carregados {len(ids)} {id_col} de {table_name}")
        return ids
    except Exception as e:
        raise RuntimeError(f"Erro ao carregar {table_name}: {str(e)}")

def rand_element(arr_col, seed_col):
    """Pega elemento aleatório de array"""
    return F.element_at(arr_col, (F.pmod(F.abs(F.xxhash64(seed_col)), F.size(arr_col)) + 1).cast("int"))

# =========================
# GENERATORS
# =========================
def gen_customers_snapshot(num: int, batch_id: int, seed: int):
    """Gera clientes com GUIDs"""
    return (spark.range(num)
        .withColumn("seed", F.lit(seed + batch_id) + F.col("id"))
        .withColumn("operation", F.lit("UPSERT"))
        .withColumn("customer_id", F.lower(F.regexp_replace(F.expr("uuid()"), "-", "")))
        .withColumn("customer_unique_id", F.lower(F.regexp_replace(F.expr("uuid()"), "-", "")))
        .withColumn("customer_zip_code_prefix", 
            F.lpad((F.pmod(F.xxhash64("seed"), F.lit(99999)) + 1).cast("string"), 5, "0"))
        .withColumn("customer_city", 
            F.concat(F.lit("Cidade"), (F.pmod(F.xxhash64("seed"), F.lit(200)) + 1).cast("string")))
        .withColumn("customer_state", 
            F.element_at(F.array(*[F.lit(x) for x in ["SP","RJ","MG","RS","SC","PR","BA"]]),
                        (F.pmod(F.xxhash64("seed"), F.lit(7)) + 1).cast("int")))
        .select("operation","customer_id","customer_unique_id",
                "customer_zip_code_prefix","customer_city","customer_state"))

def gen_orders_snapshot(num: int, customer_ids: list, batch_id: int, seed: int):
    """Gera pedidos com GUIDs e customer_ids reais"""
    cust_arr = F.array(*[F.lit(c) for c in customer_ids])
    
    return (spark.range(num)
        .withColumn("seed", F.lit(seed + batch_id*100) + F.col("id"))
        .withColumn("operation", F.lit("UPSERT"))
        .withColumn("order_id", F.lower(F.regexp_replace(F.expr("uuid()"), "-", "")))
        .withColumn("customer_id", rand_element(cust_arr, "seed"))
        .withColumn("order_status", 
            F.element_at(F.array(*[F.lit(x) for x in ["delivered","shipped","processing","approved"]]),
                        (F.pmod(F.xxhash64("seed"), F.lit(4)) + 1).cast("int")))
        .withColumn("order_purchase_timestamp",
            F.date_format(F.expr("timestampadd(DAY, int(pmod(abs(xxhash64(seed)), 90)), timestamp('2018-01-01'))"),
                         "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_approved_at",
            F.date_format(F.expr("timestampadd(HOUR, 2, to_timestamp(order_purchase_timestamp))"),
                         "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_delivered_carrier_date",
            F.date_format(F.expr("timestampadd(DAY, 3, to_timestamp(order_purchase_timestamp))"),
                         "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_delivered_customer_date",
            F.date_format(F.expr("timestampadd(DAY, 8, to_timestamp(order_purchase_timestamp))"),
                         "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_estimated_delivery_date",
            F.date_format(F.expr("timestampadd(DAY, 12, to_timestamp(order_purchase_timestamp))"),
                         "yyyy-MM-dd HH:mm:ss"))
        .select("operation","order_id","customer_id","order_status",
                "order_purchase_timestamp","order_approved_at",
                "order_delivered_carrier_date","order_delivered_customer_date",
                "order_estimated_delivery_date"))

def gen_items(orders_df, product_ids: list, seller_ids: list, max_items: int, batch_id: int, seed: int):
    """Gera itens com product_id e seller_id reais"""
    prod_arr = F.array(*[F.lit(p) for p in product_ids])
    sell_arr = F.array(*[F.lit(s) for s in seller_ids])
    
    return (orders_df.select("order_id")
        .withColumn("seed", F.abs(F.xxhash64("order_id")) + F.lit(seed + batch_id*7))
        .withColumn("items_count", (F.pmod(F.xxhash64("seed"), F.lit(max_items)) + 1).cast("int"))
        .withColumn("order_item_id", F.explode(F.sequence(F.lit(1), F.col("items_count"))))
        .withColumn("product_id", rand_element(prod_arr, F.concat("seed", "order_item_id")))
        .withColumn("seller_id", rand_element(sell_arr, "seed"))
        .withColumn("shipping_limit_date", 
            F.date_format(F.expr("timestampadd(DAY, 5, current_timestamp())"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("price", 
            (F.pmod(F.abs(F.xxhash64("seed","order_item_id")), F.lit(50000)) / 100.0 + 10.0))
        .withColumn("freight_value",
            (F.pmod(F.abs(F.xxhash64("seed")), F.lit(3000)) / 100.0))
        .withColumn("operation", F.lit("UPSERT"))
        .select("operation","order_id","order_item_id","product_id","seller_id",
                "shipping_limit_date","price","freight_value"))

def gen_payments(orders_df, batch_id: int, seed: int):
    """Gera pagamentos"""
    return (orders_df.select("order_id")
        .withColumn("seed", F.abs(F.xxhash64("order_id")) + F.lit(seed + batch_id*9))
        .withColumn("payment_count", (F.pmod(F.xxhash64("seed"), F.lit(2)) + 1).cast("int"))
        .withColumn("payment_sequential", F.explode(F.sequence(F.lit(1), F.col("payment_count"))))
        .withColumn("payment_type",
            F.element_at(F.array(*[F.lit(x) for x in ["credit_card","boleto","voucher"]]),
                        (F.pmod(F.xxhash64("seed"), F.lit(3)) + 1).cast("int")))
        .withColumn("payment_installments",
            (F.pmod(F.xxhash64("seed","payment_sequential"), F.lit(12)) + 1).cast("int"))
        .withColumn("payment_value",
            (F.pmod(F.abs(F.xxhash64("seed")), F.lit(100000)) / 100.0 + 20.0))
        .withColumn("operation", F.lit("UPSERT"))
        .select("operation","order_id","payment_sequential","payment_type",
                "payment_installments","payment_value"))

# =========================
# MAIN
# =========================
def generate_cdc_batches(
    num_customers: int = 100,
    num_orders: int = 300,
    max_items_per_order: int = 3,
    n_batches: int = 3,
    update_frac: float = 0.10,
    seed: int = 2025
):
    # Carrega IDs reais das tabelas bronze
    print("📦 Carregando IDs das tabelas bronze...")
    customer_ids = load_ids_from_table("dev.default.bronze_customers", "customer_id")
    product_ids = load_ids_from_table("dev.default.bronze_products", "product_id")
    seller_ids = load_ids_from_table("dev.default.bronze_sellers", "seller_id")
    
    for p in PATHS.values():
        ensure_dir(p)
    
    tag = now_tag()
    seq = 1
    
    # ==================
    # BATCH 0 - SNAPSHOT INICIAL
    # ==================
    print(f"\n📝 Gerando batch 0 (snapshot inicial)...")
    
    cust0 = gen_customers_snapshot(num_customers, 0, seed)
    cust0 = with_sequence(cust0, seq, ["customer_id"])
    seq += cust0.count()
    
    # Pega os IDs gerados para usar em orders
    new_customer_ids = [r["customer_id"] for r in cust0.select("customer_id").collect()]
    all_customer_ids = customer_ids + new_customer_ids
    
    ord0 = gen_orders_snapshot(num_orders, all_customer_ids, 0, seed)
    ord0 = with_sequence(ord0, seq, ["order_id"])
    seq += ord0.count()
    
    items0 = gen_items(ord0, product_ids, seller_ids, max_items_per_order, 0, seed)
    items0 = with_sequence(items0, seq, ["order_id","order_item_id"])
    seq += items0.count()
    
    pay0 = gen_payments(ord0, 0, seed)
    pay0 = with_sequence(pay0, seq, ["order_id","payment_sequential"])
    seq += pay0.count()
    
    write_single_csv_to_volume(cust0, PATHS["customers"], f"customers_{tag}_b0.csv")
    write_single_csv_to_volume(ord0, PATHS["orders"], f"orders_{tag}_b0.csv")
    write_single_csv_to_volume(items0, PATHS["order_items"], f"order_items_{tag}_b0.csv")
    write_single_csv_to_volume(pay0, PATHS["order_payments"], f"order_payments_{tag}_b0.csv")
    
    print(f"✅ Batch 0 completo: {cust0.count()} customers, {ord0.count()} orders")
    
    # Mantém ordem dos IDs para próximos batches
    order_ids = [r["order_id"] for r in ord0.select("order_id").collect()]
    
    # ==================
    # BATCHES 1+ - APENAS UPDATES E INSERTS (SEM DELETES)
    # ==================
    for b in range(1, n_batches):
        print(f"\n📝 Gerando batch {b}...")
        btag = f"{tag}_b{b}"
        
        # CUSTOMERS: só updates de alguns registros existentes
        num_upd_cust = max(1, int(len(new_customer_ids) * update_frac))
        upd_cust_ids = new_customer_ids[:num_upd_cust]  # pega os primeiros
        
        updC = (spark.createDataFrame([(cid,) for cid in upd_cust_ids], ["customer_id"])
            .withColumn("seed", F.abs(F.xxhash64("customer_id")) + F.lit(seed + b*10))
            .withColumn("operation", F.lit("UPSERT"))
            .withColumn("customer_unique_id", F.lower(F.regexp_replace(F.expr("uuid()"), "-", "")))
            .withColumn("customer_zip_code_prefix",
                F.lpad((F.pmod(F.xxhash64("seed"), F.lit(99999)) + 1).cast("string"), 5, "0"))
            .withColumn("customer_city",
                F.concat(F.lit("NovaCidade"), (F.pmod(F.xxhash64("seed"), F.lit(100)) + 1).cast("string")))
            .withColumn("customer_state",
                F.element_at(F.array(*[F.lit(x) for x in ["SP","RJ","MG","RS"]]),
                            (F.pmod(F.xxhash64("seed"), F.lit(4)) + 1).cast("int")))
            .select("operation","customer_id","customer_unique_id",
                    "customer_zip_code_prefix","customer_city","customer_state"))
        
        cust_cdc = with_sequence(updC, seq, ["customer_id"])
        seq += cust_cdc.count()
        write_single_csv_to_volume(cust_cdc, PATHS["customers"], f"customers_{btag}.csv")
        
        # ORDERS: updates de alguns + insert de novos
        num_upd_ord = max(1, int(len(order_ids) * update_frac))
        num_ins_ord = max(1, int(num_orders * 0.1))  # 10% novos
        
        upd_ord_ids = order_ids[:num_upd_ord]
        
        cust_arr = F.array(*[F.lit(c) for c in all_customer_ids])
        
        updO = (spark.createDataFrame([(oid,) for oid in upd_ord_ids], ["order_id"])
            .withColumn("seed", F.abs(F.xxhash64("order_id")) + F.lit(seed + b*20))
            .withColumn("operation", F.lit("UPSERT"))
            .withColumn("customer_id", rand_element(cust_arr, "seed"))
            .withColumn("order_status",
                F.element_at(F.array(*[F.lit(x) for x in ["delivered","canceled"]]),
                            (F.pmod(F.xxhash64("seed"), F.lit(2)) + 1).cast("int")))
            .withColumn("order_purchase_timestamp", F.lit("2018-03-01 10:00:00"))
            .withColumn("order_approved_at", F.lit("2018-03-01 12:00:00"))
            .withColumn("order_delivered_carrier_date", F.lit("2018-03-04 10:00:00"))
            .withColumn("order_delivered_customer_date", F.lit("2018-03-10 10:00:00"))
            .withColumn("order_estimated_delivery_date", F.lit("2018-03-15 10:00:00"))
            .select("operation","order_id","customer_id","order_status",
                    "order_purchase_timestamp","order_approved_at",
                    "order_delivered_carrier_date","order_delivered_customer_date",
                    "order_estimated_delivery_date"))
        
        insO = gen_orders_snapshot(num_ins_ord, all_customer_ids, b, seed)
        
        orders_cdc = updO.unionByName(insO)
        orders_cdc = with_sequence(orders_cdc, seq, ["order_id"])
        seq += orders_cdc.count()
        write_single_csv_to_volume(orders_cdc, PATHS["orders"], f"orders_{btag}.csv")
        
        # Atualiza pool de order_ids
        new_order_ids = [r["order_id"] for r in insO.select("order_id").collect()]
        order_ids = order_ids + new_order_ids
        
        # ITEMS + PAYMENTS para todos os orders tocados
        items_cdc = gen_items(orders_cdc.select("order_id"), product_ids, seller_ids, max_items_per_order, b, seed)
        items_cdc = with_sequence(items_cdc, seq, ["order_id","order_item_id"])
        seq += items_cdc.count()
        write_single_csv_to_volume(items_cdc, PATHS["order_items"], f"order_items_{btag}.csv")
        
        pay_cdc = gen_payments(orders_cdc.select("order_id"), b, seed)
        pay_cdc = with_sequence(pay_cdc, seq, ["order_id","payment_sequential"])
        seq += pay_cdc.count()
        write_single_csv_to_volume(pay_cdc, PATHS["order_payments"], f"order_payments_{btag}.csv")
        
        print(f"✅ Batch {b} completo: {cust_cdc.count()} customers, {orders_cdc.count()} orders")
    
    print(f"\n🎉 CDC gerado com sucesso!")
    print(f"📊 Total de {n_batches} batches")
    for k, v in PATHS.items():
        print(f"   • {k}: {v}")

# ===========
# EXECUTE
# ===========
generate_cdc_batches(
    num_customers=100,
    num_orders=300,
    max_items_per_order=3,
    n_batches=3,
    update_frac=0.10,
    seed=2025
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM dev.default.bronze_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM csv.`/Volumes/dev/raw/olist/cdc/customers/customers_20251207_164728_b0.csv`

# COMMAND ----------

